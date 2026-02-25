//! Runtime dispatch/execution path for transactions and direct command ingress.

use super::{ServerApp, is_runtime_dispatch_error};
use dfly_common::config::ClusterMode;
use dfly_common::error::{DflyError, DflyResult};
use dfly_common::ids::TxId;
use dfly_core::CommandRouting;
use dfly_core::command::{CommandFrame, CommandReply};
use dfly_core::runtime::RuntimeEnvelope;
use dfly_transaction::plan::{TransactionHop, TransactionMode, TransactionPlan};
use dfly_transaction::scheduler::TransactionScheduler;
use std::collections::{BTreeMap, BTreeSet, HashMap};

impl ServerApp {
    pub(super) fn execute_transaction_plan(
        &mut self,
        db: u16,
        plan: &TransactionPlan,
    ) -> Vec<CommandReply> {
        let mut replies = Vec::new();
        for (hop_index, hop) in plan.hops.iter().enumerate() {
            match plan.mode {
                TransactionMode::NonAtomic => {
                    for (_, command) in &hop.per_shard {
                        if matches!(
                            self.core.command_routing(command),
                            CommandRouting::SingleKey { is_write: false }
                        ) {
                            replies.push(self.execute_user_command(db, command, None));
                        } else {
                            replies.push(CommandReply::Error(
                                "transaction planning error: NonAtomic mode requires read-only single-key commands".to_owned(),
                            ));
                        }
                    }
                }
                TransactionMode::LockAhead | TransactionMode::Global => {
                    let hop_replies = self.execute_runtime_scheduled_hop(db, plan.txid, hop);
                    let runtime_failure = hop_replies.iter().any(is_runtime_dispatch_error);
                    replies.extend(hop_replies);
                    if runtime_failure {
                        // Runtime dispatch failure means execution order/liveness guarantees were
                        // already broken for this transaction wave, so later hops are aborted.
                        for remaining_hop in plan.hops.iter().skip(hop_index + 1) {
                            for _ in &remaining_hop.per_shard {
                                replies.push(CommandReply::Error(
                                    "runtime dispatch failed: transaction aborted after hop failure"
                                        .to_owned(),
                                ));
                            }
                        }
                        break;
                    }
                }
            }
        }
        replies
    }

    pub(super) fn execute_runtime_scheduled_hop(
        &mut self,
        db: u16,
        txid: TxId,
        hop: &TransactionHop,
    ) -> Vec<CommandReply> {
        let mut per_command_error = vec![None; hop.per_shard.len()];
        let (target_shards_by_command, execute_on_worker_by_command) =
            self.prepare_runtime_hop_dispatch(hop, &mut per_command_error);
        if per_command_error.iter().any(std::option::Option::is_some) {
            return Self::runtime_error_replies_for_hop(hop, &per_command_error, &HashMap::new());
        }

        let mut worker_sequence_by_command = vec![None; hop.per_shard.len()];
        let mut target_sequence_by_shard = HashMap::<u16, u64>::new();

        for (index, (_, command)) in hop.per_shard.iter().enumerate() {
            let execute_on_worker = execute_on_worker_by_command[index];
            for target_shard in &target_shards_by_command[index] {
                let should_execute_on_worker =
                    execute_on_worker && worker_sequence_by_command[index].is_none();
                match self.submit_runtime_envelope_with_sequence(
                    *target_shard,
                    db,
                    command,
                    should_execute_on_worker,
                ) {
                    Ok(sequence) => {
                        if should_execute_on_worker {
                            worker_sequence_by_command[index] = Some((*target_shard, sequence));
                        }
                        target_sequence_by_shard
                            .entry(*target_shard)
                            .and_modify(|current| {
                                *current = (*current).max(sequence);
                            })
                            .or_insert(sequence);
                    }
                    Err(error) => {
                        if per_command_error[index].is_none() {
                            per_command_error[index] =
                                Some(format!("runtime dispatch failed: {error}"));
                        }
                    }
                }
            }
        }
        let mut shard_wait_error = HashMap::<u16, String>::new();
        for (shard, target_sequence) in target_sequence_by_shard {
            match self
                .runtime
                .wait_until_processed_sequence(shard, target_sequence)
            {
                Ok(()) => {}
                Err(error) => {
                    shard_wait_error.insert(shard, format!("runtime dispatch failed: {error}"));
                }
            }
        }

        let hop_has_runtime_error = per_command_error.iter().any(std::option::Option::is_some)
            || !shard_wait_error.is_empty();
        if hop_has_runtime_error {
            return Self::runtime_error_replies_for_hop(hop, &per_command_error, &shard_wait_error);
        }

        let worker_replies_by_command =
            self.collect_worker_hop_replies(&worker_sequence_by_command, &mut per_command_error);

        if per_command_error.iter().any(std::option::Option::is_some) {
            return Self::runtime_error_replies_for_hop(hop, &per_command_error, &shard_wait_error);
        }

        let mut replies = Vec::with_capacity(hop.per_shard.len());
        for (index, (_, command)) in hop.per_shard.iter().enumerate() {
            let reply = if let Some(worker_reply) = worker_replies_by_command[index].clone() {
                worker_reply
            } else {
                self.execute_user_command(db, command, Some(txid))
            };
            self.maybe_append_journal_for_command(Some(txid), db, command, &reply);
            replies.push(reply);
        }
        replies
    }

    pub(super) fn prepare_runtime_hop_dispatch(
        &self,
        hop: &TransactionHop,
        per_command_error: &mut [Option<String>],
    ) -> (Vec<Vec<u16>>, Vec<bool>) {
        let mut target_shards_by_command = Vec::with_capacity(hop.per_shard.len());
        let mut execute_on_worker_by_command = Vec::with_capacity(hop.per_shard.len());
        let out_of_range_error = format!(
            "runtime dispatch failed: {}",
            DflyError::InvalidState("target shard is out of range")
        );
        for (index, (shard_hint, command)) in hop.per_shard.iter().enumerate() {
            let target_shards =
                self.runtime_target_shards_for_scheduled_command(*shard_hint, command);
            if target_shards
                .iter()
                .any(|shard| usize::from(*shard) >= usize::from(self.config.shard_count.get()))
            {
                per_command_error[index] = Some(out_of_range_error.clone());
            }
            let execute_on_worker = self.command_should_execute_on_worker(command, &target_shards);
            target_shards_by_command.push(target_shards);
            execute_on_worker_by_command.push(execute_on_worker);
        }
        (target_shards_by_command, execute_on_worker_by_command)
    }

    fn command_should_execute_on_worker(
        &self,
        command: &CommandFrame,
        target_shards: &[u16],
    ) -> bool {
        if matches!(
            self.core.command_routing(command),
            CommandRouting::SingleKey { .. }
        ) {
            return true;
        }

        // Keep cluster redirection/CROSSSLOT handling on the coordinator path in
        // transaction mode. Worker execution for non-single-key commands is enabled
        // only in standalone mode where coordinator-side cluster checks are not
        // required.
        if self.cluster.mode != ClusterMode::Disabled {
            return false;
        }

        let same_shard = self
            .same_shard_copy_rename_target_shard(command)
            .or_else(|| self.same_shard_multikey_count_target_shard(command))
            .or_else(|| self.same_shard_multikey_string_target_shard(command));
        let Some(same_shard) = same_shard else {
            return false;
        };
        target_shards.len() == 1 && target_shards.first().copied() == Some(same_shard)
    }

    pub(super) fn collect_worker_hop_replies(
        &self,
        worker_sequence_by_command: &[Option<(u16, u64)>],
        per_command_error: &mut [Option<String>],
    ) -> Vec<Option<CommandReply>> {
        let mut worker_replies_by_command = vec![None; worker_sequence_by_command.len()];
        for (index, worker_sequence) in worker_sequence_by_command.iter().copied().enumerate() {
            let Some((worker_shard, worker_sequence)) = worker_sequence else {
                continue;
            };
            match self
                .runtime
                .take_processed_reply(worker_shard, worker_sequence)
            {
                Ok(Some(reply)) => worker_replies_by_command[index] = Some(reply),
                Ok(None) => {
                    per_command_error[index] =
                        Some("runtime dispatch failed: missing worker reply".to_owned());
                }
                Err(error) => {
                    per_command_error[index] = Some(format!("runtime dispatch failed: {error}"));
                }
            }
        }
        worker_replies_by_command
    }

    pub(super) fn runtime_error_replies_for_hop(
        hop: &TransactionHop,
        per_command_error: &[Option<String>],
        shard_wait_error: &HashMap<u16, String>,
    ) -> Vec<CommandReply> {
        let fallback = "runtime dispatch failed: hop barrier aborted".to_owned();
        hop.per_shard
            .iter()
            .enumerate()
            .map(|(index, (shard, _))| {
                if let Some(error) = &per_command_error[index] {
                    return CommandReply::Error(error.clone());
                }
                if let Some(error) = shard_wait_error.get(shard) {
                    return CommandReply::Error(error.clone());
                }
                CommandReply::Error(fallback.clone())
            })
            .collect::<Vec<_>>()
    }

    pub(super) fn submit_runtime_envelope_with_sequence(
        &self,
        shard: u16,
        db: u16,
        command: &CommandFrame,
        execute_on_worker: bool,
    ) -> DflyResult<u64> {
        // Dragonfly routes each hop command to its owner shard queue before execution.
        self.runtime.submit_with_sequence(RuntimeEnvelope {
            target_shard: shard,
            db,
            execute_on_worker,
            command: command.clone(),
        })
    }

    fn execute_runtime_command_on_worker_shard(
        &mut self,
        db: u16,
        frame: &CommandFrame,
        shard: u16,
        enforce_scheduler_barrier: bool,
    ) -> CommandReply {
        if enforce_scheduler_barrier
            && let Err(error) = self.transaction.scheduler.ensure_shards_available(&[shard])
        {
            return CommandReply::Error(format!("runtime dispatch failed: {error}"));
        }

        let sequence = match self.submit_runtime_envelope_with_sequence(shard, db, frame, true) {
            Ok(sequence) => sequence,
            Err(error) => return CommandReply::Error(format!("runtime dispatch failed: {error}")),
        };

        if let Err(error) = self.runtime.wait_until_processed_sequence(shard, sequence) {
            return CommandReply::Error(format!("runtime dispatch failed: {error}"));
        }

        match self.runtime.take_processed_reply(shard, sequence) {
            Ok(Some(reply)) => reply,
            Ok(None) => {
                CommandReply::Error("runtime dispatch failed: missing worker reply".to_owned())
            }
            Err(error) => CommandReply::Error(format!("runtime dispatch failed: {error}")),
        }
    }

    pub(super) fn execute_user_command(
        &mut self,
        db: u16,
        frame: &CommandFrame,
        txid: Option<TxId>,
    ) -> CommandReply {
        if txid.is_none() {
            if matches!(
                self.core.command_routing(frame),
                CommandRouting::SingleKey { .. }
            ) {
                let reply = self.execute_single_shard_command_via_runtime(db, frame);
                self.maybe_append_journal_for_command(txid, db, frame, &reply);
                return reply;
            }

            if let Some(reply) = self.execute_same_shard_copy_rename_via_runtime(db, frame) {
                self.maybe_append_journal_for_command(txid, db, frame, &reply);
                return reply;
            }

            if let Some(reply) = self.execute_multikey_string_commands_via_runtime(db, frame) {
                self.maybe_append_journal_for_command(txid, db, frame, &reply);
                return reply;
            }

            if let Some(reply) = self.execute_multi_key_counting_command_via_runtime(db, frame) {
                self.maybe_append_journal_for_command(txid, db, frame, &reply);
                return reply;
            }

            match self.dispatch_direct_command_runtime(db, frame) {
                Ok(()) => {}
                Err(error) => {
                    return CommandReply::Error(format!("runtime dispatch failed: {error}"));
                }
            }
        }

        let reply = self.execute_command_without_side_effects(db, frame);
        self.maybe_append_journal_for_command(txid, db, frame, &reply);
        reply
    }

    pub(super) fn execute_same_shard_copy_rename_via_runtime(
        &mut self,
        db: u16,
        frame: &CommandFrame,
    ) -> Option<CommandReply> {
        let shard = self.same_shard_copy_rename_target_shard(frame)?;

        let keys_for_cluster_check: Vec<&[u8]> = match frame.name.as_str() {
            "COPY" => frame.args.iter().take(2).map(Vec::as_slice).collect(),
            _ => frame.args.iter().map(Vec::as_slice).collect(),
        };
        if let Some(error_reply) = self.ensure_cluster_multi_key_constraints(keys_for_cluster_check)
        {
            return Some(error_reply);
        }
        Some(self.execute_runtime_command_on_worker_shard(db, frame, shard, true))
    }

    fn same_shard_copy_rename_target_shard(&self, frame: &CommandFrame) -> Option<u16> {
        if !matches!(frame.name.as_str(), "COPY" | "RENAME" | "RENAMENX") || frame.args.len() < 2 {
            return None;
        }

        let source_shard = self.core.resolve_shard_for_key(&frame.args[0]);
        let destination_shard = self.core.resolve_shard_for_key(&frame.args[1]);
        (source_shard == destination_shard).then_some(source_shard)
    }

    fn same_shard_multikey_count_target_shard(&self, frame: &CommandFrame) -> Option<u16> {
        if !matches!(frame.name.as_str(), "DEL" | "UNLINK" | "EXISTS" | "TOUCH")
            || frame.args.len() < 2
        {
            return None;
        }

        let mut keys = frame.args.iter();
        let first_shard = self.core.resolve_shard_for_key(keys.next()?);
        if keys.any(|key| self.core.resolve_shard_for_key(key) != first_shard) {
            return None;
        }
        Some(first_shard)
    }

    fn group_multikey_count_frame_by_shard(
        &self,
        frame: &CommandFrame,
    ) -> Vec<(u16, CommandFrame)> {
        let mut grouped_keys = BTreeMap::<u16, Vec<Vec<u8>>>::new();
        for key in &frame.args {
            let shard = self.core.resolve_shard_for_key(key);
            grouped_keys.entry(shard).or_default().push(key.clone());
        }

        grouped_keys
            .into_iter()
            .map(|(shard, args)| (shard, CommandFrame::new(frame.name.as_str(), args)))
            .collect()
    }

    fn same_shard_multikey_string_target_shard(&self, frame: &CommandFrame) -> Option<u16> {
        let keys: Vec<&[u8]> = match frame.name.as_str() {
            "MGET" if frame.args.len() >= 2 => frame.args.iter().map(Vec::as_slice).collect(),
            "MSET" | "MSETNX" if frame.args.len() >= 4 && frame.args.len().is_multiple_of(2) => {
                frame.args.iter().step_by(2).map(Vec::as_slice).collect()
            }
            _ => return None,
        };
        let (first_key, remaining) = keys.split_first()?;
        let first_shard = self.core.resolve_shard_for_key(first_key);
        if remaining
            .iter()
            .any(|key| self.core.resolve_shard_for_key(key) != first_shard)
        {
            return None;
        }
        Some(first_shard)
    }

    pub(super) fn execute_multikey_string_commands_via_runtime(
        &mut self,
        db: u16,
        frame: &CommandFrame,
    ) -> Option<CommandReply> {
        if let Some(reply) = self.execute_same_shard_multikey_string_via_runtime(db, frame) {
            return Some(reply);
        }

        match frame.name.as_str() {
            "MGET" => Some(self.execute_mget_via_runtime(db, frame)),
            "MSET" => Some(self.execute_mset_via_runtime(db, frame)),
            "MSETNX" => Some(self.execute_msetnx_via_runtime(db, frame)),
            _ => None,
        }
    }

    fn execute_same_shard_multikey_string_via_runtime(
        &mut self,
        db: u16,
        frame: &CommandFrame,
    ) -> Option<CommandReply> {
        let shard = self.same_shard_multikey_string_target_shard(frame)?;

        let keys_for_cluster_check: Vec<&[u8]> = match frame.name.as_str() {
            "MGET" => frame.args.iter().map(Vec::as_slice).collect(),
            "MSET" | "MSETNX" => frame
                .args
                .chunks_exact(2)
                .map(|pair| pair[0].as_slice())
                .collect(),
            _ => return None,
        };
        if let Some(error_reply) = self.ensure_cluster_multi_key_constraints(keys_for_cluster_check)
        {
            return Some(error_reply);
        }
        Some(self.execute_runtime_command_on_worker_shard(db, frame, shard, true))
    }

    fn execute_mget_via_runtime(&mut self, db: u16, frame: &CommandFrame) -> CommandReply {
        if frame.args.is_empty() {
            return CommandReply::Error("wrong number of arguments for 'MGET' command".to_owned());
        }
        if let Some(error_reply) =
            self.ensure_cluster_multi_key_constraints(frame.args.iter().map(Vec::as_slice))
        {
            return error_reply;
        }

        let mut replies = Vec::with_capacity(frame.args.len());
        for key in &frame.args {
            let get_frame = CommandFrame::new("GET", vec![key.clone()]);
            let reply = self.execute_single_shard_command_via_runtime(db, &get_frame);
            match reply {
                CommandReply::BulkString(_) | CommandReply::Null => replies.push(reply),
                CommandReply::Moved { .. } | CommandReply::Error(_) => return reply,
                _ => {
                    return CommandReply::Error(
                        "internal error: GET did not return bulk-string or null reply".to_owned(),
                    );
                }
            }
        }

        CommandReply::Array(replies)
    }

    fn execute_mset_via_runtime(&mut self, db: u16, frame: &CommandFrame) -> CommandReply {
        if frame.args.is_empty() || !frame.args.len().is_multiple_of(2) {
            return CommandReply::Error("wrong number of arguments for 'MSET' command".to_owned());
        }
        if let Some(error_reply) = self.ensure_cluster_multi_key_constraints(
            frame.args.chunks_exact(2).map(|pair| pair[0].as_slice()),
        ) {
            return error_reply;
        }

        for pair in frame.args.chunks_exact(2) {
            let set_frame = CommandFrame::new("SET", vec![pair[0].clone(), pair[1].clone()]);
            let reply = self.execute_single_shard_command_via_runtime(db, &set_frame);
            if !matches!(reply, CommandReply::SimpleString(ref ok) if ok == "OK") {
                return reply;
            }
        }

        CommandReply::SimpleString("OK".to_owned())
    }

    fn execute_msetnx_via_runtime(&mut self, db: u16, frame: &CommandFrame) -> CommandReply {
        if frame.args.is_empty() || !frame.args.len().is_multiple_of(2) {
            return CommandReply::Error(
                "wrong number of arguments for 'MSETNX' command".to_owned(),
            );
        }
        if let Some(error_reply) = self.ensure_cluster_multi_key_constraints(
            frame.args.chunks_exact(2).map(|pair| pair[0].as_slice()),
        ) {
            return error_reply;
        }

        for pair in frame.args.chunks_exact(2) {
            let exists_frame = CommandFrame::new("EXISTS", vec![pair[0].clone()]);
            let reply = self.execute_single_shard_command_via_runtime(db, &exists_frame);
            match reply {
                CommandReply::Integer(1) => return CommandReply::Integer(0),
                CommandReply::Integer(_) => {}
                CommandReply::Moved { .. } | CommandReply::Error(_) => return reply,
                _ => {
                    return CommandReply::Error(
                        "internal error: EXISTS did not return integer reply".to_owned(),
                    );
                }
            }
        }

        for pair in frame.args.chunks_exact(2) {
            let set_frame = CommandFrame::new("SET", vec![pair[0].clone(), pair[1].clone()]);
            let reply = self.execute_single_shard_command_via_runtime(db, &set_frame);
            if !matches!(reply, CommandReply::SimpleString(ref ok) if ok == "OK") {
                return CommandReply::Error("MSETNX failed while setting key".to_owned());
            }
        }

        CommandReply::Integer(1)
    }

    pub(super) fn execute_multi_key_counting_command_via_runtime(
        &mut self,
        db: u16,
        frame: &CommandFrame,
    ) -> Option<CommandReply> {
        if !matches!(frame.name.as_str(), "DEL" | "UNLINK" | "EXISTS" | "TOUCH")
            || frame.args.len() <= 1
        {
            return None;
        }

        if let Some(reply) = self.execute_same_shard_multikey_count_via_runtime(db, frame) {
            return Some(reply);
        }
        if let Some(error_reply) =
            self.ensure_cluster_multi_key_constraints(frame.args.iter().map(Vec::as_slice))
        {
            return Some(error_reply);
        }

        let grouped_commands = self.group_multikey_count_frame_by_shard(frame);
        let touched_shards = grouped_commands
            .iter()
            .map(|(shard, _)| *shard)
            .collect::<Vec<_>>();
        if let Err(error) = self
            .transaction
            .scheduler
            .ensure_shards_available(&touched_shards)
        {
            return Some(CommandReply::Error(format!(
                "runtime dispatch failed: {error}"
            )));
        }

        let mut barriers = Vec::with_capacity(grouped_commands.len());
        for (shard, grouped_frame) in grouped_commands {
            let sequence =
                match self.submit_runtime_envelope_with_sequence(shard, db, &grouped_frame, true) {
                    Ok(sequence) => sequence,
                    Err(error) => {
                        return Some(CommandReply::Error(format!(
                            "runtime dispatch failed: {error}"
                        )));
                    }
                };
            barriers.push((shard, sequence));
        }
        for (shard, sequence) in &barriers {
            if let Err(error) = self
                .runtime
                .wait_until_processed_sequence(*shard, *sequence)
            {
                return Some(CommandReply::Error(format!(
                    "runtime dispatch failed: {error}"
                )));
            }
        }

        let mut total = 0_i64;
        for (shard, sequence) in barriers {
            let reply = match self.runtime.take_processed_reply(shard, sequence) {
                Ok(Some(reply)) => reply,
                Ok(None) => {
                    return Some(CommandReply::Error(
                        "runtime dispatch failed: missing worker reply".to_owned(),
                    ));
                }
                Err(error) => {
                    return Some(CommandReply::Error(format!(
                        "runtime dispatch failed: {error}"
                    )));
                }
            };

            match reply {
                CommandReply::Integer(delta) => {
                    total = total.saturating_add(delta.max(0));
                }
                CommandReply::Moved { slot, endpoint } => {
                    return Some(CommandReply::Moved { slot, endpoint });
                }
                CommandReply::Error(message) => {
                    return Some(CommandReply::Error(message));
                }
                _ => {
                    return Some(CommandReply::Error(format!(
                        "internal error: {} did not return integer reply",
                        frame.name
                    )));
                }
            }
        }

        Some(CommandReply::Integer(total))
    }

    fn execute_same_shard_multikey_count_via_runtime(
        &mut self,
        db: u16,
        frame: &CommandFrame,
    ) -> Option<CommandReply> {
        let shard = self.same_shard_multikey_count_target_shard(frame)?;

        if let Some(error_reply) =
            self.ensure_cluster_multi_key_constraints(frame.args.iter().map(Vec::as_slice))
        {
            return Some(error_reply);
        }
        Some(self.execute_runtime_command_on_worker_shard(db, frame, shard, true))
    }

    pub(super) fn execute_single_shard_command_via_runtime(
        &mut self,
        db: u16,
        frame: &CommandFrame,
    ) -> CommandReply {
        self.execute_single_shard_command_via_runtime_internal(db, frame, true)
    }

    fn execute_single_shard_command_via_runtime_internal(
        &mut self,
        db: u16,
        frame: &CommandFrame,
        enforce_scheduler_barrier: bool,
    ) -> CommandReply {
        let Some(key) = frame.args.first() else {
            return self.execute_command_without_side_effects(db, frame);
        };
        if let Some(moved) = self.cluster_moved_reply_for_key(key) {
            return moved;
        }

        let shard = self.core.resolve_target_shard(frame);
        self.execute_runtime_command_on_worker_shard(db, frame, shard, enforce_scheduler_barrier)
    }

    fn replay_same_shard_runtime_target(&self, frame: &CommandFrame) -> Option<u16> {
        self.same_shard_copy_rename_target_shard(frame)
            .or_else(|| self.same_shard_multikey_count_target_shard(frame))
            .or_else(|| self.same_shard_multikey_string_target_shard(frame))
    }

    pub(super) fn execute_replay_command_without_journal(
        &mut self,
        db: u16,
        frame: &CommandFrame,
    ) -> CommandReply {
        if matches!(
            self.core.command_routing(frame),
            CommandRouting::SingleKey { .. }
        ) {
            return self.execute_single_shard_command_via_runtime_internal(db, frame, false);
        }
        if let Some(shard) = self.replay_same_shard_runtime_target(frame) {
            return self.execute_runtime_command_on_worker_shard(db, frame, shard, false);
        }

        if let Err(error) = self.dispatch_replay_command_runtime(db, frame) {
            return CommandReply::Error(format!("runtime dispatch failed: {error}"));
        }

        self.execute_command_without_side_effects(db, frame)
    }

    pub(super) fn dispatch_direct_command_runtime(
        &self,
        db: u16,
        frame: &CommandFrame,
    ) -> DflyResult<()> {
        self.dispatch_runtime_for_command(db, frame, true)
    }

    pub(super) fn dispatch_replay_command_runtime(
        &self,
        db: u16,
        frame: &CommandFrame,
    ) -> DflyResult<()> {
        // Recovery replay must stay independent from live transaction queue ownership checks.
        // During restore we rebuild state from persisted journal entries and should not depend
        // on transient in-memory scheduler leases.
        self.dispatch_runtime_for_command(db, frame, false)
    }

    pub(super) fn dispatch_runtime_for_command(
        &self,
        db: u16,
        frame: &CommandFrame,
        enforce_scheduler_barrier: bool,
    ) -> DflyResult<()> {
        let target_shards = self.runtime_target_shards_for_command(frame);
        if target_shards.is_empty() {
            return Ok(());
        }

        if enforce_scheduler_barrier {
            self.transaction
                .scheduler
                .ensure_shards_available(&target_shards)?;
        }

        // Direct command path mirrors Dragonfly's coordinator ingress:
        // 1) push one envelope to each touched shard queue,
        // 2) wait until all shard workers report they consumed the envelope.
        let mut barriers = Vec::with_capacity(target_shards.len());
        for shard in target_shards {
            let sequence = self.submit_runtime_envelope_with_sequence(shard, db, frame, false)?;
            barriers.push((shard, sequence));
        }
        for (shard, sequence) in barriers {
            self.runtime
                .wait_until_processed_sequence(shard, sequence)?;
        }
        Ok(())
    }

    pub(super) fn runtime_target_shards_for_scheduled_command(
        &self,
        shard_hint: u16,
        frame: &CommandFrame,
    ) -> Vec<u16> {
        let target_shards = self.runtime_target_shards_for_command(frame);
        if target_shards.is_empty() {
            return vec![shard_hint];
        }
        target_shards
    }

    pub(super) fn runtime_target_shards_for_command(&self, frame: &CommandFrame) -> Vec<u16> {
        if matches!(
            self.core.command_routing(frame),
            CommandRouting::SingleKey { .. }
        ) {
            return vec![self.core.resolve_target_shard(frame)];
        }

        match frame.name.as_str() {
            // Multi-key fanout commands: each key can belong to a different shard owner.
            "MGET" | "DEL" | "UNLINK" | "EXISTS" | "TOUCH" if !frame.args.is_empty() => {
                self.collect_unique_runtime_shards_for_keys(frame.args.iter().map(Vec::as_slice))
            }
            // MSET and MSETNX use key/value pairs, so key positions are 0,2,4,...
            "MSET" | "MSETNX" if frame.args.len() >= 2 && frame.args.len().is_multiple_of(2) => {
                self.collect_unique_runtime_shards_for_keys(
                    frame.args.iter().step_by(2).map(Vec::as_slice),
                )
            }
            // COPY/RENAME family touches source and destination owners.
            "COPY" if frame.args.len() >= 2 => self.collect_unique_runtime_shards_for_keys([
                frame.args[0].as_slice(),
                frame.args[1].as_slice(),
            ]),
            "RENAME" | "RENAMENX" if frame.args.len() == 2 => self
                .collect_unique_runtime_shards_for_keys([
                    frame.args[0].as_slice(),
                    frame.args[1].as_slice(),
                ]),
            // Global keyspace commands must observe a full-shard barrier.
            "FLUSHDB" | "FLUSHALL" | "DBSIZE" | "KEYS" | "RANDOMKEY" => self.all_runtime_shards(),
            _ => Vec::new(),
        }
    }

    pub(super) fn collect_unique_runtime_shards_for_keys<'a, I>(&self, keys: I) -> Vec<u16>
    where
        I: IntoIterator<Item = &'a [u8]>,
    {
        let mut shards = BTreeSet::new();
        for key in keys {
            let _ = shards.insert(self.core.resolve_shard_for_key(key));
        }
        shards.into_iter().collect()
    }

    pub(super) fn all_runtime_shards(&self) -> Vec<u16> {
        (0_u16..self.config.shard_count.get()).collect()
    }
}
