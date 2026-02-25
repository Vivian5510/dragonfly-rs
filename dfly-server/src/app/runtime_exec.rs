//! Runtime dispatch/execution path for transactions and direct command ingress.

use super::{
    RuntimeMgetReplyGroup, RuntimeMsetNxStage, RuntimeReplyAggregation, RuntimeReplyTicket,
    RuntimeSequenceBarrier, ServerApp, ServerConnection, is_runtime_dispatch_error,
};
use dfly_common::config::ClusterMode;
use dfly_common::error::{DflyError, DflyResult};
use dfly_common::ids::TxId;
use dfly_core::CommandRouting;
use dfly_core::command::{CommandFrame, CommandReply};
use dfly_core::runtime::RuntimeEnvelope;
use dfly_facade::protocol::ClientProtocol;
use dfly_transaction::plan::{TransactionHop, TransactionMode, TransactionPlan};
use dfly_transaction::scheduler::TransactionScheduler;
use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::time::Duration;

impl ServerApp {
    pub(super) fn execute_transaction_plan(
        &self,
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
        &self,
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
            } else if target_shards_by_command[index].is_empty() {
                self.execute_command_after_runtime_barrier(db, command)
            } else {
                self.execute_command_after_runtime_barrier_internal(db, command, true)
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
            if Self::command_uses_grouped_worker_post_barrier(command, &target_shards) {
                // This command will be executed via grouped worker dispatch in the
                // post-barrier phase, so the pre-dispatch wave should not emit
                // redundant fanout envelopes.
                target_shards_by_command.push(Vec::new());
                execute_on_worker_by_command.push(false);
                continue;
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
        if self.cluster_read_guard().mode != ClusterMode::Disabled {
            return false;
        }

        if matches!(command.name.as_str(), "COPY" | "RENAME" | "RENAMENX")
            && !target_shards.is_empty()
        {
            // Cross-shard copy/rename still runs as one shard-owned callback in Dragonfly's
            // transaction runtime model. We execute it on one owner worker and keep the other
            // touched shards in the hop barrier.
            return true;
        }

        let same_shard = self
            .same_shard_multikey_count_target_shard(command)
            .or_else(|| self.same_shard_multikey_string_target_shard(command));
        let Some(same_shard) = same_shard else {
            return false;
        };
        target_shards.len() == 1 && target_shards.first().copied() == Some(same_shard)
    }

    fn command_uses_grouped_worker_post_barrier(
        command: &CommandFrame,
        target_shards: &[u16],
    ) -> bool {
        if target_shards.len() <= 1 {
            return false;
        }

        matches!(
            command.name.as_str(),
            "MGET" | "MSET" | "MSETNX" | "DEL" | "UNLINK" | "EXISTS" | "TOUCH"
        )
    }

    pub(super) fn execute_command_after_runtime_barrier(
        &self,
        db: u16,
        command: &CommandFrame,
    ) -> CommandReply {
        self.execute_command_after_runtime_barrier_internal(db, command, false)
    }

    fn execute_command_after_runtime_barrier_internal(
        &self,
        db: u16,
        command: &CommandFrame,
        pre_dispatched: bool,
    ) -> CommandReply {
        // If pre-dispatch did not assign a worker callback for one key-affine command, try
        // to recover by routing it through the shard worker now instead of falling back to
        // coordinator-local execution.
        if let Some(reply) = self.execute_copy_rename_via_runtime(db, command, false) {
            return reply;
        }
        if matches!(
            self.core.command_routing(command),
            CommandRouting::SingleKey { .. }
        ) {
            return self.execute_single_shard_command_via_runtime_internal(db, command, false);
        }
        if let Some(reply) =
            self.execute_multikey_string_commands_via_runtime_internal(db, command, false)
        {
            return reply;
        }
        if let Some(reply) =
            self.execute_multi_key_counting_command_via_runtime_internal(db, command, false)
        {
            return reply;
        }
        if !pre_dispatched
            && let Some(reply) = self.dispatch_runtime_command_post_barrier(db, command)
        {
            return reply;
        }
        if self.cluster_read_guard().mode == ClusterMode::Disabled
            && self.command_requires_shard_worker_execution(command)
        {
            return CommandReply::Error(
                "runtime dispatch failed: key command escaped shard worker execution".to_owned(),
            );
        }
        self.execute_command_without_side_effects(db, command)
    }

    fn dispatch_runtime_command_post_barrier(
        &self,
        db: u16,
        command: &CommandFrame,
    ) -> Option<CommandReply> {
        match self.dispatch_runtime_for_command(db, command, false) {
            Ok(Some(reply)) => Some(reply),
            Ok(None) => None,
            Err(error) => Some(CommandReply::Error(format!(
                "runtime dispatch failed: {error}"
            ))),
        }
    }

    fn command_requires_shard_worker_execution(&self, command: &CommandFrame) -> bool {
        if matches!(
            self.core.command_routing(command),
            CommandRouting::SingleKey { .. }
        ) {
            return true;
        }
        matches!(command.name.as_str(), "COPY" | "RENAME" | "RENAMENX")
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

    pub(crate) fn try_dispatch_parsed_command_runtime_deferred(
        &self,
        connection: &ServerConnection,
        frame: &CommandFrame,
    ) -> DflyResult<Option<RuntimeReplyTicket>> {
        if self.cluster_read_guard().mode != ClusterMode::Disabled {
            return Ok(None);
        }
        if connection.transaction.in_multi() {
            return Ok(None);
        }

        self.build_direct_runtime_ticket(
            connection.context.db_index,
            connection.context.protocol,
            frame,
        )
    }

    fn build_direct_runtime_ticket(
        &self,
        db: u16,
        protocol: ClientProtocol,
        frame: &CommandFrame,
    ) -> DflyResult<Option<RuntimeReplyTicket>> {
        if matches!(
            self.core.command_routing(frame),
            CommandRouting::SingleKey { .. }
        ) {
            let shard = self.core.resolve_target_shard(frame);
            return self.build_single_shard_runtime_ticket(db, protocol, frame, shard);
        }

        if let Some(ticket) = self.build_copy_rename_runtime_ticket(db, protocol, frame)? {
            return Ok(Some(ticket));
        }

        if let Some(ticket) = self.build_multikey_string_runtime_ticket(db, protocol, frame)? {
            return Ok(Some(ticket));
        }

        if let Some(ticket) = self.build_multikey_count_runtime_ticket(db, protocol, frame)? {
            return Ok(Some(ticket));
        }

        self.dispatch_runtime_for_command_deferred(db, protocol, frame, true)
    }

    fn build_copy_rename_runtime_ticket(
        &self,
        db: u16,
        protocol: ClientProtocol,
        frame: &CommandFrame,
    ) -> DflyResult<Option<RuntimeReplyTicket>> {
        let Some((source_shard, destination_shard)) = self.copy_rename_shards(frame) else {
            return Ok(None);
        };
        let mut touched_shards = vec![source_shard];
        if destination_shard != source_shard {
            touched_shards.push(destination_shard);
        }
        self.transaction
            .scheduler
            .ensure_shards_available(&touched_shards)?;
        let source_sequence =
            self.submit_runtime_envelope_with_sequence(source_shard, db, frame, true)?;
        let mut barriers = vec![RuntimeSequenceBarrier {
            shard: source_shard,
            sequence: source_sequence,
        }];
        if destination_shard != source_shard {
            let destination_sequence =
                self.submit_runtime_envelope_with_sequence(destination_shard, db, frame, false)?;
            barriers.push(RuntimeSequenceBarrier {
                shard: destination_shard,
                sequence: destination_sequence,
            });
        }
        Ok(Some(RuntimeReplyTicket {
            db,
            txid: None,
            protocol,
            frame: frame.clone(),
            barriers,
            aggregation: RuntimeReplyAggregation::Worker {
                shard: source_shard,
                sequence: source_sequence,
            },
        }))
    }

    fn build_multikey_string_runtime_ticket(
        &self,
        db: u16,
        protocol: ClientProtocol,
        frame: &CommandFrame,
    ) -> DflyResult<Option<RuntimeReplyTicket>> {
        if let Some(shard) = self.same_shard_multikey_string_target_shard(frame) {
            return self.build_single_shard_runtime_ticket(db, protocol, frame, shard);
        }
        match frame.name.as_str() {
            "MGET" if frame.args.len() > 1 => self.build_deferred_mget_ticket(db, protocol, frame),
            "MSET" if !frame.args.is_empty() && frame.args.len().is_multiple_of(2) => {
                self.build_deferred_mset_ticket(db, protocol, frame)
            }
            "MSETNX" if !frame.args.is_empty() && frame.args.len().is_multiple_of(2) => {
                self.build_deferred_msetnx_ticket(db, protocol, frame)
            }
            _ => Ok(None),
        }
    }

    fn build_deferred_mget_ticket(
        &self,
        db: u16,
        protocol: ClientProtocol,
        frame: &CommandFrame,
    ) -> DflyResult<Option<RuntimeReplyTicket>> {
        let mut grouped_keys = BTreeMap::<u16, Vec<(usize, Vec<u8>)>>::new();
        for (position, key) in frame.args.iter().enumerate() {
            let shard = self.core.resolve_shard_for_key(key);
            grouped_keys
                .entry(shard)
                .or_default()
                .push((position, key.clone()));
        }

        let grouped_entries = grouped_keys.into_iter().collect::<Vec<_>>();
        let grouped_commands = grouped_entries
            .iter()
            .map(|(shard, keys)| {
                let args = keys.iter().map(|(_, key)| key.clone()).collect::<Vec<_>>();
                (*shard, CommandFrame::new("MGET", args))
            })
            .collect::<Vec<_>>();
        let replies = self.submit_grouped_worker_commands_deferred(db, grouped_commands, true)?;
        let groups = grouped_entries
            .into_iter()
            .zip(replies.iter())
            .map(|((_, keys), reply_sequence)| RuntimeMgetReplyGroup {
                shard: reply_sequence.shard,
                sequence: reply_sequence.sequence,
                positions: keys.into_iter().map(|(position, _)| position).collect(),
            })
            .collect::<Vec<_>>();
        Ok(Some(RuntimeReplyTicket {
            db,
            txid: None,
            protocol,
            frame: frame.clone(),
            barriers: replies,
            aggregation: RuntimeReplyAggregation::GroupedMget { groups },
        }))
    }

    fn build_deferred_mset_ticket(
        &self,
        db: u16,
        protocol: ClientProtocol,
        frame: &CommandFrame,
    ) -> DflyResult<Option<RuntimeReplyTicket>> {
        let mut grouped_pairs = BTreeMap::<u16, Vec<Vec<u8>>>::new();
        for pair in frame.args.chunks_exact(2) {
            let shard = self.core.resolve_shard_for_key(&pair[0]);
            let grouped = grouped_pairs.entry(shard).or_default();
            grouped.push(pair[0].clone());
            grouped.push(pair[1].clone());
        }
        let grouped_commands = grouped_pairs
            .into_iter()
            .map(|(shard, args)| (shard, CommandFrame::new("MSET", args)))
            .collect::<Vec<_>>();
        let replies = self.submit_grouped_worker_commands_deferred(db, grouped_commands, true)?;
        Ok(Some(RuntimeReplyTicket {
            db,
            txid: None,
            protocol,
            frame: frame.clone(),
            barriers: replies.clone(),
            aggregation: RuntimeReplyAggregation::GroupedAllOk {
                replies,
                command: frame.name.clone(),
            },
        }))
    }

    fn build_deferred_msetnx_ticket(
        &self,
        db: u16,
        protocol: ClientProtocol,
        frame: &CommandFrame,
    ) -> DflyResult<Option<RuntimeReplyTicket>> {
        let mut grouped_exists_keys = BTreeMap::<u16, Vec<Vec<u8>>>::new();
        let mut grouped_set_pairs = BTreeMap::<u16, Vec<Vec<u8>>>::new();
        for pair in frame.args.chunks_exact(2) {
            let shard = self.core.resolve_shard_for_key(&pair[0]);
            grouped_exists_keys
                .entry(shard)
                .or_default()
                .push(pair[0].clone());
            let grouped = grouped_set_pairs.entry(shard).or_default();
            grouped.push(pair[0].clone());
            grouped.push(pair[1].clone());
        }
        let grouped_exists_commands = grouped_exists_keys
            .into_iter()
            .map(|(shard, keys)| (shard, CommandFrame::new("EXISTS", keys)))
            .collect::<Vec<_>>();
        let exists_replies =
            self.submit_grouped_worker_commands_deferred(db, grouped_exists_commands, true)?;
        let grouped_set_commands = grouped_set_pairs
            .into_iter()
            .map(|(shard, args)| (shard, CommandFrame::new("MSET", args)))
            .collect::<Vec<_>>();
        Ok(Some(RuntimeReplyTicket {
            db,
            txid: None,
            protocol,
            frame: frame.clone(),
            barriers: exists_replies.clone(),
            aggregation: RuntimeReplyAggregation::MsetNx {
                grouped_set_commands,
                exists_replies,
                set_replies: Vec::new(),
                stage: RuntimeMsetNxStage::WaitingExists,
            },
        }))
    }

    fn build_multikey_count_runtime_ticket(
        &self,
        db: u16,
        protocol: ClientProtocol,
        frame: &CommandFrame,
    ) -> DflyResult<Option<RuntimeReplyTicket>> {
        if let Some(shard) = self.same_shard_multikey_count_target_shard(frame) {
            return self.build_single_shard_runtime_ticket(db, protocol, frame, shard);
        }

        if matches!(frame.name.as_str(), "DEL" | "UNLINK" | "EXISTS" | "TOUCH")
            && frame.args.len() > 1
        {
            let grouped_commands = self.group_multikey_count_frame_by_shard(frame);
            let replies =
                self.submit_grouped_worker_commands_deferred(db, grouped_commands, true)?;
            return Ok(Some(RuntimeReplyTicket {
                db,
                txid: None,
                protocol,
                frame: frame.clone(),
                barriers: replies.clone(),
                aggregation: RuntimeReplyAggregation::GroupedIntegerSum {
                    replies,
                    command: frame.name.clone(),
                },
            }));
        }

        Ok(None)
    }

    fn build_single_shard_runtime_ticket(
        &self,
        db: u16,
        protocol: ClientProtocol,
        frame: &CommandFrame,
        shard: u16,
    ) -> DflyResult<Option<RuntimeReplyTicket>> {
        self.transaction
            .scheduler
            .ensure_shards_available(&[shard])?;
        let sequence = self.submit_runtime_envelope_with_sequence(shard, db, frame, true)?;
        Ok(Some(RuntimeReplyTicket {
            db,
            txid: None,
            protocol,
            frame: frame.clone(),
            barriers: vec![RuntimeSequenceBarrier { shard, sequence }],
            aggregation: RuntimeReplyAggregation::Worker { shard, sequence },
        }))
    }

    fn submit_grouped_worker_commands_deferred(
        &self,
        db: u16,
        grouped_commands: Vec<(u16, CommandFrame)>,
        enforce_scheduler_barrier: bool,
    ) -> DflyResult<Vec<RuntimeSequenceBarrier>> {
        if grouped_commands.is_empty() {
            return Ok(Vec::new());
        }

        if enforce_scheduler_barrier {
            let mut touched_shards = grouped_commands
                .iter()
                .map(|(shard, _)| *shard)
                .collect::<Vec<_>>();
            touched_shards.sort_unstable();
            touched_shards.dedup();
            self.transaction
                .scheduler
                .ensure_shards_available(&touched_shards)?;
        }

        let mut replies = Vec::with_capacity(grouped_commands.len());
        for (shard, grouped_frame) in grouped_commands {
            let sequence =
                self.submit_runtime_envelope_with_sequence(shard, db, &grouped_frame, true)?;
            replies.push(RuntimeSequenceBarrier { shard, sequence });
        }
        Ok(replies)
    }

    fn runtime_sequences_ready(&self, barriers: &[RuntimeSequenceBarrier]) -> DflyResult<bool> {
        for barrier in barriers {
            let ready = self.runtime.wait_for_processed_sequence(
                barrier.shard,
                barrier.sequence,
                Duration::ZERO,
            )?;
            if !ready {
                return Ok(false);
            }
        }
        Ok(true)
    }

    fn take_runtime_reply_from_sequence(
        &self,
        barrier: RuntimeSequenceBarrier,
    ) -> DflyResult<CommandReply> {
        let reply = self
            .runtime
            .take_processed_reply(barrier.shard, barrier.sequence)?;
        reply.ok_or(DflyError::InvalidState("missing worker reply"))
    }

    fn take_grouped_worker_replies(
        &self,
        replies: &[RuntimeSequenceBarrier],
    ) -> DflyResult<Vec<CommandReply>> {
        let mut grouped = Vec::with_capacity(replies.len());
        for barrier in replies {
            grouped.push(self.take_runtime_reply_from_sequence(*barrier)?);
        }
        Ok(grouped)
    }

    fn reduce_grouped_integer_sum(
        &self,
        replies: &[RuntimeSequenceBarrier],
        command: &str,
    ) -> DflyResult<CommandReply> {
        let grouped = self.take_grouped_worker_replies(replies)?;
        let mut total = 0_i64;
        for reply in grouped {
            match reply {
                CommandReply::Integer(delta) => total = total.saturating_add(delta.max(0)),
                CommandReply::Moved { slot, endpoint } => {
                    return Ok(CommandReply::Moved { slot, endpoint });
                }
                CommandReply::Error(message) => return Ok(CommandReply::Error(message)),
                _ => {
                    return Ok(CommandReply::Error(format!(
                        "internal error: {command} did not return integer reply"
                    )));
                }
            }
        }
        Ok(CommandReply::Integer(total))
    }

    fn reduce_grouped_all_ok(
        &self,
        replies: &[RuntimeSequenceBarrier],
        command: &str,
    ) -> DflyResult<CommandReply> {
        let grouped = self.take_grouped_worker_replies(replies)?;
        for reply in grouped {
            match reply {
                CommandReply::SimpleString(ok) if ok == "OK" => {}
                CommandReply::Moved { slot, endpoint } => {
                    return Ok(CommandReply::Moved { slot, endpoint });
                }
                CommandReply::Error(message) => return Ok(CommandReply::Error(message)),
                _ => {
                    return Ok(CommandReply::Error(format!(
                        "internal error: {command} did not return simple-string reply"
                    )));
                }
            }
        }
        Ok(CommandReply::SimpleString("OK".to_owned()))
    }

    fn reduce_grouped_mget(
        &self,
        frame: &CommandFrame,
        groups: &[RuntimeMgetReplyGroup],
    ) -> DflyResult<CommandReply> {
        let mut replies = vec![CommandReply::Null; frame.args.len()];
        for group in groups {
            let reply = self.take_runtime_reply_from_sequence(RuntimeSequenceBarrier {
                shard: group.shard,
                sequence: group.sequence,
            })?;
            let values = match reply {
                CommandReply::Array(values) => values,
                CommandReply::Moved { slot, endpoint } => {
                    return Ok(CommandReply::Moved { slot, endpoint });
                }
                CommandReply::Error(message) => return Ok(CommandReply::Error(message)),
                _ => {
                    return Ok(CommandReply::Error(
                        "internal error: MGET did not return array reply".to_owned(),
                    ));
                }
            };
            if values.len() != group.positions.len() {
                return Ok(CommandReply::Error(
                    "internal error: MGET worker reply cardinality mismatch".to_owned(),
                ));
            }
            for (position, value) in group.positions.iter().copied().zip(values) {
                match value {
                    CommandReply::BulkString(_) | CommandReply::Null => replies[position] = value,
                    CommandReply::Moved { slot, endpoint } => {
                        return Ok(CommandReply::Moved { slot, endpoint });
                    }
                    CommandReply::Error(message) => return Ok(CommandReply::Error(message)),
                    _ => {
                        return Ok(CommandReply::Error(
                            "internal error: MGET value reply was not bulk-string or null"
                                .to_owned(),
                        ));
                    }
                }
            }
        }
        Ok(CommandReply::Array(replies))
    }

    fn try_advance_msetnx_ticket(&self, ticket: &mut RuntimeReplyTicket) -> DflyResult<bool> {
        let RuntimeReplyAggregation::MsetNx {
            grouped_set_commands,
            exists_replies,
            set_replies,
            stage,
        } = &mut ticket.aggregation
        else {
            return self.runtime_sequences_ready(&ticket.barriers);
        };

        loop {
            match stage {
                RuntimeMsetNxStage::WaitingExists => {
                    if !self.runtime_sequences_ready(exists_replies)? {
                        return Ok(false);
                    }
                    let exists_grouped = self.take_grouped_worker_replies(exists_replies)?;
                    let mut existing = 0_i64;
                    for reply in exists_grouped {
                        match reply {
                            CommandReply::Integer(count) => {
                                existing = existing.saturating_add(count.max(0));
                            }
                            CommandReply::Moved { slot, endpoint } => {
                                *stage = RuntimeMsetNxStage::Completed(CommandReply::Moved {
                                    slot,
                                    endpoint,
                                });
                                return Ok(true);
                            }
                            CommandReply::Error(message) => {
                                *stage =
                                    RuntimeMsetNxStage::Completed(CommandReply::Error(message));
                                return Ok(true);
                            }
                            _ => {
                                *stage = RuntimeMsetNxStage::Completed(CommandReply::Error(
                                    "internal error: EXISTS did not return integer reply"
                                        .to_owned(),
                                ));
                                return Ok(true);
                            }
                        }
                    }
                    if existing > 0 {
                        *stage = RuntimeMsetNxStage::Completed(CommandReply::Integer(0));
                        return Ok(true);
                    }

                    let submitted = match self.submit_grouped_worker_commands_deferred(
                        ticket.db,
                        grouped_set_commands.clone(),
                        true,
                    ) {
                        Ok(submitted) => submitted,
                        Err(error) => {
                            *stage = RuntimeMsetNxStage::Completed(CommandReply::Error(format!(
                                "runtime dispatch failed: {error}"
                            )));
                            return Ok(true);
                        }
                    };
                    set_replies.clone_from(&submitted);
                    ticket.barriers = submitted;
                    *stage = RuntimeMsetNxStage::WaitingSet;
                }
                RuntimeMsetNxStage::WaitingSet => {
                    if !self.runtime_sequences_ready(set_replies)? {
                        return Ok(false);
                    }
                    let grouped_set = self.take_grouped_worker_replies(set_replies)?;
                    for reply in grouped_set {
                        match reply {
                            CommandReply::SimpleString(ok) if ok == "OK" => {}
                            CommandReply::Moved { slot, endpoint } => {
                                *stage = RuntimeMsetNxStage::Completed(CommandReply::Moved {
                                    slot,
                                    endpoint,
                                });
                                return Ok(true);
                            }
                            CommandReply::Error(message) => {
                                *stage =
                                    RuntimeMsetNxStage::Completed(CommandReply::Error(message));
                                return Ok(true);
                            }
                            _ => {
                                *stage = RuntimeMsetNxStage::Completed(CommandReply::Error(
                                    "MSETNX failed while setting key".to_owned(),
                                ));
                                return Ok(true);
                            }
                        }
                    }
                    *stage = RuntimeMsetNxStage::Completed(CommandReply::Integer(1));
                    return Ok(true);
                }
                RuntimeMsetNxStage::Completed(_) => return Ok(true),
            }
        }
    }

    pub(crate) fn runtime_reply_ticket_ready(
        &self,
        ticket: &mut RuntimeReplyTicket,
    ) -> DflyResult<bool> {
        if matches!(&ticket.aggregation, RuntimeReplyAggregation::MsetNx { .. }) {
            return self.try_advance_msetnx_ticket(ticket);
        }
        self.runtime_sequences_ready(&ticket.barriers)
    }

    pub(crate) fn take_runtime_reply_ticket(
        &self,
        ticket: &mut RuntimeReplyTicket,
    ) -> DflyResult<Vec<u8>> {
        if matches!(&ticket.aggregation, RuntimeReplyAggregation::MsetNx { .. })
            && !self.try_advance_msetnx_ticket(ticket)?
        {
            return Err(DflyError::InvalidState(
                "runtime deferred reply is not ready",
            ));
        }

        let reply = match &ticket.aggregation {
            RuntimeReplyAggregation::Worker { shard, sequence } => self
                .take_runtime_reply_from_sequence(RuntimeSequenceBarrier {
                    shard: *shard,
                    sequence: *sequence,
                })?,
            RuntimeReplyAggregation::CoordinatorAfterBarrier => {
                self.execute_command_without_side_effects(ticket.db, &ticket.frame)
            }
            RuntimeReplyAggregation::GroupedIntegerSum { replies, command } => {
                self.reduce_grouped_integer_sum(replies, command)?
            }
            RuntimeReplyAggregation::GroupedAllOk { replies, command } => {
                self.reduce_grouped_all_ok(replies, command)?
            }
            RuntimeReplyAggregation::GroupedMget { groups } => {
                self.reduce_grouped_mget(&ticket.frame, groups)?
            }
            RuntimeReplyAggregation::MsetNx { stage, .. } => match stage {
                RuntimeMsetNxStage::Completed(reply) => reply.clone(),
                RuntimeMsetNxStage::WaitingExists | RuntimeMsetNxStage::WaitingSet => {
                    return Err(DflyError::InvalidState(
                        "runtime deferred MSETNX reply is not ready",
                    ));
                }
            },
        };

        self.maybe_append_journal_for_command(ticket.txid, ticket.db, &ticket.frame, &reply);
        Ok(super::encode_reply_for_protocol(
            ticket.protocol,
            &ticket.frame,
            reply,
        ))
    }

    fn execute_runtime_command_on_worker_shard(
        &self,
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

    fn execute_grouped_worker_commands(
        &self,
        db: u16,
        grouped_commands: Vec<(u16, CommandFrame)>,
        enforce_scheduler_barrier: bool,
    ) -> Result<Vec<CommandReply>, CommandReply> {
        if grouped_commands.is_empty() {
            return Ok(Vec::new());
        }

        let touched_shards = grouped_commands
            .iter()
            .map(|(shard, _)| *shard)
            .collect::<Vec<_>>();
        if enforce_scheduler_barrier
            && let Err(error) = self
                .transaction
                .scheduler
                .ensure_shards_available(&touched_shards)
        {
            return Err(CommandReply::Error(format!(
                "runtime dispatch failed: {error}"
            )));
        }

        let mut barriers = Vec::with_capacity(grouped_commands.len());
        for (shard, grouped_frame) in grouped_commands {
            let sequence =
                match self.submit_runtime_envelope_with_sequence(shard, db, &grouped_frame, true) {
                    Ok(sequence) => sequence,
                    Err(error) => {
                        return Err(CommandReply::Error(format!(
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
                return Err(CommandReply::Error(format!(
                    "runtime dispatch failed: {error}"
                )));
            }
        }

        let mut replies = Vec::with_capacity(barriers.len());
        for (shard, sequence) in barriers {
            let reply = match self.runtime.take_processed_reply(shard, sequence) {
                Ok(Some(reply)) => reply,
                Ok(None) => {
                    return Err(CommandReply::Error(
                        "runtime dispatch failed: missing worker reply".to_owned(),
                    ));
                }
                Err(error) => {
                    return Err(CommandReply::Error(format!(
                        "runtime dispatch failed: {error}"
                    )));
                }
            };
            replies.push(reply);
        }

        Ok(replies)
    }

    pub(super) fn execute_user_command(
        &self,
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

            if let Some(reply) = self.execute_copy_rename_via_runtime(db, frame, true) {
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
                Ok(Some(reply)) => {
                    self.maybe_append_journal_for_command(txid, db, frame, &reply);
                    return reply;
                }
                Ok(None) => {}
                Err(error) => {
                    return CommandReply::Error(format!("runtime dispatch failed: {error}"));
                }
            }
        }

        let reply = self.execute_command_without_side_effects(db, frame);
        self.maybe_append_journal_for_command(txid, db, frame, &reply);
        reply
    }

    pub(super) fn execute_copy_rename_via_runtime(
        &self,
        db: u16,
        frame: &CommandFrame,
        enforce_scheduler_barrier: bool,
    ) -> Option<CommandReply> {
        let (source_shard, destination_shard) = self.copy_rename_shards(frame)?;

        let keys_for_cluster_check: Vec<&[u8]> = match frame.name.as_str() {
            "COPY" => frame.args.iter().take(2).map(Vec::as_slice).collect(),
            _ => frame.args.iter().map(Vec::as_slice).collect(),
        };
        if let Some(error_reply) = self.ensure_cluster_multi_key_constraints(keys_for_cluster_check)
        {
            return Some(error_reply);
        }
        Some(self.execute_copy_rename_on_owner_worker(
            db,
            frame,
            source_shard,
            destination_shard,
            enforce_scheduler_barrier,
        ))
    }

    fn execute_copy_rename_on_owner_worker(
        &self,
        db: u16,
        frame: &CommandFrame,
        source_shard: u16,
        destination_shard: u16,
        enforce_scheduler_barrier: bool,
    ) -> CommandReply {
        let mut touched_shards = vec![source_shard];
        if destination_shard != source_shard {
            touched_shards.push(destination_shard);
        }
        if enforce_scheduler_barrier
            && let Err(error) = self
                .transaction
                .scheduler
                .ensure_shards_available(&touched_shards)
        {
            return CommandReply::Error(format!("runtime dispatch failed: {error}"));
        }

        let source_sequence =
            match self.submit_runtime_envelope_with_sequence(source_shard, db, frame, true) {
                Ok(sequence) => sequence,
                Err(error) => {
                    return CommandReply::Error(format!("runtime dispatch failed: {error}"));
                }
            };

        let destination_barrier_sequence = if destination_shard == source_shard {
            None
        } else {
            match self.submit_runtime_envelope_with_sequence(destination_shard, db, frame, false) {
                Ok(sequence) => Some(sequence),
                Err(error) => {
                    return CommandReply::Error(format!("runtime dispatch failed: {error}"));
                }
            }
        };

        if let Err(error) = self
            .runtime
            .wait_until_processed_sequence(source_shard, source_sequence)
        {
            return CommandReply::Error(format!("runtime dispatch failed: {error}"));
        }
        if let Some(sequence) = destination_barrier_sequence
            && let Err(error) = self
                .runtime
                .wait_until_processed_sequence(destination_shard, sequence)
        {
            return CommandReply::Error(format!("runtime dispatch failed: {error}"));
        }

        match self
            .runtime
            .take_processed_reply(source_shard, source_sequence)
        {
            Ok(Some(reply)) => reply,
            Ok(None) => {
                CommandReply::Error("runtime dispatch failed: missing worker reply".to_owned())
            }
            Err(error) => CommandReply::Error(format!("runtime dispatch failed: {error}")),
        }
    }

    fn copy_rename_shards(&self, frame: &CommandFrame) -> Option<(u16, u16)> {
        let valid_shape = match frame.name.as_str() {
            "COPY" => frame.args.len() >= 2,
            "RENAME" | "RENAMENX" => frame.args.len() == 2,
            _ => false,
        };
        if !valid_shape {
            return None;
        }
        let source_shard = self.core.resolve_shard_for_key(&frame.args[0]);
        let destination_shard = self.core.resolve_shard_for_key(&frame.args[1]);
        Some((source_shard, destination_shard))
    }

    fn copy_rename_runtime_target_shards(&self, frame: &CommandFrame) -> Option<Vec<u16>> {
        let (source_shard, destination_shard) = self.copy_rename_shards(frame)?;
        if source_shard == destination_shard {
            return Some(vec![source_shard]);
        }
        // Keep source owner first so cross-shard copy/rename executes on source worker.
        Some(vec![source_shard, destination_shard])
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
        &self,
        db: u16,
        frame: &CommandFrame,
    ) -> Option<CommandReply> {
        self.execute_multikey_string_commands_via_runtime_internal(db, frame, true)
    }

    fn execute_multikey_string_commands_via_runtime_internal(
        &self,
        db: u16,
        frame: &CommandFrame,
        enforce_scheduler_barrier: bool,
    ) -> Option<CommandReply> {
        if let Some(reply) = self.execute_same_shard_multikey_string_via_runtime(
            db,
            frame,
            enforce_scheduler_barrier,
        ) {
            return Some(reply);
        }

        match frame.name.as_str() {
            "MGET" => Some(self.execute_mget_via_runtime(db, frame, enforce_scheduler_barrier)),
            "MSET" => Some(self.execute_mset_via_runtime(db, frame, enforce_scheduler_barrier)),
            "MSETNX" => Some(self.execute_msetnx_via_runtime(db, frame, enforce_scheduler_barrier)),
            _ => None,
        }
    }

    fn execute_same_shard_multikey_string_via_runtime(
        &self,
        db: u16,
        frame: &CommandFrame,
        enforce_scheduler_barrier: bool,
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
        Some(self.execute_runtime_command_on_worker_shard(
            db,
            frame,
            shard,
            enforce_scheduler_barrier,
        ))
    }

    fn execute_mget_via_runtime(
        &self,
        db: u16,
        frame: &CommandFrame,
        enforce_scheduler_barrier: bool,
    ) -> CommandReply {
        if frame.args.is_empty() {
            return CommandReply::Error("wrong number of arguments for 'MGET' command".to_owned());
        }
        if let Some(error_reply) =
            self.ensure_cluster_multi_key_constraints(frame.args.iter().map(Vec::as_slice))
        {
            return error_reply;
        }

        let mut grouped_keys = BTreeMap::<u16, Vec<(usize, Vec<u8>)>>::new();
        for (position, key) in frame.args.iter().enumerate() {
            let shard = self.core.resolve_shard_for_key(key);
            grouped_keys
                .entry(shard)
                .or_default()
                .push((position, key.clone()));
        }

        let grouped_entries = grouped_keys.into_iter().collect::<Vec<_>>();
        let grouped_commands = grouped_entries
            .iter()
            .map(|(shard, keys)| {
                let args = keys.iter().map(|(_, key)| key.clone()).collect::<Vec<_>>();
                (*shard, CommandFrame::new("MGET", args))
            })
            .collect::<Vec<_>>();
        let worker_replies = match self.execute_grouped_worker_commands(
            db,
            grouped_commands,
            enforce_scheduler_barrier,
        ) {
            Ok(replies) => replies,
            Err(reply) => return reply,
        };

        let mut replies = vec![CommandReply::Null; frame.args.len()];
        for ((_, keys), worker_reply) in grouped_entries.into_iter().zip(worker_replies) {
            let values = match worker_reply {
                CommandReply::Array(values) => values,
                CommandReply::Moved { slot, endpoint } => {
                    return CommandReply::Moved { slot, endpoint };
                }
                CommandReply::Error(message) => return CommandReply::Error(message),
                _ => {
                    return CommandReply::Error(
                        "internal error: MGET did not return array reply".to_owned(),
                    );
                }
            };
            if values.len() != keys.len() {
                return CommandReply::Error(
                    "internal error: MGET worker reply cardinality mismatch".to_owned(),
                );
            }

            for ((position, _), value) in keys.into_iter().zip(values) {
                match value {
                    CommandReply::BulkString(_) | CommandReply::Null => {
                        replies[position] = value;
                    }
                    CommandReply::Moved { slot, endpoint } => {
                        return CommandReply::Moved { slot, endpoint };
                    }
                    CommandReply::Error(message) => return CommandReply::Error(message),
                    _ => {
                        return CommandReply::Error(
                            "internal error: MGET value reply was not bulk-string or null"
                                .to_owned(),
                        );
                    }
                }
            }
        }

        CommandReply::Array(replies)
    }

    fn execute_mset_via_runtime(
        &self,
        db: u16,
        frame: &CommandFrame,
        enforce_scheduler_barrier: bool,
    ) -> CommandReply {
        if frame.args.is_empty() || !frame.args.len().is_multiple_of(2) {
            return CommandReply::Error("wrong number of arguments for 'MSET' command".to_owned());
        }
        if let Some(error_reply) = self.ensure_cluster_multi_key_constraints(
            frame.args.chunks_exact(2).map(|pair| pair[0].as_slice()),
        ) {
            return error_reply;
        }

        let mut grouped_pairs = BTreeMap::<u16, Vec<Vec<u8>>>::new();
        for pair in frame.args.chunks_exact(2) {
            let shard = self.core.resolve_shard_for_key(&pair[0]);
            let grouped = grouped_pairs.entry(shard).or_default();
            grouped.push(pair[0].clone());
            grouped.push(pair[1].clone());
        }
        let grouped_commands = grouped_pairs
            .into_iter()
            .map(|(shard, args)| (shard, CommandFrame::new("MSET", args)))
            .collect::<Vec<_>>();
        let worker_replies = match self.execute_grouped_worker_commands(
            db,
            grouped_commands,
            enforce_scheduler_barrier,
        ) {
            Ok(replies) => replies,
            Err(reply) => return reply,
        };

        for reply in worker_replies {
            match reply {
                CommandReply::SimpleString(ok) if ok == "OK" => {}
                CommandReply::Moved { slot, endpoint } => {
                    return CommandReply::Moved { slot, endpoint };
                }
                CommandReply::Error(message) => return CommandReply::Error(message),
                _ => {
                    return CommandReply::Error(
                        "internal error: MSET did not return simple-string reply".to_owned(),
                    );
                }
            }
        }

        CommandReply::SimpleString("OK".to_owned())
    }

    fn execute_msetnx_via_runtime(
        &self,
        db: u16,
        frame: &CommandFrame,
        enforce_scheduler_barrier: bool,
    ) -> CommandReply {
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

        let mut grouped_exists_keys = BTreeMap::<u16, Vec<Vec<u8>>>::new();
        for pair in frame.args.chunks_exact(2) {
            let shard = self.core.resolve_shard_for_key(&pair[0]);
            grouped_exists_keys
                .entry(shard)
                .or_default()
                .push(pair[0].clone());
        }
        let grouped_exists_commands = grouped_exists_keys
            .into_iter()
            .map(|(shard, keys)| (shard, CommandFrame::new("EXISTS", keys)))
            .collect::<Vec<_>>();
        let exists_replies = match self.execute_grouped_worker_commands(
            db,
            grouped_exists_commands,
            enforce_scheduler_barrier,
        ) {
            Ok(replies) => replies,
            Err(reply) => return reply,
        };

        let mut existing = 0_i64;
        for reply in exists_replies {
            match reply {
                CommandReply::Integer(count) => {
                    existing = existing.saturating_add(count.max(0));
                }
                CommandReply::Moved { slot, endpoint } => {
                    return CommandReply::Moved { slot, endpoint };
                }
                CommandReply::Error(message) => return CommandReply::Error(message),
                _ => {
                    return CommandReply::Error(
                        "internal error: EXISTS did not return integer reply".to_owned(),
                    );
                }
            }
        }
        if existing > 0 {
            return CommandReply::Integer(0);
        }

        let mut grouped_pairs = BTreeMap::<u16, Vec<Vec<u8>>>::new();
        for pair in frame.args.chunks_exact(2) {
            let shard = self.core.resolve_shard_for_key(&pair[0]);
            let grouped = grouped_pairs.entry(shard).or_default();
            grouped.push(pair[0].clone());
            grouped.push(pair[1].clone());
        }
        let grouped_set_commands = grouped_pairs
            .into_iter()
            .map(|(shard, args)| (shard, CommandFrame::new("MSET", args)))
            .collect::<Vec<_>>();
        let set_replies = match self.execute_grouped_worker_commands(
            db,
            grouped_set_commands,
            enforce_scheduler_barrier,
        ) {
            Ok(replies) => replies,
            Err(reply) => return reply,
        };

        for reply in set_replies {
            match reply {
                CommandReply::SimpleString(ok) if ok == "OK" => {}
                CommandReply::Moved { slot, endpoint } => {
                    return CommandReply::Moved { slot, endpoint };
                }
                CommandReply::Error(message) => return CommandReply::Error(message),
                _ => {
                    return CommandReply::Error("MSETNX failed while setting key".to_owned());
                }
            }
        }

        CommandReply::Integer(1)
    }

    pub(super) fn execute_multi_key_counting_command_via_runtime(
        &self,
        db: u16,
        frame: &CommandFrame,
    ) -> Option<CommandReply> {
        self.execute_multi_key_counting_command_via_runtime_internal(db, frame, true)
    }

    fn execute_multi_key_counting_command_via_runtime_internal(
        &self,
        db: u16,
        frame: &CommandFrame,
        enforce_scheduler_barrier: bool,
    ) -> Option<CommandReply> {
        if !matches!(frame.name.as_str(), "DEL" | "UNLINK" | "EXISTS" | "TOUCH")
            || frame.args.len() <= 1
        {
            return None;
        }

        if let Some(reply) =
            self.execute_same_shard_multikey_count_via_runtime(db, frame, enforce_scheduler_barrier)
        {
            return Some(reply);
        }
        if let Some(error_reply) =
            self.ensure_cluster_multi_key_constraints(frame.args.iter().map(Vec::as_slice))
        {
            return Some(error_reply);
        }

        let grouped_commands = self.group_multikey_count_frame_by_shard(frame);
        let replies = match self.execute_grouped_worker_commands(
            db,
            grouped_commands,
            enforce_scheduler_barrier,
        ) {
            Ok(replies) => replies,
            Err(reply) => return Some(reply),
        };

        let mut total = 0_i64;
        for reply in replies {
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
        &self,
        db: u16,
        frame: &CommandFrame,
        enforce_scheduler_barrier: bool,
    ) -> Option<CommandReply> {
        let shard = self.same_shard_multikey_count_target_shard(frame)?;

        if let Some(error_reply) =
            self.ensure_cluster_multi_key_constraints(frame.args.iter().map(Vec::as_slice))
        {
            return Some(error_reply);
        }
        Some(self.execute_runtime_command_on_worker_shard(
            db,
            frame,
            shard,
            enforce_scheduler_barrier,
        ))
    }

    pub(super) fn execute_single_shard_command_via_runtime(
        &self,
        db: u16,
        frame: &CommandFrame,
    ) -> CommandReply {
        self.execute_single_shard_command_via_runtime_internal(db, frame, true)
    }

    fn execute_single_shard_command_via_runtime_internal(
        &self,
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
        self.same_shard_multikey_count_target_shard(frame)
            .or_else(|| self.same_shard_multikey_string_target_shard(frame))
    }

    pub(super) fn execute_replay_command_without_journal(
        &self,
        db: u16,
        frame: &CommandFrame,
    ) -> CommandReply {
        if matches!(
            self.core.command_routing(frame),
            CommandRouting::SingleKey { .. }
        ) {
            return self.execute_single_shard_command_via_runtime_internal(db, frame, false);
        }
        if let Some(reply) = self.execute_copy_rename_via_runtime(db, frame, false) {
            return reply;
        }
        if let Some(shard) = self.replay_same_shard_runtime_target(frame) {
            return self.execute_runtime_command_on_worker_shard(db, frame, shard, false);
        }
        if let Some(reply) =
            self.execute_multikey_string_commands_via_runtime_internal(db, frame, false)
        {
            return reply;
        }
        if let Some(reply) =
            self.execute_multi_key_counting_command_via_runtime_internal(db, frame, false)
        {
            return reply;
        }

        let runtime_reply = match self.dispatch_replay_command_runtime(db, frame) {
            Ok(reply) => reply,
            Err(error) => {
                return CommandReply::Error(format!("runtime dispatch failed: {error}"));
            }
        };
        if let Some(reply) = runtime_reply {
            return reply;
        }

        self.execute_command_without_side_effects(db, frame)
    }

    pub(super) fn dispatch_direct_command_runtime(
        &self,
        db: u16,
        frame: &CommandFrame,
    ) -> DflyResult<Option<CommandReply>> {
        self.dispatch_runtime_for_command(db, frame, true)
    }

    pub(super) fn dispatch_replay_command_runtime(
        &self,
        db: u16,
        frame: &CommandFrame,
    ) -> DflyResult<Option<CommandReply>> {
        // Recovery replay must stay independent from live transaction queue ownership checks.
        // During restore we rebuild state from persisted journal entries and should not depend
        // on transient in-memory scheduler leases.
        self.dispatch_runtime_for_command(db, frame, false)
    }

    fn dispatch_runtime_for_command_deferred(
        &self,
        db: u16,
        protocol: ClientProtocol,
        frame: &CommandFrame,
        enforce_scheduler_barrier: bool,
    ) -> DflyResult<Option<RuntimeReplyTicket>> {
        let target_shards = self.runtime_target_shards_for_command(frame);
        if target_shards.is_empty() {
            return Ok(None);
        }

        if enforce_scheduler_barrier {
            self.transaction
                .scheduler
                .ensure_shards_available(&target_shards)?;
        }

        let execute_on_worker = self.cluster_read_guard().mode == ClusterMode::Disabled
            && self.command_requires_shard_worker_execution(frame);

        let mut barriers = Vec::with_capacity(target_shards.len());
        let mut worker_sequence = None;
        for shard in target_shards {
            let should_execute_on_worker = execute_on_worker && worker_sequence.is_none();
            let sequence = self.submit_runtime_envelope_with_sequence(
                shard,
                db,
                frame,
                should_execute_on_worker,
            )?;
            if should_execute_on_worker {
                worker_sequence = Some((shard, sequence));
            }
            barriers.push(RuntimeSequenceBarrier { shard, sequence });
        }

        let aggregation = match worker_sequence {
            Some((shard, sequence)) => RuntimeReplyAggregation::Worker { shard, sequence },
            None => RuntimeReplyAggregation::CoordinatorAfterBarrier,
        };
        Ok(Some(RuntimeReplyTicket {
            db,
            txid: None,
            protocol,
            frame: frame.clone(),
            barriers,
            aggregation,
        }))
    }

    pub(super) fn dispatch_runtime_for_command(
        &self,
        db: u16,
        frame: &CommandFrame,
        enforce_scheduler_barrier: bool,
    ) -> DflyResult<Option<CommandReply>> {
        let target_shards = self.runtime_target_shards_for_command(frame);
        if target_shards.is_empty() {
            return Ok(None);
        }

        if enforce_scheduler_barrier {
            self.transaction
                .scheduler
                .ensure_shards_available(&target_shards)?;
        }
        let execute_on_worker = self.cluster_read_guard().mode == ClusterMode::Disabled
            && self.command_requires_shard_worker_execution(frame);

        // Direct command path mirrors Dragonfly's coordinator ingress:
        // 1) push one envelope to each touched shard queue,
        // 2) wait until all shard workers report they consumed the envelope.
        let mut barriers = Vec::with_capacity(target_shards.len());
        let mut worker_sequence = None;
        for shard in target_shards {
            let should_execute_on_worker = execute_on_worker && worker_sequence.is_none();
            let sequence = self.submit_runtime_envelope_with_sequence(
                shard,
                db,
                frame,
                should_execute_on_worker,
            )?;
            if should_execute_on_worker {
                worker_sequence = Some((shard, sequence));
            }
            barriers.push((shard, sequence));
        }
        for (shard, sequence) in barriers {
            self.runtime
                .wait_until_processed_sequence(shard, sequence)?;
        }
        let Some((shard, sequence)) = worker_sequence else {
            return Ok(None);
        };
        let reply = self.runtime.take_processed_reply(shard, sequence)?;
        let reply = reply.ok_or(DflyError::InvalidState("missing worker reply"))?;
        Ok(Some(reply))
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
        if let Some(shards) = self.copy_rename_runtime_target_shards(frame) {
            return shards;
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
