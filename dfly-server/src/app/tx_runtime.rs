//! Transaction hop planning and runtime scheduling path.

use super::{ServerApp, is_runtime_dispatch_error};
use dfly_common::config::ClusterMode;
use dfly_common::error::DflyError;
use dfly_common::ids::TxId;
use dfly_core::CommandRouting;
use dfly_core::command::{CommandFrame, CommandReply};
use dfly_transaction::plan::{TransactionHop, TransactionMode, TransactionPlan};
use std::collections::HashMap;

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

    fn execute_runtime_scheduled_hop(
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

        let mut worker_replies_by_command =
            self.collect_worker_hop_replies(&worker_sequence_by_command, &mut per_command_error);

        if per_command_error.iter().any(std::option::Option::is_some) {
            return Self::runtime_error_replies_for_hop(hop, &per_command_error, &shard_wait_error);
        }

        let mut replies = Vec::with_capacity(hop.per_shard.len());
        for (index, (_, command)) in hop.per_shard.iter().enumerate() {
            let reply = if let Some(worker_reply) = worker_replies_by_command[index].take() {
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

    fn prepare_runtime_hop_dispatch(
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

    fn collect_worker_hop_replies(
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

    fn runtime_error_replies_for_hop(
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
}
