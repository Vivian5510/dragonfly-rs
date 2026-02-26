//! Deferred runtime reply readiness/reduction path.

use super::{
    RuntimeMgetReplyGroup, RuntimeMsetNxStage, RuntimeReplyAggregation, RuntimeReplyTicket,
    RuntimeSequenceBarrier, ServerApp,
};
use crate::app::protocol_reply::encode_reply_for_protocol;
use dfly_common::error::{DflyError, DflyResult};
use dfly_core::command::{CommandFrame, CommandReply};

impl ServerApp {
    fn runtime_sequences_ready_from_snapshot(
        barriers: &[RuntimeSequenceBarrier],
        processed_snapshot: &[u64],
    ) -> DflyResult<bool> {
        for barrier in barriers {
            let Some(processed_sequence) = processed_snapshot.get(usize::from(barrier.shard))
            else {
                return Err(DflyError::InvalidState("target shard is out of range"));
            };
            if *processed_sequence < barrier.sequence {
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

    fn try_advance_msetnx_ticket(
        &self,
        ticket: &mut RuntimeReplyTicket,
        processed_snapshot: &[u64],
    ) -> DflyResult<bool> {
        match &ticket.aggregation {
            RuntimeReplyAggregation::MsetNx {
                stage: RuntimeMsetNxStage::WaitingExists,
                ..
            } => self.try_advance_msetnx_waiting_exists(ticket, processed_snapshot),
            RuntimeReplyAggregation::MsetNx {
                stage: RuntimeMsetNxStage::WaitingSet,
                ..
            } => self.try_advance_msetnx_waiting_set(ticket, processed_snapshot),
            RuntimeReplyAggregation::MsetNx {
                stage: RuntimeMsetNxStage::Completed(_),
                ..
            } => Ok(true),
            _ => Self::runtime_sequences_ready_from_snapshot(&ticket.barriers, processed_snapshot),
        }
    }

    fn try_advance_msetnx_waiting_exists(
        &self,
        ticket: &mut RuntimeReplyTicket,
        processed_snapshot: &[u64],
    ) -> DflyResult<bool> {
        let RuntimeReplyAggregation::MsetNx {
            grouped_set_commands,
            exists_replies,
            set_replies,
            stage,
        } = &mut ticket.aggregation
        else {
            return Ok(true);
        };
        if !Self::runtime_sequences_ready_from_snapshot(exists_replies, processed_snapshot)? {
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
                    *stage = RuntimeMsetNxStage::Completed(CommandReply::Moved { slot, endpoint });
                    return Ok(true);
                }
                CommandReply::Error(message) => {
                    *stage = RuntimeMsetNxStage::Completed(CommandReply::Error(message));
                    return Ok(true);
                }
                _ => {
                    *stage = RuntimeMsetNxStage::Completed(CommandReply::Error(
                        "internal error: EXISTS did not return integer reply".to_owned(),
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
        ticket.barriers.clone_from(&submitted);
        *stage = RuntimeMsetNxStage::WaitingSet;
        // Set phase was just submitted in this round; wait for a fresh snapshot.
        Ok(false)
    }

    fn try_advance_msetnx_waiting_set(
        &self,
        ticket: &mut RuntimeReplyTicket,
        processed_snapshot: &[u64],
    ) -> DflyResult<bool> {
        let RuntimeReplyAggregation::MsetNx {
            set_replies, stage, ..
        } = &mut ticket.aggregation
        else {
            return Ok(true);
        };
        if !Self::runtime_sequences_ready_from_snapshot(set_replies, processed_snapshot)? {
            return Ok(false);
        }
        let grouped_set = self.take_grouped_worker_replies(set_replies)?;
        for reply in grouped_set {
            match reply {
                CommandReply::SimpleString(ok) if ok == "OK" => {}
                CommandReply::Moved { slot, endpoint } => {
                    *stage = RuntimeMsetNxStage::Completed(CommandReply::Moved { slot, endpoint });
                    return Ok(true);
                }
                CommandReply::Error(message) => {
                    *stage = RuntimeMsetNxStage::Completed(CommandReply::Error(message));
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
        Ok(true)
    }

    pub(crate) fn runtime_processed_sequences_snapshot(&self) -> DflyResult<Vec<u64>> {
        self.runtime.snapshot_processed_sequences()
    }

    pub(crate) fn runtime_reply_ticket_ready_with_snapshot(
        &self,
        ticket: &mut RuntimeReplyTicket,
        processed_snapshot: &[u64],
    ) -> DflyResult<bool> {
        if matches!(&ticket.aggregation, RuntimeReplyAggregation::MsetNx { .. }) {
            return self.try_advance_msetnx_ticket(ticket, processed_snapshot);
        }
        Self::runtime_sequences_ready_from_snapshot(&ticket.barriers, processed_snapshot)
    }

    pub(crate) fn runtime_reply_ticket_ready(
        &self,
        ticket: &mut RuntimeReplyTicket,
    ) -> DflyResult<bool> {
        let processed_snapshot = self.runtime_processed_sequences_snapshot()?;
        self.runtime_reply_ticket_ready_with_snapshot(ticket, &processed_snapshot)
    }

    pub(crate) fn take_runtime_reply_ticket_ready(
        &self,
        ticket: &mut RuntimeReplyTicket,
    ) -> DflyResult<Vec<u8>> {
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
        Ok(encode_reply_for_protocol(ticket.protocol, &ticket.frame, reply))
    }

    pub(crate) fn take_runtime_reply_ticket(
        &self,
        ticket: &mut RuntimeReplyTicket,
    ) -> DflyResult<Vec<u8>> {
        let processed_snapshot = self.runtime_processed_sequences_snapshot()?;
        if !self.runtime_reply_ticket_ready_with_snapshot(ticket, &processed_snapshot)? {
            return Err(DflyError::InvalidState(
                "runtime deferred reply is not ready",
            ));
        }
        self.take_runtime_reply_ticket_ready(ticket)
    }
}
