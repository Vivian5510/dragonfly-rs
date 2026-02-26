//! Runtime dispatch/execution path for post-barrier fallback execution.

use super::ServerApp;
use dfly_common::config::ClusterMode;
use dfly_core::CommandRouting;
use dfly_core::command::{CommandFrame, CommandReply};

impl ServerApp {
    pub(super) fn execute_command_after_runtime_barrier(
        &self,
        db: u16,
        command: &CommandFrame,
    ) -> CommandReply {
        self.execute_command_after_runtime_barrier_internal(db, command, false)
    }

    pub(super) fn execute_command_after_runtime_barrier_internal(
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
}
