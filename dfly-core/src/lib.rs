//! Core runtime abstractions shared by transaction, storage, and facade layers.

pub mod command;
pub mod dispatch;
pub mod runtime;
pub mod sharding;

use command::{CommandFrame, CommandReply};
use dfly_common::ids::ShardCount;
use dispatch::{CommandRegistry, DispatchState};
use sharding::{HashTagShardResolver, ShardResolver};

/// Core module bootstrap object.
///
/// In Unit 0 this struct only wires shard resolver policy. Later units add process-wide registries
/// and cross-shard execution orchestration.
#[derive(Debug, Clone)]
pub struct CoreModule {
    /// Resolver used to map keys into owning shards.
    pub resolver: HashTagShardResolver,
    /// Command table used by coordinator/runtime execution path.
    pub command_registry: CommandRegistry,
    /// Per-shard mutable execution state.
    ///
    /// Dragonfly isolates key ownership per shard thread. This vector models that ownership
    /// boundary in the learning implementation by keeping one independent keyspace per shard.
    shard_states: Vec<DispatchState>,
}

impl CoreModule {
    /// Creates the core bootstrap module from process config.
    #[must_use]
    pub fn new(shard_count: ShardCount) -> Self {
        Self {
            resolver: HashTagShardResolver::new(shard_count),
            command_registry: CommandRegistry::with_builtin_commands(),
            shard_states: vec![DispatchState::default(); usize::from(shard_count.get())],
        }
    }

    /// Executes one command frame on its selected target shard.
    #[must_use]
    pub fn execute(&mut self, frame: &CommandFrame) -> CommandReply {
        let target_shard = self.target_shard_for_command(frame);
        let target_index = usize::from(target_shard);
        let Some(target_state) = self.shard_states.get_mut(target_index) else {
            return CommandReply::Error(format!("invalid target shard {target_shard}"));
        };
        self.command_registry.dispatch(frame, target_state)
    }

    /// Resolves the owner shard for a key using the active sharding policy.
    #[must_use]
    pub fn resolve_shard_for_key(&self, key: &[u8]) -> u16 {
        self.resolver.shard_for_key(key)
    }

    /// Selects target shard for one command.
    ///
    /// Current strategy:
    /// - key-based commands route by first key argument
    /// - keyless commands run on shard 0
    #[must_use]
    fn target_shard_for_command(&self, frame: &CommandFrame) -> u16 {
        match frame.name.as_str() {
            "GET" | "SET" => frame
                .args
                .first()
                .map_or(0, |key| self.resolve_shard_for_key(key)),
            _ => 0,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::CoreModule;
    use crate::command::{CommandFrame, CommandReply};
    use dfly_common::ids::ShardCount;
    use googletest::prelude::*;
    use rstest::rstest;

    #[rstest]
    fn core_execute_routes_set_get_through_shard_state() {
        let mut core = CoreModule::new(ShardCount::new(4).expect("valid shard count"));

        let set_reply = core.execute(&CommandFrame::new(
            "SET",
            vec![b"session:1".to_vec(), b"active".to_vec()],
        ));
        assert_that!(&set_reply, eq(&CommandReply::SimpleString("OK".to_owned())));

        let get_reply = core.execute(&CommandFrame::new("GET", vec![b"session:1".to_vec()]));
        assert_that!(
            &get_reply,
            eq(&CommandReply::BulkString(b"active".to_vec()))
        );
    }

    #[rstest]
    fn core_keeps_shard_state_isolated_between_two_keys() {
        let mut core = CoreModule::new(ShardCount::new(8).expect("valid shard count"));

        let first_key = b"user:1001".to_vec();
        let mut second_key = b"user:2002".to_vec();
        let first_shard = core.resolve_shard_for_key(&first_key);
        let mut second_shard = core.resolve_shard_for_key(&second_key);
        let mut suffix: u32 = 0;

        // Find a deterministic second key landing on another shard so the test observes
        // cross-shard isolation behavior rather than same-shard updates.
        while second_shard == first_shard {
            suffix += 1;
            second_key = format!("user:2002:{suffix}").into_bytes();
            second_shard = core.resolve_shard_for_key(&second_key);
        }

        let _ = core.execute(&CommandFrame::new(
            "SET",
            vec![first_key.clone(), b"alpha".to_vec()],
        ));
        let _ = core.execute(&CommandFrame::new(
            "SET",
            vec![second_key.clone(), b"beta".to_vec()],
        ));

        assert_that!(
            core.shard_states[usize::from(first_shard)].kv.len(),
            eq(1_usize),
        );
        assert_that!(
            core.shard_states[usize::from(second_shard)].kv.len(),
            eq(1_usize),
        );
    }
}
