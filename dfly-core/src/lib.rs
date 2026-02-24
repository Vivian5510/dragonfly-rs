//! Core runtime abstractions shared by transaction, storage, and facade layers.

pub mod command;
pub mod dispatch;
pub mod runtime;
pub mod sharding;

use command::{CommandFrame, CommandReply};
use dfly_common::error::{DflyError, DflyResult};
use dfly_common::ids::{DbIndex, ShardCount};
use dispatch::{CommandRegistry, DispatchState, ValueEntry};
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

/// One logical KV entry in snapshot form.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SnapshotEntry {
    /// Owning shard id.
    pub shard: u16,
    /// Logical DB id.
    pub db: DbIndex,
    /// Key bytes.
    pub key: Vec<u8>,
    /// Value bytes.
    pub value: Vec<u8>,
    /// Optional expire timestamp.
    pub expire_at_unix_secs: Option<u64>,
}

/// Snapshot payload for full state capture.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct CoreSnapshot {
    /// Flattened entry list.
    pub entries: Vec<SnapshotEntry>,
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
        self.execute_in_db(0, frame)
    }

    /// Executes one command frame in a selected logical DB.
    #[must_use]
    pub fn execute_in_db(&mut self, db: DbIndex, frame: &CommandFrame) -> CommandReply {
        let target_shard = self.resolve_target_shard(frame);
        let target_index = usize::from(target_shard);
        let Some(target_state) = self.shard_states.get_mut(target_index) else {
            return CommandReply::Error(format!("invalid target shard {target_shard}"));
        };
        self.command_registry.dispatch(db, frame, target_state)
    }

    /// Validates a command against core command-table arity/existence rules.
    ///
    /// # Errors
    ///
    /// Returns user-facing error text when command does not exist or argument count is invalid.
    pub fn validate_command(&self, frame: &CommandFrame) -> Result<(), String> {
        self.command_registry.validate_frame(frame)
    }

    /// Exports current in-memory state as a snapshot payload.
    #[must_use]
    pub fn export_snapshot(&self) -> CoreSnapshot {
        let mut entries = Vec::new();
        for (shard_index, state) in self.shard_states.iter().enumerate() {
            let Ok(shard) = u16::try_from(shard_index) else {
                continue;
            };
            for (db, keyspace) in &state.db_kv {
                for (key, value_entry) in keyspace {
                    entries.push(SnapshotEntry {
                        shard,
                        db: *db,
                        key: key.clone(),
                        value: value_entry.value.clone(),
                        expire_at_unix_secs: value_entry.expire_at_unix_secs,
                    });
                }
            }
        }
        entries.sort_by(|left, right| {
            left.shard
                .cmp(&right.shard)
                .then(left.db.cmp(&right.db))
                .then(left.key.cmp(&right.key))
        });
        CoreSnapshot { entries }
    }

    /// Replaces current in-memory state with provided snapshot payload.
    ///
    /// # Errors
    ///
    /// Returns `DflyError::Protocol` when snapshot carries an out-of-range shard id.
    pub fn import_snapshot(&mut self, snapshot: &CoreSnapshot) -> DflyResult<()> {
        for state in &mut self.shard_states {
            *state = DispatchState::default();
        }

        for entry in &snapshot.entries {
            let Some(state) = self.shard_states.get_mut(usize::from(entry.shard)) else {
                return Err(DflyError::Protocol(format!(
                    "snapshot entry targets unknown shard {}",
                    entry.shard
                )));
            };
            state.db_kv.entry(entry.db).or_default().insert(
                entry.key.clone(),
                ValueEntry {
                    value: entry.value.clone(),
                    expire_at_unix_secs: entry.expire_at_unix_secs,
                },
            );
            state.mark_key_loaded(entry.db, &entry.key);
        }
        Ok(())
    }

    /// Resolves the owner shard for a key using the active sharding policy.
    #[must_use]
    pub fn resolve_shard_for_key(&self, key: &[u8]) -> u16 {
        self.resolver.shard_for_key(key)
    }

    /// Returns current mutation version for one key in selected logical DB.
    #[must_use]
    pub fn key_version(&self, db: DbIndex, key: &[u8]) -> u64 {
        let shard = self.resolve_shard_for_key(key);
        self.shard_states
            .get(usize::from(shard))
            .map_or(0, |state| state.key_version(db, key))
    }

    /// Removes all keys in one logical DB across all shards.
    ///
    /// Returns the number of removed keys.
    pub fn flush_db(&mut self, db: DbIndex) -> usize {
        self.shard_states
            .iter_mut()
            .map(|state| state.flush_db(db))
            .sum()
    }

    /// Removes all keys in all logical DBs across all shards.
    ///
    /// Returns the number of removed keys.
    pub fn flush_all(&mut self) -> usize {
        self.shard_states
            .iter_mut()
            .map(dispatch::DispatchState::flush_all)
            .sum()
    }

    /// Selects target shard for one command.
    ///
    /// Current strategy:
    /// - key-based commands route by first key argument
    /// - keyless commands run on shard 0
    #[must_use]
    pub fn resolve_target_shard(&self, frame: &CommandFrame) -> u16 {
        match frame.name.as_str() {
            "GET" | "SET" | "DEL" | "EXISTS" | "EXPIRE" | "TTL" | "PERSIST" => frame
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
            core.shard_states[usize::from(first_shard)].db_len(0),
            eq(1_usize),
        );
        assert_that!(
            core.shard_states[usize::from(second_shard)].db_len(0),
            eq(1_usize),
        );
    }

    #[rstest]
    fn core_snapshot_roundtrip_preserves_db_state() {
        let mut source = CoreModule::new(ShardCount::new(4).expect("valid shard count"));

        let _ = source.execute_in_db(
            0,
            &CommandFrame::new("SET", vec![b"user".to_vec(), b"alice".to_vec()]),
        );
        let _ = source.execute_in_db(
            2,
            &CommandFrame::new("SET", vec![b"user".to_vec(), b"bob".to_vec()]),
        );

        let snapshot = source.export_snapshot();

        let mut restored = CoreModule::new(ShardCount::new(4).expect("valid shard count"));
        restored
            .import_snapshot(&snapshot)
            .expect("snapshot import should succeed");

        let db0_get = restored.execute_in_db(0, &CommandFrame::new("GET", vec![b"user".to_vec()]));
        let db2_get = restored.execute_in_db(2, &CommandFrame::new("GET", vec![b"user".to_vec()]));
        assert_that!(&db0_get, eq(&CommandReply::BulkString(b"alice".to_vec())));
        assert_that!(&db2_get, eq(&CommandReply::BulkString(b"bob".to_vec())));
    }

    #[rstest]
    fn core_snapshot_import_marks_loaded_keys_as_versioned() {
        let mut source = CoreModule::new(ShardCount::new(4).expect("valid shard count"));
        let key = b"loaded:key".to_vec();
        let _ = source.execute_in_db(
            0,
            &CommandFrame::new("SET", vec![key.clone(), b"value".to_vec()]),
        );
        let snapshot = source.export_snapshot();

        let mut restored = CoreModule::new(ShardCount::new(4).expect("valid shard count"));
        restored
            .import_snapshot(&snapshot)
            .expect("snapshot import should succeed");

        assert_that!(restored.key_version(0, &key), eq(1_u64));
    }

    #[rstest]
    fn core_reports_key_version_for_watched_mutations() {
        let mut core = CoreModule::new(ShardCount::new(4).expect("valid shard count"));
        let key = b"watch:1".to_vec();

        assert_that!(core.key_version(0, &key), eq(0_u64));
        let _ = core.execute_in_db(
            0,
            &CommandFrame::new("SET", vec![key.clone(), b"value".to_vec()]),
        );
        assert_that!(core.key_version(0, &key), eq(1_u64));
    }

    #[rstest]
    fn core_flush_db_clears_only_selected_database() {
        let mut core = CoreModule::new(ShardCount::new(4).expect("valid shard count"));

        let _ = core.execute_in_db(
            0,
            &CommandFrame::new("SET", vec![b"flush:key".to_vec(), b"db0".to_vec()]),
        );
        let _ = core.execute_in_db(
            2,
            &CommandFrame::new("SET", vec![b"flush:key".to_vec(), b"db2".to_vec()]),
        );

        let removed = core.flush_db(2);
        assert_that!(removed, eq(1_usize));

        let db0_value =
            core.execute_in_db(0, &CommandFrame::new("GET", vec![b"flush:key".to_vec()]));
        let db2_value =
            core.execute_in_db(2, &CommandFrame::new("GET", vec![b"flush:key".to_vec()]));
        assert_that!(&db0_value, eq(&CommandReply::BulkString(b"db0".to_vec())));
        assert_that!(&db2_value, eq(&CommandReply::Null));
    }

    #[rstest]
    fn core_flush_all_clears_all_databases() {
        let mut core = CoreModule::new(ShardCount::new(4).expect("valid shard count"));

        let _ = core.execute_in_db(
            0,
            &CommandFrame::new("SET", vec![b"key:0".to_vec(), b"v0".to_vec()]),
        );
        let _ = core.execute_in_db(
            3,
            &CommandFrame::new("SET", vec![b"key:3".to_vec(), b"v3".to_vec()]),
        );

        let removed = core.flush_all();
        assert_that!(removed, eq(2_usize));

        let db0_value = core.execute_in_db(0, &CommandFrame::new("GET", vec![b"key:0".to_vec()]));
        let db3_value = core.execute_in_db(3, &CommandFrame::new("GET", vec![b"key:3".to_vec()]));
        assert_that!(&db0_value, eq(&CommandReply::Null));
        assert_that!(&db3_value, eq(&CommandReply::Null));
    }
}
