//! Core runtime abstractions shared by transaction, storage, and facade layers.

pub mod command;
pub mod dispatch;
pub mod runtime;
pub mod sharding;

use command::{CommandFrame, CommandReply};
use dfly_common::error::{DflyError, DflyResult};
use dfly_common::ids::{DbIndex, ShardCount};
use dispatch::{
    CommandRegistry, DispatchState, ValueEntry, copy_between_states, rename_between_states,
};
use sharding::{HashTagShardResolver, ShardResolver};
use std::time::{SystemTime, UNIX_EPOCH};

/// Coordinator-facing routing class used by transaction planning.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CommandRouting {
    /// Command targets one primary key routed by argument #1.
    SingleKey {
        /// Whether command mutates keyspace/value state.
        is_write: bool,
    },
    /// Command has no single-key affinity.
    NonKey,
}

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
        match frame.name.as_str() {
            "COPY" => return self.execute_copy_in_db(db, frame),
            "RENAME" => return self.execute_rename_in_db(db, frame, false),
            "RENAMENX" => return self.execute_rename_in_db(db, frame, true),
            _ => {}
        }

        let target_shard = self.resolve_target_shard(frame);
        let target_index = usize::from(target_shard);
        let Some(target_state) = self.shard_states.get_mut(target_index) else {
            return CommandReply::Error(format!("invalid target shard {target_shard}"));
        };
        self.command_registry.dispatch(db, frame, target_state)
    }

    fn execute_rename_in_db(
        &mut self,
        db: DbIndex,
        frame: &CommandFrame,
        destination_should_not_exist: bool,
    ) -> CommandReply {
        if frame.args.len() != 2 {
            let command = if destination_should_not_exist {
                "RENAMENX"
            } else {
                "RENAME"
            };
            return CommandReply::Error(format!(
                "wrong number of arguments for '{command}' command"
            ));
        }

        let source_key = &frame.args[0];
        let destination_key = &frame.args[1];

        let source_shard = usize::from(self.resolve_shard_for_key(source_key));
        let destination_shard = usize::from(self.resolve_shard_for_key(destination_key));

        if source_shard == destination_shard {
            let Some(target_state) = self.shard_states.get_mut(source_shard) else {
                return CommandReply::Error(format!("invalid target shard {source_shard}"));
            };
            return self.command_registry.dispatch(db, frame, target_state);
        }

        let Some((source_state, destination_state)) =
            shard_pair_mut(&mut self.shard_states, source_shard, destination_shard)
        else {
            return CommandReply::Error(format!(
                "invalid rename shard pair {source_shard}->{destination_shard}"
            ));
        };

        rename_between_states(
            db,
            source_key,
            destination_key,
            destination_should_not_exist,
            source_state,
            destination_state,
        )
    }

    fn execute_copy_in_db(&mut self, db: DbIndex, frame: &CommandFrame) -> CommandReply {
        if frame.args.len() < 2 {
            return CommandReply::Error("wrong number of arguments for 'COPY' command".to_owned());
        }

        let source_key = &frame.args[0];
        let destination_key = &frame.args[1];

        let source_shard = usize::from(self.resolve_shard_for_key(source_key));
        let destination_shard = usize::from(self.resolve_shard_for_key(destination_key));

        if source_shard == destination_shard {
            let Some(target_state) = self.shard_states.get_mut(source_shard) else {
                return CommandReply::Error(format!("invalid target shard {source_shard}"));
            };
            return self.command_registry.dispatch(db, frame, target_state);
        }

        let Some((source_state, destination_state)) =
            shard_pair_mut(&mut self.shard_states, source_shard, destination_shard)
        else {
            return CommandReply::Error(format!(
                "invalid copy shard pair {source_shard}->{destination_shard}"
            ));
        };

        copy_between_states(db, &frame.args, source_state, destination_state)
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

    /// Returns total key count for one logical DB across all shards.
    #[must_use]
    pub fn db_size(&self, db: DbIndex) -> usize {
        self.shard_states.iter().map(|state| state.db_len(db)).sum()
    }

    /// Returns one random key from selected logical DB across all shards.
    ///
    /// Expired keys are ignored even if they are still physically present in shard maps.
    #[must_use]
    pub fn random_key(&self, db: DbIndex) -> Option<Vec<u8>> {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_or(0_u64, |duration| duration.as_secs());

        let mut candidates = Vec::new();
        for state in &self.shard_states {
            let Some(keyspace) = state.db_kv.get(&db) else {
                continue;
            };
            for (key, value) in keyspace {
                if value
                    .expire_at_unix_secs
                    .is_none_or(|expire_at| expire_at > now)
                {
                    candidates.push(key.clone());
                }
            }
        }
        if candidates.is_empty() {
            return None;
        }

        let entropy = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_or(0_u128, |duration| duration.as_nanos());
        let len = u128::try_from(candidates.len()).unwrap_or(1);
        let index = usize::try_from(entropy % len).unwrap_or(0);
        Some(candidates.swap_remove(index))
    }

    /// Returns keys in selected logical DB that match one glob-style pattern.
    ///
    /// Pattern semantics follow Redis-style glob matching:
    /// - `*` matches any byte sequence
    /// - `?` matches one byte
    /// - `[a-z]` and `[^a-z]` define character classes
    /// - `\` escapes the next byte
    ///
    /// Returned keys are sorted lexicographically to keep test assertions deterministic.
    #[must_use]
    pub fn keys_matching(&self, db: DbIndex, pattern: &[u8]) -> Vec<Vec<u8>> {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_or(0_u64, |duration| duration.as_secs());
        let mut matched = Vec::new();

        for state in &self.shard_states {
            let Some(keyspace) = state.db_kv.get(&db) else {
                continue;
            };
            for (key, value) in keyspace {
                if value
                    .expire_at_unix_secs
                    .is_some_and(|expire_at| expire_at <= now)
                {
                    continue;
                }
                if redis_glob_match(pattern, key) {
                    matched.push(key.clone());
                }
            }
        }

        matched.sort_unstable();
        matched
    }

    /// Selects target shard for one command.
    ///
    /// Current strategy:
    /// - key-based commands route by first key argument
    /// - keyless commands run on shard 0
    #[must_use]
    pub fn resolve_target_shard(&self, frame: &CommandFrame) -> u16 {
        match self.command_routing(frame) {
            CommandRouting::SingleKey { .. } => frame
                .args
                .first()
                .map_or(0, |key| self.resolve_shard_for_key(key)),
            CommandRouting::NonKey => 0,
        }
    }

    /// Returns transaction-planning routing class for one command frame.
    #[must_use]
    pub fn command_routing(&self, frame: &CommandFrame) -> CommandRouting {
        let name = frame.name.as_str();
        let has_exactly_one_key_arg = frame.args.len() == 1;
        let has_primary_key = !frame.args.is_empty();

        if has_exactly_one_key_arg
            && matches!(
                name,
                "GET"
                    | "TYPE"
                    | "STRLEN"
                    | "GETRANGE"
                    | "TTL"
                    | "PTTL"
                    | "EXPIRETIME"
                    | "PEXPIRETIME"
                    | "EXISTS"
                    | "TOUCH"
            )
        {
            return CommandRouting::SingleKey { is_write: false };
        }

        if (has_primary_key
            && matches!(
                name,
                "SET"
                    | "GETSET"
                    | "GETDEL"
                    | "APPEND"
                    | "MOVE"
                    | "SETRANGE"
                    | "SETEX"
                    | "PSETEX"
                    | "EXPIRE"
                    | "PEXPIRE"
                    | "EXPIREAT"
                    | "PEXPIREAT"
                    | "PERSIST"
                    | "INCR"
                    | "DECR"
                    | "INCRBY"
                    | "DECRBY"
                    | "SETNX"
            ))
            || (has_exactly_one_key_arg && matches!(name, "DEL" | "UNLINK"))
        {
            return CommandRouting::SingleKey { is_write: true };
        }

        CommandRouting::NonKey
    }

    /// Returns whether command can participate in single-key lock-ahead planning.
    #[must_use]
    pub fn is_single_key_command(&self, frame: &CommandFrame) -> bool {
        matches!(
            self.command_routing(frame),
            CommandRouting::SingleKey { .. }
        )
    }
}

fn shard_pair_mut(
    shard_states: &mut [DispatchState],
    source_shard: usize,
    destination_shard: usize,
) -> Option<(&mut DispatchState, &mut DispatchState)> {
    if source_shard == destination_shard {
        return None;
    }

    if source_shard < destination_shard {
        let (left, right) = shard_states.split_at_mut(destination_shard);
        let source = left.get_mut(source_shard)?;
        let destination = right.get_mut(0)?;
        return Some((source, destination));
    }

    let (left, right) = shard_states.split_at_mut(source_shard);
    let destination = left.get_mut(destination_shard)?;
    let source = right.get_mut(0)?;
    Some((source, destination))
}

fn redis_glob_match(pattern: &[u8], text: &[u8]) -> bool {
    if pattern.is_empty() {
        return text.is_empty();
    }

    match pattern[0] {
        b'*' => {
            let mut suffix = &pattern[1..];
            while suffix.first().is_some_and(|byte| *byte == b'*') {
                suffix = &suffix[1..];
            }
            if suffix.is_empty() {
                return true;
            }
            for index in 0..=text.len() {
                if redis_glob_match(suffix, &text[index..]) {
                    return true;
                }
            }
            false
        }
        b'?' => text
            .split_first()
            .is_some_and(|(_, rest)| redis_glob_match(&pattern[1..], rest)),
        b'[' => {
            let Some((class_match, consumed)) = glob_match_class(pattern, text.first().copied())
            else {
                return text.split_first().is_some_and(|(first, rest)| {
                    *first == b'[' && redis_glob_match(&pattern[1..], rest)
                });
            };
            class_match && redis_glob_match(&pattern[consumed..], &text[1..])
        }
        b'\\' => match (pattern.get(1), text.first()) {
            (Some(escaped), Some(first)) if escaped == first => {
                redis_glob_match(&pattern[2..], &text[1..])
            }
            (None, Some(first)) if *first == b'\\' => redis_glob_match(&pattern[1..], &text[1..]),
            _ => false,
        },
        literal => text.split_first().is_some_and(|(first, rest)| {
            *first == literal && redis_glob_match(&pattern[1..], rest)
        }),
    }
}

fn glob_match_class(pattern: &[u8], candidate: Option<u8>) -> Option<(bool, usize)> {
    let candidate = candidate?;
    if pattern.first().copied() != Some(b'[') {
        return None;
    }

    let mut index = 1_usize;
    let mut negated = false;
    if pattern
        .get(index)
        .is_some_and(|byte| *byte == b'^' || *byte == b'!')
    {
        negated = true;
        index += 1;
    }

    let mut matched = false;
    while index < pattern.len() {
        if pattern[index] == b']' {
            let class_match = if negated { !matched } else { matched };
            return Some((class_match, index + 1));
        }

        let (start, next_index) = parse_class_atom(pattern, index)?;
        index = next_index;

        if index + 1 < pattern.len() && pattern[index] == b'-' && pattern[index + 1] != b']' {
            let (end, range_end_index) = parse_class_atom(pattern, index + 1)?;
            index = range_end_index;

            let (range_start, range_end) = if start <= end {
                (start, end)
            } else {
                (end, start)
            };
            if (range_start..=range_end).contains(&candidate) {
                matched = true;
            }
        } else if candidate == start {
            matched = true;
        }
    }

    None
}

fn parse_class_atom(pattern: &[u8], index: usize) -> Option<(u8, usize)> {
    let byte = *pattern.get(index)?;
    if byte == b'\\' {
        return pattern
            .get(index + 1)
            .copied()
            .map(|escaped| (escaped, index + 2));
    }
    Some((byte, index + 1))
}

#[cfg(test)]
mod tests {
    use super::{CommandRouting, CoreModule};
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
    fn core_classifies_single_key_and_non_key_routing() {
        let core = CoreModule::new(ShardCount::new(4).expect("valid shard count"));

        let set = CommandFrame::new("SET", vec![b"k".to_vec(), b"v".to_vec()]);
        assert_that!(core.is_single_key_command(&set), eq(true));
        assert_that!(
            core.command_routing(&set),
            eq(CommandRouting::SingleKey { is_write: true })
        );

        let get = CommandFrame::new("GET", vec![b"k".to_vec()]);
        assert_that!(core.is_single_key_command(&get), eq(true));
        assert_that!(
            core.command_routing(&get),
            eq(CommandRouting::SingleKey { is_write: false })
        );

        let del_single = CommandFrame::new("DEL", vec![b"k".to_vec()]);
        assert_that!(core.is_single_key_command(&del_single), eq(true));
        assert_that!(
            core.command_routing(&del_single),
            eq(CommandRouting::SingleKey { is_write: true })
        );

        let del_multi = CommandFrame::new("DEL", vec![b"k1".to_vec(), b"k2".to_vec()]);
        assert_that!(core.is_single_key_command(&del_multi), eq(false));
        assert_that!(core.command_routing(&del_multi), eq(CommandRouting::NonKey));

        let exists_multi = CommandFrame::new(
            "EXISTS",
            vec![b"k1".to_vec(), b"k2".to_vec(), b"k3".to_vec()],
        );
        assert_that!(core.is_single_key_command(&exists_multi), eq(false));
        assert_that!(
            core.command_routing(&exists_multi),
            eq(CommandRouting::NonKey)
        );

        let rename = CommandFrame::new("RENAME", vec![b"src".to_vec(), b"dst".to_vec()]);
        assert_that!(core.is_single_key_command(&rename), eq(false));
        assert_that!(core.command_routing(&rename), eq(CommandRouting::NonKey));

        let ping = CommandFrame::new("PING", Vec::new());
        assert_that!(core.is_single_key_command(&ping), eq(false));
        assert_that!(core.command_routing(&ping), eq(CommandRouting::NonKey));
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

    #[rstest]
    fn core_db_size_counts_keys_across_shards_per_database() {
        let mut core = CoreModule::new(ShardCount::new(8).expect("valid shard count"));

        let first_key = b"user:1001".to_vec();
        let first_shard = core.resolve_shard_for_key(&first_key);
        let mut second_key = b"user:2002".to_vec();
        let mut second_shard = core.resolve_shard_for_key(&second_key);
        let mut suffix = 0_u32;
        while second_shard == first_shard {
            suffix += 1;
            second_key = format!("user:2002:{suffix}").into_bytes();
            second_shard = core.resolve_shard_for_key(&second_key);
        }

        let _ = core.execute_in_db(
            0,
            &CommandFrame::new("SET", vec![first_key.clone(), b"alpha".to_vec()]),
        );
        let _ = core.execute_in_db(
            0,
            &CommandFrame::new("SET", vec![second_key.clone(), b"beta".to_vec()]),
        );
        let _ = core.execute_in_db(
            1,
            &CommandFrame::new("SET", vec![b"db1:key".to_vec(), b"value".to_vec()]),
        );

        assert_that!(core.db_size(0), eq(2_usize));
        assert_that!(core.db_size(1), eq(1_usize));
        assert_that!(core.db_size(2), eq(0_usize));
    }

    #[rstest]
    fn core_rename_moves_value_across_shards() {
        let mut core = CoreModule::new(ShardCount::new(8).expect("valid shard count"));

        let source_key = b"rename:src".to_vec();
        let source_shard = core.resolve_shard_for_key(&source_key);
        let mut destination_key = b"rename:dst".to_vec();
        let mut destination_shard = core.resolve_shard_for_key(&destination_key);
        let mut suffix = 0_u32;
        while destination_shard == source_shard {
            suffix = suffix.saturating_add(1);
            destination_key = format!("rename:dst:{suffix}").into_bytes();
            destination_shard = core.resolve_shard_for_key(&destination_key);
        }

        let _ = core.execute_in_db(
            0,
            &CommandFrame::new("SET", vec![source_key.clone(), b"value".to_vec()]),
        );
        let _ = core.execute_in_db(
            0,
            &CommandFrame::new("SET", vec![destination_key.clone(), b"old".to_vec()]),
        );

        let rename = core.execute_in_db(
            0,
            &CommandFrame::new("RENAME", vec![source_key.clone(), destination_key.clone()]),
        );
        assert_that!(&rename, eq(&CommandReply::SimpleString("OK".to_owned())));

        let source = core.execute_in_db(0, &CommandFrame::new("GET", vec![source_key]));
        let destination = core.execute_in_db(0, &CommandFrame::new("GET", vec![destination_key]));
        assert_that!(&source, eq(&CommandReply::Null));
        assert_that!(
            &destination,
            eq(&CommandReply::BulkString(b"value".to_vec()))
        );
    }

    #[rstest]
    fn core_renamenx_blocks_when_destination_exists_across_shards() {
        let mut core = CoreModule::new(ShardCount::new(8).expect("valid shard count"));

        let source_key = b"rename:nx:src".to_vec();
        let source_shard = core.resolve_shard_for_key(&source_key);
        let mut destination_key = b"rename:nx:dst".to_vec();
        let mut destination_shard = core.resolve_shard_for_key(&destination_key);
        let mut suffix = 0_u32;
        while destination_shard == source_shard {
            suffix = suffix.saturating_add(1);
            destination_key = format!("rename:nx:dst:{suffix}").into_bytes();
            destination_shard = core.resolve_shard_for_key(&destination_key);
        }

        let _ = core.execute_in_db(
            0,
            &CommandFrame::new("SET", vec![source_key.clone(), b"src".to_vec()]),
        );
        let _ = core.execute_in_db(
            0,
            &CommandFrame::new("SET", vec![destination_key.clone(), b"dst".to_vec()]),
        );

        let renamenx = core.execute_in_db(
            0,
            &CommandFrame::new(
                "RENAMENX",
                vec![source_key.clone(), destination_key.clone()],
            ),
        );
        assert_that!(&renamenx, eq(&CommandReply::Integer(0)));

        let source = core.execute_in_db(0, &CommandFrame::new("GET", vec![source_key]));
        let destination = core.execute_in_db(0, &CommandFrame::new("GET", vec![destination_key]));
        assert_that!(&source, eq(&CommandReply::BulkString(b"src".to_vec())));
        assert_that!(&destination, eq(&CommandReply::BulkString(b"dst".to_vec())));
    }

    #[rstest]
    fn core_copy_duplicates_value_across_shards_without_removing_source() {
        let mut core = CoreModule::new(ShardCount::new(8).expect("valid shard count"));

        let source_key = b"copy:src".to_vec();
        let source_shard = core.resolve_shard_for_key(&source_key);
        let mut destination_key = b"copy:dst".to_vec();
        let mut destination_shard = core.resolve_shard_for_key(&destination_key);
        let mut suffix = 0_u32;
        while destination_shard == source_shard {
            suffix = suffix.saturating_add(1);
            destination_key = format!("copy:dst:{suffix}").into_bytes();
            destination_shard = core.resolve_shard_for_key(&destination_key);
        }

        let _ = core.execute_in_db(
            0,
            &CommandFrame::new("SET", vec![source_key.clone(), b"value".to_vec()]),
        );

        let copied = core.execute_in_db(
            0,
            &CommandFrame::new("COPY", vec![source_key.clone(), destination_key.clone()]),
        );
        assert_that!(&copied, eq(&CommandReply::Integer(1)));

        let source = core.execute_in_db(0, &CommandFrame::new("GET", vec![source_key]));
        let destination = core.execute_in_db(0, &CommandFrame::new("GET", vec![destination_key]));
        assert_that!(&source, eq(&CommandReply::BulkString(b"value".to_vec())));
        assert_that!(
            &destination,
            eq(&CommandReply::BulkString(b"value".to_vec()))
        );
    }

    #[rstest]
    fn core_random_key_returns_none_for_empty_database() {
        let core = CoreModule::new(ShardCount::new(4).expect("valid shard count"));
        assert_that!(&core.random_key(0), eq(&None));
    }

    #[rstest]
    fn core_random_key_returns_live_key_from_selected_database() {
        let mut core = CoreModule::new(ShardCount::new(8).expect("valid shard count"));

        let key_one = b"random:key:1".to_vec();
        let key_two = b"random:key:2".to_vec();
        let _ = core.execute_in_db(
            0,
            &CommandFrame::new("SET", vec![key_one.clone(), b"v1".to_vec()]),
        );
        let _ = core.execute_in_db(
            0,
            &CommandFrame::new("SET", vec![key_two.clone(), b"v2".to_vec()]),
        );
        let _ = core.execute_in_db(
            1,
            &CommandFrame::new("SET", vec![b"other-db".to_vec(), b"x".to_vec()]),
        );

        let selected = core.random_key(0);
        assert_that!(selected.is_some(), eq(true));
        let selected = selected.unwrap_or_default();
        assert_that!(selected == key_one || selected == key_two, eq(true));
    }

    #[rstest]
    fn core_keys_matching_filters_by_pattern_and_database() {
        let mut core = CoreModule::new(ShardCount::new(8).expect("valid shard count"));

        let _ = core.execute_in_db(
            0,
            &CommandFrame::new("SET", vec![b"user:1".to_vec(), b"a".to_vec()]),
        );
        let _ = core.execute_in_db(
            0,
            &CommandFrame::new("SET", vec![b"user:2".to_vec(), b"b".to_vec()]),
        );
        let _ = core.execute_in_db(
            0,
            &CommandFrame::new("SET", vec![b"admin:1".to_vec(), b"c".to_vec()]),
        );
        let _ = core.execute_in_db(
            1,
            &CommandFrame::new("SET", vec![b"user:db1".to_vec(), b"x".to_vec()]),
        );

        let user_keys = core.keys_matching(0, b"user:*");
        assert_that!(
            &user_keys,
            eq(&vec![b"user:1".to_vec(), b"user:2".to_vec()])
        );

        let two_char_keys = core.keys_matching(0, b"?????:1");
        assert_that!(&two_char_keys, eq(&vec![b"admin:1".to_vec()]));
    }

    #[rstest]
    fn core_keys_matching_supports_character_classes_and_escaping() {
        let mut core = CoreModule::new(ShardCount::new(4).expect("valid shard count"));

        let _ = core.execute_in_db(
            0,
            &CommandFrame::new("SET", vec![br"k[1]".to_vec(), b"v1".to_vec()]),
        );
        let _ = core.execute_in_db(
            0,
            &CommandFrame::new("SET", vec![br"kx1".to_vec(), b"v2".to_vec()]),
        );
        let _ = core.execute_in_db(
            0,
            &CommandFrame::new("SET", vec![br"ky2".to_vec(), b"v3".to_vec()]),
        );

        let range = core.keys_matching(0, b"k[xy][12]");
        assert_that!(&range, eq(&vec![b"kx1".to_vec(), b"ky2".to_vec()]));

        let escaped = core.keys_matching(0, br"k\[1\]");
        assert_that!(&escaped, eq(&vec![br"k[1]".to_vec()]));
    }
}
