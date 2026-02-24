//! Command registration and dispatch skeleton.
//!
//! This module intentionally mirrors Dragonfly's "command table + execution path" concept.
//! Unit 1 keeps the implementation minimal while preserving the same architectural idea:
//! protocol parsing produces a canonical command frame, then a registry resolves and executes
//! the matching handler.

use std::collections::HashMap;
use std::str;
use std::time::{SystemTime, UNIX_EPOCH};

use dfly_cluster::slot::key_slot;
use dfly_common::ids::DbIndex;

use crate::command::{CommandFrame, CommandReply};

/// Handler function signature used by command registry entries.
pub type CommandHandler = fn(DbIndex, &CommandFrame, &mut DispatchState) -> CommandReply;

/// Arity constraints for a command.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CommandArity {
    /// Command must have exactly this many arguments.
    Exact(usize),
    /// Command must have at least this many arguments.
    AtLeast(usize),
}

/// Metadata and callback for one command table entry.
#[derive(Debug, Clone)]
pub struct CommandSpec {
    /// Canonical uppercase command name.
    pub name: &'static str,
    /// Arity constraint used for lightweight input validation.
    pub arity: CommandArity,
    /// Handler callback.
    pub handler: CommandHandler,
}

/// One logical DB table in the shard-local slice.
///
/// This shape mirrors Dragonfly's `DbTable` layering:
/// - `prime`: main key/value dictionary (`PrimeTable` concept),
/// - `expire`: expiration index (`ExpireTable` concept),
/// - `slot_stats`: per-slot key cardinality used by slot-aware operations.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct DbTable {
    /// Main key/value dictionary.
    pub prime: HashMap<Vec<u8>, ValueEntry>,
    /// Expiration index with absolute unix-second deadlines.
    pub expire: HashMap<Vec<u8>, u64>,
    /// Key cardinality by Redis cluster slot.
    pub slot_stats: HashMap<u16, usize>,
}

/// Mutable execution state used by command handlers.
///
/// Dragonfly keeps per-DB tables in one shard-local `DbSlice`. This learning implementation uses
/// the same high-level structure and maintains a layered table per DB.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct DispatchState {
    /// Per-DB layered tables in this shard.
    pub db_tables: HashMap<DbIndex, DbTable>,
    /// Monotonic per-key versions used by optimistic transaction checks (`WATCH`).
    db_versions: HashMap<DbIndex, HashMap<Vec<u8>, u64>>,
}

/// Stored value with optional expiration metadata.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ValueEntry {
    /// User payload.
    pub value: Vec<u8>,
    /// Unix timestamp in seconds when the key expires.
    pub expire_at_unix_secs: Option<u64>,
}

impl DispatchState {
    /// Returns current Unix timestamp in seconds.
    #[must_use]
    fn now_unix_seconds() -> u64 {
        match SystemTime::now().duration_since(UNIX_EPOCH) {
            Ok(duration) => duration.as_secs(),
            Err(_) => 0,
        }
    }

    /// Returns current Unix timestamp in milliseconds.
    #[must_use]
    fn now_unix_millis() -> u64 {
        match SystemTime::now().duration_since(UNIX_EPOCH) {
            Ok(duration) => u64::try_from(duration.as_millis()).unwrap_or(u64::MAX),
            Err(_) => 0,
        }
    }

    /// Returns mutable table for one logical DB, creating it when missing.
    fn db_table_mut(&mut self, db: DbIndex) -> &mut DbTable {
        self.db_tables.entry(db).or_default()
    }

    /// Returns immutable table for one logical DB.
    fn db_table(&self, db: DbIndex) -> Option<&DbTable> {
        self.db_tables.get(&db)
    }

    /// Returns mutable map for one logical DB, creating it when missing.
    fn db_map_mut(&mut self, db: DbIndex) -> &mut HashMap<Vec<u8>, ValueEntry> {
        &mut self.db_table_mut(db).prime
    }

    /// Returns immutable map for one logical DB.
    fn db_map(&self, db: DbIndex) -> Option<&HashMap<Vec<u8>, ValueEntry>> {
        self.db_table(db).map(|table| &table.prime)
    }

    /// Rebuilds expiration index and per-slot cardinality for one DB table.
    ///
    /// This keeps the layered table model consistent even while command handlers are still
    /// written in terms of `prime` map updates.
    fn rebuild_table_indexes(&mut self, db: DbIndex) {
        let Some(table) = self.db_tables.get_mut(&db) else {
            return;
        };

        table.expire.clear();
        table.slot_stats.clear();
        for (key, entry) in &table.prime {
            if let Some(expire_at) = entry.expire_at_unix_secs {
                let _ = table.expire.insert(key.clone(), expire_at);
            }
            let slot = key_slot(key);
            let cardinality = table.slot_stats.entry(slot).or_default();
            *cardinality = cardinality.saturating_add(1);
        }
    }

    /// Rebuilds all DB table secondary indexes (`expire`, `slot_stats`).
    fn rebuild_all_table_indexes(&mut self) {
        let dbs = self.db_tables.keys().copied().collect::<Vec<_>>();
        for db in dbs {
            self.rebuild_table_indexes(db);
        }
    }

    fn decrement_slot_stat(table: &mut DbTable, key: &[u8]) {
        let slot = key_slot(key);
        let mut should_remove = false;
        if let Some(cardinality) = table.slot_stats.get_mut(&slot) {
            *cardinality = cardinality.saturating_sub(1);
            should_remove = *cardinality == 0;
        }
        if should_remove {
            let _ = table.slot_stats.remove(&slot);
        }
    }

    /// Returns tracked version for one key.
    #[must_use]
    pub fn key_version(&self, db: DbIndex, key: &[u8]) -> u64 {
        self.db_versions
            .get(&db)
            .and_then(|versions| versions.get(key))
            .copied()
            .unwrap_or(0)
    }

    /// Bumps version of one key after successful mutation.
    fn bump_key_version(&mut self, db: DbIndex, key: &[u8]) {
        let versions = self.db_versions.entry(db).or_default();
        let next = versions.get(key).copied().unwrap_or(0).saturating_add(1);
        versions.insert(key.to_vec(), next);
    }

    /// Marks one key as loaded from snapshot and advances its mutation version.
    ///
    /// This keeps optimistic watch checks consistent after `LOAD`/snapshot import.
    pub fn mark_key_loaded(&mut self, db: DbIndex, key: &[u8]) {
        self.bump_key_version(db, key);
        self.rebuild_table_indexes(db);
    }

    /// Removes one key when its expiration timestamp has already passed.
    fn purge_expired_key(&mut self, db: DbIndex, key: &[u8]) {
        let should_remove = self
            .db_table(db)
            .and_then(|table| table.expire.get(key))
            .copied()
            .is_some_and(|expire_at| expire_at <= Self::now_unix_seconds());

        if should_remove
            && let Some(table) = self.db_tables.get_mut(&db)
            && table.prime.remove(key).is_some()
        {
            let _ = table.expire.remove(key);
            Self::decrement_slot_stat(table, key);
            self.bump_key_version(db, key);
        }
    }

    /// Number of keys currently stored in one logical DB.
    #[must_use]
    pub fn db_len(&self, db: DbIndex) -> usize {
        self.db_map(db).map_or(0, HashMap::len)
    }

    /// Removes all keys in one logical DB and bumps their key versions.
    ///
    /// Returns the number of removed keys.
    pub fn flush_db(&mut self, db: DbIndex) -> usize {
        let Some(keyspace) = self.db_tables.get(&db).map(|table| &table.prime) else {
            return 0;
        };

        let keys = keyspace.keys().cloned().collect::<Vec<_>>();
        let removed = keys.len();
        for key in &keys {
            self.bump_key_version(db, key);
        }
        let _ = self.db_tables.remove(&db);
        removed
    }

    /// Removes all keys in all logical DBs and bumps their key versions.
    ///
    /// Returns the number of removed keys.
    pub fn flush_all(&mut self) -> usize {
        let mut keys_per_db = Vec::new();
        let mut removed = 0_usize;

        for (db, table) in &self.db_tables {
            let keys = table.prime.keys().cloned().collect::<Vec<_>>();
            removed = removed.saturating_add(keys.len());
            keys_per_db.push((*db, keys));
        }

        for (db, keys) in keys_per_db {
            for key in keys {
                self.bump_key_version(db, &key);
            }
        }
        self.db_tables.clear();
        removed
    }
}

/// Runtime command registry.
#[derive(Debug, Clone, Default)]
pub struct CommandRegistry {
    entries: HashMap<String, CommandSpec>,
}

impl CommandRegistry {
    /// Builds an empty command registry.
    #[must_use]
    pub fn new() -> Self {
        Self {
            entries: HashMap::new(),
        }
    }

    /// Builds a registry preloaded with core learning-path commands.
    #[must_use]
    pub fn with_builtin_commands() -> Self {
        let mut registry = Self::new();
        registry.register_connection_commands();
        registry.register_string_commands();
        registry.register_expiry_commands();
        registry.register_counter_commands();
        registry
    }

    fn register_connection_commands(&mut self) {
        self.register(CommandSpec {
            name: "PING",
            arity: CommandArity::AtLeast(0),
            handler: handle_ping,
        });
        self.register(CommandSpec {
            name: "ECHO",
            arity: CommandArity::Exact(1),
            handler: handle_echo,
        });
    }

    fn register_string_commands(&mut self) {
        self.register(CommandSpec {
            name: "SET",
            arity: CommandArity::AtLeast(2),
            handler: handle_set,
        });
        self.register(CommandSpec {
            name: "SETNX",
            arity: CommandArity::Exact(2),
            handler: handle_setnx,
        });
        self.register(CommandSpec {
            name: "GET",
            arity: CommandArity::Exact(1),
            handler: handle_get,
        });
        self.register(CommandSpec {
            name: "TYPE",
            arity: CommandArity::Exact(1),
            handler: handle_type,
        });
        self.register(CommandSpec {
            name: "GETSET",
            arity: CommandArity::Exact(2),
            handler: handle_getset,
        });
        self.register(CommandSpec {
            name: "GETDEL",
            arity: CommandArity::Exact(1),
            handler: handle_getdel,
        });
        self.register(CommandSpec {
            name: "APPEND",
            arity: CommandArity::Exact(2),
            handler: handle_append,
        });
        self.register(CommandSpec {
            name: "STRLEN",
            arity: CommandArity::Exact(1),
            handler: handle_strlen,
        });
        self.register(CommandSpec {
            name: "GETRANGE",
            arity: CommandArity::Exact(3),
            handler: handle_getrange,
        });
        self.register(CommandSpec {
            name: "SETRANGE",
            arity: CommandArity::Exact(3),
            handler: handle_setrange,
        });
        self.register(CommandSpec {
            name: "DEL",
            arity: CommandArity::AtLeast(1),
            handler: handle_del,
        });
        self.register(CommandSpec {
            name: "UNLINK",
            arity: CommandArity::AtLeast(1),
            handler: handle_del,
        });
        self.register(CommandSpec {
            name: "EXISTS",
            arity: CommandArity::AtLeast(1),
            handler: handle_exists,
        });
        self.register(CommandSpec {
            name: "TOUCH",
            arity: CommandArity::AtLeast(1),
            handler: handle_exists,
        });
        self.register(CommandSpec {
            name: "MOVE",
            arity: CommandArity::Exact(2),
            handler: handle_move,
        });
        self.register(CommandSpec {
            name: "COPY",
            arity: CommandArity::AtLeast(2),
            handler: handle_copy,
        });
        self.register(CommandSpec {
            name: "RENAME",
            arity: CommandArity::Exact(2),
            handler: handle_rename,
        });
        self.register(CommandSpec {
            name: "RENAMENX",
            arity: CommandArity::Exact(2),
            handler: handle_renamenx,
        });
    }

    fn register_expiry_commands(&mut self) {
        self.register(CommandSpec {
            name: "PSETEX",
            arity: CommandArity::Exact(3),
            handler: handle_psetex,
        });
        self.register(CommandSpec {
            name: "SETEX",
            arity: CommandArity::Exact(3),
            handler: handle_setex,
        });
        self.register(CommandSpec {
            name: "EXPIRE",
            arity: CommandArity::AtLeast(2),
            handler: handle_expire,
        });
        self.register(CommandSpec {
            name: "PEXPIRE",
            arity: CommandArity::AtLeast(2),
            handler: handle_pexpire,
        });
        self.register(CommandSpec {
            name: "PEXPIREAT",
            arity: CommandArity::AtLeast(2),
            handler: handle_pexpireat,
        });
        self.register(CommandSpec {
            name: "EXPIREAT",
            arity: CommandArity::AtLeast(2),
            handler: handle_expireat,
        });
        self.register(CommandSpec {
            name: "TTL",
            arity: CommandArity::Exact(1),
            handler: handle_ttl,
        });
        self.register(CommandSpec {
            name: "PTTL",
            arity: CommandArity::Exact(1),
            handler: handle_pttl,
        });
        self.register(CommandSpec {
            name: "EXPIRETIME",
            arity: CommandArity::Exact(1),
            handler: handle_expiretime,
        });
        self.register(CommandSpec {
            name: "PEXPIRETIME",
            arity: CommandArity::Exact(1),
            handler: handle_pexpiretime,
        });
        self.register(CommandSpec {
            name: "PERSIST",
            arity: CommandArity::Exact(1),
            handler: handle_persist,
        });
    }

    fn register_counter_commands(&mut self) {
        self.register(CommandSpec {
            name: "INCR",
            arity: CommandArity::Exact(1),
            handler: handle_incr,
        });
        self.register(CommandSpec {
            name: "DECR",
            arity: CommandArity::Exact(1),
            handler: handle_decr,
        });
        self.register(CommandSpec {
            name: "INCRBY",
            arity: CommandArity::Exact(2),
            handler: handle_incrby,
        });
        self.register(CommandSpec {
            name: "DECRBY",
            arity: CommandArity::Exact(2),
            handler: handle_decrby,
        });
    }

    /// Registers or replaces one command in the table.
    pub fn register(&mut self, spec: CommandSpec) {
        self.entries.insert(spec.name.to_owned(), spec);
    }

    /// Validates command existence and arity without executing handler logic.
    ///
    /// # Errors
    ///
    /// Returns user-facing error text for unknown command names or invalid argument count.
    pub fn validate_frame(&self, frame: &CommandFrame) -> Result<(), String> {
        let command_name = frame.name.to_ascii_uppercase();
        let Some(spec) = self.entries.get(&command_name) else {
            return Err(format!("unknown command '{command_name}'"));
        };

        match spec.arity {
            CommandArity::Exact(expected) if frame.args.len() != expected => Err(format!(
                "wrong number of arguments for '{}' command",
                spec.name
            )),
            CommandArity::AtLeast(minimum) if frame.args.len() < minimum => Err(format!(
                "wrong number of arguments for '{}' command",
                spec.name
            )),
            _ => Ok(()),
        }
    }

    /// Dispatches one canonical command frame to its registered handler.
    #[must_use]
    pub fn dispatch(
        &self,
        db: DbIndex,
        frame: &CommandFrame,
        state: &mut DispatchState,
    ) -> CommandReply {
        if let Err(message) = self.validate_frame(frame) {
            return CommandReply::Error(message);
        }

        let command_name = frame.name.to_ascii_uppercase();
        let Some(spec) = self.entries.get(&command_name) else {
            return CommandReply::Error(format!("unknown command '{command_name}'"));
        };
        let reply = (spec.handler)(db, frame, state);
        // Keep layered table indexes synchronized after each command execution.
        state.rebuild_all_table_indexes();
        reply
    }
}

fn handle_ping(_db: DbIndex, frame: &CommandFrame, _state: &mut DispatchState) -> CommandReply {
    if frame.args.is_empty() {
        return CommandReply::SimpleString("PONG".to_owned());
    }
    if frame.args.len() == 1 {
        return CommandReply::BulkString(frame.args[0].clone());
    }
    CommandReply::Error("wrong number of arguments for 'PING' command".to_owned())
}

fn handle_echo(_db: DbIndex, frame: &CommandFrame, _state: &mut DispatchState) -> CommandReply {
    CommandReply::BulkString(frame.args[0].clone())
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum SetCondition {
    Always,
    IfMissing,
    IfExists,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum SetExpire {
    Seconds(u64),
    Milliseconds(u64),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct SetOptions {
    condition: SetCondition,
    return_previous: bool,
    keep_ttl: bool,
    expire: Option<SetExpire>,
}

impl Default for SetOptions {
    fn default() -> Self {
        Self {
            condition: SetCondition::Always,
            return_previous: false,
            keep_ttl: false,
            expire: None,
        }
    }
}

fn handle_set(db: DbIndex, frame: &CommandFrame, state: &mut DispatchState) -> CommandReply {
    let key = frame.args[0].clone();
    let value = frame.args[1].clone();
    let options = match parse_set_options(&frame.args[2..]) {
        Ok(options) => options,
        Err(error) => return CommandReply::Error(error),
    };
    state.purge_expired_key(db, &key);

    let existing = state.db_map(db).and_then(|map| map.get(&key)).cloned();
    let key_exists = existing.is_some();
    let previous_value = existing.as_ref().map(|entry| entry.value.clone());
    let previous_expire_at = existing.and_then(|entry| entry.expire_at_unix_secs);

    if !set_condition_satisfied(options.condition, key_exists) {
        if options.return_previous {
            return previous_value.map_or(CommandReply::Null, CommandReply::BulkString);
        }
        return CommandReply::Null;
    }

    let expire_at_unix_secs = if let Some(expire) = options.expire {
        Some(resolve_set_expire_at_unix_secs(expire))
    } else if options.keep_ttl {
        previous_expire_at
    } else {
        None
    };

    state.db_map_mut(db).insert(
        key.clone(),
        ValueEntry {
            value,
            expire_at_unix_secs,
        },
    );
    state.bump_key_version(db, &key);

    if options.return_previous {
        previous_value.map_or(CommandReply::Null, CommandReply::BulkString)
    } else {
        CommandReply::SimpleString("OK".to_owned())
    }
}

fn handle_setnx(db: DbIndex, frame: &CommandFrame, state: &mut DispatchState) -> CommandReply {
    let key = frame.args[0].clone();
    let value = frame.args[1].clone();
    state.purge_expired_key(db, &key);

    if state.db_map(db).is_some_and(|map| map.contains_key(&key)) {
        return CommandReply::Integer(0);
    }

    state.db_map_mut(db).insert(
        key.clone(),
        ValueEntry {
            value,
            expire_at_unix_secs: None,
        },
    );
    state.bump_key_version(db, &key);
    CommandReply::Integer(1)
}

fn handle_get(db: DbIndex, frame: &CommandFrame, state: &mut DispatchState) -> CommandReply {
    let key = &frame.args[0];
    state.purge_expired_key(db, key);
    match state.db_map(db).and_then(|map| map.get(key)) {
        Some(value) => CommandReply::BulkString(value.value.clone()),
        None => CommandReply::Null,
    }
}

fn handle_type(db: DbIndex, frame: &CommandFrame, state: &mut DispatchState) -> CommandReply {
    let key = &frame.args[0];
    state.purge_expired_key(db, key);
    if state.db_map(db).is_some_and(|map| map.contains_key(key)) {
        return CommandReply::SimpleString("string".to_owned());
    }
    CommandReply::SimpleString("none".to_owned())
}

fn handle_getset(db: DbIndex, frame: &CommandFrame, state: &mut DispatchState) -> CommandReply {
    let key = frame.args[0].clone();
    let next_value = frame.args[1].clone();
    state.purge_expired_key(db, &key);

    let previous = state
        .db_map(db)
        .and_then(|map| map.get(&key))
        .map(|entry| entry.value.clone());

    state.db_map_mut(db).insert(
        key.clone(),
        ValueEntry {
            value: next_value,
            expire_at_unix_secs: None,
        },
    );
    state.bump_key_version(db, &key);

    previous.map_or(CommandReply::Null, CommandReply::BulkString)
}

fn handle_getdel(db: DbIndex, frame: &CommandFrame, state: &mut DispatchState) -> CommandReply {
    let key = &frame.args[0];
    state.purge_expired_key(db, key);

    let removed = state.db_map_mut(db).remove(key);
    if let Some(entry) = removed {
        state.bump_key_version(db, key);
        return CommandReply::BulkString(entry.value);
    }
    CommandReply::Null
}

fn handle_append(db: DbIndex, frame: &CommandFrame, state: &mut DispatchState) -> CommandReply {
    let key = frame.args[0].clone();
    let suffix = &frame.args[1];
    state.purge_expired_key(db, &key);

    let (mut value, expire_at_unix_secs) =
        if let Some(existing) = state.db_map(db).and_then(|map| map.get(&key)) {
            (existing.value.clone(), existing.expire_at_unix_secs)
        } else {
            (Vec::new(), None)
        };
    value.extend_from_slice(suffix);

    state.db_map_mut(db).insert(
        key.clone(),
        ValueEntry {
            value: value.clone(),
            expire_at_unix_secs,
        },
    );
    state.bump_key_version(db, &key);
    CommandReply::Integer(i64::try_from(value.len()).unwrap_or(i64::MAX))
}

fn handle_strlen(db: DbIndex, frame: &CommandFrame, state: &mut DispatchState) -> CommandReply {
    let key = &frame.args[0];
    state.purge_expired_key(db, key);
    let length = state
        .db_map(db)
        .and_then(|map| map.get(key))
        .map_or(0_usize, |entry| entry.value.len());
    CommandReply::Integer(i64::try_from(length).unwrap_or(i64::MAX))
}

fn handle_getrange(db: DbIndex, frame: &CommandFrame, state: &mut DispatchState) -> CommandReply {
    let key = &frame.args[0];
    state.purge_expired_key(db, key);

    let Some(value) = state
        .db_map(db)
        .and_then(|map| map.get(key))
        .map(|entry| entry.value.as_slice())
    else {
        return CommandReply::BulkString(Vec::new());
    };

    let Ok(start) = parse_redis_i64(&frame.args[1]) else {
        return CommandReply::Error("value is not an integer or out of range".to_owned());
    };
    let Ok(end) = parse_redis_i64(&frame.args[2]) else {
        return CommandReply::Error("value is not an integer or out of range".to_owned());
    };

    let Some((start_index, end_index)) = normalize_redis_range(start, end, value.len()) else {
        return CommandReply::BulkString(Vec::new());
    };
    CommandReply::BulkString(value[start_index..=end_index].to_vec())
}

fn handle_setrange(db: DbIndex, frame: &CommandFrame, state: &mut DispatchState) -> CommandReply {
    let key = frame.args[0].clone();
    let Ok(offset_u64) = parse_redis_u64(&frame.args[1]) else {
        return CommandReply::Error("value is not an integer or out of range".to_owned());
    };
    let Ok(offset) = usize::try_from(offset_u64) else {
        return CommandReply::Error("value is not an integer or out of range".to_owned());
    };
    let payload = &frame.args[2];
    state.purge_expired_key(db, &key);

    let (mut value, expire_at_unix_secs) =
        if let Some(existing) = state.db_map(db).and_then(|map| map.get(&key)) {
            (existing.value.clone(), existing.expire_at_unix_secs)
        } else {
            (Vec::new(), None)
        };

    if payload.is_empty() {
        return CommandReply::Integer(i64::try_from(value.len()).unwrap_or(i64::MAX));
    }

    if offset > value.len() {
        value.resize(offset, 0_u8);
    }
    let needed_len = offset.saturating_add(payload.len());
    if needed_len > value.len() {
        value.resize(needed_len, 0_u8);
    }
    value[offset..offset + payload.len()].copy_from_slice(payload);

    state.db_map_mut(db).insert(
        key.clone(),
        ValueEntry {
            value: value.clone(),
            expire_at_unix_secs,
        },
    );
    state.bump_key_version(db, &key);
    CommandReply::Integer(i64::try_from(value.len()).unwrap_or(i64::MAX))
}

fn handle_del(db: DbIndex, frame: &CommandFrame, state: &mut DispatchState) -> CommandReply {
    let mut deleted = 0_i64;

    for key in &frame.args {
        state.purge_expired_key(db, key);
        if state.db_map_mut(db).remove(key).is_some() {
            state.bump_key_version(db, key);
            deleted = deleted.saturating_add(1);
        }
    }

    CommandReply::Integer(deleted)
}

fn handle_exists(db: DbIndex, frame: &CommandFrame, state: &mut DispatchState) -> CommandReply {
    let mut existing = 0_i64;

    for key in &frame.args {
        state.purge_expired_key(db, key);
        if state.db_map(db).is_some_and(|map| map.contains_key(key)) {
            existing = existing.saturating_add(1);
        }
    }

    CommandReply::Integer(existing)
}

fn handle_move(db: DbIndex, frame: &CommandFrame, state: &mut DispatchState) -> CommandReply {
    let key = frame.args[0].clone();
    state.purge_expired_key(db, &key);

    let Ok(target_db_i64) = parse_redis_i64(&frame.args[1]) else {
        return CommandReply::Error("value is not an integer or out of range".to_owned());
    };
    let Ok(target_db) = u16::try_from(target_db_i64) else {
        return CommandReply::Error("value is not an integer or out of range".to_owned());
    };

    state.purge_expired_key(target_db, &key);
    if !state.db_map(db).is_some_and(|map| map.contains_key(&key)) {
        return CommandReply::Integer(0);
    }
    if state
        .db_map(target_db)
        .is_some_and(|map| map.contains_key(&key))
    {
        return CommandReply::Integer(0);
    }

    let Some(entry) = state.db_map_mut(db).remove(&key) else {
        return CommandReply::Integer(0);
    };
    state.db_map_mut(target_db).insert(key.clone(), entry);
    state.bump_key_version(db, &key);
    state.bump_key_version(target_db, &key);
    CommandReply::Integer(1)
}

fn handle_copy(db: DbIndex, frame: &CommandFrame, state: &mut DispatchState) -> CommandReply {
    copy_single_state(db, &frame.args, state)
}

fn handle_rename(db: DbIndex, frame: &CommandFrame, state: &mut DispatchState) -> CommandReply {
    rename_single_state(db, &frame.args[0], &frame.args[1], false, state)
}

fn handle_renamenx(db: DbIndex, frame: &CommandFrame, state: &mut DispatchState) -> CommandReply {
    rename_single_state(db, &frame.args[0], &frame.args[1], true, state)
}

/// Moves one key between two different shard states.
///
/// This helper exists for cross-shard `RENAME/RENAMENX` in `CoreModule`, where source and
/// destination keys hash to different owners and therefore cannot be handled by one shard-local
/// command dispatch.
#[must_use]
pub fn rename_between_states(
    db: DbIndex,
    source_key: &[u8],
    destination_key: &[u8],
    destination_should_not_exist: bool,
    source_state: &mut DispatchState,
    destination_state: &mut DispatchState,
) -> CommandReply {
    source_state.purge_expired_key(db, source_key);
    destination_state.purge_expired_key(db, destination_key);

    if !source_state
        .db_map(db)
        .is_some_and(|map| map.contains_key(source_key))
    {
        return CommandReply::Error("no such key".to_owned());
    }
    if destination_should_not_exist
        && destination_state
            .db_map(db)
            .is_some_and(|map| map.contains_key(destination_key))
    {
        return CommandReply::Integer(0);
    }

    let Some(source_entry) = source_state.db_map_mut(db).remove(source_key) else {
        return CommandReply::Error("no such key".to_owned());
    };

    if !destination_should_not_exist {
        let _ = destination_state.db_map_mut(db).remove(destination_key);
    }
    destination_state
        .db_map_mut(db)
        .insert(destination_key.to_vec(), source_entry);

    source_state.bump_key_version(db, source_key);
    destination_state.bump_key_version(db, destination_key);

    if destination_should_not_exist {
        CommandReply::Integer(1)
    } else {
        CommandReply::SimpleString("OK".to_owned())
    }
}

/// Copies one key between two different shard states.
///
/// This helper exists for cross-shard `COPY` in `CoreModule`, where source and destination keys
/// hash to different owners and therefore cannot be handled by one shard-local command dispatch.
#[must_use]
pub fn copy_between_states(
    db: DbIndex,
    args: &[Vec<u8>],
    source_state: &mut DispatchState,
    destination_state: &mut DispatchState,
) -> CommandReply {
    let copy = match parse_copy_command(args) {
        Ok(copy) => copy,
        Err(reply) => return reply,
    };
    if copy.source_key == copy.destination_key {
        return CommandReply::Error("source and destination objects are the same".to_owned());
    }

    source_state.purge_expired_key(db, copy.source_key);
    destination_state.purge_expired_key(db, copy.destination_key);

    let Some(source_entry) = source_state
        .db_map(db)
        .and_then(|map| map.get(copy.source_key))
        .cloned()
    else {
        return CommandReply::Integer(0);
    };

    if !copy.replace
        && destination_state
            .db_map(db)
            .is_some_and(|map| map.contains_key(copy.destination_key))
    {
        return CommandReply::Integer(0);
    }

    destination_state
        .db_map_mut(db)
        .insert(copy.destination_key.to_vec(), source_entry);
    destination_state.bump_key_version(db, copy.destination_key);
    CommandReply::Integer(1)
}

fn copy_single_state(db: DbIndex, args: &[Vec<u8>], state: &mut DispatchState) -> CommandReply {
    let copy = match parse_copy_command(args) {
        Ok(copy) => copy,
        Err(reply) => return reply,
    };
    if copy.source_key == copy.destination_key {
        return CommandReply::Error("source and destination objects are the same".to_owned());
    }

    state.purge_expired_key(db, copy.source_key);
    state.purge_expired_key(db, copy.destination_key);

    let Some(source_entry) = state
        .db_map(db)
        .and_then(|map| map.get(copy.source_key))
        .cloned()
    else {
        return CommandReply::Integer(0);
    };

    if !copy.replace
        && state
            .db_map(db)
            .is_some_and(|map| map.contains_key(copy.destination_key))
    {
        return CommandReply::Integer(0);
    }

    state
        .db_map_mut(db)
        .insert(copy.destination_key.to_vec(), source_entry);
    state.bump_key_version(db, copy.destination_key);
    CommandReply::Integer(1)
}

fn rename_single_state(
    db: DbIndex,
    source_key: &[u8],
    destination_key: &[u8],
    destination_should_not_exist: bool,
    state: &mut DispatchState,
) -> CommandReply {
    state.purge_expired_key(db, source_key);
    state.purge_expired_key(db, destination_key);

    if !state
        .db_map(db)
        .is_some_and(|map| map.contains_key(source_key))
    {
        return CommandReply::Error("no such key".to_owned());
    }
    if source_key == destination_key {
        if destination_should_not_exist {
            return CommandReply::Integer(0);
        }
        return CommandReply::SimpleString("OK".to_owned());
    }
    if destination_should_not_exist
        && state
            .db_map(db)
            .is_some_and(|map| map.contains_key(destination_key))
    {
        return CommandReply::Integer(0);
    }

    let Some(source_entry) = state.db_map_mut(db).remove(source_key) else {
        return CommandReply::Error("no such key".to_owned());
    };

    if !destination_should_not_exist {
        let _ = state.db_map_mut(db).remove(destination_key);
    }
    state
        .db_map_mut(db)
        .insert(destination_key.to_vec(), source_entry);
    state.bump_key_version(db, source_key);
    state.bump_key_version(db, destination_key);

    if destination_should_not_exist {
        CommandReply::Integer(1)
    } else {
        CommandReply::SimpleString("OK".to_owned())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct CopyCommand<'a> {
    source_key: &'a [u8],
    destination_key: &'a [u8],
    replace: bool,
}

fn parse_copy_command(args: &[Vec<u8>]) -> Result<CopyCommand<'_>, CommandReply> {
    let Some(source_key) = args.first().map(Vec::as_slice) else {
        return Err(CommandReply::Error(
            "wrong number of arguments for 'COPY' command".to_owned(),
        ));
    };
    let Some(destination_key) = args.get(1).map(Vec::as_slice) else {
        return Err(CommandReply::Error(
            "wrong number of arguments for 'COPY' command".to_owned(),
        ));
    };

    let replace = match args.get(2) {
        None => false,
        Some(option) if option.eq_ignore_ascii_case(b"REPLACE") => true,
        Some(_) => return Err(CommandReply::Error("syntax error".to_owned())),
    };
    if args.len() > 3 {
        return Err(CommandReply::Error("syntax error".to_owned()));
    }

    Ok(CopyCommand {
        source_key,
        destination_key,
        replace,
    })
}

fn handle_setex(db: DbIndex, frame: &CommandFrame, state: &mut DispatchState) -> CommandReply {
    let key = frame.args[0].clone();
    let Ok(seconds) = parse_redis_i64(&frame.args[1]) else {
        return CommandReply::Error("value is not an integer or out of range".to_owned());
    };
    if seconds <= 0 {
        return CommandReply::Error("invalid expire time in 'SETEX' command".to_owned());
    }
    let Ok(expire_delta) = u64::try_from(seconds) else {
        return CommandReply::Error("value is not an integer or out of range".to_owned());
    };
    let expire_at = DispatchState::now_unix_seconds().saturating_add(expire_delta);

    state.db_map_mut(db).insert(
        key.clone(),
        ValueEntry {
            value: frame.args[2].clone(),
            expire_at_unix_secs: Some(expire_at),
        },
    );
    state.bump_key_version(db, &key);
    CommandReply::SimpleString("OK".to_owned())
}

fn handle_psetex(db: DbIndex, frame: &CommandFrame, state: &mut DispatchState) -> CommandReply {
    let key = frame.args[0].clone();
    let Ok(milliseconds) = parse_redis_i64(&frame.args[1]) else {
        return CommandReply::Error("value is not an integer or out of range".to_owned());
    };
    if milliseconds <= 0 {
        return CommandReply::Error("invalid expire time in 'PSETEX' command".to_owned());
    }
    let Ok(expire_delta_millis) = u64::try_from(milliseconds) else {
        return CommandReply::Error("value is not an integer or out of range".to_owned());
    };
    let now_millis = DispatchState::now_unix_millis();
    let expire_at_millis = now_millis.saturating_add(expire_delta_millis);
    let expire_at_secs = expire_at_millis.saturating_add(999) / 1000;

    state.db_map_mut(db).insert(
        key.clone(),
        ValueEntry {
            value: frame.args[2].clone(),
            expire_at_unix_secs: Some(expire_at_secs),
        },
    );
    state.bump_key_version(db, &key);
    CommandReply::SimpleString("OK".to_owned())
}

fn handle_expire(db: DbIndex, frame: &CommandFrame, state: &mut DispatchState) -> CommandReply {
    let key = &frame.args[0];
    state.purge_expired_key(db, key);

    let options = match parse_expire_options(&frame.args[2..]) {
        Ok(options) => options,
        Err(error) => return CommandReply::Error(error),
    };
    let seconds = match parse_i64_arg(&frame.args[1], "EXPIRE seconds") {
        Ok(seconds) => seconds,
        Err(error) => return CommandReply::Error(error),
    };
    let current_expire_at = state
        .db_map(db)
        .and_then(|map| map.get(key))
        .map(|entry| entry.expire_at_unix_secs);
    let Some(current_expire_at) = current_expire_at else {
        return CommandReply::Integer(0);
    };

    let now_secs = DispatchState::now_unix_seconds();
    let target_expire_at = if seconds <= 0 {
        now_secs.saturating_sub(1)
    } else {
        let Ok(expire_delta) = u64::try_from(seconds) else {
            return CommandReply::Error("EXPIRE seconds is out of range".to_owned());
        };
        now_secs.saturating_add(expire_delta)
    };
    if !expire_options_satisfied(options, current_expire_at, target_expire_at) {
        return CommandReply::Integer(0);
    }

    if seconds <= 0 {
        if state.db_map_mut(db).remove(key).is_some() {
            state.bump_key_version(db, key);
            return CommandReply::Integer(1);
        }
        return CommandReply::Integer(0);
    }

    let mut updated = false;
    if let Some(entry) = state.db_map_mut(db).get_mut(key) {
        entry.expire_at_unix_secs = Some(target_expire_at);
        updated = true;
    }
    if updated {
        state.bump_key_version(db, key);
        return CommandReply::Integer(1);
    }
    CommandReply::Integer(0)
}

fn handle_pexpire(db: DbIndex, frame: &CommandFrame, state: &mut DispatchState) -> CommandReply {
    let key = &frame.args[0];
    state.purge_expired_key(db, key);

    let options = match parse_expire_options(&frame.args[2..]) {
        Ok(options) => options,
        Err(error) => return CommandReply::Error(error),
    };
    let milliseconds = match parse_i64_arg(&frame.args[1], "PEXPIRE milliseconds") {
        Ok(milliseconds) => milliseconds,
        Err(error) => return CommandReply::Error(error),
    };
    let current_expire_at_secs = state
        .db_map(db)
        .and_then(|map| map.get(key))
        .map(|entry| entry.expire_at_unix_secs);
    let Some(current_expire_at_secs) = current_expire_at_secs else {
        return CommandReply::Integer(0);
    };

    let now_millis = DispatchState::now_unix_millis();
    let target_expire_at_millis = if milliseconds <= 0 {
        now_millis.saturating_sub(1)
    } else {
        let Ok(expire_delta_millis) = u64::try_from(milliseconds) else {
            return CommandReply::Error("PEXPIRE milliseconds is out of range".to_owned());
        };
        now_millis.saturating_add(expire_delta_millis)
    };
    let current_expire_at_millis = current_expire_at_secs
        .map(|expire_at_secs| expire_at_secs.saturating_mul(1000).saturating_sub(1));
    if !expire_options_satisfied(options, current_expire_at_millis, target_expire_at_millis) {
        return CommandReply::Integer(0);
    }

    if milliseconds <= 0 {
        if state.db_map_mut(db).remove(key).is_some() {
            state.bump_key_version(db, key);
            return CommandReply::Integer(1);
        }
        return CommandReply::Integer(0);
    }

    let expire_at_secs = target_expire_at_millis.saturating_add(999) / 1000;

    if let Some(entry) = state.db_map_mut(db).get_mut(key) {
        entry.expire_at_unix_secs = Some(expire_at_secs);
        state.bump_key_version(db, key);
        return CommandReply::Integer(1);
    }
    CommandReply::Integer(0)
}

fn handle_pexpireat(db: DbIndex, frame: &CommandFrame, state: &mut DispatchState) -> CommandReply {
    let key = &frame.args[0];
    state.purge_expired_key(db, key);

    let options = match parse_expire_options(&frame.args[2..]) {
        Ok(options) => options,
        Err(error) => return CommandReply::Error(error),
    };
    let timestamp_millis = match parse_i64_arg(&frame.args[1], "PEXPIREAT timestamp") {
        Ok(timestamp) => timestamp,
        Err(error) => return CommandReply::Error(error),
    };
    let current_expire_at_secs = state
        .db_map(db)
        .and_then(|map| map.get(key))
        .map(|entry| entry.expire_at_unix_secs);
    let Some(current_expire_at_secs) = current_expire_at_secs else {
        return CommandReply::Integer(0);
    };

    let timestamp_millis = timestamp_millis.max(0);
    let now_millis = i64::try_from(DispatchState::now_unix_millis()).unwrap_or(i64::MAX);
    let Ok(target_expire_at_millis) = u64::try_from(timestamp_millis) else {
        return CommandReply::Error("PEXPIREAT timestamp is out of range".to_owned());
    };
    let current_expire_at_millis = current_expire_at_secs
        .map(|expire_at_secs| expire_at_secs.saturating_mul(1000).saturating_sub(1));
    if !expire_options_satisfied(options, current_expire_at_millis, target_expire_at_millis) {
        return CommandReply::Integer(0);
    }

    if i64::try_from(target_expire_at_millis).unwrap_or(i64::MAX) <= now_millis {
        if state.db_map_mut(db).remove(key).is_some() {
            state.bump_key_version(db, key);
            return CommandReply::Integer(1);
        }
        return CommandReply::Integer(0);
    }

    let expire_at_secs = target_expire_at_millis.saturating_add(999) / 1000;
    if let Some(entry) = state.db_map_mut(db).get_mut(key) {
        entry.expire_at_unix_secs = Some(expire_at_secs);
        state.bump_key_version(db, key);
        return CommandReply::Integer(1);
    }
    CommandReply::Integer(0)
}

fn handle_expireat(db: DbIndex, frame: &CommandFrame, state: &mut DispatchState) -> CommandReply {
    let key = &frame.args[0];
    state.purge_expired_key(db, key);

    let options = match parse_expire_options(&frame.args[2..]) {
        Ok(options) => options,
        Err(error) => return CommandReply::Error(error),
    };
    let timestamp = match parse_i64_arg(&frame.args[1], "EXPIREAT timestamp") {
        Ok(timestamp) => timestamp,
        Err(error) => return CommandReply::Error(error),
    };
    let current_expire_at = state
        .db_map(db)
        .and_then(|map| map.get(key))
        .map(|entry| entry.expire_at_unix_secs);
    let Some(current_expire_at) = current_expire_at else {
        return CommandReply::Integer(0);
    };

    let timestamp = timestamp.max(0);
    let now = i64::try_from(DispatchState::now_unix_seconds()).unwrap_or(i64::MAX);
    let Ok(target_expire_at) = u64::try_from(timestamp) else {
        return CommandReply::Error("EXPIREAT timestamp is out of range".to_owned());
    };
    if !expire_options_satisfied(options, current_expire_at, target_expire_at) {
        return CommandReply::Integer(0);
    }

    if i64::try_from(target_expire_at).unwrap_or(i64::MAX) <= now {
        if state.db_map_mut(db).remove(key).is_some() {
            state.bump_key_version(db, key);
            return CommandReply::Integer(1);
        }
        return CommandReply::Integer(0);
    }

    if let Some(entry) = state.db_map_mut(db).get_mut(key) {
        entry.expire_at_unix_secs = Some(target_expire_at);
        state.bump_key_version(db, key);
        return CommandReply::Integer(1);
    }
    CommandReply::Integer(0)
}

fn handle_ttl(db: DbIndex, frame: &CommandFrame, state: &mut DispatchState) -> CommandReply {
    let key = &frame.args[0];
    state.purge_expired_key(db, key);

    let Some(expire_at) = state
        .db_map(db)
        .and_then(|map| map.get(key))
        .and_then(|entry| entry.expire_at_unix_secs)
    else {
        if state.db_map(db).is_some_and(|map| map.contains_key(key)) {
            return CommandReply::Integer(-1);
        }
        return CommandReply::Integer(-2);
    };

    let now = DispatchState::now_unix_seconds();
    if expire_at <= now {
        let _ = state.db_map_mut(db).remove(key);
        return CommandReply::Integer(-2);
    }
    let remaining = expire_at.saturating_sub(now);
    match i64::try_from(remaining) {
        Ok(remaining) => CommandReply::Integer(remaining),
        Err(_) => CommandReply::Integer(i64::MAX),
    }
}

fn handle_pttl(db: DbIndex, frame: &CommandFrame, state: &mut DispatchState) -> CommandReply {
    let key = &frame.args[0];
    state.purge_expired_key(db, key);

    let Some(expire_at_secs) = state
        .db_map(db)
        .and_then(|map| map.get(key))
        .and_then(|entry| entry.expire_at_unix_secs)
    else {
        if state.db_map(db).is_some_and(|map| map.contains_key(key)) {
            return CommandReply::Integer(-1);
        }
        return CommandReply::Integer(-2);
    };

    let now_millis = DispatchState::now_unix_millis();
    let expire_at_millis = expire_at_secs.saturating_mul(1000);
    if expire_at_millis <= now_millis {
        let _ = state.db_map_mut(db).remove(key);
        return CommandReply::Integer(-2);
    }
    let remaining = expire_at_millis.saturating_sub(now_millis);
    match i64::try_from(remaining) {
        Ok(remaining) => CommandReply::Integer(remaining),
        Err(_) => CommandReply::Integer(i64::MAX),
    }
}

fn handle_expiretime(db: DbIndex, frame: &CommandFrame, state: &mut DispatchState) -> CommandReply {
    let key = &frame.args[0];
    state.purge_expired_key(db, key);

    let Some(expire_at) = state
        .db_map(db)
        .and_then(|map| map.get(key))
        .and_then(|entry| entry.expire_at_unix_secs)
    else {
        if state.db_map(db).is_some_and(|map| map.contains_key(key)) {
            return CommandReply::Integer(-1);
        }
        return CommandReply::Integer(-2);
    };

    match i64::try_from(expire_at) {
        Ok(expire_at) => CommandReply::Integer(expire_at),
        Err(_) => CommandReply::Integer(i64::MAX),
    }
}

fn handle_pexpiretime(
    db: DbIndex,
    frame: &CommandFrame,
    state: &mut DispatchState,
) -> CommandReply {
    let key = &frame.args[0];
    state.purge_expired_key(db, key);

    let Some(expire_at_secs) = state
        .db_map(db)
        .and_then(|map| map.get(key))
        .and_then(|entry| entry.expire_at_unix_secs)
    else {
        if state.db_map(db).is_some_and(|map| map.contains_key(key)) {
            return CommandReply::Integer(-1);
        }
        return CommandReply::Integer(-2);
    };

    let expire_at_millis = expire_at_secs.saturating_mul(1000);
    match i64::try_from(expire_at_millis) {
        Ok(expire_at) => CommandReply::Integer(expire_at),
        Err(_) => CommandReply::Integer(i64::MAX),
    }
}

fn handle_persist(db: DbIndex, frame: &CommandFrame, state: &mut DispatchState) -> CommandReply {
    let key = &frame.args[0];
    state.purge_expired_key(db, key);

    let mut changed = false;
    if let Some(entry) = state.db_map_mut(db).get_mut(key)
        && entry.expire_at_unix_secs.is_some()
    {
        entry.expire_at_unix_secs = None;
        changed = true;
    }

    if changed {
        state.bump_key_version(db, key);
        return CommandReply::Integer(1);
    }
    CommandReply::Integer(0)
}

fn handle_incr(db: DbIndex, frame: &CommandFrame, state: &mut DispatchState) -> CommandReply {
    mutate_counter_by(db, &frame.args[0], 1, state)
}

fn handle_decr(db: DbIndex, frame: &CommandFrame, state: &mut DispatchState) -> CommandReply {
    mutate_counter_by(db, &frame.args[0], -1, state)
}

fn handle_incrby(db: DbIndex, frame: &CommandFrame, state: &mut DispatchState) -> CommandReply {
    let Ok(delta) = parse_redis_i64(&frame.args[1]) else {
        return CommandReply::Error("value is not an integer or out of range".to_owned());
    };
    mutate_counter_by(db, &frame.args[0], delta, state)
}

fn handle_decrby(db: DbIndex, frame: &CommandFrame, state: &mut DispatchState) -> CommandReply {
    let Ok(amount) = parse_redis_i64(&frame.args[1]) else {
        return CommandReply::Error("value is not an integer or out of range".to_owned());
    };
    let Some(delta) = amount.checked_neg() else {
        return CommandReply::Error("value is not an integer or out of range".to_owned());
    };
    mutate_counter_by(db, &frame.args[0], delta, state)
}

/// Applies one signed integer delta to a key using Redis-compatible counter semantics.
///
/// Missing keys are treated as zero and created. Existing TTL metadata is preserved.
fn mutate_counter_by(
    db: DbIndex,
    key: &[u8],
    delta: i64,
    state: &mut DispatchState,
) -> CommandReply {
    state.purge_expired_key(db, key);

    let (current, expire_at_unix_secs) =
        if let Some(existing) = state.db_map(db).and_then(|map| map.get(key)) {
            let Ok(current) = parse_redis_i64(&existing.value) else {
                return CommandReply::Error("value is not an integer or out of range".to_owned());
            };
            (current, existing.expire_at_unix_secs)
        } else {
            (0_i64, None)
        };

    let Some(next) = current.checked_add(delta) else {
        return CommandReply::Error("value is not an integer or out of range".to_owned());
    };

    state.db_map_mut(db).insert(
        key.to_vec(),
        ValueEntry {
            value: next.to_string().into_bytes(),
            expire_at_unix_secs,
        },
    );
    state.bump_key_version(db, key);
    CommandReply::Integer(next)
}

fn parse_set_options(args: &[Vec<u8>]) -> Result<SetOptions, String> {
    let mut options = SetOptions::default();
    let mut index = 0_usize;

    while let Some(arg) = args.get(index) {
        if arg.eq_ignore_ascii_case(b"NX") {
            if options.condition == SetCondition::IfExists {
                return Err("syntax error".to_owned());
            }
            options.condition = SetCondition::IfMissing;
            index = index.saturating_add(1);
            continue;
        }
        if arg.eq_ignore_ascii_case(b"XX") {
            if options.condition == SetCondition::IfMissing {
                return Err("syntax error".to_owned());
            }
            options.condition = SetCondition::IfExists;
            index = index.saturating_add(1);
            continue;
        }
        if arg.eq_ignore_ascii_case(b"GET") {
            options.return_previous = true;
            index = index.saturating_add(1);
            continue;
        }
        if arg.eq_ignore_ascii_case(b"KEEPTTL") {
            options.keep_ttl = true;
            index = index.saturating_add(1);
            continue;
        }
        if arg.eq_ignore_ascii_case(b"EX") || arg.eq_ignore_ascii_case(b"PX") {
            if options.expire.is_some() {
                return Err("syntax error".to_owned());
            }
            let Some(raw_expire) = args.get(index.saturating_add(1)) else {
                return Err("syntax error".to_owned());
            };
            let Ok(expire) = parse_redis_i64(raw_expire) else {
                return Err("value is not an integer or out of range".to_owned());
            };
            if expire <= 0 {
                return Err("invalid expire time in 'SET' command".to_owned());
            }
            let Ok(expire) = u64::try_from(expire) else {
                return Err("value is not an integer or out of range".to_owned());
            };

            options.expire = if arg.eq_ignore_ascii_case(b"EX") {
                Some(SetExpire::Seconds(expire))
            } else {
                Some(SetExpire::Milliseconds(expire))
            };
            index = index.saturating_add(2);
            continue;
        }

        return Err("syntax error".to_owned());
    }

    if options.keep_ttl && options.expire.is_some() {
        return Err("syntax error".to_owned());
    }
    Ok(options)
}

fn set_condition_satisfied(condition: SetCondition, key_exists: bool) -> bool {
    match condition {
        SetCondition::Always => true,
        SetCondition::IfMissing => !key_exists,
        SetCondition::IfExists => key_exists,
    }
}

fn resolve_set_expire_at_unix_secs(expire: SetExpire) -> u64 {
    match expire {
        SetExpire::Seconds(seconds) => DispatchState::now_unix_seconds().saturating_add(seconds),
        SetExpire::Milliseconds(milliseconds) => {
            DispatchState::now_unix_millis()
                .saturating_add(milliseconds)
                .saturating_add(999)
                / 1000
        }
    }
}

type ExpireOptions = u8;

const EXPIRE_ALWAYS: ExpireOptions = 0;
const EXPIRE_NX: ExpireOptions = 1 << 0;
const EXPIRE_XX: ExpireOptions = 1 << 1;
const EXPIRE_GT: ExpireOptions = 1 << 2;
const EXPIRE_LT: ExpireOptions = 1 << 3;

fn parse_expire_options(args: &[Vec<u8>]) -> Result<ExpireOptions, String> {
    let mut options = EXPIRE_ALWAYS;

    for arg in args {
        if arg.eq_ignore_ascii_case(b"NX") {
            options |= EXPIRE_NX;
        } else if arg.eq_ignore_ascii_case(b"XX") {
            options |= EXPIRE_XX;
        } else if arg.eq_ignore_ascii_case(b"GT") {
            options |= EXPIRE_GT;
        } else if arg.eq_ignore_ascii_case(b"LT") {
            options |= EXPIRE_LT;
        } else {
            let option = String::from_utf8_lossy(arg).to_ascii_uppercase();
            return Err(format!("Unsupported option: {option}"));
        }
    }

    if (options & EXPIRE_NX != 0) && (options & EXPIRE_XX != 0) {
        return Err("NX and XX options at the same time are not compatible".to_owned());
    }
    if (options & EXPIRE_GT != 0) && (options & EXPIRE_LT != 0) {
        return Err("GT and LT options at the same time are not compatible".to_owned());
    }

    Ok(options)
}

/// Evaluates EXPIRE-option predicates using Dragonfly's update semantics.
///
/// When no current expiry exists, comparisons treat it as an infinite timestamp.
fn expire_options_satisfied(
    options: ExpireOptions,
    current_expire_at: Option<u64>,
    next_expire_at: u64,
) -> bool {
    if options == EXPIRE_ALWAYS {
        return true;
    }

    let mut satisfied = false;
    let current_cmp = current_expire_at.unwrap_or(u64::MAX);
    if current_expire_at.is_some() {
        satisfied |= options & EXPIRE_XX != 0;
    } else {
        satisfied |= options & EXPIRE_NX != 0;
    }
    satisfied |= options & EXPIRE_LT != 0 && next_expire_at < current_cmp;
    satisfied |= options & EXPIRE_GT != 0 && next_expire_at > current_cmp;
    satisfied
}

fn parse_i64_arg(arg: &[u8], field_name: &str) -> Result<i64, String> {
    let text = str::from_utf8(arg).map_err(|_| format!("{field_name} must be valid UTF-8"))?;
    text.parse::<i64>()
        .map_err(|_| format!("{field_name} must be an integer"))
}

fn parse_redis_i64(payload: &[u8]) -> Result<i64, ()> {
    let Ok(text) = str::from_utf8(payload) else {
        return Err(());
    };
    text.parse::<i64>().map_err(|_| ())
}

fn parse_redis_u64(payload: &[u8]) -> Result<u64, ()> {
    let Ok(text) = str::from_utf8(payload) else {
        return Err(());
    };
    text.parse::<u64>().map_err(|_| ())
}

fn normalize_redis_range(start: i64, end: i64, len: usize) -> Option<(usize, usize)> {
    if len == 0 {
        return None;
    }

    let len_i64 = i64::try_from(len).unwrap_or(i64::MAX);
    let mut start = if start < 0 {
        len_i64.saturating_add(start)
    } else {
        start
    };
    let mut end = if end < 0 {
        len_i64.saturating_add(end)
    } else {
        end
    };

    if start < 0 {
        start = 0;
    }
    if end < 0 {
        return None;
    }
    if start >= len_i64 {
        return None;
    }
    if end >= len_i64 {
        end = len_i64.saturating_sub(1);
    }
    if start > end {
        return None;
    }

    let Ok(start_index) = usize::try_from(start) else {
        return None;
    };
    let Ok(end_index) = usize::try_from(end) else {
        return None;
    };
    Some((start_index, end_index))
}

#[cfg(test)]
mod tests {
    use super::{CommandRegistry, DispatchState};
    use crate::command::{CommandFrame, CommandReply};
    use dfly_cluster::slot::key_slot;
    use googletest::prelude::*;
    use rstest::rstest;

    #[rstest]
    fn dispatch_rebuilds_expire_index_for_layered_db_table() {
        let registry = CommandRegistry::with_builtin_commands();
        let mut state = DispatchState::default();
        let key = b"layered:expire:key".to_vec();

        let set = registry.dispatch(
            0,
            &CommandFrame::new(
                "SET",
                vec![
                    key.clone(),
                    b"value".to_vec(),
                    b"EX".to_vec(),
                    b"30".to_vec(),
                ],
            ),
            &mut state,
        );
        assert_that!(&set, eq(&CommandReply::SimpleString("OK".to_owned())));

        let Some(table) = state.db_tables.get(&0) else {
            panic!("db table must exist after SET");
        };
        assert_that!(table.expire.contains_key(&key), eq(true));

        let _ = registry.dispatch(0, &CommandFrame::new("DEL", vec![key.clone()]), &mut state);
        let Some(table) = state.db_tables.get(&0) else {
            panic!("db table should be kept after key deletion");
        };
        assert_that!(table.expire.contains_key(&key), eq(false));
    }

    #[rstest]
    fn dispatch_rebuilds_slot_stats_for_layered_db_table() {
        let registry = CommandRegistry::with_builtin_commands();
        let mut state = DispatchState::default();
        let key = b"layered:slot:key".to_vec();
        let slot = key_slot(&key);

        let _ = registry.dispatch(
            0,
            &CommandFrame::new("SET", vec![key.clone(), b"value".to_vec()]),
            &mut state,
        );
        let Some(table) = state.db_tables.get(&0) else {
            panic!("db table must exist after SET");
        };
        assert_that!(table.slot_stats.get(&slot).copied(), eq(Some(1_usize)));

        let _ = registry.dispatch(0, &CommandFrame::new("DEL", vec![key.clone()]), &mut state);
        let Some(table) = state.db_tables.get(&0) else {
            panic!("db table should be kept after key deletion");
        };
        assert_that!(table.slot_stats.get(&slot).copied(), eq(None));
    }

    #[rstest]
    fn dispatch_ping_and_echo() {
        let registry = CommandRegistry::with_builtin_commands();
        let mut state = DispatchState::default();

        let ping = registry.dispatch(0, &CommandFrame::new("PING", Vec::new()), &mut state);
        assert_that!(&ping, eq(&CommandReply::SimpleString("PONG".to_owned())));

        let echo = registry.dispatch(
            0,
            &CommandFrame::new("ECHO", vec![b"hello".to_vec()]),
            &mut state,
        );
        assert_that!(&echo, eq(&CommandReply::BulkString(b"hello".to_vec())));
    }

    #[rstest]
    fn dispatch_set_then_get_roundtrip() {
        let registry = CommandRegistry::with_builtin_commands();
        let mut state = DispatchState::default();

        let set_reply = registry.dispatch(
            0,
            &CommandFrame::new("SET", vec![b"user:1".to_vec(), b"alice".to_vec()]),
            &mut state,
        );
        assert_that!(&set_reply, eq(&CommandReply::SimpleString("OK".to_owned())));

        let get_reply = registry.dispatch(
            0,
            &CommandFrame::new("GET", vec![b"user:1".to_vec()]),
            &mut state,
        );
        assert_that!(&get_reply, eq(&CommandReply::BulkString(b"alice".to_vec())));
    }

    #[rstest]
    fn dispatch_set_supports_conditional_and_get_options() {
        let registry = CommandRegistry::with_builtin_commands();
        let mut state = DispatchState::default();

        let first = registry.dispatch(
            0,
            &CommandFrame::new("SET", vec![b"k".to_vec(), b"v1".to_vec(), b"NX".to_vec()]),
            &mut state,
        );
        assert_that!(&first, eq(&CommandReply::SimpleString("OK".to_owned())));

        let skipped = registry.dispatch(
            0,
            &CommandFrame::new(
                "SET",
                vec![b"k".to_vec(), b"ignored".to_vec(), b"nx".to_vec()],
            ),
            &mut state,
        );
        assert_that!(&skipped, eq(&CommandReply::Null));

        let skipped_with_get = registry.dispatch(
            0,
            &CommandFrame::new(
                "SET",
                vec![
                    b"k".to_vec(),
                    b"ignored".to_vec(),
                    b"NX".to_vec(),
                    b"GET".to_vec(),
                ],
            ),
            &mut state,
        );
        assert_that!(
            &skipped_with_get,
            eq(&CommandReply::BulkString(b"v1".to_vec()))
        );

        let xx_missing = registry.dispatch(
            0,
            &CommandFrame::new(
                "SET",
                vec![
                    b"missing".to_vec(),
                    b"value".to_vec(),
                    b"XX".to_vec(),
                    b"GET".to_vec(),
                ],
            ),
            &mut state,
        );
        assert_that!(&xx_missing, eq(&CommandReply::Null));

        let xx_existing = registry.dispatch(
            0,
            &CommandFrame::new(
                "SET",
                vec![
                    b"k".to_vec(),
                    b"v2".to_vec(),
                    b"xx".to_vec(),
                    b"get".to_vec(),
                ],
            ),
            &mut state,
        );
        assert_that!(&xx_existing, eq(&CommandReply::BulkString(b"v1".to_vec())));

        let current = registry.dispatch(
            0,
            &CommandFrame::new("GET", vec![b"k".to_vec()]),
            &mut state,
        );
        assert_that!(&current, eq(&CommandReply::BulkString(b"v2".to_vec())));
    }

    #[rstest]
    fn dispatch_set_applies_expire_and_keepttl_options() {
        let registry = CommandRegistry::with_builtin_commands();
        let mut state = DispatchState::default();

        let with_ex = registry.dispatch(
            0,
            &CommandFrame::new(
                "SET",
                vec![
                    b"ttl:key".to_vec(),
                    b"v1".to_vec(),
                    b"EX".to_vec(),
                    b"60".to_vec(),
                ],
            ),
            &mut state,
        );
        assert_that!(&with_ex, eq(&CommandReply::SimpleString("OK".to_owned())));

        let first_ttl = registry.dispatch(
            0,
            &CommandFrame::new("TTL", vec![b"ttl:key".to_vec()]),
            &mut state,
        );
        let CommandReply::Integer(first_ttl) = first_ttl else {
            panic!("TTL must return integer");
        };
        assert_that!(first_ttl > 0, eq(true));

        let keep_ttl = registry.dispatch(
            0,
            &CommandFrame::new(
                "SET",
                vec![b"ttl:key".to_vec(), b"v2".to_vec(), b"KEEPTTL".to_vec()],
            ),
            &mut state,
        );
        assert_that!(&keep_ttl, eq(&CommandReply::SimpleString("OK".to_owned())));

        let ttl_after_keep = registry.dispatch(
            0,
            &CommandFrame::new("TTL", vec![b"ttl:key".to_vec()]),
            &mut state,
        );
        let CommandReply::Integer(ttl_after_keep) = ttl_after_keep else {
            panic!("TTL must return integer");
        };
        assert_that!(ttl_after_keep > 0, eq(true));

        let clear_ttl = registry.dispatch(
            0,
            &CommandFrame::new("SET", vec![b"ttl:key".to_vec(), b"v3".to_vec()]),
            &mut state,
        );
        assert_that!(&clear_ttl, eq(&CommandReply::SimpleString("OK".to_owned())));

        let ttl_after_plain = registry.dispatch(
            0,
            &CommandFrame::new("TTL", vec![b"ttl:key".to_vec()]),
            &mut state,
        );
        assert_that!(&ttl_after_plain, eq(&CommandReply::Integer(-1)));

        let with_px = registry.dispatch(
            0,
            &CommandFrame::new(
                "SET",
                vec![
                    b"px:key".to_vec(),
                    b"value".to_vec(),
                    b"PX".to_vec(),
                    b"1200".to_vec(),
                ],
            ),
            &mut state,
        );
        assert_that!(&with_px, eq(&CommandReply::SimpleString("OK".to_owned())));

        let pttl = registry.dispatch(
            0,
            &CommandFrame::new("PTTL", vec![b"px:key".to_vec()]),
            &mut state,
        );
        let CommandReply::Integer(pttl) = pttl else {
            panic!("PTTL must return integer");
        };
        assert_that!(pttl > 0, eq(true));
    }

    #[rstest]
    fn dispatch_set_rejects_invalid_option_combinations() {
        let registry = CommandRegistry::with_builtin_commands();
        let mut state = DispatchState::default();

        let nx_xx = registry.dispatch(
            0,
            &CommandFrame::new(
                "SET",
                vec![
                    b"key".to_vec(),
                    b"value".to_vec(),
                    b"NX".to_vec(),
                    b"XX".to_vec(),
                ],
            ),
            &mut state,
        );
        assert_that!(&nx_xx, eq(&CommandReply::Error("syntax error".to_owned())));

        let duplicate_expire = registry.dispatch(
            0,
            &CommandFrame::new(
                "SET",
                vec![
                    b"key".to_vec(),
                    b"value".to_vec(),
                    b"EX".to_vec(),
                    b"1".to_vec(),
                    b"PX".to_vec(),
                    b"1000".to_vec(),
                ],
            ),
            &mut state,
        );
        assert_that!(
            &duplicate_expire,
            eq(&CommandReply::Error("syntax error".to_owned()))
        );

        let keepttl_with_expire = registry.dispatch(
            0,
            &CommandFrame::new(
                "SET",
                vec![
                    b"key".to_vec(),
                    b"value".to_vec(),
                    b"EX".to_vec(),
                    b"5".to_vec(),
                    b"KEEPTTL".to_vec(),
                ],
            ),
            &mut state,
        );
        assert_that!(
            &keepttl_with_expire,
            eq(&CommandReply::Error("syntax error".to_owned()))
        );

        let missing_expire_value = registry.dispatch(
            0,
            &CommandFrame::new(
                "SET",
                vec![b"key".to_vec(), b"value".to_vec(), b"EX".to_vec()],
            ),
            &mut state,
        );
        assert_that!(
            &missing_expire_value,
            eq(&CommandReply::Error("syntax error".to_owned()))
        );

        let invalid_expire_integer = registry.dispatch(
            0,
            &CommandFrame::new(
                "SET",
                vec![
                    b"key".to_vec(),
                    b"value".to_vec(),
                    b"EX".to_vec(),
                    b"abc".to_vec(),
                ],
            ),
            &mut state,
        );
        assert_that!(
            &invalid_expire_integer,
            eq(&CommandReply::Error(
                "value is not an integer or out of range".to_owned()
            ))
        );

        let invalid_expire_time = registry.dispatch(
            0,
            &CommandFrame::new(
                "SET",
                vec![
                    b"key".to_vec(),
                    b"value".to_vec(),
                    b"PX".to_vec(),
                    b"0".to_vec(),
                ],
            ),
            &mut state,
        );
        assert_that!(
            &invalid_expire_time,
            eq(&CommandReply::Error(
                "invalid expire time in 'SET' command".to_owned()
            ))
        );

        let unknown_option = registry.dispatch(
            0,
            &CommandFrame::new(
                "SET",
                vec![b"key".to_vec(), b"value".to_vec(), b"SOMETHING".to_vec()],
            ),
            &mut state,
        );
        assert_that!(
            &unknown_option,
            eq(&CommandReply::Error("syntax error".to_owned()))
        );
    }

    #[rstest]
    fn dispatch_type_reports_none_or_string() {
        let registry = CommandRegistry::with_builtin_commands();
        let mut state = DispatchState::default();

        let missing = registry.dispatch(
            0,
            &CommandFrame::new("TYPE", vec![b"nope".to_vec()]),
            &mut state,
        );
        assert_that!(&missing, eq(&CommandReply::SimpleString("none".to_owned())));

        let _ = registry.dispatch(
            0,
            &CommandFrame::new("SET", vec![b"k".to_vec(), b"v".to_vec()]),
            &mut state,
        );
        let existing = registry.dispatch(
            0,
            &CommandFrame::new("TYPE", vec![b"k".to_vec()]),
            &mut state,
        );
        assert_that!(
            &existing,
            eq(&CommandReply::SimpleString("string".to_owned()))
        );
    }

    #[rstest]
    fn dispatch_setnx_sets_only_when_key_is_missing() {
        let registry = CommandRegistry::with_builtin_commands();
        let mut state = DispatchState::default();

        let first = registry.dispatch(
            0,
            &CommandFrame::new("SETNX", vec![b"k".to_vec(), b"v1".to_vec()]),
            &mut state,
        );
        assert_that!(&first, eq(&CommandReply::Integer(1)));

        let second = registry.dispatch(
            0,
            &CommandFrame::new("SETNX", vec![b"k".to_vec(), b"v2".to_vec()]),
            &mut state,
        );
        assert_that!(&second, eq(&CommandReply::Integer(0)));

        let value = registry.dispatch(
            0,
            &CommandFrame::new("GET", vec![b"k".to_vec()]),
            &mut state,
        );
        assert_that!(&value, eq(&CommandReply::BulkString(b"v1".to_vec())));
    }

    #[rstest]
    fn dispatch_getset_returns_previous_value_and_overwrites_key() {
        let registry = CommandRegistry::with_builtin_commands();
        let mut state = DispatchState::default();

        let first = registry.dispatch(
            0,
            &CommandFrame::new("GETSET", vec![b"k".to_vec(), b"v1".to_vec()]),
            &mut state,
        );
        assert_that!(&first, eq(&CommandReply::Null));

        let second = registry.dispatch(
            0,
            &CommandFrame::new("GETSET", vec![b"k".to_vec(), b"v2".to_vec()]),
            &mut state,
        );
        assert_that!(&second, eq(&CommandReply::BulkString(b"v1".to_vec())));

        let value = registry.dispatch(
            0,
            &CommandFrame::new("GET", vec![b"k".to_vec()]),
            &mut state,
        );
        assert_that!(&value, eq(&CommandReply::BulkString(b"v2".to_vec())));
    }

    #[rstest]
    fn dispatch_getdel_returns_value_and_removes_key() {
        let registry = CommandRegistry::with_builtin_commands();
        let mut state = DispatchState::default();

        let _ = registry.dispatch(
            0,
            &CommandFrame::new("SET", vec![b"k".to_vec(), b"v".to_vec()]),
            &mut state,
        );
        let removed = registry.dispatch(
            0,
            &CommandFrame::new("GETDEL", vec![b"k".to_vec()]),
            &mut state,
        );
        assert_that!(&removed, eq(&CommandReply::BulkString(b"v".to_vec())));

        let missing = registry.dispatch(
            0,
            &CommandFrame::new("GETDEL", vec![b"k".to_vec()]),
            &mut state,
        );
        assert_that!(&missing, eq(&CommandReply::Null));
    }

    #[rstest]
    fn dispatch_append_and_strlen_manage_string_length() {
        let registry = CommandRegistry::with_builtin_commands();
        let mut state = DispatchState::default();

        let first = registry.dispatch(
            0,
            &CommandFrame::new("APPEND", vec![b"k".to_vec(), b"he".to_vec()]),
            &mut state,
        );
        assert_that!(&first, eq(&CommandReply::Integer(2)));

        let second = registry.dispatch(
            0,
            &CommandFrame::new("APPEND", vec![b"k".to_vec(), b"llo".to_vec()]),
            &mut state,
        );
        assert_that!(&second, eq(&CommandReply::Integer(5)));

        let strlen = registry.dispatch(
            0,
            &CommandFrame::new("STRLEN", vec![b"k".to_vec()]),
            &mut state,
        );
        assert_that!(&strlen, eq(&CommandReply::Integer(5)));

        let value = registry.dispatch(
            0,
            &CommandFrame::new("GET", vec![b"k".to_vec()]),
            &mut state,
        );
        assert_that!(&value, eq(&CommandReply::BulkString(b"hello".to_vec())));
    }

    #[rstest]
    fn dispatch_getrange_returns_slice_with_redis_index_rules() {
        let registry = CommandRegistry::with_builtin_commands();
        let mut state = DispatchState::default();

        let _ = registry.dispatch(
            0,
            &CommandFrame::new("SET", vec![b"k".to_vec(), b"hello".to_vec()]),
            &mut state,
        );

        let middle = registry.dispatch(
            0,
            &CommandFrame::new(
                "GETRANGE",
                vec![b"k".to_vec(), b"1".to_vec(), b"3".to_vec()],
            ),
            &mut state,
        );
        assert_that!(&middle, eq(&CommandReply::BulkString(b"ell".to_vec())));

        let tail = registry.dispatch(
            0,
            &CommandFrame::new(
                "GETRANGE",
                vec![b"k".to_vec(), b"-2".to_vec(), b"-1".to_vec()],
            ),
            &mut state,
        );
        assert_that!(&tail, eq(&CommandReply::BulkString(b"lo".to_vec())));
    }

    #[rstest]
    fn dispatch_setrange_updates_offset_and_zero_pads_missing_bytes() {
        let registry = CommandRegistry::with_builtin_commands();
        let mut state = DispatchState::default();

        let _ = registry.dispatch(
            0,
            &CommandFrame::new("SET", vec![b"k".to_vec(), b"hello".to_vec()]),
            &mut state,
        );
        let updated = registry.dispatch(
            0,
            &CommandFrame::new(
                "SETRANGE",
                vec![b"k".to_vec(), b"1".to_vec(), b"i".to_vec()],
            ),
            &mut state,
        );
        assert_that!(&updated, eq(&CommandReply::Integer(5)));

        let _ = registry.dispatch(
            0,
            &CommandFrame::new(
                "SETRANGE",
                vec![b"k2".to_vec(), b"3".to_vec(), b"ab".to_vec()],
            ),
            &mut state,
        );

        let key1 = registry.dispatch(
            0,
            &CommandFrame::new("GET", vec![b"k".to_vec()]),
            &mut state,
        );
        assert_that!(&key1, eq(&CommandReply::BulkString(b"hillo".to_vec())));

        let key2 = registry.dispatch(
            0,
            &CommandFrame::new("GET", vec![b"k2".to_vec()]),
            &mut state,
        );
        assert_that!(
            &key2,
            eq(&CommandReply::BulkString(vec![0, 0, 0, b'a', b'b']))
        );
    }

    #[rstest]
    fn dispatch_del_and_exists_follow_redis_counting_semantics() {
        let registry = CommandRegistry::with_builtin_commands();
        let mut state = DispatchState::default();

        let _ = registry.dispatch(
            0,
            &CommandFrame::new("SET", vec![b"k1".to_vec(), b"v1".to_vec()]),
            &mut state,
        );
        let _ = registry.dispatch(
            0,
            &CommandFrame::new("SET", vec![b"k2".to_vec(), b"v2".to_vec()]),
            &mut state,
        );

        let exists = registry.dispatch(
            0,
            &CommandFrame::new(
                "EXISTS",
                vec![
                    b"k1".to_vec(),
                    b"k2".to_vec(),
                    b"missing".to_vec(),
                    b"k1".to_vec(),
                ],
            ),
            &mut state,
        );
        assert_that!(&exists, eq(&CommandReply::Integer(3)));

        let deleted = registry.dispatch(
            0,
            &CommandFrame::new(
                "DEL",
                vec![
                    b"k1".to_vec(),
                    b"k2".to_vec(),
                    b"missing".to_vec(),
                    b"k1".to_vec(),
                ],
            ),
            &mut state,
        );
        assert_that!(&deleted, eq(&CommandReply::Integer(2)));

        let exists_after_delete = registry.dispatch(
            0,
            &CommandFrame::new("EXISTS", vec![b"k1".to_vec(), b"k2".to_vec()]),
            &mut state,
        );
        assert_that!(&exists_after_delete, eq(&CommandReply::Integer(0)));
    }

    #[rstest]
    fn dispatch_unlink_removes_keys_with_del_counting_semantics() {
        let registry = CommandRegistry::with_builtin_commands();
        let mut state = DispatchState::default();

        let _ = registry.dispatch(
            0,
            &CommandFrame::new("SET", vec![b"k1".to_vec(), b"v1".to_vec()]),
            &mut state,
        );
        let _ = registry.dispatch(
            0,
            &CommandFrame::new("SET", vec![b"k2".to_vec(), b"v2".to_vec()]),
            &mut state,
        );

        let unlinked = registry.dispatch(
            0,
            &CommandFrame::new(
                "UNLINK",
                vec![
                    b"k1".to_vec(),
                    b"k2".to_vec(),
                    b"missing".to_vec(),
                    b"k1".to_vec(),
                ],
            ),
            &mut state,
        );
        assert_that!(&unlinked, eq(&CommandReply::Integer(2)));

        let exists_after = registry.dispatch(
            0,
            &CommandFrame::new("EXISTS", vec![b"k1".to_vec(), b"k2".to_vec()]),
            &mut state,
        );
        assert_that!(&exists_after, eq(&CommandReply::Integer(0)));
    }

    #[rstest]
    fn dispatch_touch_counts_existing_keys_like_exists() {
        let registry = CommandRegistry::with_builtin_commands();
        let mut state = DispatchState::default();

        let _ = registry.dispatch(
            0,
            &CommandFrame::new("SET", vec![b"k1".to_vec(), b"v1".to_vec()]),
            &mut state,
        );
        let _ = registry.dispatch(
            0,
            &CommandFrame::new("SET", vec![b"k2".to_vec(), b"v2".to_vec()]),
            &mut state,
        );

        let touched = registry.dispatch(
            0,
            &CommandFrame::new(
                "TOUCH",
                vec![
                    b"k1".to_vec(),
                    b"k2".to_vec(),
                    b"missing".to_vec(),
                    b"k1".to_vec(),
                ],
            ),
            &mut state,
        );
        assert_that!(&touched, eq(&CommandReply::Integer(3)));
    }

    #[rstest]
    fn dispatch_move_transfers_key_between_databases() {
        let registry = CommandRegistry::with_builtin_commands();
        let mut state = DispatchState::default();

        let _ = registry.dispatch(
            0,
            &CommandFrame::new("SET", vec![b"move:key".to_vec(), b"value".to_vec()]),
            &mut state,
        );

        let moved = registry.dispatch(
            0,
            &CommandFrame::new("MOVE", vec![b"move:key".to_vec(), b"1".to_vec()]),
            &mut state,
        );
        assert_that!(&moved, eq(&CommandReply::Integer(1)));

        let source = registry.dispatch(
            0,
            &CommandFrame::new("GET", vec![b"move:key".to_vec()]),
            &mut state,
        );
        assert_that!(&source, eq(&CommandReply::Null));
        let target = registry.dispatch(
            1,
            &CommandFrame::new("GET", vec![b"move:key".to_vec()]),
            &mut state,
        );
        assert_that!(&target, eq(&CommandReply::BulkString(b"value".to_vec())));
    }

    #[rstest]
    fn dispatch_move_returns_zero_when_source_missing_or_target_exists() {
        let registry = CommandRegistry::with_builtin_commands();
        let mut state = DispatchState::default();

        let missing = registry.dispatch(
            0,
            &CommandFrame::new("MOVE", vec![b"missing".to_vec(), b"1".to_vec()]),
            &mut state,
        );
        assert_that!(&missing, eq(&CommandReply::Integer(0)));

        let _ = registry.dispatch(
            0,
            &CommandFrame::new("SET", vec![b"dup".to_vec(), b"db0".to_vec()]),
            &mut state,
        );
        let _ = registry.dispatch(
            1,
            &CommandFrame::new("SET", vec![b"dup".to_vec(), b"db1".to_vec()]),
            &mut state,
        );
        let blocked = registry.dispatch(
            0,
            &CommandFrame::new("MOVE", vec![b"dup".to_vec(), b"1".to_vec()]),
            &mut state,
        );
        assert_that!(&blocked, eq(&CommandReply::Integer(0)));

        let source = registry.dispatch(
            0,
            &CommandFrame::new("GET", vec![b"dup".to_vec()]),
            &mut state,
        );
        let target = registry.dispatch(
            1,
            &CommandFrame::new("GET", vec![b"dup".to_vec()]),
            &mut state,
        );
        assert_that!(&source, eq(&CommandReply::BulkString(b"db0".to_vec())));
        assert_that!(&target, eq(&CommandReply::BulkString(b"db1".to_vec())));
    }

    #[rstest]
    fn dispatch_copy_duplicates_source_without_removing_it() {
        let registry = CommandRegistry::with_builtin_commands();
        let mut state = DispatchState::default();

        let _ = registry.dispatch(
            0,
            &CommandFrame::new(
                "SETEX",
                vec![b"src".to_vec(), b"60".to_vec(), b"value".to_vec()],
            ),
            &mut state,
        );

        let copied = registry.dispatch(
            0,
            &CommandFrame::new("COPY", vec![b"src".to_vec(), b"dst".to_vec()]),
            &mut state,
        );
        assert_that!(&copied, eq(&CommandReply::Integer(1)));

        let source = registry.dispatch(
            0,
            &CommandFrame::new("GET", vec![b"src".to_vec()]),
            &mut state,
        );
        let destination = registry.dispatch(
            0,
            &CommandFrame::new("GET", vec![b"dst".to_vec()]),
            &mut state,
        );
        assert_that!(&source, eq(&CommandReply::BulkString(b"value".to_vec())));
        assert_that!(
            &destination,
            eq(&CommandReply::BulkString(b"value".to_vec()))
        );

        let source_expire = state
            .db_map(0)
            .and_then(|map| map.get(b"src".as_slice()))
            .and_then(|entry| entry.expire_at_unix_secs);
        let destination_expire = state
            .db_map(0)
            .and_then(|map| map.get(b"dst".as_slice()))
            .and_then(|entry| entry.expire_at_unix_secs);
        assert_that!(source_expire, eq(destination_expire));
    }

    #[rstest]
    fn dispatch_copy_requires_replace_to_overwrite_existing_destination() {
        let registry = CommandRegistry::with_builtin_commands();
        let mut state = DispatchState::default();

        let _ = registry.dispatch(
            0,
            &CommandFrame::new("SET", vec![b"src".to_vec(), b"next".to_vec()]),
            &mut state,
        );
        let _ = registry.dispatch(
            0,
            &CommandFrame::new("SET", vec![b"dst".to_vec(), b"old".to_vec()]),
            &mut state,
        );

        let blocked = registry.dispatch(
            0,
            &CommandFrame::new("COPY", vec![b"src".to_vec(), b"dst".to_vec()]),
            &mut state,
        );
        assert_that!(&blocked, eq(&CommandReply::Integer(0)));

        let replaced = registry.dispatch(
            0,
            &CommandFrame::new(
                "COPY",
                vec![b"src".to_vec(), b"dst".to_vec(), b"REPLACE".to_vec()],
            ),
            &mut state,
        );
        assert_that!(&replaced, eq(&CommandReply::Integer(1)));

        let destination = registry.dispatch(
            0,
            &CommandFrame::new("GET", vec![b"dst".to_vec()]),
            &mut state,
        );
        assert_that!(
            &destination,
            eq(&CommandReply::BulkString(b"next".to_vec()))
        );
    }

    #[rstest]
    fn dispatch_copy_validates_same_key_and_options() {
        let registry = CommandRegistry::with_builtin_commands();
        let mut state = DispatchState::default();

        let same = registry.dispatch(
            0,
            &CommandFrame::new("COPY", vec![b"k".to_vec(), b"k".to_vec()]),
            &mut state,
        );
        assert_that!(
            &same,
            eq(&CommandReply::Error(
                "source and destination objects are the same".to_owned()
            ))
        );

        let invalid_option = registry.dispatch(
            0,
            &CommandFrame::new(
                "COPY",
                vec![
                    b"src".to_vec(),
                    b"dst".to_vec(),
                    b"DB".to_vec(),
                    b"1".to_vec(),
                ],
            ),
            &mut state,
        );
        assert_that!(
            &invalid_option,
            eq(&CommandReply::Error("syntax error".to_owned()))
        );
    }

    #[rstest]
    fn dispatch_rename_moves_value_to_destination_key() {
        let registry = CommandRegistry::with_builtin_commands();
        let mut state = DispatchState::default();

        let _ = registry.dispatch(
            0,
            &CommandFrame::new("SET", vec![b"src".to_vec(), b"v1".to_vec()]),
            &mut state,
        );
        let _ = registry.dispatch(
            0,
            &CommandFrame::new("SET", vec![b"dst".to_vec(), b"v2".to_vec()]),
            &mut state,
        );

        let renamed = registry.dispatch(
            0,
            &CommandFrame::new("RENAME", vec![b"src".to_vec(), b"dst".to_vec()]),
            &mut state,
        );
        assert_that!(&renamed, eq(&CommandReply::SimpleString("OK".to_owned())));

        let src_value = registry.dispatch(
            0,
            &CommandFrame::new("GET", vec![b"src".to_vec()]),
            &mut state,
        );
        assert_that!(&src_value, eq(&CommandReply::Null));

        let dst_value = registry.dispatch(
            0,
            &CommandFrame::new("GET", vec![b"dst".to_vec()]),
            &mut state,
        );
        assert_that!(&dst_value, eq(&CommandReply::BulkString(b"v1".to_vec())));
    }

    #[rstest]
    fn dispatch_renamenx_only_moves_when_destination_is_missing() {
        let registry = CommandRegistry::with_builtin_commands();
        let mut state = DispatchState::default();

        let _ = registry.dispatch(
            0,
            &CommandFrame::new("SET", vec![b"src".to_vec(), b"v1".to_vec()]),
            &mut state,
        );
        let _ = registry.dispatch(
            0,
            &CommandFrame::new("SET", vec![b"dst".to_vec(), b"v2".to_vec()]),
            &mut state,
        );

        let blocked = registry.dispatch(
            0,
            &CommandFrame::new("RENAMENX", vec![b"src".to_vec(), b"dst".to_vec()]),
            &mut state,
        );
        assert_that!(&blocked, eq(&CommandReply::Integer(0)));

        let moved = registry.dispatch(
            0,
            &CommandFrame::new("RENAMENX", vec![b"src".to_vec(), b"new".to_vec()]),
            &mut state,
        );
        assert_that!(&moved, eq(&CommandReply::Integer(1)));

        let src_value = registry.dispatch(
            0,
            &CommandFrame::new("GET", vec![b"src".to_vec()]),
            &mut state,
        );
        assert_that!(&src_value, eq(&CommandReply::Null));

        let new_value = registry.dispatch(
            0,
            &CommandFrame::new("GET", vec![b"new".to_vec()]),
            &mut state,
        );
        assert_that!(&new_value, eq(&CommandReply::BulkString(b"v1".to_vec())));
    }

    #[rstest]
    fn dispatch_rename_same_name_requires_existing_key() {
        let registry = CommandRegistry::with_builtin_commands();
        let mut state = DispatchState::default();

        let missing = registry.dispatch(
            0,
            &CommandFrame::new("RENAME", vec![b"same".to_vec(), b"same".to_vec()]),
            &mut state,
        );
        assert_that!(&missing, eq(&CommandReply::Error("no such key".to_owned())));

        let _ = registry.dispatch(
            0,
            &CommandFrame::new("SET", vec![b"same".to_vec(), b"value".to_vec()]),
            &mut state,
        );
        let rename_ok = registry.dispatch(
            0,
            &CommandFrame::new("RENAME", vec![b"same".to_vec(), b"same".to_vec()]),
            &mut state,
        );
        assert_that!(&rename_ok, eq(&CommandReply::SimpleString("OK".to_owned())));

        let renamenx_same = registry.dispatch(
            0,
            &CommandFrame::new("RENAMENX", vec![b"same".to_vec(), b"same".to_vec()]),
            &mut state,
        );
        assert_that!(&renamenx_same, eq(&CommandReply::Integer(0)));
    }

    #[rstest]
    fn dispatch_unknown_command_returns_error() {
        let registry = CommandRegistry::with_builtin_commands();
        let mut state = DispatchState::default();

        let reply = registry.dispatch(0, &CommandFrame::new("NOPE", Vec::new()), &mut state);
        let CommandReply::Error(message) = reply else {
            panic!("expected error reply");
        };
        assert_that!(message.contains("unknown command"), eq(true));
    }

    #[rstest]
    fn dispatch_ttl_and_expire_lifecycle() {
        let registry = CommandRegistry::with_builtin_commands();
        let mut state = DispatchState::default();

        let _ = registry.dispatch(
            0,
            &CommandFrame::new("SET", vec![b"temp".to_vec(), b"value".to_vec()]),
            &mut state,
        );

        let ttl_without_expire = registry.dispatch(
            0,
            &CommandFrame::new("TTL", vec![b"temp".to_vec()]),
            &mut state,
        );
        assert_that!(&ttl_without_expire, eq(&CommandReply::Integer(-1)));

        let expire_now = registry.dispatch(
            0,
            &CommandFrame::new("EXPIRE", vec![b"temp".to_vec(), b"0".to_vec()]),
            &mut state,
        );
        assert_that!(&expire_now, eq(&CommandReply::Integer(1)));

        let get_after_expire = registry.dispatch(
            0,
            &CommandFrame::new("GET", vec![b"temp".to_vec()]),
            &mut state,
        );
        assert_that!(&get_after_expire, eq(&CommandReply::Null));

        let ttl_after_expire = registry.dispatch(
            0,
            &CommandFrame::new("TTL", vec![b"temp".to_vec()]),
            &mut state,
        );
        assert_that!(&ttl_after_expire, eq(&CommandReply::Integer(-2)));
    }

    #[rstest]
    fn dispatch_pttl_and_pexpire_lifecycle() {
        let registry = CommandRegistry::with_builtin_commands();
        let mut state = DispatchState::default();

        let _ = registry.dispatch(
            0,
            &CommandFrame::new("SET", vec![b"temp".to_vec(), b"value".to_vec()]),
            &mut state,
        );

        let pttl_without_expire = registry.dispatch(
            0,
            &CommandFrame::new("PTTL", vec![b"temp".to_vec()]),
            &mut state,
        );
        assert_that!(&pttl_without_expire, eq(&CommandReply::Integer(-1)));

        let pexpire = registry.dispatch(
            0,
            &CommandFrame::new("PEXPIRE", vec![b"temp".to_vec(), b"1500".to_vec()]),
            &mut state,
        );
        assert_that!(&pexpire, eq(&CommandReply::Integer(1)));

        let pttl = registry.dispatch(
            0,
            &CommandFrame::new("PTTL", vec![b"temp".to_vec()]),
            &mut state,
        );
        let CommandReply::Integer(pttl_ms) = pttl else {
            panic!("PTTL must return integer");
        };
        assert_that!(pttl_ms > 0, eq(true));

        let pexpire_now = registry.dispatch(
            0,
            &CommandFrame::new("PEXPIRE", vec![b"temp".to_vec(), b"0".to_vec()]),
            &mut state,
        );
        assert_that!(&pexpire_now, eq(&CommandReply::Integer(1)));

        let pttl_after = registry.dispatch(
            0,
            &CommandFrame::new("PTTL", vec![b"temp".to_vec()]),
            &mut state,
        );
        assert_that!(&pttl_after, eq(&CommandReply::Integer(-2)));
    }

    #[rstest]
    fn dispatch_expiretime_reports_missing_persistent_and_expiring_keys() {
        let registry = CommandRegistry::with_builtin_commands();
        let mut state = DispatchState::default();

        let missing = registry.dispatch(
            0,
            &CommandFrame::new("EXPIRETIME", vec![b"missing".to_vec()]),
            &mut state,
        );
        assert_that!(&missing, eq(&CommandReply::Integer(-2)));

        let _ = registry.dispatch(
            0,
            &CommandFrame::new("SET", vec![b"persist".to_vec(), b"v".to_vec()]),
            &mut state,
        );
        let persistent = registry.dispatch(
            0,
            &CommandFrame::new("EXPIRETIME", vec![b"persist".to_vec()]),
            &mut state,
        );
        assert_that!(&persistent, eq(&CommandReply::Integer(-1)));

        let _ = registry.dispatch(
            0,
            &CommandFrame::new(
                "SETEX",
                vec![b"temp".to_vec(), b"60".to_vec(), b"v".to_vec()],
            ),
            &mut state,
        );
        let expiring = registry.dispatch(
            0,
            &CommandFrame::new("EXPIRETIME", vec![b"temp".to_vec()]),
            &mut state,
        );
        let CommandReply::Integer(expire_at) = expiring else {
            panic!("EXPIRETIME must return integer");
        };
        let now = i64::try_from(DispatchState::now_unix_seconds()).unwrap_or(i64::MAX);
        assert_that!(expire_at >= now, eq(true));
    }

    #[rstest]
    fn dispatch_pexpiretime_reports_missing_persistent_and_expiring_keys() {
        let registry = CommandRegistry::with_builtin_commands();
        let mut state = DispatchState::default();

        let missing = registry.dispatch(
            0,
            &CommandFrame::new("PEXPIRETIME", vec![b"missing".to_vec()]),
            &mut state,
        );
        assert_that!(&missing, eq(&CommandReply::Integer(-2)));

        let _ = registry.dispatch(
            0,
            &CommandFrame::new("SET", vec![b"persist".to_vec(), b"v".to_vec()]),
            &mut state,
        );
        let persistent = registry.dispatch(
            0,
            &CommandFrame::new("PEXPIRETIME", vec![b"persist".to_vec()]),
            &mut state,
        );
        assert_that!(&persistent, eq(&CommandReply::Integer(-1)));

        let _ = registry.dispatch(
            0,
            &CommandFrame::new(
                "SETEX",
                vec![b"temp".to_vec(), b"60".to_vec(), b"v".to_vec()],
            ),
            &mut state,
        );
        let expiring = registry.dispatch(
            0,
            &CommandFrame::new("PEXPIRETIME", vec![b"temp".to_vec()]),
            &mut state,
        );
        let CommandReply::Integer(expire_at) = expiring else {
            panic!("PEXPIRETIME must return integer");
        };
        let now = i64::try_from(DispatchState::now_unix_millis()).unwrap_or(i64::MAX);
        assert_that!(expire_at >= now, eq(true));
    }

    #[rstest]
    fn dispatch_persist_removes_expiry_without_deleting_key() {
        let registry = CommandRegistry::with_builtin_commands();
        let mut state = DispatchState::default();

        let _ = registry.dispatch(
            0,
            &CommandFrame::new("SET", vec![b"temp".to_vec(), b"value".to_vec()]),
            &mut state,
        );
        let _ = registry.dispatch(
            0,
            &CommandFrame::new("EXPIRE", vec![b"temp".to_vec(), b"10".to_vec()]),
            &mut state,
        );

        let persist = registry.dispatch(
            0,
            &CommandFrame::new("PERSIST", vec![b"temp".to_vec()]),
            &mut state,
        );
        assert_that!(&persist, eq(&CommandReply::Integer(1)));

        let ttl = registry.dispatch(
            0,
            &CommandFrame::new("TTL", vec![b"temp".to_vec()]),
            &mut state,
        );
        assert_that!(&ttl, eq(&CommandReply::Integer(-1)));

        let value = registry.dispatch(
            0,
            &CommandFrame::new("GET", vec![b"temp".to_vec()]),
            &mut state,
        );
        assert_that!(&value, eq(&CommandReply::BulkString(b"value".to_vec())));
    }

    #[rstest]
    fn dispatch_incr_family_updates_counter_with_redis_semantics() {
        let registry = CommandRegistry::with_builtin_commands();
        let mut state = DispatchState::default();

        let incr = registry.dispatch(
            0,
            &CommandFrame::new("INCR", vec![b"counter".to_vec()]),
            &mut state,
        );
        assert_that!(&incr, eq(&CommandReply::Integer(1)));

        let incrby = registry.dispatch(
            0,
            &CommandFrame::new("INCRBY", vec![b"counter".to_vec(), b"9".to_vec()]),
            &mut state,
        );
        assert_that!(&incrby, eq(&CommandReply::Integer(10)));

        let decr = registry.dispatch(
            0,
            &CommandFrame::new("DECR", vec![b"counter".to_vec()]),
            &mut state,
        );
        assert_that!(&decr, eq(&CommandReply::Integer(9)));

        let decrby = registry.dispatch(
            0,
            &CommandFrame::new("DECRBY", vec![b"counter".to_vec(), b"4".to_vec()]),
            &mut state,
        );
        assert_that!(&decrby, eq(&CommandReply::Integer(5)));
    }

    #[rstest]
    fn dispatch_incr_family_rejects_non_integer_or_overflow_values() {
        let registry = CommandRegistry::with_builtin_commands();
        let mut state = DispatchState::default();

        let _ = registry.dispatch(
            0,
            &CommandFrame::new("SET", vec![b"counter".to_vec(), b"abc".to_vec()]),
            &mut state,
        );
        let non_integer = registry.dispatch(
            0,
            &CommandFrame::new("INCR", vec![b"counter".to_vec()]),
            &mut state,
        );
        assert_that!(
            &non_integer,
            eq(&CommandReply::Error(
                "value is not an integer or out of range".to_owned()
            ))
        );

        let _ = registry.dispatch(
            0,
            &CommandFrame::new(
                "SET",
                vec![b"counter".to_vec(), i64::MAX.to_string().into_bytes()],
            ),
            &mut state,
        );
        let overflow = registry.dispatch(
            0,
            &CommandFrame::new("INCR", vec![b"counter".to_vec()]),
            &mut state,
        );
        assert_that!(
            &overflow,
            eq(&CommandReply::Error(
                "value is not an integer or out of range".to_owned()
            ))
        );
    }

    #[rstest]
    fn dispatch_expire_missing_key_returns_zero() {
        let registry = CommandRegistry::with_builtin_commands();
        let mut state = DispatchState::default();

        let reply = registry.dispatch(
            0,
            &CommandFrame::new("EXPIRE", vec![b"nope".to_vec(), b"10".to_vec()]),
            &mut state,
        );
        assert_that!(&reply, eq(&CommandReply::Integer(0)));
    }

    #[rstest]
    fn dispatch_expire_options_control_update_conditions() {
        let registry = CommandRegistry::with_builtin_commands();
        let mut state = DispatchState::default();

        let _ = registry.dispatch(
            0,
            &CommandFrame::new("SET", vec![b"key".to_vec(), b"value".to_vec()]),
            &mut state,
        );

        let gt_on_persistent = registry.dispatch(
            0,
            &CommandFrame::new(
                "EXPIRE",
                vec![b"key".to_vec(), b"10".to_vec(), b"GT".to_vec()],
            ),
            &mut state,
        );
        assert_that!(&gt_on_persistent, eq(&CommandReply::Integer(0)));

        let lt_on_persistent = registry.dispatch(
            0,
            &CommandFrame::new(
                "EXPIRE",
                vec![b"key".to_vec(), b"10".to_vec(), b"LT".to_vec()],
            ),
            &mut state,
        );
        assert_that!(&lt_on_persistent, eq(&CommandReply::Integer(1)));

        let first_expiretime = registry.dispatch(
            0,
            &CommandFrame::new("EXPIRETIME", vec![b"key".to_vec()]),
            &mut state,
        );
        let CommandReply::Integer(first_expiretime) = first_expiretime else {
            panic!("EXPIRETIME must return integer");
        };

        let nx_with_existing_ttl = registry.dispatch(
            0,
            &CommandFrame::new(
                "EXPIRE",
                vec![b"key".to_vec(), b"20".to_vec(), b"NX".to_vec()],
            ),
            &mut state,
        );
        assert_that!(&nx_with_existing_ttl, eq(&CommandReply::Integer(0)));

        let xx_with_existing_ttl = registry.dispatch(
            0,
            &CommandFrame::new(
                "EXPIRE",
                vec![b"key".to_vec(), b"20".to_vec(), b"XX".to_vec()],
            ),
            &mut state,
        );
        assert_that!(&xx_with_existing_ttl, eq(&CommandReply::Integer(1)));

        let second_expiretime = registry.dispatch(
            0,
            &CommandFrame::new("EXPIRETIME", vec![b"key".to_vec()]),
            &mut state,
        );
        let CommandReply::Integer(second_expiretime) = second_expiretime else {
            panic!("EXPIRETIME must return integer");
        };
        assert_that!(second_expiretime > first_expiretime, eq(true));

        let gt_with_smaller_target = registry.dispatch(
            0,
            &CommandFrame::new(
                "EXPIRE",
                vec![b"key".to_vec(), b"5".to_vec(), b"GT".to_vec()],
            ),
            &mut state,
        );
        assert_that!(&gt_with_smaller_target, eq(&CommandReply::Integer(0)));

        let unchanged_expiretime = registry.dispatch(
            0,
            &CommandFrame::new("EXPIRETIME", vec![b"key".to_vec()]),
            &mut state,
        );
        assert_that!(
            &unchanged_expiretime,
            eq(&CommandReply::Integer(second_expiretime))
        );

        let lt_with_smaller_target = registry.dispatch(
            0,
            &CommandFrame::new(
                "EXPIRE",
                vec![b"key".to_vec(), b"5".to_vec(), b"LT".to_vec()],
            ),
            &mut state,
        );
        assert_that!(&lt_with_smaller_target, eq(&CommandReply::Integer(1)));

        let third_expiretime = registry.dispatch(
            0,
            &CommandFrame::new("EXPIRETIME", vec![b"key".to_vec()]),
            &mut state,
        );
        let CommandReply::Integer(third_expiretime) = third_expiretime else {
            panic!("EXPIRETIME must return integer");
        };
        assert_that!(third_expiretime < second_expiretime, eq(true));
    }

    #[rstest]
    fn dispatch_pexpire_options_control_millisecond_predicates() {
        let registry = CommandRegistry::with_builtin_commands();
        let mut state = DispatchState::default();

        let _ = registry.dispatch(
            0,
            &CommandFrame::new("SET", vec![b"key".to_vec(), b"value".to_vec()]),
            &mut state,
        );
        let first = registry.dispatch(
            0,
            &CommandFrame::new(
                "PEXPIRE",
                vec![b"key".to_vec(), b"1000".to_vec(), b"LT".to_vec()],
            ),
            &mut state,
        );
        assert_that!(&first, eq(&CommandReply::Integer(1)));

        let first_expiretime = registry.dispatch(
            0,
            &CommandFrame::new("PEXPIRETIME", vec![b"key".to_vec()]),
            &mut state,
        );
        let CommandReply::Integer(first_expiretime) = first_expiretime else {
            panic!("PEXPIRETIME must return integer");
        };

        let gt_smaller = registry.dispatch(
            0,
            &CommandFrame::new(
                "PEXPIRE",
                vec![b"key".to_vec(), b"500".to_vec(), b"GT".to_vec()],
            ),
            &mut state,
        );
        assert_that!(&gt_smaller, eq(&CommandReply::Integer(0)));

        let gt_bigger = registry.dispatch(
            0,
            &CommandFrame::new(
                "PEXPIRE",
                vec![b"key".to_vec(), b"2000".to_vec(), b"GT".to_vec()],
            ),
            &mut state,
        );
        assert_that!(&gt_bigger, eq(&CommandReply::Integer(1)));

        let second_expiretime = registry.dispatch(
            0,
            &CommandFrame::new("PEXPIRETIME", vec![b"key".to_vec()]),
            &mut state,
        );
        let CommandReply::Integer(second_expiretime) = second_expiretime else {
            panic!("PEXPIRETIME must return integer");
        };
        assert_that!(second_expiretime > first_expiretime, eq(true));
    }

    #[rstest]
    fn dispatch_expire_options_reject_invalid_combinations_or_unknown_tokens() {
        let registry = CommandRegistry::with_builtin_commands();
        let mut state = DispatchState::default();

        let nx_xx = registry.dispatch(
            0,
            &CommandFrame::new(
                "EXPIRE",
                vec![
                    b"key".to_vec(),
                    b"10".to_vec(),
                    b"NX".to_vec(),
                    b"XX".to_vec(),
                ],
            ),
            &mut state,
        );
        assert_that!(
            &nx_xx,
            eq(&CommandReply::Error(
                "NX and XX options at the same time are not compatible".to_owned()
            ))
        );

        let gt_lt = registry.dispatch(
            0,
            &CommandFrame::new(
                "PEXPIRE",
                vec![
                    b"key".to_vec(),
                    b"10".to_vec(),
                    b"GT".to_vec(),
                    b"LT".to_vec(),
                ],
            ),
            &mut state,
        );
        assert_that!(
            &gt_lt,
            eq(&CommandReply::Error(
                "GT and LT options at the same time are not compatible".to_owned()
            ))
        );

        let unknown = registry.dispatch(
            0,
            &CommandFrame::new(
                "EXPIREAT",
                vec![b"key".to_vec(), b"1".to_vec(), b"SOMETHING".to_vec()],
            ),
            &mut state,
        );
        assert_that!(
            &unknown,
            eq(&CommandReply::Error(
                "Unsupported option: SOMETHING".to_owned()
            ))
        );
    }

    #[rstest]
    fn dispatch_setex_sets_value_and_ttl() {
        let registry = CommandRegistry::with_builtin_commands();
        let mut state = DispatchState::default();

        let setex = registry.dispatch(
            0,
            &CommandFrame::new(
                "SETEX",
                vec![b"session".to_vec(), b"60".to_vec(), b"alive".to_vec()],
            ),
            &mut state,
        );
        assert_that!(&setex, eq(&CommandReply::SimpleString("OK".to_owned())));

        let value = registry.dispatch(
            0,
            &CommandFrame::new("GET", vec![b"session".to_vec()]),
            &mut state,
        );
        assert_that!(&value, eq(&CommandReply::BulkString(b"alive".to_vec())));

        let ttl = registry.dispatch(
            0,
            &CommandFrame::new("TTL", vec![b"session".to_vec()]),
            &mut state,
        );
        let CommandReply::Integer(ttl_secs) = ttl else {
            panic!("TTL must return integer");
        };
        assert_that!(ttl_secs > 0, eq(true));
    }

    #[rstest]
    fn dispatch_psetex_sets_value_and_pttl() {
        let registry = CommandRegistry::with_builtin_commands();
        let mut state = DispatchState::default();

        let psetex = registry.dispatch(
            0,
            &CommandFrame::new(
                "PSETEX",
                vec![b"session".to_vec(), b"1500".to_vec(), b"alive".to_vec()],
            ),
            &mut state,
        );
        assert_that!(&psetex, eq(&CommandReply::SimpleString("OK".to_owned())));

        let value = registry.dispatch(
            0,
            &CommandFrame::new("GET", vec![b"session".to_vec()]),
            &mut state,
        );
        assert_that!(&value, eq(&CommandReply::BulkString(b"alive".to_vec())));

        let pttl = registry.dispatch(
            0,
            &CommandFrame::new("PTTL", vec![b"session".to_vec()]),
            &mut state,
        );
        let CommandReply::Integer(pttl_ms) = pttl else {
            panic!("PTTL must return integer");
        };
        assert_that!(pttl_ms > 0, eq(true));
    }

    #[rstest]
    fn dispatch_setex_rejects_non_positive_expire_values() {
        let registry = CommandRegistry::with_builtin_commands();
        let mut state = DispatchState::default();

        let reply = registry.dispatch(
            0,
            &CommandFrame::new(
                "SETEX",
                vec![b"session".to_vec(), b"0".to_vec(), b"alive".to_vec()],
            ),
            &mut state,
        );
        assert_that!(
            &reply,
            eq(&CommandReply::Error(
                "invalid expire time in 'SETEX' command".to_owned()
            ))
        );
    }

    #[rstest]
    fn dispatch_psetex_rejects_non_positive_expire_values() {
        let registry = CommandRegistry::with_builtin_commands();
        let mut state = DispatchState::default();

        let reply = registry.dispatch(
            0,
            &CommandFrame::new(
                "PSETEX",
                vec![b"session".to_vec(), b"0".to_vec(), b"alive".to_vec()],
            ),
            &mut state,
        );
        assert_that!(
            &reply,
            eq(&CommandReply::Error(
                "invalid expire time in 'PSETEX' command".to_owned()
            ))
        );
    }

    #[rstest]
    fn dispatch_expireat_sets_future_or_removes_past_keys() {
        let registry = CommandRegistry::with_builtin_commands();
        let mut state = DispatchState::default();

        let _ = registry.dispatch(
            0,
            &CommandFrame::new("SET", vec![b"future".to_vec(), b"v".to_vec()]),
            &mut state,
        );
        let future_timestamp = DispatchState::now_unix_seconds()
            .saturating_add(120)
            .to_string()
            .into_bytes();
        let future = registry.dispatch(
            0,
            &CommandFrame::new("EXPIREAT", vec![b"future".to_vec(), future_timestamp]),
            &mut state,
        );
        assert_that!(&future, eq(&CommandReply::Integer(1)));

        let _ = registry.dispatch(
            0,
            &CommandFrame::new("SET", vec![b"past".to_vec(), b"v".to_vec()]),
            &mut state,
        );
        let past = registry.dispatch(
            0,
            &CommandFrame::new("EXPIREAT", vec![b"past".to_vec(), b"1".to_vec()]),
            &mut state,
        );
        assert_that!(&past, eq(&CommandReply::Integer(1)));

        let removed = registry.dispatch(
            0,
            &CommandFrame::new("GET", vec![b"past".to_vec()]),
            &mut state,
        );
        assert_that!(&removed, eq(&CommandReply::Null));
    }

    #[rstest]
    fn dispatch_pexpireat_sets_future_or_removes_past_keys() {
        let registry = CommandRegistry::with_builtin_commands();
        let mut state = DispatchState::default();

        let _ = registry.dispatch(
            0,
            &CommandFrame::new("SET", vec![b"future".to_vec(), b"v".to_vec()]),
            &mut state,
        );
        let future_timestamp = DispatchState::now_unix_millis()
            .saturating_add(120_000)
            .to_string()
            .into_bytes();
        let future = registry.dispatch(
            0,
            &CommandFrame::new("PEXPIREAT", vec![b"future".to_vec(), future_timestamp]),
            &mut state,
        );
        assert_that!(&future, eq(&CommandReply::Integer(1)));

        let _ = registry.dispatch(
            0,
            &CommandFrame::new("SET", vec![b"past".to_vec(), b"v".to_vec()]),
            &mut state,
        );
        let past = registry.dispatch(
            0,
            &CommandFrame::new("PEXPIREAT", vec![b"past".to_vec(), b"1".to_vec()]),
            &mut state,
        );
        assert_that!(&past, eq(&CommandReply::Integer(1)));

        let removed = registry.dispatch(
            0,
            &CommandFrame::new("GET", vec![b"past".to_vec()]),
            &mut state,
        );
        assert_that!(&removed, eq(&CommandReply::Null));
    }

    #[rstest]
    fn dispatch_keeps_values_isolated_between_databases() {
        let registry = CommandRegistry::with_builtin_commands();
        let mut state = DispatchState::default();

        let _ = registry.dispatch(
            0,
            &CommandFrame::new("SET", vec![b"shared".to_vec(), b"db0".to_vec()]),
            &mut state,
        );
        let _ = registry.dispatch(
            1,
            &CommandFrame::new("SET", vec![b"shared".to_vec(), b"db1".to_vec()]),
            &mut state,
        );

        let db0 = registry.dispatch(
            0,
            &CommandFrame::new("GET", vec![b"shared".to_vec()]),
            &mut state,
        );
        let db1 = registry.dispatch(
            1,
            &CommandFrame::new("GET", vec![b"shared".to_vec()]),
            &mut state,
        );
        assert_that!(&db0, eq(&CommandReply::BulkString(b"db0".to_vec())));
        assert_that!(&db1, eq(&CommandReply::BulkString(b"db1".to_vec())));
    }

    #[rstest]
    fn dispatch_bumps_key_version_on_mutations() {
        let registry = CommandRegistry::with_builtin_commands();
        let mut state = DispatchState::default();
        let key = b"watched:key".to_vec();

        assert_that!(state.key_version(0, &key), eq(0_u64));

        let _ = registry.dispatch(
            0,
            &CommandFrame::new("SET", vec![key.clone(), b"v1".to_vec()]),
            &mut state,
        );
        assert_that!(state.key_version(0, &key), eq(1_u64));

        let _ = registry.dispatch(
            0,
            &CommandFrame::new("EXPIRE", vec![key.clone(), b"10".to_vec()]),
            &mut state,
        );
        assert_that!(state.key_version(0, &key), eq(2_u64));

        let _ = registry.dispatch(
            0,
            &CommandFrame::new("EXPIRE", vec![key.clone(), b"0".to_vec()]),
            &mut state,
        );
        assert_that!(state.key_version(0, &key), eq(3_u64));
    }
}
