//! Command registration and dispatch skeleton.
//!
//! This module intentionally mirrors Dragonfly's "command table + execution path" concept.
//! Unit 1 keeps the implementation minimal while preserving the same architectural idea:
//! protocol parsing produces a canonical command frame, then a registry resolves and executes
//! the matching handler.

use std::collections::HashMap;
use std::str;
use std::time::{SystemTime, UNIX_EPOCH};

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

/// Mutable execution state used by command handlers.
///
/// The `kv` map is a minimal placeholder representing per-node key-value state.
/// In later units this will be replaced by shard-aware storage and transaction views.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct DispatchState {
    /// In-memory key/value dictionary for learning-path command execution.
    ///
    /// First key is logical DB index (`SELECT`), second key is user key bytes.
    pub db_kv: HashMap<DbIndex, HashMap<Vec<u8>, ValueEntry>>,
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

    /// Returns mutable map for one logical DB, creating it when missing.
    fn db_map_mut(&mut self, db: DbIndex) -> &mut HashMap<Vec<u8>, ValueEntry> {
        self.db_kv.entry(db).or_default()
    }

    /// Returns immutable map for one logical DB.
    fn db_map(&self, db: DbIndex) -> Option<&HashMap<Vec<u8>, ValueEntry>> {
        self.db_kv.get(&db)
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
    }

    /// Removes one key when its expiration timestamp has already passed.
    fn purge_expired_key(&mut self, db: DbIndex, key: &[u8]) {
        let should_remove = self
            .db_map(db)
            .and_then(|map| map.get(key))
            .and_then(|entry| entry.expire_at_unix_secs)
            .is_some_and(|expire_at| expire_at <= Self::now_unix_seconds());

        if should_remove
            && let Some(map) = self.db_kv.get_mut(&db)
            && map.remove(key).is_some()
        {
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
        let Some(keyspace) = self.db_kv.get(&db) else {
            return 0;
        };

        let keys = keyspace.keys().cloned().collect::<Vec<_>>();
        let removed = keys.len();
        for key in &keys {
            self.bump_key_version(db, key);
        }
        let _ = self.db_kv.remove(&db);
        removed
    }

    /// Removes all keys in all logical DBs and bumps their key versions.
    ///
    /// Returns the number of removed keys.
    pub fn flush_all(&mut self) -> usize {
        let mut keys_per_db = Vec::new();
        let mut removed = 0_usize;

        for (db, keyspace) in &self.db_kv {
            let keys = keyspace.keys().cloned().collect::<Vec<_>>();
            removed = removed.saturating_add(keys.len());
            keys_per_db.push((*db, keys));
        }

        for (db, keys) in keys_per_db {
            for key in keys {
                self.bump_key_version(db, &key);
            }
        }
        self.db_kv.clear();
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
            arity: CommandArity::Exact(2),
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
            name: "EXISTS",
            arity: CommandArity::AtLeast(1),
            handler: handle_exists,
        });
    }

    fn register_expiry_commands(&mut self) {
        self.register(CommandSpec {
            name: "SETEX",
            arity: CommandArity::Exact(3),
            handler: handle_setex,
        });
        self.register(CommandSpec {
            name: "EXPIRE",
            arity: CommandArity::Exact(2),
            handler: handle_expire,
        });
        self.register(CommandSpec {
            name: "PEXPIRE",
            arity: CommandArity::Exact(2),
            handler: handle_pexpire,
        });
        self.register(CommandSpec {
            name: "EXPIREAT",
            arity: CommandArity::Exact(2),
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
        (spec.handler)(db, frame, state)
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

fn handle_set(db: DbIndex, frame: &CommandFrame, state: &mut DispatchState) -> CommandReply {
    let key = frame.args[0].clone();
    let value = frame.args[1].clone();
    state.db_map_mut(db).insert(
        key.clone(),
        ValueEntry {
            value,
            expire_at_unix_secs: None,
        },
    );
    state.bump_key_version(db, &key);
    CommandReply::SimpleString("OK".to_owned())
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

fn handle_expire(db: DbIndex, frame: &CommandFrame, state: &mut DispatchState) -> CommandReply {
    let key = &frame.args[0];
    state.purge_expired_key(db, key);

    let seconds = match parse_i64_arg(&frame.args[1], "EXPIRE seconds") {
        Ok(seconds) => seconds,
        Err(error) => return CommandReply::Error(error),
    };
    if !state.db_map(db).is_some_and(|map| map.contains_key(key)) {
        return CommandReply::Integer(0);
    }

    if seconds <= 0 {
        if state.db_map_mut(db).remove(key).is_some() {
            state.bump_key_version(db, key);
            return CommandReply::Integer(1);
        }
        return CommandReply::Integer(0);
    }

    let Ok(expire_delta) = u64::try_from(seconds) else {
        return CommandReply::Error("EXPIRE seconds is out of range".to_owned());
    };
    let expire_at = DispatchState::now_unix_seconds().saturating_add(expire_delta);

    let mut updated = false;
    if let Some(entry) = state.db_map_mut(db).get_mut(key) {
        entry.expire_at_unix_secs = Some(expire_at);
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

    let milliseconds = match parse_i64_arg(&frame.args[1], "PEXPIRE milliseconds") {
        Ok(milliseconds) => milliseconds,
        Err(error) => return CommandReply::Error(error),
    };
    if !state.db_map(db).is_some_and(|map| map.contains_key(key)) {
        return CommandReply::Integer(0);
    }

    if milliseconds <= 0 {
        if state.db_map_mut(db).remove(key).is_some() {
            state.bump_key_version(db, key);
            return CommandReply::Integer(1);
        }
        return CommandReply::Integer(0);
    }

    let Ok(expire_delta_millis) = u64::try_from(milliseconds) else {
        return CommandReply::Error("PEXPIRE milliseconds is out of range".to_owned());
    };
    let now_millis = DispatchState::now_unix_millis();
    let expire_at_millis = now_millis.saturating_add(expire_delta_millis);
    let expire_at_secs = expire_at_millis.saturating_add(999) / 1000;

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

    let timestamp = match parse_i64_arg(&frame.args[1], "EXPIREAT timestamp") {
        Ok(timestamp) => timestamp,
        Err(error) => return CommandReply::Error(error),
    };
    if !state.db_map(db).is_some_and(|map| map.contains_key(key)) {
        return CommandReply::Integer(0);
    }

    let now = i64::try_from(DispatchState::now_unix_seconds()).unwrap_or(i64::MAX);
    if timestamp <= now {
        if state.db_map_mut(db).remove(key).is_some() {
            state.bump_key_version(db, key);
            return CommandReply::Integer(1);
        }
        return CommandReply::Integer(0);
    }

    let Ok(expire_at) = u64::try_from(timestamp) else {
        return CommandReply::Error("EXPIREAT timestamp is out of range".to_owned());
    };
    if let Some(entry) = state.db_map_mut(db).get_mut(key) {
        entry.expire_at_unix_secs = Some(expire_at);
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
    use googletest::prelude::*;
    use rstest::rstest;

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
