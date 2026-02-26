//! Command registration and dispatch skeleton.
//!
//! This module intentionally mirrors Dragonfly's "command table + execution path" concept.
//! Unit 1 keeps the implementation minimal while preserving the same architectural idea:
//! protocol parsing produces a canonical command frame, then a registry resolves and executes
//! the matching handler.

use std::collections::BTreeMap;
use std::time::{SystemTime, UNIX_EPOCH};

use dfly_cluster::slot::key_slot;
use dfly_common::ids::{DbIndex, MAX_SLOT_ID};

use crate::command::{CommandFrame, CommandReply};
use crate::containers::{HotMap as HashMap, HotSet as HashSet};

#[path = "dispatch/parse_opts.rs"]
mod parse_opts;
#[path = "dispatch/parse_numbers.rs"]
mod parse_numbers;
#[path = "dispatch/handlers_string.rs"]
mod handlers_string;
#[path = "dispatch/handlers_expiry.rs"]
mod handlers_expiry;
#[path = "dispatch/handlers_counter.rs"]
mod handlers_counter;
#[path = "dispatch/handlers_keyspace.rs"]
mod handlers_keyspace;

use handlers_counter::{handle_decr, handle_decrby, handle_incr, handle_incrby};
use handlers_expiry::{
    handle_expire, handle_expireat, handle_expiretime, handle_pexpire, handle_pexpireat,
    handle_pexpiretime, handle_persist, handle_psetex, handle_pttl, handle_setex, handle_ttl,
};
use handlers_keyspace::{handle_copy, handle_del, handle_exists, handle_move, handle_rename, handle_renamenx};
use handlers_string::{
    handle_append, handle_echo, handle_get, handle_getdel, handle_getrange, handle_getset,
    handle_mget, handle_mset, handle_msetnx, handle_ping, handle_set, handle_setnx,
    handle_setrange, handle_strlen, handle_type,
};

pub use handlers_keyspace::{copy_between_states, rename_between_states};

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

/// Redis cluster defines a fixed 16384-slot hash space.
const CLUSTER_SLOT_COUNT: usize = (MAX_SLOT_ID as usize) + 1;
const DEFAULT_DB_PRIME_CAPACITY: usize = 64;
const DEFAULT_DB_EXPIRE_CAPACITY: usize = 64;
const DEFAULT_DB_SLOT_INDEX_CAPACITY: usize = 32;
const DEFAULT_SHARD_DB_TABLE_CAPACITY: usize = 16;
const DEFAULT_SHARD_VERSION_TABLE_CAPACITY: usize = 16;

/// Dense per-slot key cardinality table.
///
/// Dragonfly tracks slot-level stats in a fixed slot domain, so we keep a direct-indexed
/// structure instead of a sparse hashmap to avoid allocator and hash overhead on hot paths.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SlotStats {
    key_counts: Box<[usize]>,
    total_reads: Box<[u64]>,
    total_writes: Box<[u64]>,
    memory_bytes: Box<[usize]>,
}

impl Default for SlotStats {
    fn default() -> Self {
        Self {
            key_counts: vec![0; CLUSTER_SLOT_COUNT].into_boxed_slice(),
            total_reads: vec![0; CLUSTER_SLOT_COUNT].into_boxed_slice(),
            total_writes: vec![0; CLUSTER_SLOT_COUNT].into_boxed_slice(),
            memory_bytes: vec![0; CLUSTER_SLOT_COUNT].into_boxed_slice(),
        }
    }
}

impl SlotStats {
    fn increment(&mut self, slot: u16) {
        let index = usize::from(slot);
        self.key_counts[index] = self.key_counts[index].saturating_add(1);
    }

    fn decrement(&mut self, slot: u16) -> usize {
        let index = usize::from(slot);
        self.key_counts[index] = self.key_counts[index].saturating_sub(1);
        self.key_counts[index]
    }

    #[must_use]
    fn get(&self, slot: u16) -> usize {
        self.key_counts[usize::from(slot)]
    }

    fn clear(&mut self, slot: u16) {
        self.key_counts[usize::from(slot)] = 0;
    }

    fn set_count(&mut self, slot: u16, count: usize) {
        self.key_counts[usize::from(slot)] = count;
    }

    fn set_memory_bytes(&mut self, slot: u16, bytes: usize) {
        self.memory_bytes[usize::from(slot)] = bytes;
    }

    fn increment_write(&mut self, slot: u16) {
        let index = usize::from(slot);
        self.total_writes[index] = self.total_writes[index].saturating_add(1);
    }

    fn increment_read(&mut self, slot: u16) {
        let index = usize::from(slot);
        self.total_reads[index] = self.total_reads[index].saturating_add(1);
    }

    fn add_memory_bytes(&mut self, slot: u16, bytes: usize) {
        let index = usize::from(slot);
        self.memory_bytes[index] = self.memory_bytes[index].saturating_add(bytes);
    }

    fn sub_memory_bytes(&mut self, slot: u16, bytes: usize) {
        let index = usize::from(slot);
        self.memory_bytes[index] = self.memory_bytes[index].saturating_sub(bytes);
    }

    /// Returns cumulative successful read count for one slot.
    ///
    /// Counter shape mirrors Dragonfly's slot stats bookkeeping that tracks
    /// slot-local read pressure independently from key cardinality.
    #[must_use]
    pub fn total_reads(&self, slot: u16) -> u64 {
        self.total_reads[usize::from(slot)]
    }

    /// Returns cumulative successful write count for one slot.
    ///
    /// Dragonfly keeps per-slot write counters in `slots_stats`; this mirrors
    /// the same shape so slot-local mutation pressure is observable.
    #[must_use]
    pub fn total_writes(&self, slot: u16) -> u64 {
        self.total_writes[usize::from(slot)]
    }

    /// Returns current approximate user payload bytes in one slot.
    ///
    /// This learning counter accounts for key bytes + value bytes and updates
    /// synchronously with key mutations.
    #[must_use]
    pub fn memory_bytes(&self, slot: u16) -> usize {
        self.memory_bytes[usize::from(slot)]
    }

    /// Returns current live key count tracked for one slot.
    #[must_use]
    pub fn key_count(&self, slot: u16) -> usize {
        self.key_counts[usize::from(slot)]
    }
}

/// Snapshot view of one slot's counters.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct SlotStatsSnapshot {
    /// Live key cardinality in this slot.
    pub key_count: usize,
    /// Cumulative successful read operations for keys in this slot.
    pub total_reads: u64,
    /// Cumulative successful write operations for keys in this slot.
    pub total_writes: u64,
    /// Approximate payload bytes (key + value) in this slot.
    pub memory_bytes: usize,
}

/// One logical DB table in the shard-local slice.
///
/// This shape mirrors Dragonfly's `DbTable` layering:
/// - `prime`: main key/value dictionary (`PrimeTable` concept),
/// - `expire`: expiration index (`ExpireTable` concept),
/// - `slot_stats`: per-slot counters (cardinality + read/write totals),
/// - `slot_keys`: per-slot key membership index used by slot-local scans/migrations.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DbTable {
    /// Main key/value dictionary.
    pub prime: HashMap<Vec<u8>, ValueEntry>,
    /// Expiration index with absolute unix-second deadlines.
    pub expire: HashMap<Vec<u8>, u64>,
    /// Reverse expiration index ordered by deadline for active-expiry scans.
    pub expire_order: BTreeMap<u64, HashSet<Vec<u8>>>,
    /// Key cardinality by Redis cluster slot.
    pub slot_stats: SlotStats,
    /// Key membership by Redis cluster slot.
    pub slot_keys: HashMap<u16, HashSet<Vec<u8>>>,
}

impl Default for DbTable {
    fn default() -> Self {
        Self {
            prime: HashMap::with_capacity(DEFAULT_DB_PRIME_CAPACITY),
            expire: HashMap::with_capacity(DEFAULT_DB_EXPIRE_CAPACITY),
            expire_order: BTreeMap::new(),
            slot_stats: SlotStats::default(),
            slot_keys: HashMap::with_capacity(DEFAULT_DB_SLOT_INDEX_CAPACITY),
        }
    }
}

/// Mutable execution state used by command handlers.
///
/// Dragonfly keeps per-DB tables in one shard-local `DbSlice`. This learning implementation uses
/// the same high-level structure and maintains a layered table per DB.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DispatchState {
    /// Per-DB layered tables in this shard.
    pub db_tables: HashMap<DbIndex, DbTable>,
    /// Monotonic per-key versions used by optimistic transaction checks (`WATCH`).
    db_versions: HashMap<DbIndex, HashMap<Vec<u8>, u64>>,
    /// Round-robin starting point for per-pass active-expire DB scans.
    active_expire_db_cursor: usize,
}

impl Default for DispatchState {
    fn default() -> Self {
        Self {
            db_tables: HashMap::with_capacity(DEFAULT_SHARD_DB_TABLE_CAPACITY),
            db_versions: HashMap::with_capacity(DEFAULT_SHARD_VERSION_TABLE_CAPACITY),
            active_expire_db_cursor: 0,
        }
    }
}

/// Stored value with optional expiration metadata.
///
/// The storage layer intentionally models value payload as an enum even when only
/// string is implemented. This keeps dispatch/container shape extensible for future
/// Dragonfly data types without changing `prime` table entry ownership.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum StoredValue {
    /// Redis string payload stored in compact exact-sized buffer.
    String(Box<[u8]>),
}

impl StoredValue {
    #[must_use]
    fn string_len(&self) -> usize {
        match self {
            Self::String(payload) => payload.len(),
        }
    }

    #[must_use]
    fn string_bytes(&self) -> &[u8] {
        match self {
            Self::String(payload) => payload.as_ref(),
        }
    }

    #[must_use]
    fn clone_string_bytes(&self) -> Vec<u8> {
        self.string_bytes().to_vec()
    }

    #[must_use]
    fn into_string_bytes(self) -> Vec<u8> {
        match self {
            Self::String(payload) => payload.into_vec(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
/// One prime-table value record with typed payload and optional expiry.
pub struct ValueEntry {
    /// User payload.
    pub value: StoredValue,
    /// Unix timestamp in seconds when the key expires.
    pub expire_at_unix_secs: Option<u64>,
}

impl ValueEntry {
    #[must_use]
    fn new_string(value: Vec<u8>, expire_at_unix_secs: Option<u64>) -> Self {
        Self {
            value: StoredValue::String(value.into_boxed_slice()),
            expire_at_unix_secs,
        }
    }

    #[must_use]
    fn string_len(&self) -> usize {
        self.value.string_len()
    }

    #[must_use]
    fn string_bytes(&self) -> &[u8] {
        self.value.string_bytes()
    }

    #[must_use]
    pub(crate) fn clone_string_bytes(&self) -> Vec<u8> {
        self.value.clone_string_bytes()
    }

    #[must_use]
    fn into_string_bytes(self) -> Vec<u8> {
        self.value.into_string_bytes()
    }
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

    /// Returns immutable map for one logical DB.
    fn db_map(&self, db: DbIndex) -> Option<&HashMap<Vec<u8>, ValueEntry>> {
        self.db_table(db).map(|table| &table.prime)
    }

    fn remove_expire_deadline(table: &mut DbTable, key: &[u8], expire_at: u64) {
        let mut should_remove_deadline = false;
        if let Some(keys) = table.expire_order.get_mut(&expire_at) {
            let _ = keys.remove(key);
            should_remove_deadline = keys.is_empty();
        }
        if should_remove_deadline {
            let _ = table.expire_order.remove(&expire_at);
        }
    }

    fn add_expire_deadline(table: &mut DbTable, key: &[u8], expire_at: u64) {
        let _ = table
            .expire_order
            .entry(expire_at)
            .or_default()
            .insert(key.to_vec());
    }

    fn update_expire_index(table: &mut DbTable, key: &[u8], expire_at: Option<u64>) {
        if let Some(previous_expire) = table.expire.get(key).copied() {
            Self::remove_expire_deadline(table, key, previous_expire);
        }
        if let Some(expire_at_unix_secs) = expire_at {
            let _ = table.expire.insert(key.to_vec(), expire_at_unix_secs);
            Self::add_expire_deadline(table, key, expire_at_unix_secs);
        } else {
            let _ = table.expire.remove(key);
        }
    }

    fn increment_slot_stat(table: &mut DbTable, key: &[u8]) {
        let slot = key_slot(key);
        let inserted = table
            .slot_keys
            .entry(slot)
            .or_default()
            .insert(key.to_vec());
        if inserted {
            table.slot_stats.increment(slot);
        }
    }

    fn increment_slot_write_stat(table: &mut DbTable, key: &[u8]) {
        table.slot_stats.increment_write(key_slot(key));
    }

    fn increment_slot_read_stat(table: &mut DbTable, key: &[u8]) {
        table.slot_stats.increment_read(key_slot(key));
    }

    fn slot_payload_bytes(key: &[u8], entry: &ValueEntry) -> usize {
        key.len().saturating_add(entry.string_len())
    }

    fn account_upsert_slot_memory(
        table: &mut DbTable,
        key: &[u8],
        previous: Option<&ValueEntry>,
        next_value_len: usize,
    ) {
        let slot = key_slot(key);
        let next_bytes = key.len().saturating_add(next_value_len);
        let previous_bytes = previous.map_or(0, |entry| Self::slot_payload_bytes(key, entry));
        if next_bytes >= previous_bytes {
            table
                .slot_stats
                .add_memory_bytes(slot, next_bytes.saturating_sub(previous_bytes));
        } else {
            table
                .slot_stats
                .sub_memory_bytes(slot, previous_bytes.saturating_sub(next_bytes));
        }
    }

    fn account_remove_slot_memory(table: &mut DbTable, key: &[u8], removed: &ValueEntry) {
        table
            .slot_stats
            .sub_memory_bytes(key_slot(key), Self::slot_payload_bytes(key, removed));
    }

    fn decrement_slot_stat(table: &mut DbTable, key: &[u8]) {
        let slot = key_slot(key);
        let mut should_remove_slot = false;
        if let Some(keys) = table.slot_keys.get_mut(&slot) {
            if keys.remove(key) {
                should_remove_slot = table.slot_stats.decrement(slot) == 0;
            }
            should_remove_slot |= keys.is_empty();
        } else if table.slot_stats.get(slot) > 0 {
            should_remove_slot = table.slot_stats.decrement(slot) == 0;
        }
        if should_remove_slot {
            table.slot_stats.clear(slot);
            let _ = table.slot_keys.remove(&slot);
        }
    }

    fn upsert_key(&mut self, db: DbIndex, key: &[u8], value: ValueEntry) -> Option<ValueEntry> {
        let key_bytes = key.to_vec();
        let expire_at = value.expire_at_unix_secs;
        let next_value_len = value.string_len();
        let table = self.db_table_mut(db);
        let previous = table.prime.insert(key_bytes.clone(), value);
        Self::update_expire_index(table, key, expire_at);
        Self::increment_slot_stat(table, key);
        Self::account_upsert_slot_memory(table, key, previous.as_ref(), next_value_len);
        Self::increment_slot_write_stat(table, key);
        previous
    }

    fn remove_key(&mut self, db: DbIndex, key: &[u8]) -> Option<ValueEntry> {
        let table = self.db_tables.get_mut(&db)?;
        let removed = table.prime.remove(key)?;
        if let Some(previous_expire) = table.expire.remove(key) {
            Self::remove_expire_deadline(table, key, previous_expire);
        }
        Self::account_remove_slot_memory(table, key, &removed);
        Self::decrement_slot_stat(table, key);
        Self::increment_slot_write_stat(table, key);
        Some(removed)
    }

    fn set_key_expire(&mut self, db: DbIndex, key: &[u8], expire_at: Option<u64>) -> bool {
        let Some(table) = self.db_tables.get_mut(&db) else {
            return false;
        };
        let Some(entry) = table.prime.get_mut(key) else {
            return false;
        };
        entry.expire_at_unix_secs = expire_at;
        Self::update_expire_index(table, key, expire_at);
        Self::increment_slot_write_stat(table, key);
        true
    }

    fn slot_candidate_keys(table: &DbTable, slot: u16) -> Vec<Vec<u8>> {
        let mut indexed = table
            .slot_keys
            .get(&slot)
            .map(|keys| keys.iter().cloned().collect::<Vec<_>>())
            .unwrap_or_default();
        indexed.retain(|key| table.prime.contains_key(key.as_slice()));

        // Slot index is the fast path. If it is empty (or cardinality counters claim
        // there should be more keys), fall back to a bounded slot scan in `prime` to
        // heal possible drift from previous recovery/import stages.
        let tracked_count = table.slot_stats.key_count(slot);
        if indexed.is_empty() || indexed.len() < tracked_count {
            let mut seen = indexed.iter().cloned().collect::<HashSet<_>>();
            for key in table.prime.keys() {
                if key_slot(key) == slot && seen.insert(key.clone()) {
                    indexed.push(key.clone());
                }
            }
        }
        indexed
    }

    /// Returns all keys tracked for one cluster slot in one logical DB.
    #[must_use]
    pub fn slot_keys(&self, db: DbIndex, slot: u16) -> Vec<Vec<u8>> {
        let mut keys = self
            .db_table(db)
            .map(|table| Self::slot_candidate_keys(table, slot))
            .unwrap_or_default();
        keys.sort();
        keys
    }

    fn reconcile_slot_membership(&mut self, db: DbIndex, slot: u16, live: &[Vec<u8>]) {
        let Some(table) = self.db_tables.get_mut(&db) else {
            return;
        };
        if live.is_empty() {
            table.slot_stats.clear(slot);
            table.slot_stats.set_memory_bytes(slot, 0);
            let _ = table.slot_keys.remove(&slot);
            return;
        }
        let memory_bytes = live
            .iter()
            .filter_map(|key| {
                table
                    .prime
                    .get(key)
                    .map(|entry| Self::slot_payload_bytes(key, entry))
            })
            .sum::<usize>();
        table.slot_stats.set_count(slot, live.len());
        table.slot_stats.set_memory_bytes(slot, memory_bytes);
        let _ = table
            .slot_keys
            .insert(slot, live.iter().cloned().collect::<HashSet<_>>());
    }

    fn collect_live_slot_keys(&mut self, db: DbIndex, slot: u16) -> Vec<Vec<u8>> {
        let Some(candidate_keys) = self
            .db_table(db)
            .map(|table| Self::slot_candidate_keys(table, slot))
        else {
            return Vec::new();
        };

        let now = Self::now_unix_seconds();
        let mut live = Vec::new();
        for key in candidate_keys {
            let expire_at = self
                .db_table(db)
                .and_then(|table| table.prime.get(&key))
                .and_then(|entry| entry.expire_at_unix_secs);
            match expire_at {
                Some(expire_at_unix_secs) if expire_at_unix_secs <= now => {
                    if self.remove_key(db, &key).is_some() {
                        self.bump_key_version(db, &key);
                    }
                }
                Some(_) | None => {
                    if self
                        .db_table(db)
                        .and_then(|table| table.prime.get(&key))
                        .is_some()
                    {
                        live.push(key);
                    }
                }
            }
        }
        self.reconcile_slot_membership(db, slot, &live);
        live
    }

    /// Returns live keys for one slot and lazily purges already-expired entries.
    #[must_use]
    pub fn slot_keys_live(&mut self, db: DbIndex, slot: u16) -> Vec<Vec<u8>> {
        let mut live = self.collect_live_slot_keys(db, slot);
        live.sort_unstable();
        live
    }

    /// Returns live key count for one slot and lazily purges already-expired entries.
    ///
    /// Snapshot loads can temporarily materialize expired keys until active expiry runs.
    /// Slot-cardinality calls trigger bounded lazy cleanup so slot indexes converge
    /// without waiting for a periodic maintenance pass.
    #[must_use]
    pub fn count_live_keys_in_slot(&mut self, db: DbIndex, slot: u16) -> usize {
        self.collect_live_slot_keys(db, slot).len()
    }

    /// Returns current slot-level counters for one logical DB.
    #[must_use]
    pub fn slot_stats_snapshot(&self, db: DbIndex, slot: u16) -> SlotStatsSnapshot {
        let Some(table) = self.db_table(db) else {
            return SlotStatsSnapshot::default();
        };
        SlotStatsSnapshot {
            key_count: table.slot_stats.key_count(slot),
            total_reads: table.slot_stats.total_reads(slot),
            total_writes: table.slot_stats.total_writes(slot),
            memory_bytes: table.slot_stats.memory_bytes(slot),
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

    /// Loads one snapshot key into a DB table and rebuilds derived indexes/counters.
    ///
    /// Snapshot recovery mutates the underlying `prime` table directly instead of going through
    /// user command handlers. This helper keeps layered state aligned (`expire` index, slot
    /// cardinality, slot memory accounting, and key versions) without incrementing runtime
    /// write counters that represent live command traffic only.
    pub fn load_snapshot_entry(
        &mut self,
        db: DbIndex,
        key: &[u8],
        value: Vec<u8>,
        expire_at_unix_secs: Option<u64>,
    ) {
        let next_value_len = value.len();
        let table = self.db_table_mut(db);
        let previous = table.prime.insert(
            key.to_vec(),
            ValueEntry::new_string(value, expire_at_unix_secs),
        );
        Self::update_expire_index(table, key, expire_at_unix_secs);
        Self::increment_slot_stat(table, key);
        Self::account_upsert_slot_memory(table, key, previous.as_ref(), next_value_len);
        self.bump_key_version(db, key);
    }

    /// Removes one key when its expiration timestamp has already passed.
    fn purge_expired_key(&mut self, db: DbIndex, key: &[u8]) {
        let should_remove = self
            .db_table(db)
            .and_then(|table| table.expire.get(key))
            .copied()
            .is_some_and(|expire_at| expire_at <= Self::now_unix_seconds());

        if should_remove && self.remove_key(db, key).is_some() {
            self.bump_key_version(db, key);
        }
    }

    /// Runs one active-expiration pass on this shard state.
    ///
    /// Dragonfly uses periodic expiration fibers to reclaim stale keys even
    /// when clients do not touch them. This pass mirrors that behavior by
    /// scanning the expiration index and deleting up to `limit` expired keys.
    ///
    /// Returns the number of keys physically removed from this shard state.
    pub fn active_expire_pass(&mut self, limit: usize) -> usize {
        if limit == 0 {
            return 0;
        }

        let now = Self::now_unix_seconds();
        let mut dbs = self.db_tables.keys().copied().collect::<Vec<_>>();
        dbs.sort_unstable();
        if dbs.is_empty() {
            return 0;
        }

        let start = self.active_expire_db_cursor % dbs.len();
        let mut candidates = Vec::with_capacity(limit);
        let mut cycle_start = start;
        while candidates.len() < limit {
            let mut progressed = false;
            for offset in 0..dbs.len() {
                if candidates.len() == limit {
                    break;
                }

                let index = (cycle_start + offset) % dbs.len();
                let db = dbs[index];
                let Some(table) = self.db_tables.get_mut(&db) else {
                    continue;
                };
                let Some(key) = Self::pop_next_expired_key(table, now) else {
                    continue;
                };
                candidates.push((db, key));
                progressed = true;
            }

            if !progressed {
                break;
            }
            cycle_start = (cycle_start + 1) % dbs.len();
        }
        self.active_expire_db_cursor = (start + 1) % dbs.len();

        let mut removed = 0_usize;
        for (db, key) in candidates {
            if self.remove_key(db, &key).is_some() {
                self.bump_key_version(db, &key);
                removed = removed.saturating_add(1);
            }
        }
        removed
    }

    fn pop_next_expired_key(table: &mut DbTable, now: u64) -> Option<Vec<u8>> {
        let earliest_deadline = table
            .expire_order
            .first_key_value()
            .map(|(deadline, _)| *deadline)?;
        if earliest_deadline > now {
            return None;
        }

        let (expired_key, should_remove_deadline) = {
            let keys = table.expire_order.get_mut(&earliest_deadline)?;
            let key = keys.iter().next().cloned()?;
            let _ = keys.remove(&key);
            (key, keys.is_empty())
        };
        if should_remove_deadline {
            let _ = table.expire_order.remove(&earliest_deadline);
        }
        let _ = table.expire.remove(&expired_key);
        Some(expired_key)
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
        self.register_multi_key_string_commands();
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

    fn register_multi_key_string_commands(&mut self) {
        self.register(CommandSpec {
            name: "MSET",
            arity: CommandArity::AtLeast(2),
            handler: handle_mset,
        });
        self.register(CommandSpec {
            name: "MSETNX",
            arity: CommandArity::AtLeast(2),
            handler: handle_msetnx,
        });
        self.register(CommandSpec {
            name: "MGET",
            arity: CommandArity::AtLeast(1),
            handler: handle_mget,
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
        (spec.handler)(db, frame, state)
    }
}

#[cfg(test)]
#[path = "dispatch/tests.rs"]
mod tests;

