//! Process composition root for `dfly-server`.

mod runtime_exec;

use crate::network::{ServerReactorConfig, ThreadedServerReactor};
use dfly_cluster::ClusterModule;
use dfly_cluster::slot::key_slot;
use dfly_common::config::{ClusterMode, RuntimeConfig};
use dfly_common::error::DflyError;
use dfly_common::error::DflyResult;
use dfly_common::ids::TxId;
use dfly_core::CommandRouting;
use dfly_core::CoreSnapshot;
use dfly_core::SharedCore;
use dfly_core::command::{CommandFrame, CommandReply};
use dfly_core::runtime::InMemoryShardRuntime;
use dfly_facade::FacadeModule;
use dfly_facade::connection::ConnectionContext;
use dfly_facade::connection::ConnectionState;
use dfly_facade::protocol::{ClientProtocol, ParseStatus, ParsedCommand, parse_next_command};
use dfly_replication::ReplicationModule;
use dfly_replication::journal::{JournalEntry, JournalOp};
use dfly_replication::state::{FlowSyncType, SyncSessionError};
use dfly_search::SearchModule;
use dfly_storage::StorageModule;
use dfly_tiering::TieringModule;
use dfly_transaction::TransactionModule;
use dfly_transaction::plan::{TransactionHop, TransactionMode, TransactionPlan};
use dfly_transaction::scheduler::TransactionScheduler;
use dfly_transaction::session::TransactionSession;
use std::collections::{HashMap, HashSet};
use std::time::Duration;

const ACTIVE_EXPIRE_KEYS_PER_TICK: usize = 64;

/// Unit 0 composition container.
///
/// The fields intentionally mirror Dragonfly's major subsystem boundaries so later units can
/// replace placeholders with real implementations without changing the overall process topology.
#[derive(Debug)]
pub struct ServerApp {
    /// Runtime configuration.
    pub config: RuntimeConfig,
    /// Connection/protocol entry layer.
    pub facade: FacadeModule,
    /// Core routing and command model layer.
    pub core: SharedCore,
    /// Shard-thread runtime queue layer.
    pub runtime: InMemoryShardRuntime,
    /// Transaction planner/scheduler layer.
    pub transaction: TransactionModule,
    /// Storage layer.
    pub storage: StorageModule,
    /// Replication layer.
    pub replication: ReplicationModule,
    /// Cluster orchestration layer.
    pub cluster: ClusterModule,
    /// Search subsystem.
    pub search: SearchModule,
    /// Tiered-storage subsystem.
    pub tiering: TieringModule,
    /// Monotonic transaction id allocator.
    next_txid: TxId,
    /// Monotonic id used for implicit replica endpoint registration on ACK-only connections.
    next_implicit_replica_id: u64,
}

impl ServerApp {
    /// Creates a process composition from runtime config.
    #[must_use]
    pub fn new(config: RuntimeConfig) -> Self {
        let facade = FacadeModule::from_config(&config);
        let core = SharedCore::new(config.shard_count);
        let runtime_core = core.clone();
        let maintenance_core = core.clone();
        let runtime = InMemoryShardRuntime::new_with_executor_and_maintenance(
            config.shard_count,
            move |envelope| {
                runtime_core.execute_on_shard_in_db(
                    envelope.target_shard,
                    envelope.db,
                    &envelope.command,
                )
            },
            move |shard| {
                let _ = maintenance_core
                    .active_expire_pass_on_shard(shard, ACTIVE_EXPIRE_KEYS_PER_TICK);
            },
        );
        let transaction = TransactionModule::new(config.shard_count);
        let storage = StorageModule::new();
        let replication = ReplicationModule::new(true);
        let cluster = ClusterModule::new(config.cluster_mode);
        let search = SearchModule::new(true);
        let tiering = TieringModule::new(true);

        Self {
            config,
            facade,
            core,
            runtime,
            transaction,
            storage,
            replication,
            cluster,
            search,
            tiering,
            next_txid: 1,
            next_implicit_replica_id: 1,
        }
    }

    /// Human-readable startup summary.
    #[must_use]
    pub fn startup_summary(&self) -> String {
        format!(
            "dfly-server bootstrap: shard_count={}, io_threads={}, redis_port={}, memcached_port={:?}, \
core_mod={:?}, tx_mod={:?}, storage_mod={:?}, repl_enabled={}, cluster_mode={:?}, search_enabled={}, tiering_enabled={}",
            self.config.shard_count.get(),
            self.facade.io_thread_count,
            self.facade.redis_port,
            self.facade.memcached_port,
            self.core,
            self.transaction,
            self.storage,
            self.replication.enabled,
            self.cluster.mode,
            self.search.enabled,
            self.tiering.enabled
        )
    }

    /// Builds serialized snapshot bytes from current in-memory core state.
    ///
    /// # Errors
    ///
    /// Returns `DflyError::Protocol` when snapshot payload encoding fails.
    pub fn create_snapshot_bytes(&self) -> DflyResult<Vec<u8>> {
        let snapshot = self.core.export_snapshot();
        self.storage.serialize_snapshot(&snapshot)
    }

    /// Replaces current in-memory state by decoding and importing snapshot bytes.
    ///
    /// # Errors
    ///
    /// Returns `DflyError::Protocol` when payload cannot be decoded or contains invalid shard ids.
    pub fn load_snapshot_bytes(&mut self, payload: &[u8]) -> DflyResult<()> {
        let snapshot = self.storage.deserialize_snapshot(payload)?;
        self.install_snapshot(&snapshot)
    }

    fn install_snapshot(&mut self, snapshot: &CoreSnapshot) -> DflyResult<()> {
        self.core.import_snapshot(snapshot)?;
        self.replication.reset_after_snapshot_load();
        Ok(())
    }

    /// Replays currently buffered replication journal entries into the in-memory core state.
    ///
    /// # Errors
    ///
    /// Returns `DflyError::Protocol` when a journal payload is not a valid single RESP command
    /// or when command execution returns an error reply.
    pub fn recover_from_replication_journal(&mut self) -> DflyResult<usize> {
        let entries = self.replication.journal_entries();
        self.replay_journal_entries(&entries)
    }

    /// Replays journal entries from one specific starting LSN (inclusive).
    ///
    /// # Errors
    ///
    /// Returns `DflyError::Protocol` when starting LSN is stale (already evicted from backlog),
    /// when payload parsing fails, or when replayed command execution fails.
    pub fn recover_from_replication_journal_from_lsn(
        &mut self,
        start_lsn: u64,
    ) -> DflyResult<usize> {
        let current_lsn = self.replication.journal_lsn();
        if start_lsn == current_lsn {
            return Ok(0);
        }
        if !self.replication.journal_contains_lsn(start_lsn) {
            return Err(DflyError::Protocol(format!(
                "journal LSN {start_lsn} is not available in in-memory backlog (current lsn {current_lsn})"
            )));
        }

        let Some(entries) = self.replication.journal_entries_from_lsn(start_lsn) else {
            return Err(DflyError::Protocol(format!(
                "journal LSN {start_lsn} is not available in in-memory backlog (current lsn {current_lsn})"
            )));
        };
        self.replay_journal_entries(&entries)
    }

    /// Creates a new logical client connection state.
    ///
    /// A real server would attach this state to a socket file descriptor. Here we keep it as
    /// an explicit object to make parser/dispatcher flow easy to test.
    #[must_use]
    pub fn new_connection(protocol: ClientProtocol) -> ServerConnection {
        let context = ConnectionContext {
            protocol,
            db_index: 0,
            privileged: false,
        };
        ServerConnection {
            parser: ConnectionState::new(context.clone()),
            context,
            transaction: TransactionSession::default(),
            replica_endpoint: None,
            replica_client_id: None,
            replica_client_version: None,
        }
    }

    /// Detaches one connection and releases worker-owned/parser-replication resources.
    ///
    /// Network ingress uses this when a TCP peer disconnects so worker-local parser state and
    /// replica endpoint identity do not leak across reused OS sockets.
    pub fn disconnect_connection(&mut self, connection: &mut ServerConnection) {
        if let Some(endpoint) = connection.replica_endpoint.take() {
            let _ = self
                .replication
                .remove_replica_endpoint(&endpoint.address, endpoint.listening_port);
        }
    }

    pub(crate) fn execute_parsed_command_deferred(
        &mut self,
        connection: &mut ServerConnection,
        parsed: ParsedCommand,
    ) -> ParsedCommandExecution {
        // Protocol parser has already normalized command shape, so dispatcher can operate
        // on a stable frame independent from socket/wire details.
        let frame = CommandFrame::new(parsed.name, parsed.args);

        // Redis `MULTI/EXEC/DISCARD` is connection-scoped control flow and is handled
        // before core command execution, matching Dragonfly's high-level transaction entry.
        if connection.context.protocol == ClientProtocol::Resp {
            if let Some(select_reply) = Self::handle_resp_select(connection, &frame) {
                return ParsedCommandExecution::Immediate(Some(select_reply));
            }

            if let Some(transaction_reply) =
                self.handle_resp_transaction_control(connection, &frame)
            {
                return ParsedCommandExecution::Immediate(Some(transaction_reply));
            }

            if connection.transaction.in_multi() {
                return ParsedCommandExecution::Immediate(Some(
                    match self.validate_queued_command(&frame) {
                        Ok(()) => {
                            let queued = connection.transaction.queue_command(frame);
                            if queued {
                                CommandReply::SimpleString("QUEUED".to_owned()).to_resp_bytes()
                            } else {
                                CommandReply::Error("transaction queue is unavailable".to_owned())
                                    .to_resp_bytes()
                            }
                        }
                        Err(message) => {
                            connection.transaction.mark_queued_error();
                            CommandReply::Error(message).to_resp_bytes()
                        }
                    },
                ));
            }
        }

        if frame.name == "REPLCONF" {
            let reply = self.execute_replconf(connection, &frame);
            if is_replconf_ack_command(&frame) && !matches!(&reply, CommandReply::Error(_)) {
                // Dragonfly does not interleave ACK replies with journal stream writes.
                // We model the same behavior by consuming successful ACK silently.
                return ParsedCommandExecution::Immediate(None);
            }
            return ParsedCommandExecution::Immediate(Some(encode_reply_for_protocol(
                connection.context.protocol,
                &frame,
                reply,
            )));
        }

        match self.try_dispatch_parsed_command_runtime_deferred(connection, &frame) {
            Ok(Some(ticket)) => {
                return ParsedCommandExecution::Deferred(ticket);
            }
            Ok(None) => {}
            Err(error) => {
                return ParsedCommandExecution::Immediate(Some(encode_reply_for_protocol(
                    connection.context.protocol,
                    &frame,
                    CommandReply::Error(format!("runtime dispatch failed: {error}")),
                )));
            }
        }

        let db = connection.context.db_index;
        let reply = self.execute_user_command(db, &frame, None);
        if is_replconf_ack_command(&frame) && !matches!(&reply, CommandReply::Error(_)) {
            // Dragonfly does not interleave ACK replies with journal stream writes.
            // We model the same behavior by consuming successful ACK silently.
            return ParsedCommandExecution::Immediate(None);
        }

        // Unit 1 keeps protocol-specific encoding at the facade edge. This mirrors Dragonfly
        // where execution result is translated back to client protocol right before writeback.
        ParsedCommandExecution::Immediate(Some(encode_reply_for_protocol(
            connection.context.protocol,
            &frame,
            reply,
        )))
    }

    fn handle_resp_select(
        connection: &mut ServerConnection,
        frame: &CommandFrame,
    ) -> Option<Vec<u8>> {
        if frame.name != "SELECT" {
            return None;
        }
        let in_multi = connection.transaction.in_multi();
        if frame.args.len() != 1 {
            if in_multi {
                connection.transaction.mark_queued_error();
            }
            return Some(wrong_arity("SELECT"));
        }
        if in_multi {
            connection.transaction.mark_queued_error();
            return Some(
                CommandReply::Error("SELECT is not allowed in MULTI".to_owned()).to_resp_bytes(),
            );
        }

        let db_index = match parse_db_index_arg(&frame.args[0]) {
            Ok(db_index) => db_index,
            Err(error) => return Some(CommandReply::Error(error).to_resp_bytes()),
        };
        connection.context.db_index = db_index;
        Some(CommandReply::SimpleString("OK".to_owned()).to_resp_bytes())
    }

    /// Handles RESP transaction control commands.
    ///
    /// Returns `Some(encoded_reply)` when the command is a transaction control verb.
    fn handle_resp_transaction_control(
        &mut self,
        connection: &mut ServerConnection,
        frame: &CommandFrame,
    ) -> Option<Vec<u8>> {
        match frame.name.as_str() {
            "MULTI" => Some(Self::handle_multi(connection, frame)),
            "EXEC" => Some(self.handle_exec(connection, frame)),
            "DISCARD" => Some(Self::handle_discard(connection, frame)),
            "WATCH" => Some(self.handle_watch(connection, frame)),
            "UNWATCH" => Some(Self::handle_unwatch(connection, frame)),
            _ => None,
        }
    }

    fn handle_multi(connection: &mut ServerConnection, frame: &CommandFrame) -> Vec<u8> {
        let in_multi = connection.transaction.in_multi();
        if !frame.args.is_empty() {
            if in_multi {
                connection.transaction.mark_queued_error();
            }
            return wrong_arity("MULTI");
        }
        if connection.transaction.begin_multi() {
            return CommandReply::SimpleString("OK".to_owned()).to_resp_bytes();
        }
        connection.transaction.mark_queued_error();
        CommandReply::Error("MULTI calls can not be nested".to_owned()).to_resp_bytes()
    }

    fn handle_discard(connection: &mut ServerConnection, frame: &CommandFrame) -> Vec<u8> {
        let in_multi = connection.transaction.in_multi();
        if !frame.args.is_empty() {
            if in_multi {
                connection.transaction.mark_queued_error();
            }
            return wrong_arity("DISCARD");
        }
        if connection.transaction.discard() {
            return CommandReply::SimpleString("OK".to_owned()).to_resp_bytes();
        }
        CommandReply::Error("DISCARD without MULTI".to_owned()).to_resp_bytes()
    }

    fn handle_exec(&mut self, connection: &mut ServerConnection, frame: &CommandFrame) -> Vec<u8> {
        let in_multi = connection.transaction.in_multi();
        if !frame.args.is_empty() {
            if in_multi {
                connection.transaction.mark_queued_error();
            }
            return wrong_arity("EXEC");
        }
        if !in_multi {
            return CommandReply::Error("EXEC without MULTI".to_owned()).to_resp_bytes();
        }
        if connection.transaction.has_queued_error() {
            let _ = connection.transaction.discard();
            return CommandReply::Error(
                "EXECABORT Transaction discarded because of previous errors".to_owned(),
            )
            .to_resp_bytes();
        }
        if connection
            .transaction
            .watching_other_dbs(connection.context.db_index)
        {
            let _ = connection.transaction.discard();
            return CommandReply::Error(
                "Dragonfly does not allow WATCH and EXEC on different databases".to_owned(),
            )
            .to_resp_bytes();
        }
        if !connection
            .transaction
            .watched_keys_are_clean(|db, key| self.core.key_version(db, key))
        {
            let _ = connection.transaction.discard();
            return CommandReply::NullArray.to_resp_bytes();
        }
        let Some(queued_commands) = connection.transaction.take_queued_for_exec() else {
            return CommandReply::Error("EXEC without MULTI".to_owned()).to_resp_bytes();
        };
        if queued_commands.is_empty() {
            return CommandReply::Array(Vec::new()).to_resp_bytes();
        }

        let plan = self.build_exec_plan(&queued_commands);
        if let Err(error) = self.transaction.scheduler.schedule(&plan) {
            return CommandReply::Error(format!("transaction scheduling failed: {error}"))
                .to_resp_bytes();
        }

        let db = connection.context.db_index;
        let replies = self.execute_transaction_plan(db, &plan);
        if let Err(error) = self.transaction.scheduler.conclude(plan.txid) {
            return CommandReply::Error(format!("transaction completion failed: {error}"))
                .to_resp_bytes();
        }
        CommandReply::Array(replies).to_resp_bytes()
    }

    fn handle_watch(&mut self, connection: &mut ServerConnection, frame: &CommandFrame) -> Vec<u8> {
        let in_multi = connection.transaction.in_multi();
        if frame.args.is_empty() {
            if in_multi {
                connection.transaction.mark_queued_error();
            }
            return wrong_arity("WATCH");
        }
        if in_multi {
            connection.transaction.mark_queued_error();
            return CommandReply::Error("WATCH inside MULTI is not allowed".to_owned())
                .to_resp_bytes();
        }

        let db = connection.context.db_index;
        for key in &frame.args {
            let version = self.core.key_version(db, key);
            let _ = connection.transaction.watch_key(db, key.clone(), version);
        }
        CommandReply::SimpleString("OK".to_owned()).to_resp_bytes()
    }

    fn handle_unwatch(connection: &mut ServerConnection, frame: &CommandFrame) -> Vec<u8> {
        let in_multi = connection.transaction.in_multi();
        if !frame.args.is_empty() {
            if in_multi {
                connection.transaction.mark_queued_error();
            }
            return wrong_arity("UNWATCH");
        }
        if in_multi {
            connection.transaction.mark_queued_error();
            return CommandReply::Error("UNWATCH inside MULTI is not allowed".to_owned())
                .to_resp_bytes();
        }

        connection.transaction.unwatch();
        CommandReply::SimpleString("OK".to_owned()).to_resp_bytes()
    }

    fn validate_queued_command(&self, frame: &CommandFrame) -> Result<(), String> {
        if matches!(frame.name.as_str(), "FLUSHDB" | "FLUSHALL") {
            return Err(format!("'{}' not allowed inside a transaction", frame.name));
        }

        match self.core.validate_command(frame) {
            Ok(()) => return Ok(()),
            Err(message) if !message.starts_with("unknown command ") => return Err(message),
            Err(_) => {}
        }

        Self::validate_non_core_queued_command(frame)
    }

    fn validate_non_core_queued_command(frame: &CommandFrame) -> Result<(), String> {
        match frame.name.as_str() {
            "INFO" => Ok(()),
            "KEYS" => (frame.args.len() == 1)
                .then_some(())
                .ok_or_else(|| wrong_arity_message("KEYS")),
            "TIME" | "ROLE" | "READONLY" | "READWRITE" | "DBSIZE" | "RANDOMKEY" => frame
                .args
                .is_empty()
                .then_some(())
                .ok_or_else(|| wrong_arity_message(frame.name.as_str())),
            "PSYNC" | "WAIT" => (frame.args.len() == 2)
                .then_some(())
                .ok_or_else(|| wrong_arity_message(frame.name.as_str())),
            "CLUSTER" | "DFLY" | "MGET" => (!frame.args.is_empty())
                .then_some(())
                .ok_or_else(|| wrong_arity_message(frame.name.as_str())),
            "MSET" | "MSETNX" => {
                if frame.args.is_empty() || !frame.args.len().is_multiple_of(2) {
                    Err(wrong_arity_message(frame.name.as_str()))
                } else {
                    Ok(())
                }
            }
            "REPLCONF" => {
                if frame.args.is_empty() || !frame.args.len().is_multiple_of(2) {
                    Err("syntax error".to_owned())
                } else {
                    Ok(())
                }
            }
            _ => Err(format!("unknown command '{}'", frame.name)),
        }
    }

    fn build_exec_plan(&mut self, queued_commands: &[CommandFrame]) -> TransactionPlan {
        let txid = self.allocate_txid();
        let touched_shards = self.collect_plan_touched_shards(queued_commands);
        let mut all_single_key = true;
        let mut all_read_only_single_key = true;
        for command in queued_commands {
            match self.core.command_routing(command) {
                CommandRouting::SingleKey { is_write } => {
                    if is_write {
                        all_read_only_single_key = false;
                    }
                }
                CommandRouting::NonKey => {
                    all_single_key = false;
                    all_read_only_single_key = false;
                }
            }
        }

        if all_read_only_single_key {
            return TransactionPlan {
                txid,
                mode: TransactionMode::NonAtomic,
                hops: self.build_single_key_hops(queued_commands),
                touched_shards: touched_shards.clone(),
            };
        }
        if all_single_key {
            return TransactionPlan {
                txid,
                mode: TransactionMode::LockAhead,
                hops: self.build_single_key_hops(queued_commands),
                touched_shards: touched_shards.clone(),
            };
        }

        let hops = self.build_global_hops(queued_commands);
        TransactionPlan {
            txid,
            mode: TransactionMode::Global,
            hops,
            touched_shards,
        }
    }

    fn collect_plan_touched_shards(&self, queued_commands: &[CommandFrame]) -> Vec<u16> {
        // Scheduler reservation follows planner dependency footprints. For known multi-shard
        // commands this is the exact runtime fanout; for non-key commands with unknown routing
        // we conservatively reserve all shards to preserve global ordering barriers.
        let mut touched = queued_commands
            .iter()
            .flat_map(|command| self.planner_dependency_shards_for_command(command))
            .collect::<Vec<_>>();
        touched.sort_unstable();
        touched.dedup();
        touched
    }

    fn build_single_key_hops(&self, queued_commands: &[CommandFrame]) -> Vec<TransactionHop> {
        // One hop is one "parallelizable wave": at most one command per shard while preserving
        // original command order across hops.
        let mut hops = Vec::new();
        let mut current_hop = TransactionHop {
            per_shard: Vec::new(),
        };
        let mut occupied_shards = HashSet::new();

        for command in queued_commands.iter().cloned() {
            let shard = self.core.resolve_target_shard(&command);
            if occupied_shards.contains(&shard) {
                hops.push(current_hop);
                current_hop = TransactionHop {
                    per_shard: Vec::new(),
                };
                occupied_shards.clear();
            }
            current_hop.per_shard.push((shard, command));
            let _ = occupied_shards.insert(shard);
        }
        if !current_hop.per_shard.is_empty() {
            hops.push(current_hop);
        }
        hops
    }

    fn build_global_hops(&self, queued_commands: &[CommandFrame]) -> Vec<TransactionHop> {
        // Dependency-aware hop layering for mixed workloads.
        //
        // Each command contributes a shard footprint. The command is placed in the earliest hop
        // that satisfies:
        // 1) all conflicting shard footprints from earlier commands are in previous hops;
        // 2) hop indices never decrease, so reply order remains identical to input order.
        //
        // This is a stable topological layering over shard-conflict edges.
        let mut hops = Vec::<TransactionHop>::new();
        let mut last_hop_by_shard = HashMap::<u16, usize>::new();
        let mut last_assigned_hop = 0_usize;

        for (command_index, command) in queued_commands.iter().cloned().enumerate() {
            let dependency_shards = self.planner_dependency_shards_for_command(&command);

            let dependency_hop = dependency_shards
                .iter()
                .filter_map(|shard| last_hop_by_shard.get(shard).copied())
                .max()
                .map_or(0_usize, |hop| hop.saturating_add(1));
            let hop_index = if command_index == 0 {
                dependency_hop
            } else {
                dependency_hop.max(last_assigned_hop)
            };

            while hops.len() <= hop_index {
                hops.push(TransactionHop {
                    per_shard: Vec::new(),
                });
            }

            hops[hop_index]
                .per_shard
                .push((self.core.resolve_target_shard(&command), command));
            for shard in dependency_shards {
                let _ = last_hop_by_shard.insert(shard, hop_index);
            }
            last_assigned_hop = hop_index;
        }

        hops
    }

    fn planner_dependency_shards_for_command(&self, frame: &CommandFrame) -> Vec<u16> {
        let runtime_targets = self.runtime_target_shards_for_command(frame);
        if !runtime_targets.is_empty() {
            return runtime_targets;
        }

        if matches!(
            self.core.command_routing(frame),
            CommandRouting::SingleKey { .. }
        ) {
            return vec![self.core.resolve_target_shard(frame)];
        }

        // Non-key command with no explicit runtime fanout hint: treat as global barrier.
        self.all_runtime_shards()
    }

    fn execute_command_without_side_effects(
        &mut self,
        db: u16,
        frame: &CommandFrame,
    ) -> CommandReply {
        match frame.name.as_str() {
            "INFO" => self.execute_info(frame),
            "TIME" => Self::execute_time(frame),
            "ROLE" => self.execute_role(frame),
            "READONLY" => Self::execute_readonly(frame),
            "READWRITE" => self.execute_readwrite(frame),
            "DBSIZE" => self.execute_dbsize(db, frame),
            "KEYS" => self.execute_keys(db, frame),
            "RANDOMKEY" => self.execute_randomkey(db, frame),
            "FLUSHDB" => self.execute_flushdb(db, frame),
            "FLUSHALL" => self.execute_flushall(frame),
            "PSYNC" => self.execute_psync(frame),
            "WAIT" => self.execute_wait(frame),
            "CLUSTER" => self.execute_cluster(db, frame),
            "DFLY" => self.execute_dfly_admin(frame),
            "DEL" => self.execute_del(db, frame),
            "UNLINK" => self.execute_unlink(db, frame),
            "EXISTS" => self.execute_exists(db, frame),
            "TOUCH" => self.execute_touch(db, frame),
            "MGET" => self.execute_mget(db, frame),
            "MSET" => self.execute_mset(db, frame),
            "MSETNX" => self.execute_msetnx(db, frame),
            "COPY" => self.execute_copy(db, frame),
            "RENAME" | "RENAMENX" => self.execute_rename(db, frame),
            "GET" | "SET" | "TYPE" | "SETEX" | "PSETEX" | "GETSET" | "GETDEL" | "APPEND"
            | "STRLEN" | "MOVE" | "GETRANGE" | "SETRANGE" | "EXPIRE" | "PEXPIRE" | "EXPIREAT"
            | "PEXPIREAT" | "TTL" | "PTTL" | "EXPIRETIME" | "PEXPIRETIME" | "PERSIST" | "INCR"
            | "DECR" | "INCRBY" | "DECRBY" | "SETNX" => self.execute_key_command(db, frame),
            _ => self.core.execute_in_db(db, frame),
        }
    }

    fn execute_info(&self, frame: &CommandFrame) -> CommandReply {
        use std::fmt::Write as _;

        let mut include_replication = frame.args.is_empty();
        let mut include_persistence = frame.args.is_empty();
        for raw in &frame.args {
            let Ok(section_raw) = std::str::from_utf8(raw) else {
                return CommandReply::Error("INFO section must be valid UTF-8".to_owned());
            };

            for token in section_raw.split(',') {
                let section = token.trim().to_ascii_uppercase();
                match section.as_str() {
                    "ALL" | "DEFAULT" => {
                        include_replication = true;
                        include_persistence = true;
                    }
                    "REPLICATION" => include_replication = true,
                    "PERSISTENCE" => include_persistence = true,
                    _ => {}
                }
            }
        }

        let mut info = String::new();
        if include_replication {
            write!(
                info,
                "# Replication\r\nrole:master\r\nconnected_slaves:{}\r\nmaster_replid:{}\r\nmaster_repl_offset:{}\r\nlast_ack_lsn:{}\r\n",
                self.replication.connected_replicas(),
                self.replication.master_replid(),
                self.replication.replication_offset(),
                self.replication.last_acked_lsn(),
            )
            .expect("writing to String should not fail");
            for (index, (address, port, state, lag)) in
                self.replication.replica_info_rows().into_iter().enumerate()
            {
                write!(
                    info,
                    "slave{index}:ip={address},port={port},state={state},lag={lag}\r\n"
                )
                .expect("writing to String should not fail");
            }
        }
        if include_persistence {
            // Snapshot internals are intentionally simple in this learning path, but INFO shape
            // keeps Dragonfly-compatible section naming so section routing remains faithful.
            info.push_str("# Persistence\r\nloading:0\r\n");
        }
        CommandReply::BulkString(info.into_bytes())
    }

    fn execute_time(frame: &CommandFrame) -> CommandReply {
        if !frame.args.is_empty() {
            return CommandReply::Error("wrong number of arguments for 'TIME' command".to_owned());
        }

        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map_or((0_u64, 0_u32), |duration| {
                (duration.as_secs(), duration.subsec_micros())
            });
        CommandReply::Array(vec![
            CommandReply::BulkString(now.0.to_string().into_bytes()),
            CommandReply::BulkString(now.1.to_string().into_bytes()),
        ])
    }

    fn execute_readonly(frame: &CommandFrame) -> CommandReply {
        if !frame.args.is_empty() {
            return CommandReply::Error(
                "wrong number of arguments for 'READONLY' command".to_owned(),
            );
        }
        CommandReply::SimpleString("OK".to_owned())
    }

    fn execute_readwrite(&self, frame: &CommandFrame) -> CommandReply {
        if !frame.args.is_empty() {
            return CommandReply::Error(
                "wrong number of arguments for 'READWRITE' command".to_owned(),
            );
        }
        if self.cluster.mode != ClusterMode::Emulated {
            return CommandReply::Error(
                "Cluster is disabled. Use --cluster_mode=yes to enable.".to_owned(),
            );
        }
        CommandReply::SimpleString("OK".to_owned())
    }

    fn execute_flushdb(&mut self, db: u16, frame: &CommandFrame) -> CommandReply {
        if !frame.args.is_empty() {
            return CommandReply::Error(
                "wrong number of arguments for 'FLUSHDB' command".to_owned(),
            );
        }
        let _ = self.core.flush_db(db);
        CommandReply::SimpleString("OK".to_owned())
    }

    fn execute_dbsize(&self, db: u16, frame: &CommandFrame) -> CommandReply {
        if !frame.args.is_empty() {
            return CommandReply::Error(
                "wrong number of arguments for 'DBSIZE' command".to_owned(),
            );
        }

        CommandReply::Integer(i64::try_from(self.core.db_size(db)).unwrap_or(i64::MAX))
    }

    fn execute_randomkey(&self, db: u16, frame: &CommandFrame) -> CommandReply {
        if !frame.args.is_empty() {
            return CommandReply::Error(
                "wrong number of arguments for 'RANDOMKEY' command".to_owned(),
            );
        }

        self.core
            .random_key(db)
            .map_or(CommandReply::Null, CommandReply::BulkString)
    }

    fn execute_keys(&self, db: u16, frame: &CommandFrame) -> CommandReply {
        if frame.args.len() != 1 {
            return CommandReply::Error("wrong number of arguments for 'KEYS' command".to_owned());
        }

        let keys = self.core.keys_matching(db, &frame.args[0]);
        let replies = keys
            .into_iter()
            .map(CommandReply::BulkString)
            .collect::<Vec<_>>();
        CommandReply::Array(replies)
    }

    fn execute_flushall(&mut self, frame: &CommandFrame) -> CommandReply {
        if !frame.args.is_empty() {
            return CommandReply::Error(
                "wrong number of arguments for 'FLUSHALL' command".to_owned(),
            );
        }
        let _ = self.core.flush_all();
        CommandReply::SimpleString("OK".to_owned())
    }

    fn execute_del(&mut self, db: u16, frame: &CommandFrame) -> CommandReply {
        self.execute_counting_key_command(db, frame, "DEL")
    }

    fn execute_unlink(&mut self, db: u16, frame: &CommandFrame) -> CommandReply {
        self.execute_counting_key_command(db, frame, "UNLINK")
    }

    fn execute_exists(&mut self, db: u16, frame: &CommandFrame) -> CommandReply {
        self.execute_counting_key_command(db, frame, "EXISTS")
    }

    fn execute_touch(&mut self, db: u16, frame: &CommandFrame) -> CommandReply {
        self.execute_counting_key_command(db, frame, "TOUCH")
    }

    fn execute_counting_key_command(
        &mut self,
        db: u16,
        frame: &CommandFrame,
        command: &str,
    ) -> CommandReply {
        if frame.args.is_empty() {
            return CommandReply::Error(wrong_arity_message(command));
        }

        let mut total = 0_i64;
        for key in &frame.args {
            if let Some(moved) = self.cluster_moved_reply_for_key(key) {
                return moved;
            }

            let single_key_frame = CommandFrame::new(command, vec![key.clone()]);
            let reply = self.core.execute_in_db(db, &single_key_frame);
            let CommandReply::Integer(delta) = reply else {
                return CommandReply::Error(format!(
                    "internal error: {command} did not return integer reply"
                ));
            };
            total = total.saturating_add(delta.max(0));
        }

        CommandReply::Integer(total)
    }

    fn execute_role(&self, frame: &CommandFrame) -> CommandReply {
        if !frame.args.is_empty() {
            return CommandReply::Error("wrong number of arguments for 'ROLE' command".to_owned());
        }

        let replicas = self
            .replication
            .replica_role_rows()
            .into_iter()
            .map(|(address, listening_port, state)| {
                CommandReply::Array(vec![
                    CommandReply::BulkString(address.into_bytes()),
                    CommandReply::BulkString(listening_port.to_string().into_bytes()),
                    CommandReply::BulkString(state.as_bytes().to_vec()),
                ])
            })
            .collect::<Vec<_>>();
        // Dragonfly reports master mode as:
        // ["master", [[replica_ip, replica_port, replica_state], ...]]
        CommandReply::Array(vec![
            CommandReply::BulkString(b"master".to_vec()),
            CommandReply::Array(replicas),
        ])
    }

    fn execute_replconf(
        &mut self,
        connection: &mut ServerConnection,
        frame: &CommandFrame,
    ) -> CommandReply {
        if frame.args.is_empty() || !frame.args.len().is_multiple_of(2) {
            return CommandReply::Error("syntax error".to_owned());
        }

        if frame.args.len() == 2
            && frame.args[0].eq_ignore_ascii_case(b"CAPA")
            && frame.args[1].eq_ignore_ascii_case(b"dragonfly")
        {
            let sync_id = self
                .replication
                .create_sync_session(usize::from(self.config.shard_count.get()));
            return CommandReply::Array(vec![
                CommandReply::BulkString(self.replication.master_replid().as_bytes().to_vec()),
                CommandReply::BulkString(sync_id.into_bytes()),
                CommandReply::Integer(i64::from(self.config.shard_count.get())),
                CommandReply::Integer(1),
            ]);
        }

        let mut announced_port: Option<u16> = None;
        let mut announced_ip: Option<String> = None;

        for pair in frame.args.chunks_exact(2) {
            let option = pair[0].as_slice();
            let value = pair[1].as_slice();
            if option.eq_ignore_ascii_case(b"ACK") {
                if frame.args.len() != 2 {
                    return CommandReply::Error("syntax error".to_owned());
                }
                if let Err(error_reply) = self.handle_replconf_ack(connection, value) {
                    return error_reply;
                }
                continue;
            }
            if option.eq_ignore_ascii_case(b"CLIENT-ID") {
                if frame.args.len() != 2 {
                    return CommandReply::Error("syntax error".to_owned());
                }
                let Ok(text) = std::str::from_utf8(value) else {
                    return CommandReply::Error(
                        "REPLCONF CLIENT-ID must be valid UTF-8".to_owned(),
                    );
                };
                connection.replica_client_id = Some(text.to_owned());
                continue;
            }
            if option.eq_ignore_ascii_case(b"CLIENT-VERSION") {
                if frame.args.len() != 2 {
                    return CommandReply::Error("syntax error".to_owned());
                }
                let Ok(text) = std::str::from_utf8(value) else {
                    return CommandReply::Error(
                        "REPLCONF CLIENT-VERSION must be valid UTF-8".to_owned(),
                    );
                };
                let Ok(version) = text.parse::<u64>() else {
                    return CommandReply::Error(
                        "value is not an integer or out of range".to_owned(),
                    );
                };
                connection.replica_client_version = Some(version);
                continue;
            }
            if option.eq_ignore_ascii_case(b"LISTENING-PORT") {
                let Ok(text) = std::str::from_utf8(value) else {
                    return CommandReply::Error(
                        "REPLCONF LISTENING-PORT must be valid UTF-8".to_owned(),
                    );
                };
                let Ok(port) = text.parse::<u16>() else {
                    return CommandReply::Error(
                        "value is not an integer or out of range".to_owned(),
                    );
                };
                announced_port = Some(port);
                continue;
            }
            if option.eq_ignore_ascii_case(b"IP-ADDRESS") {
                let Ok(text) = std::str::from_utf8(value) else {
                    return CommandReply::Error(
                        "REPLCONF IP-ADDRESS must be valid UTF-8".to_owned(),
                    );
                };
                announced_ip = Some(text.to_owned());
                continue;
            }
            if option.eq_ignore_ascii_case(b"CAPA") {
                continue;
            }
            return CommandReply::Error("syntax error".to_owned());
        }

        self.apply_replconf_endpoint_update(connection, announced_port, announced_ip);

        CommandReply::SimpleString("OK".to_owned())
    }

    fn handle_replconf_ack(
        &mut self,
        connection: &mut ServerConnection,
        value: &[u8],
    ) -> Result<(), CommandReply> {
        let Ok(text) = std::str::from_utf8(value) else {
            return Err(CommandReply::Error(
                "REPLCONF ACK offset must be valid UTF-8".to_owned(),
            ));
        };
        let Ok(ack_lsn) = text.parse::<u64>() else {
            return Err(CommandReply::Error(
                "value is not an integer or out of range".to_owned(),
            ));
        };
        self.ensure_replica_endpoint_for_ack(connection);
        if let Some(endpoint) = connection.replica_endpoint.as_ref() {
            let _ = self.replication.record_replica_ack_for_endpoint(
                &endpoint.address,
                endpoint.listening_port,
                ack_lsn,
            );
        }
        Ok(())
    }

    fn ensure_replica_endpoint_for_ack(&mut self, connection: &mut ServerConnection) {
        if connection.replica_endpoint.is_some() {
            return;
        }

        // Some replicas may ACK before announcing LISTENING-PORT/IP metadata.
        // Register a stable synthetic endpoint so WAIT/INFO can account for that replica.
        let endpoint = ReplicaEndpointIdentity {
            address: format!("implicit-replica-{}", self.next_implicit_replica_id),
            listening_port: 0,
        };
        self.next_implicit_replica_id = self.next_implicit_replica_id.saturating_add(1);
        self.register_connection_replica_endpoint(connection, endpoint);
    }

    fn register_connection_replica_endpoint(
        &mut self,
        connection: &mut ServerConnection,
        endpoint: ReplicaEndpointIdentity,
    ) {
        if let Some(existing) = connection.replica_endpoint.clone()
            && existing != endpoint
        {
            let _ = self
                .replication
                .remove_replica_endpoint(&existing.address, existing.listening_port);
        }

        self.replication
            .register_replica_endpoint(endpoint.address.clone(), endpoint.listening_port);
        connection.replica_endpoint = Some(endpoint);
    }

    fn apply_replconf_endpoint_update(
        &mut self,
        connection: &mut ServerConnection,
        announced_port: Option<u16>,
        announced_ip: Option<String>,
    ) {
        if let Some(port) = announced_port {
            let address = announced_ip
                .or_else(|| {
                    connection
                        .replica_endpoint
                        .as_ref()
                        .map(|endpoint| endpoint.address.clone())
                })
                .unwrap_or_else(|| "127.0.0.1".to_owned());
            self.register_connection_replica_endpoint(
                connection,
                ReplicaEndpointIdentity {
                    address,
                    listening_port: port,
                },
            );
            return;
        }

        if let Some(address) = announced_ip
            && let Some(existing_port) = connection
                .replica_endpoint
                .as_ref()
                .map(|endpoint| endpoint.listening_port)
        {
            self.register_connection_replica_endpoint(
                connection,
                ReplicaEndpointIdentity {
                    address,
                    listening_port: existing_port,
                },
            );
        }
    }

    fn execute_psync(&mut self, frame: &CommandFrame) -> CommandReply {
        if frame.args.len() != 2 {
            return CommandReply::Error("wrong number of arguments for 'PSYNC' command".to_owned());
        }

        let Ok(requested_replid) = std::str::from_utf8(&frame.args[0]) else {
            return CommandReply::Error("PSYNC replid must be valid UTF-8".to_owned());
        };
        let Ok(requested_offset_text) = std::str::from_utf8(&frame.args[1]) else {
            return CommandReply::Error("PSYNC offset must be valid UTF-8".to_owned());
        };
        let Ok(requested_offset) = requested_offset_text.parse::<i64>() else {
            return CommandReply::Error("PSYNC offset must be a valid integer".to_owned());
        };

        let master_replid = self.replication.master_replid().to_owned();
        let master_offset = self.replication.replication_offset();
        let requested_offset_u64 = u64::try_from(requested_offset).ok();
        if requested_replid == master_replid
            && requested_offset_u64
                .is_some_and(|offset| self.replication.can_partial_sync_from_offset(offset))
        {
            self.replication.mark_replicas_stable_sync();
            return CommandReply::SimpleString("CONTINUE".to_owned());
        }

        self.replication.mark_replicas_full_sync();
        CommandReply::SimpleString(format!("FULLRESYNC {master_replid} {master_offset}"))
    }

    fn execute_wait(&self, frame: &CommandFrame) -> CommandReply {
        if frame.args.len() != 2 {
            return CommandReply::Error("wrong number of arguments for 'WAIT' command".to_owned());
        }

        let Ok(required_text) = std::str::from_utf8(&frame.args[0]) else {
            return CommandReply::Error("WAIT numreplicas must be valid UTF-8".to_owned());
        };
        let Ok(required) = required_text.parse::<u64>() else {
            return CommandReply::Error("value is not an integer or out of range".to_owned());
        };
        let Ok(timeout_text) = std::str::from_utf8(&frame.args[1]) else {
            return CommandReply::Error("WAIT timeout must be valid UTF-8".to_owned());
        };
        let Ok(timeout_millis) = timeout_text.parse::<u64>() else {
            return CommandReply::Error("value is not an integer or out of range".to_owned());
        };

        // WAIT returns the number of replicas that acknowledged the master offset captured at
        // WAIT entry time. We block on replication ACK progress notifications instead of polling.
        let target_offset = self.replication.replication_offset();
        let timeout = Duration::from_millis(timeout_millis);
        let wait_started_at = std::time::Instant::now();
        let current_replica_count = || {
            u64::try_from(
                self.replication
                    .acked_replica_count_at_or_above(target_offset),
            )
            .unwrap_or(u64::MAX)
        };
        let integer_reply =
            |replicated: u64| CommandReply::Integer(i64::try_from(replicated).unwrap_or(i64::MAX));

        loop {
            let observed_progress = self.replication.ack_progress_token();
            let replicated = current_replica_count();
            if replicated >= required || timeout.is_zero() {
                return integer_reply(replicated);
            }

            let elapsed = wait_started_at.elapsed();
            if elapsed >= timeout {
                return integer_reply(replicated);
            }

            let remaining = timeout.saturating_sub(elapsed);
            let progress = self
                .replication
                .wait_for_ack_progress(observed_progress, remaining);
            if !progress {
                return integer_reply(current_replica_count());
            }
        }
    }

    fn execute_dfly_flow(&mut self, frame: &CommandFrame) -> CommandReply {
        if !(frame.args.len() == 4 || frame.args.len() == 5 || frame.args.len() == 6) {
            return CommandReply::Error(
                "wrong number of arguments for 'DFLY FLOW' command".to_owned(),
            );
        }

        let Ok(master_id) = std::str::from_utf8(&frame.args[1]) else {
            return CommandReply::Error("DFLY FLOW master id must be valid UTF-8".to_owned());
        };
        if master_id != self.replication.master_replid() {
            return CommandReply::Error("bad master id".to_owned());
        }

        let Ok(sync_id) = std::str::from_utf8(&frame.args[2]) else {
            return CommandReply::Error("DFLY FLOW sync id must be valid UTF-8".to_owned());
        };
        if !self.replication.is_known_sync_session(sync_id) {
            return CommandReply::Error("syncid not found".to_owned());
        }

        let Ok(flow_id_text) = std::str::from_utf8(&frame.args[3]) else {
            return CommandReply::Error("DFLY FLOW flow id must be valid UTF-8".to_owned());
        };
        let Ok(flow_id) = flow_id_text.parse::<usize>() else {
            return CommandReply::Error("value is not an integer or out of range".to_owned());
        };
        if flow_id >= usize::from(self.config.shard_count.get()) {
            return CommandReply::Error("value is not an integer or out of range".to_owned());
        }

        let requested_offset = if frame.args.len() == 6 {
            if !frame.args[4].eq_ignore_ascii_case(b"LASTMASTER") {
                return CommandReply::Error("syntax error".to_owned());
            }
            let Ok(lsn_vec_text) = std::str::from_utf8(&frame.args[5]) else {
                return CommandReply::Error("DFLY FLOW lsn vector must be valid UTF-8".to_owned());
            };
            match parse_flow_lsn_vector_for_flow(
                lsn_vec_text,
                flow_id,
                usize::from(self.config.shard_count.get()),
            ) {
                Ok(lsn) => Some(lsn),
                Err(FlowLsnParseError::Syntax) => {
                    return CommandReply::Error("syntax error".to_owned());
                }
                Err(FlowLsnParseError::InvalidInt) => {
                    return CommandReply::Error(
                        "value is not an integer or out of range".to_owned(),
                    );
                }
            }
        } else if let Some(lsn_arg) = frame.args.get(4) {
            let Ok(lsn_text) = std::str::from_utf8(lsn_arg) else {
                return CommandReply::Error("DFLY FLOW lsn must be valid UTF-8".to_owned());
            };
            let Ok(lsn) = lsn_text.parse::<u64>() else {
                return CommandReply::Error("value is not an integer or out of range".to_owned());
            };
            Some(lsn)
        } else {
            None
        };

        let (sync_type, flow_start_offset) = requested_offset
            .filter(|offset| self.replication.can_partial_sync_from_offset(*offset))
            .map_or((FlowSyncType::Full, None), |offset| {
                (FlowSyncType::Partial, Some(offset))
            });

        let eof_token = self.replication.allocate_flow_eof_token();
        if let Err(error) = self.replication.register_sync_flow(
            sync_id,
            flow_id,
            sync_type,
            flow_start_offset,
            eof_token.clone(),
        ) {
            return sync_session_error_reply(error);
        }

        CommandReply::Array(vec![
            CommandReply::SimpleString(match sync_type {
                FlowSyncType::Full => "FULL".to_owned(),
                FlowSyncType::Partial => "PARTIAL".to_owned(),
            }),
            CommandReply::SimpleString(eof_token),
        ])
    }

    fn execute_dfly_sync(&mut self, frame: &CommandFrame) -> CommandReply {
        if frame.args.len() != 2 {
            return CommandReply::Error(
                "wrong number of arguments for 'DFLY SYNC' command".to_owned(),
            );
        }
        let Ok(sync_id) = std::str::from_utf8(&frame.args[1]) else {
            return CommandReply::Error("DFLY SYNC sync id must be valid UTF-8".to_owned());
        };
        if let Err(error) = self.replication.mark_sync_session_full_sync(sync_id) {
            return sync_session_error_reply(error);
        }

        self.replication.mark_replicas_full_sync();
        CommandReply::SimpleString("OK".to_owned())
    }

    fn execute_dfly_startstable(&mut self, frame: &CommandFrame) -> CommandReply {
        if frame.args.len() != 2 {
            return CommandReply::Error(
                "wrong number of arguments for 'DFLY STARTSTABLE' command".to_owned(),
            );
        }
        let Ok(sync_id) = std::str::from_utf8(&frame.args[1]) else {
            return CommandReply::Error("DFLY STARTSTABLE sync id must be valid UTF-8".to_owned());
        };
        if let Err(error) = self.replication.mark_sync_session_stable_sync(sync_id) {
            return sync_session_error_reply(error);
        }

        self.replication.mark_replicas_stable_sync();
        CommandReply::SimpleString("OK".to_owned())
    }

    fn execute_dfly_admin(&mut self, frame: &CommandFrame) -> CommandReply {
        let Some(subcommand_raw) = frame.args.first() else {
            return CommandReply::Error("wrong number of arguments for 'DFLY' command".to_owned());
        };
        let Ok(subcommand) = std::str::from_utf8(subcommand_raw) else {
            return CommandReply::Error("DFLY subcommand must be valid UTF-8".to_owned());
        };

        match subcommand.to_ascii_uppercase().as_str() {
            "FLOW" => self.execute_dfly_flow(frame),
            "REPLICAOFFSET" => self.execute_dfly_replicaoffset(frame),
            "SAVE" => self.execute_dfly_save(frame),
            "SYNC" => self.execute_dfly_sync(frame),
            "STARTSTABLE" => self.execute_dfly_startstable(frame),
            "LOAD" => self.execute_dfly_load(frame),
            _ => CommandReply::Error(format!("unknown DFLY subcommand '{subcommand}'")),
        }
    }

    fn execute_dfly_replicaoffset(&self, frame: &CommandFrame) -> CommandReply {
        if frame.args.len() != 1 {
            return CommandReply::Error(
                "wrong number of arguments for 'DFLY REPLICAOFFSET' command".to_owned(),
            );
        }

        let offset = self.replication.replication_offset();
        let offsets = (0..usize::from(self.config.shard_count.get()))
            .map(|_| CommandReply::Integer(i64::try_from(offset).unwrap_or(i64::MAX)))
            .collect::<Vec<_>>();
        CommandReply::Array(offsets)
    }

    fn execute_dfly_save(&mut self, frame: &CommandFrame) -> CommandReply {
        if frame.args.len() != 2 {
            return CommandReply::Error(
                "wrong number of arguments for 'DFLY SAVE' command".to_owned(),
            );
        }
        let Ok(path) = std::str::from_utf8(&frame.args[1]) else {
            return CommandReply::Error("DFLY SAVE path must be valid UTF-8".to_owned());
        };

        let snapshot = self.core.export_snapshot();
        match self.storage.write_snapshot_file(path, &snapshot) {
            Ok(()) => CommandReply::SimpleString("OK".to_owned()),
            Err(error) => CommandReply::Error(format!("DFLY SAVE failed: {error}")),
        }
    }

    fn execute_dfly_load(&mut self, frame: &CommandFrame) -> CommandReply {
        if frame.args.len() != 2 {
            return CommandReply::Error(
                "wrong number of arguments for 'DFLY LOAD' command".to_owned(),
            );
        }
        let Ok(path) = std::str::from_utf8(&frame.args[1]) else {
            return CommandReply::Error("DFLY LOAD path must be valid UTF-8".to_owned());
        };

        let snapshot = match self.storage.read_snapshot_file(path) {
            Ok(snapshot) => snapshot,
            Err(error) => return CommandReply::Error(format!("DFLY LOAD failed: {error}")),
        };
        if let Err(error) = self.install_snapshot(&snapshot) {
            return CommandReply::Error(format!("DFLY LOAD failed: {error}"));
        }
        CommandReply::SimpleString("OK".to_owned())
    }

    fn execute_key_command(&mut self, db: u16, frame: &CommandFrame) -> CommandReply {
        let Some(key) = frame.args.first() else {
            return self.core.execute_in_db(db, frame);
        };
        if let Some(moved) = self.cluster_moved_reply_for_key(key) {
            return moved;
        }
        self.core.execute_in_db(db, frame)
    }

    fn execute_cluster(&self, db: u16, frame: &CommandFrame) -> CommandReply {
        if self.cluster.mode == ClusterMode::Disabled {
            return CommandReply::Error(
                "Cluster is disabled. Use --cluster_mode=yes to enable.".to_owned(),
            );
        }

        let Some(subcommand_raw) = frame.args.first() else {
            return CommandReply::Error(
                "wrong number of arguments for 'CLUSTER' command".to_owned(),
            );
        };
        let Ok(subcommand) = std::str::from_utf8(subcommand_raw) else {
            return CommandReply::Error("CLUSTER subcommand must be valid UTF-8".to_owned());
        };

        match subcommand.to_ascii_uppercase().as_str() {
            "HELP" => Self::execute_cluster_help(frame),
            "COUNTKEYSINSLOT" => self.execute_cluster_countkeysinslot(db, frame),
            "GETKEYSINSLOT" => self.execute_cluster_getkeysinslot(db, frame),
            "INFO" => self.execute_cluster_info(frame),
            "KEYSLOT" => Self::execute_cluster_keyslot(frame),
            "MYID" => self.execute_cluster_myid(frame),
            "NODES" => self.execute_cluster_nodes(frame),
            "SHARDS" => self.execute_cluster_shards(frame),
            "SLOTS" => self.execute_cluster_slots(frame),
            _ => CommandReply::Error(format!("unknown CLUSTER subcommand '{subcommand}'")),
        }
    }

    fn execute_cluster_keyslot(frame: &CommandFrame) -> CommandReply {
        if frame.args.len() != 2 {
            return CommandReply::Error(
                "wrong number of arguments for 'CLUSTER KEYSLOT' command".to_owned(),
            );
        }
        CommandReply::Integer(i64::from(key_slot(&frame.args[1])))
    }

    fn execute_cluster_getkeysinslot(&self, db: u16, frame: &CommandFrame) -> CommandReply {
        if frame.args.len() != 3 {
            return CommandReply::Error(
                "wrong number of arguments for 'CLUSTER GETKEYSINSLOT' command".to_owned(),
            );
        }

        let Ok(slot_text) = std::str::from_utf8(&frame.args[1]) else {
            return CommandReply::Error(
                "CLUSTER GETKEYSINSLOT slot must be valid UTF-8".to_owned(),
            );
        };
        let Ok(slot) = slot_text.parse::<u16>() else {
            return CommandReply::Error("value is not an integer or out of range".to_owned());
        };
        if slot > 16_383 {
            return CommandReply::Error("slot is out of range".to_owned());
        }

        let Ok(count_text) = std::str::from_utf8(&frame.args[2]) else {
            return CommandReply::Error(
                "CLUSTER GETKEYSINSLOT count must be valid UTF-8".to_owned(),
            );
        };
        let Ok(limit) = count_text.parse::<usize>() else {
            return CommandReply::Error("value is not an integer or out of range".to_owned());
        };

        let mut keys = self.core.keys_in_slot(db, slot);
        if keys.len() > limit {
            keys.truncate(limit);
        }
        CommandReply::Array(
            keys.into_iter()
                .map(CommandReply::BulkString)
                .collect::<Vec<_>>(),
        )
    }

    fn execute_cluster_countkeysinslot(&self, db: u16, frame: &CommandFrame) -> CommandReply {
        if frame.args.len() != 2 {
            return CommandReply::Error(
                "wrong number of arguments for 'CLUSTER COUNTKEYSINSLOT' command".to_owned(),
            );
        }

        let Ok(slot_text) = std::str::from_utf8(&frame.args[1]) else {
            return CommandReply::Error(
                "CLUSTER COUNTKEYSINSLOT slot must be valid UTF-8".to_owned(),
            );
        };
        let Ok(slot) = slot_text.parse::<u16>() else {
            return CommandReply::Error("value is not an integer or out of range".to_owned());
        };
        if slot > 16_383 {
            return CommandReply::Error("slot is out of range".to_owned());
        }

        let count = self.core.count_keys_in_slot(db, slot);
        CommandReply::Integer(i64::try_from(count).unwrap_or(i64::MAX))
    }

    fn execute_cluster_help(frame: &CommandFrame) -> CommandReply {
        if frame.args.len() != 1 {
            return CommandReply::Error(
                "wrong number of arguments for 'CLUSTER HELP' command".to_owned(),
            );
        }
        let lines = [
            "CLUSTER <subcommand> [<arg> [value] [opt] ...]. Subcommands are:",
            "SLOTS",
            "   Return information about slots range mappings. Each range is made of:",
            "   start, end, master and replicas IP addresses, ports and ids.",
            "COUNTKEYSINSLOT <slot>",
            "   Return the number of keys in one hash slot.",
            "GETKEYSINSLOT <slot> <count>",
            "   Return up to <count> keys for one hash slot.",
            "NODES",
            "   Return cluster configuration seen by node. Output format:",
            "   <id> <ip:port> <flags> <master> <pings> <pongs> <epoch> <link> <slot> ...",
            "INFO",
            "  Return information about the cluster",
            "HELP",
            "    Prints this help.",
        ];
        CommandReply::Array(
            lines
                .into_iter()
                .map(|line| CommandReply::BulkString(line.as_bytes().to_vec()))
                .collect(),
        )
    }

    fn execute_cluster_slots(&self, frame: &CommandFrame) -> CommandReply {
        if frame.args.len() != 1 {
            return CommandReply::Error(
                "wrong number of arguments for 'CLUSTER SLOTS' command".to_owned(),
            );
        }

        let (host, port) = split_endpoint_host_port(&self.cluster.redirect_endpoint);
        let slots = self
            .cluster
            .owned_ranges()
            .iter()
            .map(|range| {
                CommandReply::Array(vec![
                    CommandReply::Integer(i64::from(range.start)),
                    CommandReply::Integer(i64::from(range.end)),
                    CommandReply::Array(vec![
                        CommandReply::BulkString(host.as_bytes().to_vec()),
                        CommandReply::Integer(port),
                    ]),
                ])
            })
            .collect::<Vec<_>>();
        CommandReply::Array(slots)
    }

    fn execute_cluster_myid(&self, frame: &CommandFrame) -> CommandReply {
        if frame.args.len() != 1 {
            return CommandReply::Error(
                "wrong number of arguments for 'CLUSTER MYID' command".to_owned(),
            );
        }
        CommandReply::BulkString(self.cluster.node_id.as_bytes().to_vec())
    }

    fn execute_cluster_shards(&self, frame: &CommandFrame) -> CommandReply {
        if frame.args.len() != 1 {
            return CommandReply::Error(
                "wrong number of arguments for 'CLUSTER SHARDS' command".to_owned(),
            );
        }

        let (host, port) = split_endpoint_host_port(&self.cluster.redirect_endpoint);
        let slots = self
            .cluster
            .owned_ranges()
            .iter()
            .flat_map(|range| {
                [
                    CommandReply::Integer(i64::from(range.start)),
                    CommandReply::Integer(i64::from(range.end)),
                ]
            })
            .collect::<Vec<_>>();
        let nodes = vec![CommandReply::Array(vec![
            CommandReply::BulkString(b"id".to_vec()),
            CommandReply::BulkString(self.cluster.node_id.as_bytes().to_vec()),
            CommandReply::BulkString(b"endpoint".to_vec()),
            CommandReply::BulkString(host.as_bytes().to_vec()),
            CommandReply::BulkString(b"ip".to_vec()),
            CommandReply::BulkString(host.as_bytes().to_vec()),
            CommandReply::BulkString(b"port".to_vec()),
            CommandReply::Integer(port),
            CommandReply::BulkString(b"role".to_vec()),
            CommandReply::BulkString(b"master".to_vec()),
            CommandReply::BulkString(b"replication-offset".to_vec()),
            CommandReply::Integer(0),
            CommandReply::BulkString(b"health".to_vec()),
            CommandReply::BulkString(b"online".to_vec()),
        ])];

        CommandReply::Array(vec![CommandReply::Array(vec![
            CommandReply::BulkString(b"slots".to_vec()),
            CommandReply::Array(slots),
            CommandReply::BulkString(b"nodes".to_vec()),
            CommandReply::Array(nodes),
        ])])
    }

    fn execute_cluster_info(&self, frame: &CommandFrame) -> CommandReply {
        if frame.args.len() != 1 {
            return CommandReply::Error(
                "wrong number of arguments for 'CLUSTER INFO' command".to_owned(),
            );
        }

        let assigned = self.cluster.assigned_slots();
        let state = if assigned > 0 { "ok" } else { "fail" };
        let body = format!(
            "cluster_state:{state}\r\ncluster_slots_assigned:{assigned}\r\ncluster_slots_ok:{assigned}\r\ncluster_slots_pfail:0\r\ncluster_slots_fail:0\r\ncluster_known_nodes:1\r\ncluster_size:1\r\ncluster_current_epoch:{}\r\ncluster_my_epoch:{}\r\n",
            self.cluster.config_epoch, self.cluster.config_epoch
        );
        CommandReply::BulkString(body.into_bytes())
    }

    fn execute_cluster_nodes(&self, frame: &CommandFrame) -> CommandReply {
        if frame.args.len() != 1 {
            return CommandReply::Error(
                "wrong number of arguments for 'CLUSTER NODES' command".to_owned(),
            );
        }

        let (host, port) = split_endpoint_host_port(&self.cluster.redirect_endpoint);
        let bus_port = port.saturating_add(10_000);
        let ranges = self
            .cluster
            .owned_ranges()
            .iter()
            .map(|range| format!("{}-{}", range.start, range.end))
            .collect::<Vec<_>>()
            .join(" ");

        let mut line = format!(
            "{} {}:{}@{} myself,master - 0 0 {} connected",
            self.cluster.node_id, host, port, bus_port, self.cluster.config_epoch
        );
        if !ranges.is_empty() {
            line.push(' ');
            line.push_str(&ranges);
        }
        line.push('\n');
        CommandReply::BulkString(line.into_bytes())
    }

    fn execute_mget(&mut self, db: u16, frame: &CommandFrame) -> CommandReply {
        if frame.args.is_empty() {
            return CommandReply::Error("wrong number of arguments for 'MGET' command".to_owned());
        }
        if let Some(error_reply) =
            self.ensure_cluster_multi_key_constraints(frame.args.iter().map(Vec::as_slice))
        {
            return error_reply;
        }

        let replies = frame
            .args
            .iter()
            .map(|key| {
                let get_frame = CommandFrame::new("GET", vec![key.clone()]);
                self.core.execute_in_db(db, &get_frame)
            })
            .collect::<Vec<_>>();
        CommandReply::Array(replies)
    }

    fn execute_mset(&mut self, db: u16, frame: &CommandFrame) -> CommandReply {
        if frame.args.is_empty() || !frame.args.len().is_multiple_of(2) {
            return CommandReply::Error("wrong number of arguments for 'MSET' command".to_owned());
        }
        if let Some(error_reply) = self.ensure_cluster_multi_key_constraints(
            frame.args.chunks_exact(2).map(|pair| pair[0].as_slice()),
        ) {
            return error_reply;
        }

        for pair in frame.args.chunks_exact(2) {
            let set_frame = CommandFrame::new("SET", vec![pair[0].clone(), pair[1].clone()]);
            let reply = self.core.execute_in_db(db, &set_frame);
            if !matches!(reply, CommandReply::SimpleString(ref ok) if ok == "OK") {
                return reply;
            }
        }
        CommandReply::SimpleString("OK".to_owned())
    }

    fn execute_msetnx(&mut self, db: u16, frame: &CommandFrame) -> CommandReply {
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

        for pair in frame.args.chunks_exact(2) {
            let exists_frame = CommandFrame::new("EXISTS", vec![pair[0].clone()]);
            if self.core.execute_in_db(db, &exists_frame) == CommandReply::Integer(1) {
                return CommandReply::Integer(0);
            }
        }

        for pair in frame.args.chunks_exact(2) {
            let set_frame = CommandFrame::new("SET", vec![pair[0].clone(), pair[1].clone()]);
            let reply = self.core.execute_in_db(db, &set_frame);
            if !matches!(reply, CommandReply::SimpleString(ref ok) if ok == "OK") {
                return CommandReply::Error("MSETNX failed while setting key".to_owned());
            }
        }

        CommandReply::Integer(1)
    }

    fn execute_rename(&mut self, db: u16, frame: &CommandFrame) -> CommandReply {
        if let Some(error_reply) =
            self.ensure_cluster_multi_key_constraints(frame.args.iter().map(Vec::as_slice))
        {
            return error_reply;
        }
        self.core.execute_in_db(db, frame)
    }

    fn execute_copy(&mut self, db: u16, frame: &CommandFrame) -> CommandReply {
        if frame.args.len() >= 2
            && let Some(error_reply) = self
                .ensure_cluster_multi_key_constraints(frame.args.iter().take(2).map(Vec::as_slice))
        {
            return error_reply;
        }
        self.core.execute_in_db(db, frame)
    }

    fn ensure_cluster_multi_key_constraints<'a, I>(&self, keys: I) -> Option<CommandReply>
    where
        I: IntoIterator<Item = &'a [u8]>,
    {
        if self.cluster.mode == ClusterMode::Disabled {
            return None;
        }

        let mut key_iter = keys.into_iter();
        let first_key = key_iter.next()?;
        if let Some(moved) = self.cluster_moved_reply_for_key(first_key) {
            return Some(moved);
        }
        let first_slot = key_slot(first_key);
        if key_iter.any(|key| key_slot(key) != first_slot) {
            return Some(CommandReply::Error(
                "CROSSSLOT Keys in request don't hash to the same slot".to_owned(),
            ));
        }
        None
    }

    fn cluster_moved_reply_for_key(&self, key: &[u8]) -> Option<CommandReply> {
        self.cluster
            .moved_slot_for_key(key)
            .map(|slot| CommandReply::Moved {
                slot,
                endpoint: self.cluster.redirect_endpoint.clone(),
            })
    }

    fn replay_journal_entries(&mut self, entries: &[JournalEntry]) -> DflyResult<usize> {
        let mut applied = 0_usize;
        for entry in entries {
            if matches!(entry.op, JournalOp::Ping | JournalOp::Lsn) {
                continue;
            }

            let frame = parse_journal_command_frame(&entry.payload)?;
            let reply = self.execute_replay_command_without_journal(entry.db, &frame);
            if let CommandReply::Error(message) = reply {
                return Err(DflyError::Protocol(format!(
                    "journal replay failed for txid {}: {message}",
                    entry.txid
                )));
            }
            applied = applied.saturating_add(1);
        }
        Ok(applied)
    }

    fn allocate_txid(&mut self) -> TxId {
        let txid = self.next_txid;
        self.next_txid = self.next_txid.saturating_add(1);
        txid
    }

    fn maybe_append_journal_for_command(
        &mut self,
        txid: Option<TxId>,
        db: u16,
        frame: &CommandFrame,
        reply: &CommandReply,
    ) {
        let Some(op) = journal_op_for_command(frame, reply) else {
            return;
        };

        let txid = txid.unwrap_or_else(|| self.allocate_txid());
        let payload = serialize_command_frame(frame);
        self.replication.append_journal(JournalEntry {
            txid,
            db,
            op,
            payload,
        });
    }
}

/// Deferred reply handle for one command already submitted to a shard runtime queue.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct RuntimeReplyTicket {
    /// Target shard that owns the command execution.
    pub(crate) shard: u16,
    /// Monotonic sequence assigned at submit time on that shard queue.
    pub(crate) sequence: u64,
    /// Client protocol used to encode the command reply.
    pub(crate) protocol: ClientProtocol,
    /// Canonical command frame needed for protocol-aware reply encoding.
    pub(crate) frame: CommandFrame,
}

/// Result shape for parsed-command execution in threaded network mode.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum ParsedCommandExecution {
    /// Command completed in coordinator context and can be written back immediately.
    Immediate(Option<Vec<u8>>),
    /// Command was submitted to shard runtime and will complete asynchronously.
    Deferred(RuntimeReplyTicket),
}

/// Per-client state stored by the server.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ServerConnection {
    /// Per-connection parser state used by ingress read path.
    pub parser: ConnectionState,
    /// Stable connection metadata shared with worker-owned parser state.
    pub context: ConnectionContext,
    /// Connection-local transaction queue (`MULTI` mode).
    pub transaction: TransactionSession,
    /// Replica endpoint identity announced on this connection via `REPLCONF`.
    pub replica_endpoint: Option<ReplicaEndpointIdentity>,
    /// Optional Dragonfly replica id announced via `REPLCONF CLIENT-ID`.
    pub replica_client_id: Option<String>,
    /// Optional Dragonfly replica version announced via `REPLCONF CLIENT-VERSION`.
    pub replica_client_version: Option<u64>,
}

/// One replica endpoint attached to a client connection.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReplicaEndpointIdentity {
    /// Replica IP/address.
    pub address: String,
    /// Replica listening port.
    pub listening_port: u16,
}

/// Builds standard wrong-arity RESP error for one command.
fn wrong_arity(command_name: &str) -> Vec<u8> {
    CommandReply::Error(wrong_arity_message(command_name)).to_resp_bytes()
}

fn wrong_arity_message(command_name: &str) -> String {
    format!("wrong number of arguments for '{command_name}' command")
}

fn is_runtime_dispatch_error(reply: &CommandReply) -> bool {
    matches!(
        reply,
        CommandReply::Error(message)
            if message.starts_with("runtime dispatch failed:")
    )
}

/// Returns whether one command is `REPLCONF ACK <offset>`.
fn is_replconf_ack_command(frame: &CommandFrame) -> bool {
    frame.name == "REPLCONF" && frame.args.len() == 2 && frame.args[0].eq_ignore_ascii_case(b"ACK")
}

fn sync_session_error_reply(error: SyncSessionError) -> CommandReply {
    match error {
        SyncSessionError::SyncIdNotFound => CommandReply::Error("syncid not found".to_owned()),
        SyncSessionError::InvalidState | SyncSessionError::IncompleteFlows => {
            CommandReply::Error("invalid state".to_owned())
        }
        SyncSessionError::FlowOutOfRange => {
            CommandReply::Error("value is not an integer or out of range".to_owned())
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum FlowLsnParseError {
    Syntax,
    InvalidInt,
}

fn parse_flow_lsn_vector_for_flow(
    lsn_vec_text: &str,
    flow_id: usize,
    shard_count: usize,
) -> Result<u64, FlowLsnParseError> {
    let entries = lsn_vec_text.split('-').collect::<Vec<_>>();
    if entries.len() != shard_count {
        return Err(FlowLsnParseError::Syntax);
    }
    let Some(flow_lsn_text) = entries.get(flow_id) else {
        return Err(FlowLsnParseError::Syntax);
    };
    flow_lsn_text
        .parse::<u64>()
        .map_err(|_| FlowLsnParseError::InvalidInt)
}

/// Returns journal op kind when the command mutated keyspace state.
fn journal_op_for_command(frame: &CommandFrame, reply: &CommandReply) -> Option<JournalOp> {
    match (frame.name.as_str(), reply) {
        ("SET", _) if set_command_mutated(frame, reply) => Some(JournalOp::Command),
        ("SETEX" | "PSETEX", CommandReply::SimpleString(ok)) if ok == "OK" => {
            Some(JournalOp::Command)
        }
        ("RENAME", CommandReply::SimpleString(ok))
            if ok == "OK" && frame.args.first() != frame.args.get(1) =>
        {
            Some(JournalOp::Command)
        }
        ("SETNX" | "MSETNX" | "MOVE" | "RENAMENX" | "COPY", CommandReply::Integer(1))
        | ("GETSET", CommandReply::Null | CommandReply::BulkString(_))
        | ("GETDEL", CommandReply::BulkString(_))
        | ("APPEND", CommandReply::Integer(_)) => Some(JournalOp::Command),
        ("SETRANGE", CommandReply::Integer(_))
            if frame.args.get(2).is_some_and(|arg| !arg.is_empty()) =>
        {
            Some(JournalOp::Command)
        }
        ("EXPIREAT", CommandReply::Integer(1)) => {
            if expireat_is_delete(frame) {
                Some(JournalOp::Expired)
            } else {
                Some(JournalOp::Command)
            }
        }
        ("PEXPIREAT", CommandReply::Integer(1)) => {
            if pexpireat_is_delete(frame) {
                Some(JournalOp::Expired)
            } else {
                Some(JournalOp::Command)
            }
        }
        ("PEXPIRE", CommandReply::Integer(1)) => {
            if pexpire_is_delete(frame) {
                Some(JournalOp::Expired)
            } else {
                Some(JournalOp::Command)
            }
        }
        ("MSET", CommandReply::SimpleString(ok)) if ok == "OK" => Some(JournalOp::Command),
        ("DEL" | "UNLINK", CommandReply::Integer(count)) if *count > 0 => Some(JournalOp::Command),
        ("PERSIST" | "INCR" | "DECR" | "INCRBY" | "DECRBY", CommandReply::Integer(count))
            if *count > 0 =>
        {
            Some(JournalOp::Command)
        }
        ("FLUSHDB", CommandReply::SimpleString(ok)) if ok == "OK" => Some(JournalOp::Command),
        ("FLUSHALL", CommandReply::SimpleString(ok)) if ok == "OK" => Some(JournalOp::Command),
        ("EXPIRE", CommandReply::Integer(1)) => {
            if expire_is_delete(frame) {
                Some(JournalOp::Expired)
            } else {
                Some(JournalOp::Command)
            }
        }
        _ => None,
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum SetJournalCondition {
    Always,
    IfMissing,
    IfExists,
}

fn set_command_mutated(frame: &CommandFrame, reply: &CommandReply) -> bool {
    let mut condition = SetJournalCondition::Always;
    let mut wants_get = false;
    for option in frame.args.iter().skip(2) {
        if option.eq_ignore_ascii_case(b"NX") {
            condition = SetJournalCondition::IfMissing;
        } else if option.eq_ignore_ascii_case(b"XX") {
            condition = SetJournalCondition::IfExists;
        } else if option.eq_ignore_ascii_case(b"GET") {
            wants_get = true;
        }
    }

    match reply {
        CommandReply::SimpleString(ok) => ok == "OK",
        CommandReply::BulkString(_) => wants_get && condition != SetJournalCondition::IfMissing,
        CommandReply::Null => wants_get && condition != SetJournalCondition::IfExists,
        _ => false,
    }
}

fn expire_is_delete(frame: &CommandFrame) -> bool {
    frame
        .args
        .get(1)
        .and_then(|seconds| std::str::from_utf8(seconds).ok())
        .and_then(|seconds| seconds.parse::<i64>().ok())
        .is_some_and(|seconds| seconds <= 0)
}

fn expireat_is_delete(frame: &CommandFrame) -> bool {
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0_i64, |duration| {
            i64::try_from(duration.as_secs()).unwrap_or(i64::MAX)
        });
    frame
        .args
        .get(1)
        .and_then(|timestamp| std::str::from_utf8(timestamp).ok())
        .and_then(|timestamp| timestamp.parse::<i64>().ok())
        .is_some_and(|timestamp| timestamp <= now)
}

fn pexpireat_is_delete(frame: &CommandFrame) -> bool {
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0_i64, |duration| {
            i64::try_from(duration.as_millis()).unwrap_or(i64::MAX)
        });
    frame
        .args
        .get(1)
        .and_then(|timestamp| std::str::from_utf8(timestamp).ok())
        .and_then(|timestamp| timestamp.parse::<i64>().ok())
        .is_some_and(|timestamp| timestamp <= now)
}

fn pexpire_is_delete(frame: &CommandFrame) -> bool {
    frame
        .args
        .get(1)
        .and_then(|milliseconds| std::str::from_utf8(milliseconds).ok())
        .and_then(|milliseconds| milliseconds.parse::<i64>().ok())
        .is_some_and(|milliseconds| milliseconds <= 0)
}

fn parse_db_index_arg(arg: &[u8]) -> Result<u16, String> {
    let db_text =
        std::str::from_utf8(arg).map_err(|_| "SELECT DB index must be UTF-8".to_owned())?;
    let db_index = db_text
        .parse::<u16>()
        .map_err(|_| "SELECT DB index must be an unsigned 16-bit integer".to_owned())?;
    Ok(db_index)
}

/// Serializes one command frame as RESP array payload for journal replay.
fn serialize_command_frame(frame: &CommandFrame) -> Vec<u8> {
    let mut output = format!("*{}\r\n", frame.args.len() + 1).into_bytes();
    append_resp_bulk(&mut output, frame.name.as_bytes());
    for arg in &frame.args {
        append_resp_bulk(&mut output, arg);
    }
    output
}

fn append_resp_bulk(output: &mut Vec<u8>, payload: &[u8]) {
    output.extend_from_slice(format!("${}\r\n", payload.len()).as_bytes());
    output.extend_from_slice(payload);
    output.extend_from_slice(b"\r\n");
}

fn parse_journal_command_frame(payload: &[u8]) -> DflyResult<CommandFrame> {
    let parsed = parse_next_command(ClientProtocol::Resp, payload)?;
    let ParseStatus::Complete { command, consumed } = parsed else {
        return Err(DflyError::Protocol(
            "journal payload does not contain one complete RESP command".to_owned(),
        ));
    };
    if consumed != payload.len() {
        return Err(DflyError::Protocol(
            "journal payload must contain exactly one command".to_owned(),
        ));
    }

    Ok(CommandFrame::new(command.name, command.args))
}

fn split_endpoint_host_port(endpoint: &str) -> (String, i64) {
    let Some((host, port_text)) = endpoint.rsplit_once(':') else {
        return (endpoint.to_owned(), 0);
    };
    let port = port_text.parse::<i64>().unwrap_or(0);
    (host.to_owned(), port)
}

/// Encodes a canonical reply according to the target client protocol.
fn encode_reply_for_protocol(
    protocol: ClientProtocol,
    frame: &CommandFrame,
    reply: CommandReply,
) -> Vec<u8> {
    match protocol {
        ClientProtocol::Resp => reply.to_resp_bytes(),
        ClientProtocol::Memcache => encode_memcache_reply(frame, reply),
    }
}

/// Encodes replies in memcache text protocol style for supported command subset.
fn encode_memcache_reply(frame: &CommandFrame, reply: CommandReply) -> Vec<u8> {
    match (frame.name.as_str(), reply) {
        ("SET", CommandReply::SimpleString(ok)) if ok == "OK" => b"STORED\r\n".to_vec(),
        ("GET", CommandReply::BulkString(payload)) => {
            if let Some(key) = frame.args.first() {
                let key_text = String::from_utf8_lossy(key);
                let mut output = format!("VALUE {key_text} 0 {}\r\n", payload.len()).into_bytes();
                output.extend_from_slice(&payload);
                output.extend_from_slice(b"\r\nEND\r\n");
                return output;
            }

            // Defensive fallback for malformed frame shape: still return the payload.
            let mut output = payload;
            output.extend_from_slice(b"\r\n");
            output
        }
        (_, CommandReply::SimpleString(message)) => format!("{message}\r\n").into_bytes(),
        (_, CommandReply::Integer(value)) => format!("{value}\r\n").into_bytes(),
        (_, CommandReply::BulkString(payload)) => {
            let mut output = payload;
            output.extend_from_slice(b"\r\n");
            output
        }
        (_, CommandReply::Array(_)) => b"ERROR unsupported array reply\r\n".to_vec(),
        (_, CommandReply::NullArray) => b"ERROR unsupported null array reply\r\n".to_vec(),
        (_, CommandReply::Moved { slot, endpoint }) => {
            format!("ERROR MOVED {slot} {endpoint}\r\n").into_bytes()
        }
        (_, CommandReply::Null) => b"END\r\n".to_vec(),
        (_, CommandReply::Error(message)) => format!("ERROR {message}\r\n").into_bytes(),
    }
}

/// Starts `dfly-server` process bootstrap.
///
/// # Errors
///
/// Returns `DflyError::Io` when listener bootstrap or reactor polling fails.
pub fn run() -> DflyResult<()> {
    let config = RuntimeConfig::default();
    let redis_bind_addr = std::net::SocketAddr::from(([0, 0, 0, 0], config.redis_port));
    let memcache_bind_addr = config
        .memcached_port
        .map(|port| std::net::SocketAddr::from(([0, 0, 0, 0], port)));
    let mut app = ServerApp::new(config);

    // Keep subsystem initialization paths exercised in regular server startup.
    let snapshot = app.create_snapshot_bytes()?;
    app.load_snapshot_bytes(&snapshot)?;
    let _ = app.recover_from_replication_journal()?;
    let _ = app.recover_from_replication_journal_from_lsn(app.replication.journal_lsn())?;

    let reactor_config = ServerReactorConfig {
        io_worker_count: usize::from(app.facade.io_thread_count),
        ..ServerReactorConfig::default()
    };
    let mut reactor = if let Some(memcache_bind_addr) = memcache_bind_addr {
        ThreadedServerReactor::bind_with_memcache(
            redis_bind_addr,
            Some(memcache_bind_addr),
            reactor_config,
        )?
    } else {
        ThreadedServerReactor::bind(redis_bind_addr, reactor_config)?
    };
    println!("{}", app.startup_summary());
    loop {
        let _ = reactor.poll_once(&mut app, Some(Duration::from_millis(10)))?;
    }
}

#[cfg(test)]
mod app_tests;
