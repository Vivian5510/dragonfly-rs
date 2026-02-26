//! Process composition root for `dfly-server`.

mod runtime_exec;
mod bootstrap;
mod protocol_reply;
mod journal_lane;
mod replication_wire;
mod cluster_wire;

use dfly_cluster::ClusterModule;
use dfly_cluster::slot::key_slot;
use dfly_common::config::{ClusterMode, RuntimeConfig};
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
use dfly_facade::protocol::{ClientProtocol, ParsedCommand};
use dfly_replication::ReplicationModule;
use dfly_replication::journal::{JournalEntry, JournalOp};
use dfly_replication::state::SyncSessionError;
use dfly_search::SearchModule;
use dfly_storage::StorageModule;
use dfly_tiering::TieringModule;
use dfly_transaction::TransactionModule;
use dfly_transaction::plan::{TransactionHop, TransactionMode, TransactionPlan};
use dfly_transaction::scheduler::TransactionScheduler;
use dfly_transaction::session::TransactionSession;
use std::collections::{HashMap, HashSet};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, RwLock};

use journal_lane::JournalAppendLane;
use protocol_reply::{encode_reply_for_protocol, serialize_command_frame};

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
    pub replication: Arc<Mutex<ReplicationModule>>,
    /// Asynchronous append lane decoupling journal I/O from command hot path.
    journal_append_lane: JournalAppendLane,
    /// Cluster orchestration layer.
    pub cluster: RwLock<ClusterModule>,
    /// Search subsystem.
    pub search: SearchModule,
    /// Tiered-storage subsystem.
    pub tiering: TieringModule,
    /// Monotonic transaction id allocator.
    next_txid: AtomicU64,
    /// Monotonic id used for implicit replica endpoint registration on ACK-only connections.
    next_implicit_replica_id: AtomicU64,
}

/// Thread-safe execution handle used by I/O workers.
#[derive(Clone, Debug)]
pub struct AppExecutor {
    app: Arc<ServerApp>,
}

impl AppExecutor {
    /// Wraps one server app instance with shared synchronization for worker threads.
    #[must_use]
    pub fn new(app: ServerApp) -> Self {
        Self { app: Arc::new(app) }
    }

    /// Executes one parsed command on shared server state.
    pub fn execute_parsed_command_deferred(
        &self,
        connection: &mut ServerConnection,
        parsed: ParsedCommand,
    ) -> ParsedCommandExecution {
        self.app.execute_parsed_command_deferred(connection, parsed)
    }

    /// Checks whether one deferred runtime reply is ready.
    pub fn runtime_reply_ticket_ready(&self, ticket: &mut RuntimeReplyTicket) -> DflyResult<bool> {
        self.app.runtime_reply_ticket_ready(ticket)
    }

    /// Returns one point-in-time snapshot of processed runtime sequences for all shards.
    pub fn runtime_processed_sequences_snapshot(&self) -> DflyResult<Vec<u64>> {
        self.app.runtime_processed_sequences_snapshot()
    }

    /// Checks whether one deferred runtime reply is ready using a caller-provided shard snapshot.
    pub fn runtime_reply_ticket_ready_with_snapshot(
        &self,
        ticket: &mut RuntimeReplyTicket,
        processed_snapshot: &[u64],
    ) -> DflyResult<bool> {
        self.app
            .runtime_reply_ticket_ready_with_snapshot(ticket, processed_snapshot)
    }

    /// Takes one already-ready deferred runtime reply and encodes it for client protocol.
    pub fn take_runtime_reply_ticket_ready(
        &self,
        ticket: &mut RuntimeReplyTicket,
    ) -> DflyResult<Vec<u8>> {
        self.app.take_runtime_reply_ticket_ready(ticket)
    }

    /// Takes one deferred runtime reply and encodes it for client protocol.
    pub fn take_runtime_reply_ticket(
        &self,
        ticket: &mut RuntimeReplyTicket,
    ) -> DflyResult<Vec<u8>> {
        self.app.take_runtime_reply_ticket(ticket)
    }

    /// Runs disconnect cleanup for one connection-owned replica endpoint.
    pub fn disconnect_connection(&self, connection: &mut ServerConnection) {
        self.app.disconnect_connection(connection);
    }

    /// Returns server startup summary from the shared app instance.
    #[must_use]
    pub fn startup_summary(&self) -> String {
        self.app.startup_summary()
    }
}

impl ServerApp {
    /// Creates a process composition from runtime config.
    #[must_use]
    pub fn new(config: RuntimeConfig) -> Self {
        let facade = FacadeModule::from_config(&config);
        let core = SharedCore::new(config.shard_count);
        let runtime_core = core.clone();
        let maintenance_core = core.clone();
        let runtime = InMemoryShardRuntime::new_with_executor_and_maintenance_and_queue_limit(
            config.shard_count,
            config.runtime_queue_soft_limit,
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
        let replication = Arc::new(Mutex::new(ReplicationModule::new(true)));
        let journal_append_lane = JournalAppendLane::spawn(Arc::clone(&replication));
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
            journal_append_lane,
            cluster: RwLock::new(cluster),
            search,
            tiering,
            next_txid: AtomicU64::new(1),
            next_implicit_replica_id: AtomicU64::new(1),
        }
    }

    fn flush_journal_append_lane(&self) {
        self.journal_append_lane.flush();
    }

    fn replication_guard(&self) -> std::sync::MutexGuard<'_, ReplicationModule> {
        self.replication
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    fn replication_guard_with_latest_journal(
        &self,
    ) -> std::sync::MutexGuard<'_, ReplicationModule> {
        self.flush_journal_append_lane();
        self.replication_guard()
    }

    fn cluster_read_guard(&self) -> std::sync::RwLockReadGuard<'_, ClusterModule> {
        self.cluster
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
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
            self.replication_guard().enabled,
            self.cluster_read_guard().mode,
            self.search.enabled,
            self.tiering.enabled
        )
    }

    fn install_snapshot(&self, snapshot: &CoreSnapshot) -> DflyResult<()> {
        self.core.import_snapshot(snapshot)?;
        self.replication_guard_with_latest_journal()
            .reset_after_snapshot_load();
        Ok(())
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
    pub fn disconnect_connection(&self, connection: &mut ServerConnection) {
        if let Some(endpoint) = connection.replica_endpoint.take() {
            let _ = self
                .replication_guard()
                .remove_replica_endpoint(&endpoint.address, endpoint.listening_port);
        }
    }

    pub(crate) fn execute_parsed_command_deferred(
        &self,
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
        &self,
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

    fn handle_exec(&self, connection: &mut ServerConnection, frame: &CommandFrame) -> Vec<u8> {
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

    fn handle_watch(&self, connection: &mut ServerConnection, frame: &CommandFrame) -> Vec<u8> {
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
        match app_command_kind(frame.name.as_str()) {
            AppCommandKind::Info => Ok(()),
            AppCommandKind::Keys => (frame.args.len() == 1)
                .then_some(())
                .ok_or_else(|| wrong_arity_message("KEYS")),
            AppCommandKind::Time
            | AppCommandKind::Role
            | AppCommandKind::ReadOnly
            | AppCommandKind::ReadWrite
            | AppCommandKind::DbSize
            | AppCommandKind::RandomKey => frame
                .args
                .is_empty()
                .then_some(())
                .ok_or_else(|| wrong_arity_message(frame.name.as_str())),
            AppCommandKind::Psync | AppCommandKind::Wait => (frame.args.len() == 2)
                .then_some(())
                .ok_or_else(|| wrong_arity_message(frame.name.as_str())),
            AppCommandKind::Cluster | AppCommandKind::Dfly | AppCommandKind::Mget => {
                (!frame.args.is_empty())
                .then_some(())
                .ok_or_else(|| wrong_arity_message(frame.name.as_str()))
            }
            AppCommandKind::Mset | AppCommandKind::MsetNx => {
                if frame.args.is_empty() || !frame.args.len().is_multiple_of(2) {
                    Err(wrong_arity_message(frame.name.as_str()))
                } else {
                    Ok(())
                }
            }
            AppCommandKind::ReplConf => {
                if frame.args.is_empty() || !frame.args.len().is_multiple_of(2) {
                    Err("syntax error".to_owned())
                } else {
                    Ok(())
                }
            }
            _ => Err(format!("unknown command '{}'", frame.name)),
        }
    }

    fn build_exec_plan(&self, queued_commands: &[CommandFrame]) -> TransactionPlan {
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

    fn execute_command_without_side_effects(&self, db: u16, frame: &CommandFrame) -> CommandReply {
        match app_command_kind(frame.name.as_str()) {
            AppCommandKind::Info => self.execute_info(frame),
            AppCommandKind::Time => Self::execute_time(frame),
            AppCommandKind::Role => self.execute_role(frame),
            AppCommandKind::ReadOnly => Self::execute_readonly(frame),
            AppCommandKind::ReadWrite => self.execute_readwrite(frame),
            AppCommandKind::DbSize => self.execute_dbsize(db, frame),
            AppCommandKind::Keys => self.execute_keys(db, frame),
            AppCommandKind::RandomKey => self.execute_randomkey(db, frame),
            AppCommandKind::FlushDb => self.execute_flushdb(db, frame),
            AppCommandKind::FlushAll => self.execute_flushall(frame),
            AppCommandKind::Psync => self.execute_psync(frame),
            AppCommandKind::Wait => self.execute_wait(frame),
            AppCommandKind::Cluster => self.execute_cluster(db, frame),
            AppCommandKind::Dfly => self.execute_dfly_admin(frame),
            AppCommandKind::Del => self.execute_del(db, frame),
            AppCommandKind::Unlink => self.execute_unlink(db, frame),
            AppCommandKind::Exists => self.execute_exists(db, frame),
            AppCommandKind::Touch => self.execute_touch(db, frame),
            AppCommandKind::Mget => self.execute_mget(db, frame),
            AppCommandKind::Mset => self.execute_mset(db, frame),
            AppCommandKind::MsetNx => self.execute_msetnx(db, frame),
            AppCommandKind::Copy => self.execute_copy(db, frame),
            AppCommandKind::Rename => self.execute_rename(db, frame),
            AppCommandKind::KeyCommand => self.execute_key_command(db, frame),
            AppCommandKind::Unknown | AppCommandKind::ReplConf => self.core.execute_in_db(db, frame),
        }
    }

    fn execute_key_command(&self, db: u16, frame: &CommandFrame) -> CommandReply {
        let Some(key) = frame.args.first() else {
            return self.core.execute_in_db(db, frame);
        };
        if let Some(moved) = self.cluster_moved_reply_for_key(key) {
            return moved;
        }
        self.core.execute_in_db(db, frame)
    }

    fn execute_mget(&self, db: u16, frame: &CommandFrame) -> CommandReply {
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

    fn execute_mset(&self, db: u16, frame: &CommandFrame) -> CommandReply {
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

    fn execute_msetnx(&self, db: u16, frame: &CommandFrame) -> CommandReply {
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

    fn execute_rename(&self, db: u16, frame: &CommandFrame) -> CommandReply {
        if let Some(error_reply) =
            self.ensure_cluster_multi_key_constraints(frame.args.iter().map(Vec::as_slice))
        {
            return error_reply;
        }
        self.core.execute_in_db(db, frame)
    }

    fn execute_copy(&self, db: u16, frame: &CommandFrame) -> CommandReply {
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
        if self.cluster_read_guard().mode == ClusterMode::Disabled {
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

    fn allocate_txid(&self) -> TxId {
        self.next_txid.fetch_add(1, Ordering::AcqRel)
    }

    fn maybe_append_journal_for_command(
        &self,
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
        let entry = JournalEntry {
            txid,
            db,
            op,
            payload,
        };
        if let Err(entry) = self.journal_append_lane.enqueue(entry) {
            // Fallback path when append lane is unavailable during shutdown.
            self.replication
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .append_journal(entry);
        }
    }
}

impl Drop for ServerApp {
    fn drop(&mut self) {
        self.journal_append_lane.shutdown();
    }
}

/// Deferred reply handle for one command already submitted to a shard runtime queue.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct RuntimeReplyTicket {
    /// Logical DB selected by the originating connection.
    pub(crate) db: u16,
    /// Optional transaction id to preserve journaling grouping semantics.
    pub(crate) txid: Option<TxId>,
    /// Client protocol used to encode the command reply.
    pub(crate) protocol: ClientProtocol,
    /// Canonical command frame needed for protocol-aware reply encoding.
    pub(crate) frame: CommandFrame,
    /// Barrier sequences that must be observed before final reply reduction can run.
    pub(crate) barriers: Vec<RuntimeSequenceBarrier>,
    /// Strategy used to reduce one or more runtime replies into one command reply.
    pub(crate) aggregation: RuntimeReplyAggregation,
}

/// One processed-sequence barrier for deferred runtime completion.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct RuntimeSequenceBarrier {
    /// Target shard owning this sequence.
    pub(crate) shard: u16,
    /// Monotonic sequence id within shard runtime queue.
    pub(crate) sequence: u64,
}

/// Position map for one deferred MGET grouped shard reply.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct RuntimeMgetReplyGroup {
    /// Shard that executed grouped MGET keys.
    pub(crate) shard: u16,
    /// Sequence carrying grouped MGET reply.
    pub(crate) sequence: u64,
    /// Original command positions covered by this shard reply.
    pub(crate) positions: Vec<usize>,
}

/// Deferred reduction state for one command reply.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum RuntimeReplyAggregation {
    /// Reply is produced by one worker callback envelope.
    Worker {
        /// Worker shard owning callback execution.
        shard: u16,
        /// Sequence carrying worker reply.
        sequence: u64,
    },
    /// Reply is coordinator-local after runtime barriers complete.
    CoordinatorAfterBarrier,
    /// Reply is one integer sum across grouped worker replies.
    GroupedIntegerSum {
        /// Grouped per-shard reply sequences.
        replies: Vec<RuntimeSequenceBarrier>,
        /// Command label used in error reporting.
        command: String,
    },
    /// Reply is one all-OK reduction across grouped worker replies.
    GroupedAllOk {
        /// Grouped per-shard reply sequences.
        replies: Vec<RuntimeSequenceBarrier>,
        /// Command label used in error reporting.
        command: String,
    },
    /// Reply is one grouped MGET reconstruction.
    GroupedMget {
        /// Per-shard grouped reply layout.
        groups: Vec<RuntimeMgetReplyGroup>,
    },
    /// Reply is one two-stage MSETNX flow (EXISTS then conditional MSET fanout).
    MsetNx {
        /// Stage-local grouped MSET commands to submit if EXISTS reports zero.
        grouped_set_commands: Vec<(u16, CommandFrame)>,
        /// EXISTS phase grouped reply sequences.
        exists_replies: Vec<RuntimeSequenceBarrier>,
        /// MSET phase grouped reply sequences.
        set_replies: Vec<RuntimeSequenceBarrier>,
        /// Mutable stage state advanced by non-blocking ready checks.
        stage: RuntimeMsetNxStage,
    },
}

/// Stage machine for deferred MSETNX reduction.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum RuntimeMsetNxStage {
    /// Awaiting EXISTS replies.
    WaitingExists,
    /// Awaiting conditional MSET replies.
    WaitingSet,
    /// Final command reply ready for encoding/journaling.
    Completed(CommandReply),
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

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum AppCommandKind {
    Info,
    Time,
    Role,
    ReadOnly,
    ReadWrite,
    DbSize,
    Keys,
    RandomKey,
    FlushDb,
    FlushAll,
    Psync,
    Wait,
    Cluster,
    Dfly,
    ReplConf,
    Del,
    Unlink,
    Exists,
    Touch,
    Mget,
    Mset,
    MsetNx,
    Copy,
    Rename,
    KeyCommand,
    Unknown,
}

fn app_command_kind(name: &str) -> AppCommandKind {
    match name {
        "INFO" => AppCommandKind::Info,
        "TIME" => AppCommandKind::Time,
        "ROLE" => AppCommandKind::Role,
        "READONLY" => AppCommandKind::ReadOnly,
        "READWRITE" => AppCommandKind::ReadWrite,
        "DBSIZE" => AppCommandKind::DbSize,
        "KEYS" => AppCommandKind::Keys,
        "RANDOMKEY" => AppCommandKind::RandomKey,
        "FLUSHDB" => AppCommandKind::FlushDb,
        "FLUSHALL" => AppCommandKind::FlushAll,
        "PSYNC" => AppCommandKind::Psync,
        "WAIT" => AppCommandKind::Wait,
        "CLUSTER" => AppCommandKind::Cluster,
        "DFLY" => AppCommandKind::Dfly,
        "REPLCONF" => AppCommandKind::ReplConf,
        "DEL" => AppCommandKind::Del,
        "UNLINK" => AppCommandKind::Unlink,
        "EXISTS" => AppCommandKind::Exists,
        "TOUCH" => AppCommandKind::Touch,
        "MGET" => AppCommandKind::Mget,
        "MSET" => AppCommandKind::Mset,
        "MSETNX" => AppCommandKind::MsetNx,
        "COPY" => AppCommandKind::Copy,
        "RENAME" | "RENAMENX" => AppCommandKind::Rename,
        "GET" | "SET" | "TYPE" | "SETEX" | "PSETEX" | "GETSET" | "GETDEL" | "APPEND"
        | "STRLEN" | "MOVE" | "GETRANGE" | "SETRANGE" | "EXPIRE" | "PEXPIRE" | "EXPIREAT"
        | "PEXPIREAT" | "TTL" | "PTTL" | "EXPIRETIME" | "PEXPIRETIME" | "PERSIST" | "INCR"
        | "DECR" | "INCRBY" | "DECRBY" | "SETNX" => AppCommandKind::KeyCommand,
        _ => AppCommandKind::Unknown,
    }
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
    match (app_command_kind(frame.name.as_str()), reply) {
        (AppCommandKind::KeyCommand, _) if frame.name == "SET" && set_command_mutated(frame, reply) => {
            Some(JournalOp::Command)
        }
        (AppCommandKind::KeyCommand, CommandReply::SimpleString(ok))
            if ok == "OK" && matches!(frame.name.as_str(), "SETEX" | "PSETEX") =>
        {
            Some(JournalOp::Command)
        }
        (AppCommandKind::Rename, CommandReply::SimpleString(ok))
            if frame.name == "RENAME" && ok == "OK" && frame.args.first() != frame.args.get(1) =>
        {
            Some(JournalOp::Command)
        }
        (AppCommandKind::KeyCommand, CommandReply::Integer(1))
            if matches!(frame.name.as_str(), "SETNX" | "MOVE") =>
        {
            Some(JournalOp::Command)
        }
        (AppCommandKind::MsetNx | AppCommandKind::Copy | AppCommandKind::Rename, CommandReply::Integer(1))
            if matches!(frame.name.as_str(), "MSETNX" | "COPY" | "RENAMENX") =>
        {
            Some(JournalOp::Command)
        }
        (AppCommandKind::KeyCommand, CommandReply::Null | CommandReply::BulkString(_))
            if frame.name == "GETSET" =>
        {
            Some(JournalOp::Command)
        }
        (AppCommandKind::KeyCommand, CommandReply::BulkString(_)) if frame.name == "GETDEL" => {
            Some(JournalOp::Command)
        }
        (AppCommandKind::KeyCommand, CommandReply::Integer(_)) if frame.name == "APPEND" => {
            Some(JournalOp::Command)
        }
        (AppCommandKind::KeyCommand, CommandReply::Integer(_))
            if frame.name == "SETRANGE" && frame.args.get(2).is_some_and(|arg| !arg.is_empty()) =>
        {
            Some(JournalOp::Command)
        }
        (AppCommandKind::KeyCommand, CommandReply::Integer(1)) if frame.name == "EXPIREAT" => {
            if expireat_is_delete(frame) {
                Some(JournalOp::Expired)
            } else {
                Some(JournalOp::Command)
            }
        }
        (AppCommandKind::KeyCommand, CommandReply::Integer(1)) if frame.name == "PEXPIREAT" => {
            if pexpireat_is_delete(frame) {
                Some(JournalOp::Expired)
            } else {
                Some(JournalOp::Command)
            }
        }
        (AppCommandKind::KeyCommand, CommandReply::Integer(1)) if frame.name == "PEXPIRE" => {
            if pexpire_is_delete(frame) {
                Some(JournalOp::Expired)
            } else {
                Some(JournalOp::Command)
            }
        }
        (AppCommandKind::Mset, CommandReply::SimpleString(ok)) if ok == "OK" => Some(JournalOp::Command),
        (AppCommandKind::Del | AppCommandKind::Unlink, CommandReply::Integer(count))
            if *count > 0 =>
        {
            Some(JournalOp::Command)
        }
        (AppCommandKind::KeyCommand, CommandReply::Integer(count))
            if *count > 0 && matches!(frame.name.as_str(), "PERSIST" | "INCR" | "DECR" | "INCRBY" | "DECRBY") =>
        {
            Some(JournalOp::Command)
        }
        (AppCommandKind::FlushDb | AppCommandKind::FlushAll, CommandReply::SimpleString(ok))
            if ok == "OK" =>
        {
            Some(JournalOp::Command)
        }
        (AppCommandKind::KeyCommand, CommandReply::Integer(1)) if frame.name == "EXPIRE" => {
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

/// Starts `dfly-server` process bootstrap.
///
/// # Errors
///
/// Returns `DflyError::Io` when listener bootstrap or reactor polling fails.
pub fn run() -> DflyResult<()> {
    bootstrap::run_server()
}

#[cfg(test)]
mod app_tests;
