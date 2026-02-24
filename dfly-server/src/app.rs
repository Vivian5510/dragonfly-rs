//! Process composition root for `dfly-server`.

use dfly_cluster::ClusterModule;
use dfly_cluster::slot::key_slot;
use dfly_common::config::{ClusterMode, RuntimeConfig};
use dfly_common::error::DflyError;
use dfly_common::error::DflyResult;
use dfly_common::ids::TxId;
use dfly_core::CommandRouting;
use dfly_core::CoreModule;
use dfly_core::command::{CommandFrame, CommandReply};
use dfly_core::runtime::{InMemoryShardRuntime, RuntimeEnvelope};
use dfly_facade::FacadeModule;
use dfly_facade::connection::{ConnectionContext, ConnectionState};
use dfly_facade::protocol::{ClientProtocol, ParseStatus, parse_next_command};
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
use std::collections::{BTreeSet, HashMap, HashSet};
use std::time::Duration;

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
    pub core: CoreModule,
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
}

impl ServerApp {
    /// Creates a process composition from runtime config.
    #[must_use]
    pub fn new(config: RuntimeConfig) -> Self {
        let facade = FacadeModule::from_config(&config);
        let core = CoreModule::new(config.shard_count);
        let runtime = InMemoryShardRuntime::new(config.shard_count);
        let transaction = TransactionModule::new();
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
        }
    }

    /// Human-readable startup summary.
    #[must_use]
    pub fn startup_summary(&self) -> String {
        format!(
            "dfly-server bootstrap: shard_count={}, redis_port={}, memcached_port={:?}, \
core_mod={:?}, tx_mod={:?}, storage_mod={:?}, repl_enabled={}, cluster_mode={:?}, search_enabled={}, tiering_enabled={}",
            self.config.shard_count.get(),
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
        self.core.import_snapshot(&snapshot)
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
        ServerConnection {
            parser: ConnectionState::new(ConnectionContext {
                protocol,
                db_index: 0,
                privileged: false,
            }),
            transaction: TransactionSession::default(),
            replica_endpoint: None,
            replica_client_id: None,
            replica_client_version: None,
        }
    }

    /// Test-only helper that detaches one connection and releases its replica endpoint identity.
    #[cfg(test)]
    pub fn disconnect_connection(&mut self, connection: &mut ServerConnection) {
        if let Some(endpoint) = connection.replica_endpoint.take() {
            let _ = self
                .replication
                .remove_replica_endpoint(&endpoint.address, endpoint.listening_port);
        }
    }

    /// Feeds network bytes into one connection and executes all newly completed commands.
    ///
    /// The return vector contains one protocol-encoded reply per decoded command.
    ///
    /// # Errors
    ///
    /// Returns `DflyError::Protocol` when connection input bytes violate the active
    /// wire protocol framing rules.
    pub fn feed_connection_bytes(
        &mut self,
        connection: &mut ServerConnection,
        bytes: &[u8],
    ) -> DflyResult<Vec<Vec<u8>>> {
        connection.parser.feed_bytes(bytes);

        let mut responses = Vec::new();
        loop {
            let Some(parsed) = connection.parser.try_pop_command()? else {
                break;
            };

            // Protocol parser has already normalized command shape, so dispatcher can operate
            // on a stable frame independent from socket/wire details.
            let frame = CommandFrame::new(parsed.name, parsed.args);

            // Redis `MULTI/EXEC/DISCARD` is connection-scoped control flow and is handled
            // before core command execution, matching Dragonfly's high-level transaction entry.
            if connection.parser.context.protocol == ClientProtocol::Resp {
                if let Some(select_reply) = Self::handle_resp_select(connection, &frame) {
                    responses.push(select_reply);
                    continue;
                }

                if let Some(transaction_reply) =
                    self.handle_resp_transaction_control(connection, &frame)
                {
                    responses.push(transaction_reply);
                    continue;
                }

                if connection.transaction.in_multi() {
                    match self.validate_queued_command(&frame) {
                        Ok(()) => {
                            let queued = connection.transaction.queue_command(frame);
                            if queued {
                                responses.push(
                                    CommandReply::SimpleString("QUEUED".to_owned()).to_resp_bytes(),
                                );
                            } else {
                                responses.push(
                                    CommandReply::Error(
                                        "transaction queue is unavailable".to_owned(),
                                    )
                                    .to_resp_bytes(),
                                );
                            }
                        }
                        Err(message) => {
                            connection.transaction.mark_queued_error();
                            responses.push(CommandReply::Error(message).to_resp_bytes());
                        }
                    }
                    continue;
                }
            }

            if frame.name == "REPLCONF" {
                let reply = self.execute_replconf(connection, &frame);
                if is_replconf_ack_command(&frame) && !matches!(&reply, CommandReply::Error(_)) {
                    // Dragonfly does not interleave ACK replies with journal stream writes.
                    // We model the same behavior by consuming successful ACK silently.
                    continue;
                }
                let encoded =
                    encode_reply_for_protocol(connection.parser.context.protocol, &frame, reply);
                responses.push(encoded);
                continue;
            }

            let db = connection.parser.context.db_index;
            let reply = self.execute_user_command(db, &frame, None);
            if is_replconf_ack_command(&frame) && !matches!(&reply, CommandReply::Error(_)) {
                // Dragonfly does not interleave ACK replies with journal stream writes.
                // We model the same behavior by consuming successful ACK silently.
                continue;
            }

            // Unit 1 keeps protocol-specific encoding at the facade edge. This mirrors Dragonfly
            // where execution result is translated back to client protocol right before writeback.
            let encoded =
                encode_reply_for_protocol(connection.parser.context.protocol, &frame, reply);
            responses.push(encoded);
        }

        Ok(responses)
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
        connection.parser.context.db_index = db_index;
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
            .watching_other_dbs(connection.parser.context.db_index)
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

        let db = connection.parser.context.db_index;
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

        let db = connection.parser.context.db_index;
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
            };
        }
        if all_single_key {
            return TransactionPlan {
                txid,
                mode: TransactionMode::LockAhead,
                hops: self.build_single_key_hops(queued_commands),
            };
        }

        // Global fallback keeps strict one-command-per-hop sequencing for mixed workloads.
        let hops = queued_commands
            .iter()
            .cloned()
            .map(|command| TransactionHop {
                per_shard: vec![(self.core.resolve_target_shard(&command), command)],
            })
            .collect::<Vec<_>>();
        TransactionPlan {
            txid,
            mode: TransactionMode::Global,
            hops,
        }
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

    fn execute_transaction_plan(&mut self, db: u16, plan: &TransactionPlan) -> Vec<CommandReply> {
        let mut replies = Vec::new();
        for (hop_index, hop) in plan.hops.iter().enumerate() {
            match plan.mode {
                TransactionMode::NonAtomic => {
                    for (_, command) in &hop.per_shard {
                        if matches!(
                            self.core.command_routing(command),
                            CommandRouting::SingleKey { is_write: false }
                        ) {
                            replies.push(self.execute_user_command(db, command, None));
                        } else {
                            replies.push(CommandReply::Error(
                                "transaction planning error: NonAtomic mode requires read-only single-key commands".to_owned(),
                            ));
                        }
                    }
                }
                TransactionMode::LockAhead | TransactionMode::Global => {
                    let hop_replies = self.execute_runtime_scheduled_hop(db, plan.txid, hop);
                    let runtime_failure = hop_replies.iter().any(is_runtime_dispatch_error);
                    replies.extend(hop_replies);
                    if runtime_failure {
                        // Runtime dispatch failure means execution order/liveness guarantees were
                        // already broken for this transaction wave, so later hops are aborted.
                        for remaining_hop in plan.hops.iter().skip(hop_index + 1) {
                            for _ in &remaining_hop.per_shard {
                                replies.push(CommandReply::Error(
                                    "runtime dispatch failed: transaction aborted after hop failure"
                                        .to_owned(),
                                ));
                            }
                        }
                        break;
                    }
                }
            }
        }
        replies
    }

    fn execute_runtime_scheduled_hop(
        &mut self,
        db: u16,
        txid: TxId,
        hop: &TransactionHop,
    ) -> Vec<CommandReply> {
        let mut per_command_error = vec![None; hop.per_shard.len()];
        let mut target_sequence_by_shard = HashMap::<u16, u64>::new();

        for (index, (shard_hint, command)) in hop.per_shard.iter().enumerate() {
            let target_shards =
                self.runtime_target_shards_for_scheduled_command(*shard_hint, command);
            for target_shard in target_shards {
                match self.submit_runtime_envelope_with_sequence(target_shard, command) {
                    Ok(sequence) => {
                        target_sequence_by_shard
                            .entry(target_shard)
                            .and_modify(|current| {
                                *current = (*current).max(sequence);
                            })
                            .or_insert(sequence);
                    }
                    Err(error) => {
                        if per_command_error[index].is_none() {
                            per_command_error[index] =
                                Some(format!("runtime dispatch failed: {error}"));
                        }
                    }
                }
            }
        }

        let mut shard_wait_error = HashMap::<u16, String>::new();
        for (shard, target_sequence) in target_sequence_by_shard {
            match self.runtime.wait_for_processed_sequence(
                shard,
                target_sequence,
                Duration::from_millis(200),
            ) {
                Ok(true) => {}
                Ok(false) => {
                    shard_wait_error.insert(shard, "runtime dispatch timed out".to_owned());
                }
                Err(error) => {
                    shard_wait_error.insert(shard, format!("runtime dispatch failed: {error}"));
                }
            }
        }

        let hop_has_runtime_error = per_command_error.iter().any(std::option::Option::is_some)
            || !shard_wait_error.is_empty();
        if hop_has_runtime_error {
            let fallback = "runtime dispatch failed: hop barrier aborted".to_owned();
            return hop
                .per_shard
                .iter()
                .enumerate()
                .map(|(index, (shard, _))| {
                    if let Some(error) = &per_command_error[index] {
                        return CommandReply::Error(error.clone());
                    }
                    if let Some(error) = shard_wait_error.get(shard) {
                        return CommandReply::Error(error.clone());
                    }
                    CommandReply::Error(fallback.clone())
                })
                .collect::<Vec<_>>();
        }

        let mut replies = Vec::with_capacity(hop.per_shard.len());
        for (_, command) in &hop.per_shard {
            replies.push(self.execute_user_command(db, command, Some(txid)));
        }
        replies
    }

    fn submit_runtime_envelope_with_sequence(
        &self,
        shard: u16,
        command: &CommandFrame,
    ) -> DflyResult<u64> {
        // Dragonfly routes each hop command to its owner shard queue before execution.
        // This learning path keeps execution local for now, while still modeling queue ingress.
        self.runtime.submit_with_sequence(RuntimeEnvelope {
            target_shard: shard,
            command: command.clone(),
        })
    }

    fn execute_user_command(
        &mut self,
        db: u16,
        frame: &CommandFrame,
        txid: Option<TxId>,
    ) -> CommandReply {
        if txid.is_none() {
            match self.dispatch_direct_command_runtime(frame) {
                Ok(()) => {}
                Err(error) => {
                    return CommandReply::Error(format!("runtime dispatch failed: {error}"));
                }
            }
        }

        let reply = self.execute_command_without_side_effects(db, frame);
        self.maybe_append_journal_for_command(txid, db, frame, &reply);
        reply
    }

    fn dispatch_direct_command_runtime(&self, frame: &CommandFrame) -> DflyResult<()> {
        let target_shards = self.runtime_target_shards_for_command(frame);
        if target_shards.is_empty() {
            return Ok(());
        }

        // Direct command path mirrors Dragonfly's coordinator ingress:
        // 1) push one envelope to each touched shard queue,
        // 2) wait until all shard workers report they consumed the envelope.
        let mut barriers = Vec::with_capacity(target_shards.len());
        for shard in target_shards {
            let sequence = self.submit_runtime_envelope_with_sequence(shard, frame)?;
            barriers.push((shard, sequence));
        }
        for (shard, sequence) in barriers {
            let reached = self.runtime.wait_for_processed_sequence(
                shard,
                sequence,
                Duration::from_millis(200),
            )?;
            if !reached {
                return Err(DflyError::InvalidState("runtime dispatch timed out"));
            }
        }
        Ok(())
    }

    fn runtime_target_shards_for_scheduled_command(
        &self,
        shard_hint: u16,
        frame: &CommandFrame,
    ) -> Vec<u16> {
        let target_shards = self.runtime_target_shards_for_command(frame);
        if target_shards.is_empty() {
            return vec![shard_hint];
        }
        target_shards
    }

    fn runtime_target_shards_for_command(&self, frame: &CommandFrame) -> Vec<u16> {
        if matches!(
            self.core.command_routing(frame),
            CommandRouting::SingleKey { .. }
        ) {
            return vec![self.core.resolve_target_shard(frame)];
        }

        match frame.name.as_str() {
            // Multi-key fanout commands: each key can belong to a different shard owner.
            "MGET" | "DEL" | "UNLINK" | "EXISTS" | "TOUCH" if !frame.args.is_empty() => {
                self.collect_unique_runtime_shards_for_keys(frame.args.iter().map(Vec::as_slice))
            }
            // MSET and MSETNX use key/value pairs, so key positions are 0,2,4,...
            "MSET" | "MSETNX" if frame.args.len() >= 2 && frame.args.len().is_multiple_of(2) => {
                self.collect_unique_runtime_shards_for_keys(
                    frame.args.iter().step_by(2).map(Vec::as_slice),
                )
            }
            // COPY/RENAME family touches source and destination owners.
            "COPY" if frame.args.len() >= 2 => self.collect_unique_runtime_shards_for_keys([
                frame.args[0].as_slice(),
                frame.args[1].as_slice(),
            ]),
            "RENAME" | "RENAMENX" if frame.args.len() == 2 => self
                .collect_unique_runtime_shards_for_keys([
                    frame.args[0].as_slice(),
                    frame.args[1].as_slice(),
                ]),
            // Global keyspace commands must observe a full-shard barrier.
            "FLUSHDB" | "FLUSHALL" | "DBSIZE" | "KEYS" | "RANDOMKEY" => self.all_runtime_shards(),
            _ => Vec::new(),
        }
    }

    fn collect_unique_runtime_shards_for_keys<'a, I>(&self, keys: I) -> Vec<u16>
    where
        I: IntoIterator<Item = &'a [u8]>,
    {
        let mut shards = BTreeSet::new();
        for key in keys {
            let _ = shards.insert(self.core.resolve_shard_for_key(key));
        }
        shards.into_iter().collect()
    }

    fn all_runtime_shards(&self) -> Vec<u16> {
        (0_u16..self.config.shard_count.get()).collect()
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
            "CLUSTER" => self.execute_cluster(frame),
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
        let mut include_persistence = false;
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
        connection: &ServerConnection,
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
        if let Some(endpoint) = connection.replica_endpoint.as_ref() {
            let _ = self.replication.record_replica_ack_for_endpoint(
                &endpoint.address,
                endpoint.listening_port,
                ack_lsn,
            );
        }
        Ok(())
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
        let Ok(_required) = required_text.parse::<u64>() else {
            return CommandReply::Error("value is not an integer or out of range".to_owned());
        };
        let Ok(timeout_text) = std::str::from_utf8(&frame.args[1]) else {
            return CommandReply::Error("WAIT timeout must be valid UTF-8".to_owned());
        };
        let Ok(_timeout_millis) = timeout_text.parse::<u64>() else {
            return CommandReply::Error("value is not an integer or out of range".to_owned());
        };

        // WAIT returns "replicas reached" count, not "requested replicas" count.
        // Timeout/blocking is not modeled yet, but the return semantics match Redis/Dragonfly.
        let replicated = u64::try_from(
            self.replication
                .acked_replica_count_at_or_above(self.replication.replication_offset()),
        )
        .unwrap_or(u64::MAX);
        CommandReply::Integer(i64::try_from(replicated).unwrap_or(i64::MAX))
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
        match self.core.import_snapshot(&snapshot) {
            Ok(()) => CommandReply::SimpleString("OK".to_owned()),
            Err(error) => CommandReply::Error(format!("DFLY LOAD failed: {error}")),
        }
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

    fn execute_cluster(&self, frame: &CommandFrame) -> CommandReply {
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
            // Journal replay should follow the same shard-ingress ordering contract as direct
            // command execution, so restored state observes runtime barrier semantics.
            if let Err(error) = self.dispatch_direct_command_runtime(&frame) {
                return Err(DflyError::Protocol(format!(
                    "journal replay runtime dispatch failed for txid {}: {error}",
                    entry.txid
                )));
            }
            let reply = self.execute_command_without_side_effects(entry.db, &frame);
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

/// Per-client state stored by the server.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ServerConnection {
    /// Incremental parser and connection metadata.
    pub parser: ConnectionState,
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
/// Returns `DflyError::Protocol` if the bootstrap probe command cannot be parsed.
pub fn run() -> DflyResult<()> {
    let mut app = ServerApp::new(RuntimeConfig::default());
    let mut bootstrap_connection = ServerApp::new_connection(ClientProtocol::Resp);

    // Keep one minimal end-to-end bootstrap probe so command pipeline code is part of
    // regular binary execution path, not only test-only dead code.
    let _ = app.feed_connection_bytes(&mut bootstrap_connection, b"*1\r\n$4\r\nPING\r\n")?;
    let snapshot = app.create_snapshot_bytes()?;
    app.load_snapshot_bytes(&snapshot)?;
    let _ = app.recover_from_replication_journal()?;
    let _ = app.recover_from_replication_journal_from_lsn(app.replication.journal_lsn())?;

    println!("{}", app.startup_summary());
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::ServerApp;
    use dfly_cluster::slot::{SlotRange, key_slot};
    use dfly_common::config::{ClusterMode, RuntimeConfig};
    use dfly_common::error::DflyError;
    use dfly_core::command::{CommandFrame, CommandReply};
    use dfly_facade::protocol::ClientProtocol;
    use dfly_replication::journal::{InMemoryJournal, JournalEntry, JournalOp};
    use dfly_transaction::plan::{TransactionHop, TransactionMode, TransactionPlan};
    use googletest::prelude::*;
    use rstest::rstest;
    use std::path::PathBuf;
    use std::time::{Duration, SystemTime, UNIX_EPOCH};

    #[rstest]
    fn resp_connection_executes_set_then_get() {
        let mut app = ServerApp::new(RuntimeConfig::default());
        let mut connection = ServerApp::new_connection(ClientProtocol::Resp);

        let set_responses = app
            .feed_connection_bytes(
                &mut connection,
                b"*3\r\n$3\r\nSET\r\n$3\r\nfoo\r\n$3\r\nbar\r\n",
            )
            .expect("SET command should execute");
        let expected_set = vec![b"+OK\r\n".to_vec()];
        assert_that!(&set_responses, eq(&expected_set));

        let get_responses = app
            .feed_connection_bytes(&mut connection, b"*2\r\n$3\r\nGET\r\n$3\r\nfoo\r\n")
            .expect("GET command should execute");
        let expected_get = vec![b"$3\r\nbar\r\n".to_vec()];
        assert_that!(&get_responses, eq(&expected_get));
    }

    #[rstest]
    fn resp_set_supports_conditional_and_get_options() {
        let mut app = ServerApp::new(RuntimeConfig::default());
        let mut connection = ServerApp::new_connection(ClientProtocol::Resp);

        let first = app
            .feed_connection_bytes(
                &mut connection,
                &resp_command(&[b"SET", b"k", b"v1", b"NX"]),
            )
            .expect("SET NX should execute");
        assert_that!(&first, eq(&vec![b"+OK\r\n".to_vec()]));

        let skipped = app
            .feed_connection_bytes(
                &mut connection,
                &resp_command(&[b"SET", b"k", b"v2", b"NX"]),
            )
            .expect("SET NX on existing key should execute");
        assert_that!(&skipped, eq(&vec![b"$-1\r\n".to_vec()]));

        let skipped_with_get = app
            .feed_connection_bytes(
                &mut connection,
                &resp_command(&[b"SET", b"k", b"ignored", b"NX", b"GET"]),
            )
            .expect("SET NX GET should execute");
        assert_that!(&skipped_with_get, eq(&vec![b"$2\r\nv1\r\n".to_vec()]));

        let missing_with_xx_get = app
            .feed_connection_bytes(
                &mut connection,
                &resp_command(&[b"SET", b"missing", b"value", b"XX", b"GET"]),
            )
            .expect("SET XX GET on missing key should execute");
        assert_that!(&missing_with_xx_get, eq(&vec![b"$-1\r\n".to_vec()]));

        let existing_with_xx_get = app
            .feed_connection_bytes(
                &mut connection,
                &resp_command(&[b"SET", b"k", b"v2", b"XX", b"GET"]),
            )
            .expect("SET XX GET on existing key should execute");
        assert_that!(&existing_with_xx_get, eq(&vec![b"$2\r\nv1\r\n".to_vec()]));

        let current = app
            .feed_connection_bytes(&mut connection, &resp_command(&[b"GET", b"k"]))
            .expect("GET should execute");
        assert_that!(&current, eq(&vec![b"$2\r\nv2\r\n".to_vec()]));
    }

    #[rstest]
    fn resp_set_applies_expire_and_keepttl_options() {
        let mut app = ServerApp::new(RuntimeConfig::default());
        let mut connection = ServerApp::new_connection(ClientProtocol::Resp);

        let with_ex = app
            .feed_connection_bytes(
                &mut connection,
                &resp_command(&[b"SET", b"ttl:key", b"value", b"EX", b"30"]),
            )
            .expect("SET EX should execute");
        assert_that!(&with_ex, eq(&vec![b"+OK\r\n".to_vec()]));

        let ttl_before = app
            .feed_connection_bytes(&mut connection, &resp_command(&[b"TTL", b"ttl:key"]))
            .expect("TTL should execute");
        assert_that!(parse_resp_integer(&ttl_before[0]) > 0, eq(true));

        let keep_ttl = app
            .feed_connection_bytes(
                &mut connection,
                &resp_command(&[b"SET", b"ttl:key", b"next", b"KEEPTTL"]),
            )
            .expect("SET KEEPTTL should execute");
        assert_that!(&keep_ttl, eq(&vec![b"+OK\r\n".to_vec()]));

        let ttl_after_keep = app
            .feed_connection_bytes(&mut connection, &resp_command(&[b"TTL", b"ttl:key"]))
            .expect("TTL should execute");
        assert_that!(parse_resp_integer(&ttl_after_keep[0]) > 0, eq(true));

        let clear_ttl = app
            .feed_connection_bytes(
                &mut connection,
                &resp_command(&[b"SET", b"ttl:key", b"final"]),
            )
            .expect("plain SET should execute");
        assert_that!(&clear_ttl, eq(&vec![b"+OK\r\n".to_vec()]));

        let ttl_after_plain = app
            .feed_connection_bytes(&mut connection, &resp_command(&[b"TTL", b"ttl:key"]))
            .expect("TTL should execute");
        assert_that!(&ttl_after_plain, eq(&vec![b":-1\r\n".to_vec()]));

        let with_px = app
            .feed_connection_bytes(
                &mut connection,
                &resp_command(&[b"SET", b"px:key", b"value", b"PX", b"1200"]),
            )
            .expect("SET PX should execute");
        assert_that!(&with_px, eq(&vec![b"+OK\r\n".to_vec()]));

        let pttl = app
            .feed_connection_bytes(&mut connection, &resp_command(&[b"PTTL", b"px:key"]))
            .expect("PTTL should execute");
        assert_that!(parse_resp_integer(&pttl[0]) > 0, eq(true));
    }

    #[rstest]
    fn resp_set_rejects_invalid_option_combinations() {
        let mut app = ServerApp::new(RuntimeConfig::default());
        let mut connection = ServerApp::new_connection(ClientProtocol::Resp);

        let nx_xx = app
            .feed_connection_bytes(
                &mut connection,
                &resp_command(&[b"SET", b"key", b"value", b"NX", b"XX"]),
            )
            .expect("SET NX XX should parse");
        assert_that!(&nx_xx, eq(&vec![b"-ERR syntax error\r\n".to_vec()]));

        let duplicate_expire = app
            .feed_connection_bytes(
                &mut connection,
                &resp_command(&[b"SET", b"key", b"value", b"EX", b"1", b"PX", b"1000"]),
            )
            .expect("SET EX PX should parse");
        assert_that!(
            &duplicate_expire,
            eq(&vec![b"-ERR syntax error\r\n".to_vec()])
        );

        let keep_with_expire = app
            .feed_connection_bytes(
                &mut connection,
                &resp_command(&[b"SET", b"key", b"value", b"EX", b"1", b"KEEPTTL"]),
            )
            .expect("SET EX KEEPTTL should parse");
        assert_that!(
            &keep_with_expire,
            eq(&vec![b"-ERR syntax error\r\n".to_vec()])
        );

        let invalid_integer = app
            .feed_connection_bytes(
                &mut connection,
                &resp_command(&[b"SET", b"key", b"value", b"EX", b"abc"]),
            )
            .expect("SET EX invalid integer should parse");
        assert_that!(
            &invalid_integer,
            eq(&vec![
                b"-ERR value is not an integer or out of range\r\n".to_vec()
            ])
        );

        let invalid_expire = app
            .feed_connection_bytes(
                &mut connection,
                &resp_command(&[b"SET", b"key", b"value", b"PX", b"0"]),
            )
            .expect("SET PX zero should parse");
        assert_that!(
            &invalid_expire,
            eq(&vec![
                b"-ERR invalid expire time in 'SET' command\r\n".to_vec()
            ])
        );
    }

    #[rstest]
    fn resp_type_reports_none_or_string() {
        let mut app = ServerApp::new(RuntimeConfig::default());
        let mut connection = ServerApp::new_connection(ClientProtocol::Resp);

        let missing = app
            .feed_connection_bytes(&mut connection, &resp_command(&[b"TYPE", b"missing"]))
            .expect("TYPE should execute");
        assert_that!(&missing, eq(&vec![b"+none\r\n".to_vec()]));

        let _ = app
            .feed_connection_bytes(&mut connection, &resp_command(&[b"SET", b"k", b"v"]))
            .expect("SET should execute");
        let existing = app
            .feed_connection_bytes(&mut connection, &resp_command(&[b"TYPE", b"k"]))
            .expect("TYPE should execute");
        assert_that!(&existing, eq(&vec![b"+string\r\n".to_vec()]));
    }

    #[rstest]
    fn resp_dbsize_counts_keys_in_selected_database() {
        let mut app = ServerApp::new(RuntimeConfig::default());
        let mut connection = ServerApp::new_connection(ClientProtocol::Resp);

        let _ = app
            .feed_connection_bytes(&mut connection, &resp_command(&[b"SET", b"k0", b"v0"]))
            .expect("SET db0 should execute");
        let _ = app
            .feed_connection_bytes(&mut connection, &resp_command(&[b"SELECT", b"1"]))
            .expect("SELECT 1 should execute");
        let _ = app
            .feed_connection_bytes(&mut connection, &resp_command(&[b"SET", b"k1", b"v1"]))
            .expect("SET db1 should execute");

        let db1_size = app
            .feed_connection_bytes(&mut connection, &resp_command(&[b"DBSIZE"]))
            .expect("DBSIZE db1 should execute");
        assert_that!(&db1_size, eq(&vec![b":1\r\n".to_vec()]));

        let _ = app
            .feed_connection_bytes(&mut connection, &resp_command(&[b"SELECT", b"0"]))
            .expect("SELECT 0 should execute");
        let db0_size = app
            .feed_connection_bytes(&mut connection, &resp_command(&[b"DBSIZE"]))
            .expect("DBSIZE db0 should execute");
        assert_that!(&db0_size, eq(&vec![b":1\r\n".to_vec()]));
    }

    #[rstest]
    fn resp_dbsize_rejects_extra_arguments() {
        let mut app = ServerApp::new(RuntimeConfig::default());
        let mut connection = ServerApp::new_connection(ClientProtocol::Resp);

        let reply = app
            .feed_connection_bytes(&mut connection, &resp_command(&[b"DBSIZE", b"extra"]))
            .expect("DBSIZE should parse");
        assert_that!(
            &reply,
            eq(&vec![
                b"-ERR wrong number of arguments for 'DBSIZE' command\r\n".to_vec()
            ])
        );
    }

    #[rstest]
    fn resp_randomkey_returns_null_for_empty_database() {
        let mut app = ServerApp::new(RuntimeConfig::default());
        let mut connection = ServerApp::new_connection(ClientProtocol::Resp);

        let reply = app
            .feed_connection_bytes(&mut connection, &resp_command(&[b"RANDOMKEY"]))
            .expect("RANDOMKEY should execute");
        assert_that!(&reply, eq(&vec![b"$-1\r\n".to_vec()]));
    }

    #[rstest]
    fn resp_randomkey_returns_existing_key_from_selected_database() {
        let mut app = ServerApp::new(RuntimeConfig::default());
        let mut connection = ServerApp::new_connection(ClientProtocol::Resp);

        let _ = app
            .feed_connection_bytes(
                &mut connection,
                &resp_command(&[b"SET", b"random:one", b"v1"]),
            )
            .expect("SET random:one should execute");
        let _ = app
            .feed_connection_bytes(
                &mut connection,
                &resp_command(&[b"SET", b"random:two", b"v2"]),
            )
            .expect("SET random:two should execute");
        let _ = app
            .feed_connection_bytes(&mut connection, &resp_command(&[b"SELECT", b"1"]))
            .expect("SELECT 1 should execute");
        let _ = app
            .feed_connection_bytes(
                &mut connection,
                &resp_command(&[b"SET", b"random:db1", b"x"]),
            )
            .expect("SET random:db1 should execute");
        let _ = app
            .feed_connection_bytes(&mut connection, &resp_command(&[b"SELECT", b"0"]))
            .expect("SELECT 0 should execute");

        let reply = app
            .feed_connection_bytes(&mut connection, &resp_command(&[b"RANDOMKEY"]))
            .expect("RANDOMKEY should execute");
        assert_that!(reply.len(), eq(1_usize));
        let key = decode_resp_bulk_payload(&reply[0]);
        assert_that!(key == "random:one" || key == "random:two", eq(true));
    }

    #[rstest]
    fn resp_randomkey_rejects_arguments() {
        let mut app = ServerApp::new(RuntimeConfig::default());
        let mut connection = ServerApp::new_connection(ClientProtocol::Resp);

        let reply = app
            .feed_connection_bytes(&mut connection, &resp_command(&[b"RANDOMKEY", b"extra"]))
            .expect("RANDOMKEY should parse");
        assert_that!(
            &reply,
            eq(&vec![
                b"-ERR wrong number of arguments for 'RANDOMKEY' command\r\n".to_vec()
            ])
        );
    }

    #[rstest]
    fn resp_keys_filters_matching_keys_in_selected_database() {
        let mut app = ServerApp::new(RuntimeConfig::default());
        let mut connection = ServerApp::new_connection(ClientProtocol::Resp);

        let _ = app
            .feed_connection_bytes(&mut connection, &resp_command(&[b"SET", b"user:1", b"v1"]))
            .expect("SET user:1 should execute");
        let _ = app
            .feed_connection_bytes(&mut connection, &resp_command(&[b"SET", b"user:2", b"v2"]))
            .expect("SET user:2 should execute");
        let _ = app
            .feed_connection_bytes(&mut connection, &resp_command(&[b"SET", b"admin:1", b"v3"]))
            .expect("SET admin:1 should execute");
        let _ = app
            .feed_connection_bytes(&mut connection, &resp_command(&[b"SELECT", b"1"]))
            .expect("SELECT 1 should execute");
        let _ = app
            .feed_connection_bytes(&mut connection, &resp_command(&[b"SET", b"user:db1", b"x"]))
            .expect("SET db1 key should execute");
        let _ = app
            .feed_connection_bytes(&mut connection, &resp_command(&[b"SELECT", b"0"]))
            .expect("SELECT 0 should execute");

        let user_keys = app
            .feed_connection_bytes(&mut connection, &resp_command(&[b"KEYS", b"user:*"]))
            .expect("KEYS user:* should execute");
        assert_that!(
            &user_keys,
            eq(&vec![b"*2\r\n$6\r\nuser:1\r\n$6\r\nuser:2\r\n".to_vec()])
        );

        let no_match = app
            .feed_connection_bytes(&mut connection, &resp_command(&[b"KEYS", b"missing:*"]))
            .expect("KEYS missing:* should execute");
        assert_that!(&no_match, eq(&vec![b"*0\r\n".to_vec()]));
    }

    #[rstest]
    fn resp_keys_rejects_wrong_arity() {
        let mut app = ServerApp::new(RuntimeConfig::default());
        let mut connection = ServerApp::new_connection(ClientProtocol::Resp);

        let too_few = app
            .feed_connection_bytes(&mut connection, &resp_command(&[b"KEYS"]))
            .expect("KEYS should parse");
        assert_that!(
            &too_few,
            eq(&vec![
                b"-ERR wrong number of arguments for 'KEYS' command\r\n".to_vec()
            ])
        );

        let too_many = app
            .feed_connection_bytes(&mut connection, &resp_command(&[b"KEYS", b"a*", b"extra"]))
            .expect("KEYS should parse");
        assert_that!(
            &too_many,
            eq(&vec![
                b"-ERR wrong number of arguments for 'KEYS' command\r\n".to_vec()
            ])
        );
    }

    #[rstest]
    fn resp_time_returns_unix_seconds_and_microseconds() {
        let mut app = ServerApp::new(RuntimeConfig::default());
        let mut connection = ServerApp::new_connection(ClientProtocol::Resp);

        let reply = app
            .feed_connection_bytes(&mut connection, &resp_command(&[b"TIME"]))
            .expect("TIME should execute");
        assert_that!(reply.len(), eq(1_usize));
        let text = std::str::from_utf8(&reply[0]).expect("TIME reply should be UTF-8");
        let lines = text.split("\r\n").collect::<Vec<_>>();
        assert_that!(lines.first(), eq(Some(&"*2")));
        assert_that!(
            lines.get(2).is_some_and(|value| !value.is_empty()),
            eq(true)
        );
        assert_that!(
            lines.get(4).is_some_and(|value| !value.is_empty()),
            eq(true)
        );
    }

    #[rstest]
    fn resp_time_rejects_arguments() {
        let mut app = ServerApp::new(RuntimeConfig::default());
        let mut connection = ServerApp::new_connection(ClientProtocol::Resp);

        let reply = app
            .feed_connection_bytes(&mut connection, &resp_command(&[b"TIME", b"extra"]))
            .expect("TIME should parse");
        assert_that!(
            &reply,
            eq(&vec![
                b"-ERR wrong number of arguments for 'TIME' command\r\n".to_vec()
            ])
        );
    }

    #[rstest]
    fn resp_mset_then_mget_roundtrip() {
        let mut app = ServerApp::new(RuntimeConfig::default());
        let mut connection = ServerApp::new_connection(ClientProtocol::Resp);

        let mset = resp_command(&[b"MSET", b"foo", b"bar", b"baz", b"qux"]);
        let mset_reply = app
            .feed_connection_bytes(&mut connection, &mset)
            .expect("MSET should execute");
        assert_that!(&mset_reply, eq(&vec![b"+OK\r\n".to_vec()]));

        let mget = resp_command(&[b"MGET", b"foo", b"baz"]);
        let mget_reply = app
            .feed_connection_bytes(&mut connection, &mget)
            .expect("MGET should execute");
        let expected = vec![b"*2\r\n$3\r\nbar\r\n$3\r\nqux\r\n".to_vec()];
        assert_that!(&mget_reply, eq(&expected));
    }

    #[rstest]
    fn resp_mset_rejects_odd_arity() {
        let mut app = ServerApp::new(RuntimeConfig::default());
        let mut connection = ServerApp::new_connection(ClientProtocol::Resp);

        let invalid = resp_command(&[b"MSET", b"foo", b"bar", b"baz"]);
        let reply = app
            .feed_connection_bytes(&mut connection, &invalid)
            .expect("MSET should parse");
        assert_that!(
            &reply,
            eq(&vec![
                b"-ERR wrong number of arguments for 'MSET' command\r\n".to_vec()
            ])
        );
    }

    #[rstest]
    fn resp_msetnx_sets_all_or_none() {
        let mut app = ServerApp::new(RuntimeConfig::default());
        let mut connection = ServerApp::new_connection(ClientProtocol::Resp);

        let first = app
            .feed_connection_bytes(
                &mut connection,
                &resp_command(&[b"MSETNX", b"a", b"1", b"b", b"2"]),
            )
            .expect("first MSETNX should execute");
        assert_that!(&first, eq(&vec![b":1\r\n".to_vec()]));

        let second = app
            .feed_connection_bytes(
                &mut connection,
                &resp_command(&[b"MSETNX", b"a", b"x", b"c", b"3"]),
            )
            .expect("second MSETNX should execute");
        assert_that!(&second, eq(&vec![b":0\r\n".to_vec()]));

        let values = app
            .feed_connection_bytes(&mut connection, &resp_command(&[b"MGET", b"a", b"b", b"c"]))
            .expect("MGET should execute");
        assert_that!(
            &values,
            eq(&vec![b"*3\r\n$1\r\n1\r\n$1\r\n2\r\n$-1\r\n".to_vec()])
        );
    }

    #[rstest]
    fn resp_msetnx_rejects_odd_arity() {
        let mut app = ServerApp::new(RuntimeConfig::default());
        let mut connection = ServerApp::new_connection(ClientProtocol::Resp);

        let invalid = resp_command(&[b"MSETNX", b"a", b"1", b"b"]);
        let reply = app
            .feed_connection_bytes(&mut connection, &invalid)
            .expect("MSETNX should parse");
        assert_that!(
            &reply,
            eq(&vec![
                b"-ERR wrong number of arguments for 'MSETNX' command\r\n".to_vec()
            ])
        );
    }

    #[rstest]
    fn resp_exists_and_del_follow_multi_key_count_semantics() {
        let mut app = ServerApp::new(RuntimeConfig::default());
        let mut connection = ServerApp::new_connection(ClientProtocol::Resp);

        let _ = app
            .feed_connection_bytes(&mut connection, &resp_command(&[b"SET", b"k1", b"v1"]))
            .expect("SET k1 should succeed");
        let _ = app
            .feed_connection_bytes(&mut connection, &resp_command(&[b"SET", b"k2", b"v2"]))
            .expect("SET k2 should succeed");

        let exists = app
            .feed_connection_bytes(
                &mut connection,
                &resp_command(&[b"EXISTS", b"k1", b"k2", b"missing", b"k1"]),
            )
            .expect("EXISTS should execute");
        assert_that!(&exists, eq(&vec![b":3\r\n".to_vec()]));

        let deleted = app
            .feed_connection_bytes(
                &mut connection,
                &resp_command(&[b"DEL", b"k1", b"k2", b"missing", b"k1"]),
            )
            .expect("DEL should execute");
        assert_that!(&deleted, eq(&vec![b":2\r\n".to_vec()]));

        let exists_after = app
            .feed_connection_bytes(&mut connection, &resp_command(&[b"EXISTS", b"k1", b"k2"]))
            .expect("EXISTS should execute");
        assert_that!(&exists_after, eq(&vec![b":0\r\n".to_vec()]));
    }

    #[rstest]
    fn resp_unlink_follows_multi_key_count_semantics() {
        let mut app = ServerApp::new(RuntimeConfig::default());
        let mut connection = ServerApp::new_connection(ClientProtocol::Resp);

        let _ = app
            .feed_connection_bytes(&mut connection, &resp_command(&[b"SET", b"k1", b"v1"]))
            .expect("SET k1 should succeed");
        let _ = app
            .feed_connection_bytes(&mut connection, &resp_command(&[b"SET", b"k2", b"v2"]))
            .expect("SET k2 should succeed");

        let unlinked = app
            .feed_connection_bytes(
                &mut connection,
                &resp_command(&[b"UNLINK", b"k1", b"k2", b"missing", b"k1"]),
            )
            .expect("UNLINK should execute");
        assert_that!(&unlinked, eq(&vec![b":2\r\n".to_vec()]));

        let exists_after = app
            .feed_connection_bytes(&mut connection, &resp_command(&[b"EXISTS", b"k1", b"k2"]))
            .expect("EXISTS should execute");
        assert_that!(&exists_after, eq(&vec![b":0\r\n".to_vec()]));
    }

    #[rstest]
    fn resp_touch_counts_existing_keys() {
        let mut app = ServerApp::new(RuntimeConfig::default());
        let mut connection = ServerApp::new_connection(ClientProtocol::Resp);

        let _ = app
            .feed_connection_bytes(&mut connection, &resp_command(&[b"SET", b"k1", b"v1"]))
            .expect("SET k1 should succeed");
        let _ = app
            .feed_connection_bytes(&mut connection, &resp_command(&[b"SET", b"k2", b"v2"]))
            .expect("SET k2 should succeed");

        let touched = app
            .feed_connection_bytes(
                &mut connection,
                &resp_command(&[b"TOUCH", b"k1", b"k2", b"missing", b"k1"]),
            )
            .expect("TOUCH should execute");
        assert_that!(&touched, eq(&vec![b":3\r\n".to_vec()]));
    }

    #[rstest]
    fn resp_move_transfers_key_between_databases() {
        let mut app = ServerApp::new(RuntimeConfig::default());
        let mut connection = ServerApp::new_connection(ClientProtocol::Resp);

        let _ = app
            .feed_connection_bytes(&mut connection, &resp_command(&[b"SET", b"move:key", b"v"]))
            .expect("SET should execute");
        let moved = app
            .feed_connection_bytes(
                &mut connection,
                &resp_command(&[b"MOVE", b"move:key", b"1"]),
            )
            .expect("MOVE should execute");
        assert_that!(&moved, eq(&vec![b":1\r\n".to_vec()]));

        let source = app
            .feed_connection_bytes(&mut connection, &resp_command(&[b"GET", b"move:key"]))
            .expect("GET source should execute");
        assert_that!(&source, eq(&vec![b"$-1\r\n".to_vec()]));

        let _ = app
            .feed_connection_bytes(&mut connection, &resp_command(&[b"SELECT", b"1"]))
            .expect("SELECT 1 should execute");
        let target = app
            .feed_connection_bytes(&mut connection, &resp_command(&[b"GET", b"move:key"]))
            .expect("GET target should execute");
        assert_that!(&target, eq(&vec![b"$1\r\nv\r\n".to_vec()]));
    }

    #[rstest]
    fn resp_move_returns_zero_when_target_contains_key() {
        let mut app = ServerApp::new(RuntimeConfig::default());
        let mut connection = ServerApp::new_connection(ClientProtocol::Resp);

        let _ = app
            .feed_connection_bytes(&mut connection, &resp_command(&[b"SET", b"dup", b"db0"]))
            .expect("SET db0 should execute");
        let _ = app
            .feed_connection_bytes(&mut connection, &resp_command(&[b"SELECT", b"1"]))
            .expect("SELECT 1 should execute");
        let _ = app
            .feed_connection_bytes(&mut connection, &resp_command(&[b"SET", b"dup", b"db1"]))
            .expect("SET db1 should execute");
        let _ = app
            .feed_connection_bytes(&mut connection, &resp_command(&[b"SELECT", b"0"]))
            .expect("SELECT 0 should execute");

        let moved = app
            .feed_connection_bytes(&mut connection, &resp_command(&[b"MOVE", b"dup", b"1"]))
            .expect("MOVE should execute");
        assert_that!(&moved, eq(&vec![b":0\r\n".to_vec()]));

        let source = app
            .feed_connection_bytes(&mut connection, &resp_command(&[b"GET", b"dup"]))
            .expect("GET source should execute");
        assert_that!(&source, eq(&vec![b"$3\r\ndb0\r\n".to_vec()]));
    }

    #[rstest]
    fn resp_copy_follows_dragonfly_replace_and_error_semantics() {
        let mut app = ServerApp::new(RuntimeConfig::default());
        let mut connection = ServerApp::new_connection(ClientProtocol::Resp);

        let missing = app
            .feed_connection_bytes(
                &mut connection,
                &resp_command(&[b"COPY", b"missing", b"dst"]),
            )
            .expect("COPY missing key should parse");
        assert_that!(&missing, eq(&vec![b":0\r\n".to_vec()]));

        let _ = app
            .feed_connection_bytes(
                &mut connection,
                &resp_command(&[b"SETEX", b"src", b"60", b"v1"]),
            )
            .expect("SETEX src should execute");
        let _ = app
            .feed_connection_bytes(&mut connection, &resp_command(&[b"SET", b"dst", b"v2"]))
            .expect("SET dst should execute");

        let blocked = app
            .feed_connection_bytes(&mut connection, &resp_command(&[b"COPY", b"src", b"dst"]))
            .expect("COPY without REPLACE should execute");
        assert_that!(&blocked, eq(&vec![b":0\r\n".to_vec()]));

        let replaced = app
            .feed_connection_bytes(
                &mut connection,
                &resp_command(&[b"COPY", b"src", b"dst", b"REPLACE"]),
            )
            .expect("COPY REPLACE should execute");
        assert_that!(&replaced, eq(&vec![b":1\r\n".to_vec()]));

        let src = app
            .feed_connection_bytes(&mut connection, &resp_command(&[b"GET", b"src"]))
            .expect("GET src should execute");
        let dst = app
            .feed_connection_bytes(&mut connection, &resp_command(&[b"GET", b"dst"]))
            .expect("GET dst should execute");
        assert_that!(&src, eq(&vec![b"$2\r\nv1\r\n".to_vec()]));
        assert_that!(&dst, eq(&vec![b"$2\r\nv1\r\n".to_vec()]));

        let same = app
            .feed_connection_bytes(&mut connection, &resp_command(&[b"COPY", b"dst", b"dst"]))
            .expect("COPY same key should parse");
        assert_that!(
            &same,
            eq(&vec![
                b"-ERR source and destination objects are the same\r\n".to_vec()
            ])
        );

        let unsupported_db = app
            .feed_connection_bytes(
                &mut connection,
                &resp_command(&[b"COPY", b"src", b"dst2", b"DB", b"1"]),
            )
            .expect("COPY with DB option should parse");
        assert_that!(
            &unsupported_db,
            eq(&vec![b"-ERR syntax error\r\n".to_vec()])
        );
    }

    #[rstest]
    fn resp_copy_rejects_crossslot_keys_in_cluster_mode() {
        let config = RuntimeConfig {
            cluster_mode: ClusterMode::Emulated,
            ..RuntimeConfig::default()
        };
        let mut app = ServerApp::new(config);
        let mut connection = ServerApp::new_connection(ClientProtocol::Resp);

        let source_key = b"copy:{100}".to_vec();
        let mut destination_key = b"copy:{200}".to_vec();
        let mut suffix = 0_u32;
        while key_slot(&destination_key) == key_slot(&source_key) {
            suffix = suffix.saturating_add(1);
            destination_key = format!("copy:{{200}}:{suffix}").into_bytes();
        }

        let _ = app
            .feed_connection_bytes(
                &mut connection,
                &resp_command(&[b"SET", &source_key, b"value"]),
            )
            .expect("SET source should execute");
        let reply = app
            .feed_connection_bytes(
                &mut connection,
                &resp_command(&[b"COPY", &source_key, &destination_key]),
            )
            .expect("COPY should parse");
        assert_that!(
            &reply,
            eq(&vec![
                b"-ERR CROSSSLOT Keys in request don't hash to the same slot\r\n".to_vec()
            ])
        );
    }

    #[rstest]
    fn resp_rename_and_renamenx_follow_redis_style_outcomes() {
        let mut app = ServerApp::new(RuntimeConfig::default());
        let mut connection = ServerApp::new_connection(ClientProtocol::Resp);

        let missing = app
            .feed_connection_bytes(
                &mut connection,
                &resp_command(&[b"RENAME", b"missing", b"dst"]),
            )
            .expect("RENAME missing should parse");
        assert_that!(&missing, eq(&vec![b"-ERR no such key\r\n".to_vec()]));

        let _ = app
            .feed_connection_bytes(&mut connection, &resp_command(&[b"SET", b"src", b"v1"]))
            .expect("SET src should execute");
        let _ = app
            .feed_connection_bytes(&mut connection, &resp_command(&[b"SET", b"dst", b"v2"]))
            .expect("SET dst should execute");

        let blocked = app
            .feed_connection_bytes(
                &mut connection,
                &resp_command(&[b"RENAMENX", b"src", b"dst"]),
            )
            .expect("RENAMENX should execute");
        assert_that!(&blocked, eq(&vec![b":0\r\n".to_vec()]));

        let renamed = app
            .feed_connection_bytes(&mut connection, &resp_command(&[b"RENAME", b"src", b"dst"]))
            .expect("RENAME should execute");
        assert_that!(&renamed, eq(&vec![b"+OK\r\n".to_vec()]));

        let src = app
            .feed_connection_bytes(&mut connection, &resp_command(&[b"GET", b"src"]))
            .expect("GET src should execute");
        assert_that!(&src, eq(&vec![b"$-1\r\n".to_vec()]));
        let dst = app
            .feed_connection_bytes(&mut connection, &resp_command(&[b"GET", b"dst"]))
            .expect("GET dst should execute");
        assert_that!(&dst, eq(&vec![b"$2\r\nv1\r\n".to_vec()]));

        let same = app
            .feed_connection_bytes(&mut connection, &resp_command(&[b"RENAME", b"dst", b"dst"]))
            .expect("RENAME same key should execute");
        assert_that!(&same, eq(&vec![b"+OK\r\n".to_vec()]));

        let same_nx = app
            .feed_connection_bytes(
                &mut connection,
                &resp_command(&[b"RENAMENX", b"dst", b"dst"]),
            )
            .expect("RENAMENX same key should execute");
        assert_that!(&same_nx, eq(&vec![b":0\r\n".to_vec()]));
    }

    #[rstest]
    fn resp_rename_rejects_crossslot_keys_in_cluster_mode() {
        let config = RuntimeConfig {
            cluster_mode: ClusterMode::Emulated,
            ..RuntimeConfig::default()
        };
        let mut app = ServerApp::new(config);
        let mut connection = ServerApp::new_connection(ClientProtocol::Resp);

        let source_key = b"acct:{100}".to_vec();
        let mut destination_key = b"acct:{200}".to_vec();
        let mut suffix = 0_u32;
        while key_slot(&destination_key) == key_slot(&source_key) {
            suffix = suffix.saturating_add(1);
            destination_key = format!("acct:{{200}}:{suffix}").into_bytes();
        }

        let _ = app
            .feed_connection_bytes(&mut connection, &resp_command(&[b"SET", &source_key, b"v"]))
            .expect("SET source should execute");
        let reply = app
            .feed_connection_bytes(
                &mut connection,
                &resp_command(&[b"RENAME", &source_key, &destination_key]),
            )
            .expect("RENAME should parse");
        assert_that!(
            &reply,
            eq(&vec![
                b"-ERR CROSSSLOT Keys in request don't hash to the same slot\r\n".to_vec()
            ])
        );
    }

    #[rstest]
    fn resp_persist_clears_expire_without_removing_key() {
        let mut app = ServerApp::new(RuntimeConfig::default());
        let mut connection = ServerApp::new_connection(ClientProtocol::Resp);

        let _ = app
            .feed_connection_bytes(&mut connection, &resp_command(&[b"SET", b"ttl:key", b"v"]))
            .expect("SET should succeed");
        let _ = app
            .feed_connection_bytes(
                &mut connection,
                &resp_command(&[b"EXPIRE", b"ttl:key", b"10"]),
            )
            .expect("EXPIRE should succeed");

        let persist = app
            .feed_connection_bytes(&mut connection, &resp_command(&[b"PERSIST", b"ttl:key"]))
            .expect("PERSIST should execute");
        assert_that!(&persist, eq(&vec![b":1\r\n".to_vec()]));

        let ttl = app
            .feed_connection_bytes(&mut connection, &resp_command(&[b"TTL", b"ttl:key"]))
            .expect("TTL should execute");
        assert_that!(&ttl, eq(&vec![b":-1\r\n".to_vec()]));

        let value = app
            .feed_connection_bytes(&mut connection, &resp_command(&[b"GET", b"ttl:key"]))
            .expect("GET should execute");
        assert_that!(&value, eq(&vec![b"$1\r\nv\r\n".to_vec()]));
    }

    #[rstest]
    fn resp_setex_sets_value_with_ttl_and_rejects_non_positive_ttl() {
        let mut app = ServerApp::new(RuntimeConfig::default());
        let mut connection = ServerApp::new_connection(ClientProtocol::Resp);

        let ok = app
            .feed_connection_bytes(
                &mut connection,
                &resp_command(&[b"SETEX", b"session", b"60", b"alive"]),
            )
            .expect("SETEX should execute");
        assert_that!(&ok, eq(&vec![b"+OK\r\n".to_vec()]));

        let value = app
            .feed_connection_bytes(&mut connection, &resp_command(&[b"GET", b"session"]))
            .expect("GET should execute");
        assert_that!(&value, eq(&vec![b"$5\r\nalive\r\n".to_vec()]));

        let ttl = app
            .feed_connection_bytes(&mut connection, &resp_command(&[b"TTL", b"session"]))
            .expect("TTL should execute");
        let ttl_value = parse_resp_integer(&ttl[0]);
        assert_that!(ttl_value > 0, eq(true));

        let bad = app
            .feed_connection_bytes(
                &mut connection,
                &resp_command(&[b"SETEX", b"session", b"0", b"alive"]),
            )
            .expect("SETEX should parse");
        assert_that!(
            &bad,
            eq(&vec![
                b"-ERR invalid expire time in 'SETEX' command\r\n".to_vec()
            ])
        );
    }

    #[rstest]
    fn resp_psetex_sets_value_with_pttl_and_rejects_non_positive_ttl() {
        let mut app = ServerApp::new(RuntimeConfig::default());
        let mut connection = ServerApp::new_connection(ClientProtocol::Resp);

        let ok = app
            .feed_connection_bytes(
                &mut connection,
                &resp_command(&[b"PSETEX", b"session", b"1500", b"alive"]),
            )
            .expect("PSETEX should execute");
        assert_that!(&ok, eq(&vec![b"+OK\r\n".to_vec()]));

        let value = app
            .feed_connection_bytes(&mut connection, &resp_command(&[b"GET", b"session"]))
            .expect("GET should execute");
        assert_that!(&value, eq(&vec![b"$5\r\nalive\r\n".to_vec()]));

        let pttl = app
            .feed_connection_bytes(&mut connection, &resp_command(&[b"PTTL", b"session"]))
            .expect("PTTL should execute");
        let pttl_value = parse_resp_integer(&pttl[0]);
        assert_that!(pttl_value > 0, eq(true));

        let bad = app
            .feed_connection_bytes(
                &mut connection,
                &resp_command(&[b"PSETEX", b"session", b"0", b"alive"]),
            )
            .expect("PSETEX should parse");
        assert_that!(
            &bad,
            eq(&vec![
                b"-ERR invalid expire time in 'PSETEX' command\r\n".to_vec()
            ])
        );
    }

    #[rstest]
    fn resp_pttl_and_pexpire_follow_millisecond_expiry_lifecycle() {
        let mut app = ServerApp::new(RuntimeConfig::default());
        let mut connection = ServerApp::new_connection(ClientProtocol::Resp);

        let _ = app
            .feed_connection_bytes(&mut connection, &resp_command(&[b"SET", b"temp", b"value"]))
            .expect("SET should execute");
        let pttl_without_expire = app
            .feed_connection_bytes(&mut connection, &resp_command(&[b"PTTL", b"temp"]))
            .expect("PTTL should execute");
        assert_that!(&pttl_without_expire, eq(&vec![b":-1\r\n".to_vec()]));

        let pexpire = app
            .feed_connection_bytes(
                &mut connection,
                &resp_command(&[b"PEXPIRE", b"temp", b"1500"]),
            )
            .expect("PEXPIRE should execute");
        assert_that!(&pexpire, eq(&vec![b":1\r\n".to_vec()]));

        let pttl = app
            .feed_connection_bytes(&mut connection, &resp_command(&[b"PTTL", b"temp"]))
            .expect("PTTL should execute");
        let pttl_value = parse_resp_integer(&pttl[0]);
        assert_that!(pttl_value > 0, eq(true));

        let pexpire_now = app
            .feed_connection_bytes(&mut connection, &resp_command(&[b"PEXPIRE", b"temp", b"0"]))
            .expect("PEXPIRE should execute");
        assert_that!(&pexpire_now, eq(&vec![b":1\r\n".to_vec()]));

        let pttl_after = app
            .feed_connection_bytes(&mut connection, &resp_command(&[b"PTTL", b"temp"]))
            .expect("PTTL should execute");
        assert_that!(&pttl_after, eq(&vec![b":-2\r\n".to_vec()]));
    }

    #[rstest]
    fn resp_expire_family_supports_nx_xx_gt_lt_options() {
        let mut app = ServerApp::new(RuntimeConfig::default());
        let mut connection = ServerApp::new_connection(ClientProtocol::Resp);

        let _ = app
            .feed_connection_bytes(&mut connection, &resp_command(&[b"SET", b"opt:key", b"v"]))
            .expect("SET should execute");

        let gt_on_persistent = app
            .feed_connection_bytes(
                &mut connection,
                &resp_command(&[b"EXPIRE", b"opt:key", b"10", b"GT"]),
            )
            .expect("EXPIRE GT should parse");
        assert_that!(&gt_on_persistent, eq(&vec![b":0\r\n".to_vec()]));

        let lt_on_persistent = app
            .feed_connection_bytes(
                &mut connection,
                &resp_command(&[b"EXPIRE", b"opt:key", b"10", b"LT"]),
            )
            .expect("EXPIRE LT should parse");
        assert_that!(&lt_on_persistent, eq(&vec![b":1\r\n".to_vec()]));

        let first_expiretime = app
            .feed_connection_bytes(&mut connection, &resp_command(&[b"EXPIRETIME", b"opt:key"]))
            .expect("EXPIRETIME should execute");
        let first_expiretime = parse_resp_integer(&first_expiretime[0]);

        let nx_with_existing_expire = app
            .feed_connection_bytes(
                &mut connection,
                &resp_command(&[b"EXPIRE", b"opt:key", b"20", b"NX"]),
            )
            .expect("EXPIRE NX should parse");
        assert_that!(&nx_with_existing_expire, eq(&vec![b":0\r\n".to_vec()]));

        let xx_with_existing_expire = app
            .feed_connection_bytes(
                &mut connection,
                &resp_command(&[b"EXPIRE", b"opt:key", b"20", b"XX"]),
            )
            .expect("EXPIRE XX should parse");
        assert_that!(&xx_with_existing_expire, eq(&vec![b":1\r\n".to_vec()]));

        let second_expiretime = app
            .feed_connection_bytes(&mut connection, &resp_command(&[b"EXPIRETIME", b"opt:key"]))
            .expect("EXPIRETIME should execute");
        let second_expiretime = parse_resp_integer(&second_expiretime[0]);
        assert_that!(second_expiretime > first_expiretime, eq(true));

        let gt_with_smaller_target = app
            .feed_connection_bytes(
                &mut connection,
                &resp_command(&[b"EXPIRE", b"opt:key", b"5", b"GT"]),
            )
            .expect("EXPIRE GT should parse");
        assert_that!(&gt_with_smaller_target, eq(&vec![b":0\r\n".to_vec()]));

        let lt_with_smaller_target = app
            .feed_connection_bytes(
                &mut connection,
                &resp_command(&[b"EXPIRE", b"opt:key", b"5", b"LT"]),
            )
            .expect("EXPIRE LT should parse");
        assert_that!(&lt_with_smaller_target, eq(&vec![b":1\r\n".to_vec()]));
    }

    #[rstest]
    fn resp_expire_family_rejects_invalid_option_sets() {
        let mut app = ServerApp::new(RuntimeConfig::default());
        let mut connection = ServerApp::new_connection(ClientProtocol::Resp);

        let nx_xx = app
            .feed_connection_bytes(
                &mut connection,
                &resp_command(&[b"EXPIRE", b"missing", b"10", b"NX", b"XX"]),
            )
            .expect("EXPIRE should parse");
        assert_that!(
            &nx_xx,
            eq(&vec![
                b"-ERR NX and XX options at the same time are not compatible\r\n".to_vec()
            ])
        );

        let gt_lt = app
            .feed_connection_bytes(
                &mut connection,
                &resp_command(&[b"PEXPIRE", b"missing", b"10", b"GT", b"LT"]),
            )
            .expect("PEXPIRE should parse");
        assert_that!(
            &gt_lt,
            eq(&vec![
                b"-ERR GT and LT options at the same time are not compatible\r\n".to_vec()
            ])
        );

        let unknown = app
            .feed_connection_bytes(
                &mut connection,
                &resp_command(&[b"EXPIREAT", b"missing", b"1", b"SOMETHING"]),
            )
            .expect("EXPIREAT should parse");
        assert_that!(
            &unknown,
            eq(&vec![b"-ERR Unsupported option: SOMETHING\r\n".to_vec()])
        );
    }

    #[rstest]
    fn resp_expiretime_reports_missing_persistent_and_expiring_keys() {
        let mut app = ServerApp::new(RuntimeConfig::default());
        let mut connection = ServerApp::new_connection(ClientProtocol::Resp);

        let missing = app
            .feed_connection_bytes(&mut connection, &resp_command(&[b"EXPIRETIME", b"missing"]))
            .expect("EXPIRETIME should execute");
        assert_that!(&missing, eq(&vec![b":-2\r\n".to_vec()]));

        let _ = app
            .feed_connection_bytes(&mut connection, &resp_command(&[b"SET", b"persist", b"v"]))
            .expect("SET should execute");
        let persistent = app
            .feed_connection_bytes(&mut connection, &resp_command(&[b"EXPIRETIME", b"persist"]))
            .expect("EXPIRETIME should execute");
        assert_that!(&persistent, eq(&vec![b":-1\r\n".to_vec()]));

        let _ = app
            .feed_connection_bytes(
                &mut connection,
                &resp_command(&[b"SETEX", b"temp", b"60", b"v"]),
            )
            .expect("SETEX should execute");
        let expiring = app
            .feed_connection_bytes(&mut connection, &resp_command(&[b"EXPIRETIME", b"temp"]))
            .expect("EXPIRETIME should execute");
        let expire_at = parse_resp_integer(&expiring[0]);
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_or(0_i64, |duration| {
                i64::try_from(duration.as_secs()).unwrap_or(i64::MAX)
            });
        assert_that!(expire_at >= now, eq(true));
    }

    #[rstest]
    fn resp_pexpiretime_reports_missing_persistent_and_expiring_keys() {
        let mut app = ServerApp::new(RuntimeConfig::default());
        let mut connection = ServerApp::new_connection(ClientProtocol::Resp);

        let missing = app
            .feed_connection_bytes(
                &mut connection,
                &resp_command(&[b"PEXPIRETIME", b"missing"]),
            )
            .expect("PEXPIRETIME should execute");
        assert_that!(&missing, eq(&vec![b":-2\r\n".to_vec()]));

        let _ = app
            .feed_connection_bytes(&mut connection, &resp_command(&[b"SET", b"persist", b"v"]))
            .expect("SET should execute");
        let persistent = app
            .feed_connection_bytes(
                &mut connection,
                &resp_command(&[b"PEXPIRETIME", b"persist"]),
            )
            .expect("PEXPIRETIME should execute");
        assert_that!(&persistent, eq(&vec![b":-1\r\n".to_vec()]));

        let _ = app
            .feed_connection_bytes(
                &mut connection,
                &resp_command(&[b"SETEX", b"temp", b"60", b"v"]),
            )
            .expect("SETEX should execute");
        let expiring = app
            .feed_connection_bytes(&mut connection, &resp_command(&[b"PEXPIRETIME", b"temp"]))
            .expect("PEXPIRETIME should execute");
        let expire_at = parse_resp_integer(&expiring[0]);
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_or(0_i64, |duration| {
                i64::try_from(duration.as_millis()).unwrap_or(i64::MAX)
            });
        assert_that!(expire_at >= now, eq(true));
    }

    #[rstest]
    fn resp_incr_family_updates_counters_and_preserves_numeric_result() {
        let mut app = ServerApp::new(RuntimeConfig::default());
        let mut connection = ServerApp::new_connection(ClientProtocol::Resp);

        let incr = app
            .feed_connection_bytes(&mut connection, &resp_command(&[b"INCR", b"counter"]))
            .expect("INCR should execute");
        assert_that!(&incr, eq(&vec![b":1\r\n".to_vec()]));

        let incrby = app
            .feed_connection_bytes(
                &mut connection,
                &resp_command(&[b"INCRBY", b"counter", b"9"]),
            )
            .expect("INCRBY should execute");
        assert_that!(&incrby, eq(&vec![b":10\r\n".to_vec()]));

        let decr = app
            .feed_connection_bytes(&mut connection, &resp_command(&[b"DECR", b"counter"]))
            .expect("DECR should execute");
        assert_that!(&decr, eq(&vec![b":9\r\n".to_vec()]));

        let decrby = app
            .feed_connection_bytes(
                &mut connection,
                &resp_command(&[b"DECRBY", b"counter", b"4"]),
            )
            .expect("DECRBY should execute");
        assert_that!(&decrby, eq(&vec![b":5\r\n".to_vec()]));

        let value = app
            .feed_connection_bytes(&mut connection, &resp_command(&[b"GET", b"counter"]))
            .expect("GET should execute");
        assert_that!(&value, eq(&vec![b"$1\r\n5\r\n".to_vec()]));
    }

    #[rstest]
    fn resp_incr_rejects_non_integer_values() {
        let mut app = ServerApp::new(RuntimeConfig::default());
        let mut connection = ServerApp::new_connection(ClientProtocol::Resp);

        let _ = app
            .feed_connection_bytes(
                &mut connection,
                &resp_command(&[b"SET", b"counter", b"abc"]),
            )
            .expect("SET should execute");
        let incr = app
            .feed_connection_bytes(&mut connection, &resp_command(&[b"INCR", b"counter"]))
            .expect("INCR should parse");
        assert_that!(
            &incr,
            eq(&vec![
                b"-ERR value is not an integer or out of range\r\n".to_vec()
            ])
        );
    }

    #[rstest]
    fn resp_setnx_sets_only_missing_keys() {
        let mut app = ServerApp::new(RuntimeConfig::default());
        let mut connection = ServerApp::new_connection(ClientProtocol::Resp);

        let first = app
            .feed_connection_bytes(&mut connection, &resp_command(&[b"SETNX", b"only", b"v1"]))
            .expect("first SETNX should execute");
        assert_that!(&first, eq(&vec![b":1\r\n".to_vec()]));

        let second = app
            .feed_connection_bytes(&mut connection, &resp_command(&[b"SETNX", b"only", b"v2"]))
            .expect("second SETNX should execute");
        assert_that!(&second, eq(&vec![b":0\r\n".to_vec()]));

        let value = app
            .feed_connection_bytes(&mut connection, &resp_command(&[b"GET", b"only"]))
            .expect("GET should execute");
        assert_that!(&value, eq(&vec![b"$2\r\nv1\r\n".to_vec()]));
    }

    #[rstest]
    fn resp_getset_returns_previous_value_and_replaces_key() {
        let mut app = ServerApp::new(RuntimeConfig::default());
        let mut connection = ServerApp::new_connection(ClientProtocol::Resp);

        let first = app
            .feed_connection_bytes(&mut connection, &resp_command(&[b"GETSET", b"k", b"v1"]))
            .expect("first GETSET should execute");
        assert_that!(&first, eq(&vec![b"$-1\r\n".to_vec()]));

        let second = app
            .feed_connection_bytes(&mut connection, &resp_command(&[b"GETSET", b"k", b"v2"]))
            .expect("second GETSET should execute");
        assert_that!(&second, eq(&vec![b"$2\r\nv1\r\n".to_vec()]));

        let value = app
            .feed_connection_bytes(&mut connection, &resp_command(&[b"GET", b"k"]))
            .expect("GET should execute");
        assert_that!(&value, eq(&vec![b"$2\r\nv2\r\n".to_vec()]));
    }

    #[rstest]
    fn resp_getdel_returns_value_and_deletes_key() {
        let mut app = ServerApp::new(RuntimeConfig::default());
        let mut connection = ServerApp::new_connection(ClientProtocol::Resp);

        let _ = app
            .feed_connection_bytes(&mut connection, &resp_command(&[b"SET", b"k", b"v"]))
            .expect("SET should execute");
        let removed = app
            .feed_connection_bytes(&mut connection, &resp_command(&[b"GETDEL", b"k"]))
            .expect("GETDEL should execute");
        assert_that!(&removed, eq(&vec![b"$1\r\nv\r\n".to_vec()]));

        let missing = app
            .feed_connection_bytes(&mut connection, &resp_command(&[b"GETDEL", b"k"]))
            .expect("GETDEL should execute");
        assert_that!(&missing, eq(&vec![b"$-1\r\n".to_vec()]));
    }

    #[rstest]
    fn resp_append_and_strlen_track_string_size() {
        let mut app = ServerApp::new(RuntimeConfig::default());
        let mut connection = ServerApp::new_connection(ClientProtocol::Resp);

        let first = app
            .feed_connection_bytes(&mut connection, &resp_command(&[b"APPEND", b"k", b"he"]))
            .expect("first APPEND should execute");
        assert_that!(&first, eq(&vec![b":2\r\n".to_vec()]));

        let second = app
            .feed_connection_bytes(&mut connection, &resp_command(&[b"APPEND", b"k", b"llo"]))
            .expect("second APPEND should execute");
        assert_that!(&second, eq(&vec![b":5\r\n".to_vec()]));

        let strlen = app
            .feed_connection_bytes(&mut connection, &resp_command(&[b"STRLEN", b"k"]))
            .expect("STRLEN should execute");
        assert_that!(&strlen, eq(&vec![b":5\r\n".to_vec()]));

        let value = app
            .feed_connection_bytes(&mut connection, &resp_command(&[b"GET", b"k"]))
            .expect("GET should execute");
        assert_that!(&value, eq(&vec![b"$5\r\nhello\r\n".to_vec()]));
    }

    #[rstest]
    fn resp_getrange_and_setrange_follow_redis_offset_rules() {
        let mut app = ServerApp::new(RuntimeConfig::default());
        let mut connection = ServerApp::new_connection(ClientProtocol::Resp);

        let _ = app
            .feed_connection_bytes(&mut connection, &resp_command(&[b"SET", b"k", b"hello"]))
            .expect("SET should execute");
        let range = app
            .feed_connection_bytes(
                &mut connection,
                &resp_command(&[b"GETRANGE", b"k", b"1", b"3"]),
            )
            .expect("GETRANGE should execute");
        assert_that!(&range, eq(&vec![b"$3\r\nell\r\n".to_vec()]));

        let setrange = app
            .feed_connection_bytes(
                &mut connection,
                &resp_command(&[b"SETRANGE", b"k", b"1", b"i"]),
            )
            .expect("SETRANGE should execute");
        assert_that!(&setrange, eq(&vec![b":5\r\n".to_vec()]));

        let value = app
            .feed_connection_bytes(&mut connection, &resp_command(&[b"GET", b"k"]))
            .expect("GET should execute");
        assert_that!(&value, eq(&vec![b"$5\r\nhillo\r\n".to_vec()]));
    }

    #[rstest]
    fn resp_expireat_sets_future_expire_and_deletes_past_keys() {
        let mut app = ServerApp::new(RuntimeConfig::default());
        let mut connection = ServerApp::new_connection(ClientProtocol::Resp);

        let _ = app
            .feed_connection_bytes(&mut connection, &resp_command(&[b"SET", b"future", b"v"]))
            .expect("SET future should execute");
        let future_timestamp = (SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("system time should be after epoch")
            .as_secs()
            + 120)
            .to_string()
            .into_bytes();
        let expireat_future = app
            .feed_connection_bytes(
                &mut connection,
                &resp_command(&[b"EXPIREAT", b"future", &future_timestamp]),
            )
            .expect("EXPIREAT future should execute");
        assert_that!(&expireat_future, eq(&vec![b":1\r\n".to_vec()]));

        let ttl_future = app
            .feed_connection_bytes(&mut connection, &resp_command(&[b"TTL", b"future"]))
            .expect("TTL should execute");
        let ttl_value = parse_resp_integer(&ttl_future[0]);
        assert_that!(ttl_value > 0, eq(true));

        let _ = app
            .feed_connection_bytes(&mut connection, &resp_command(&[b"SET", b"past", b"v"]))
            .expect("SET past should execute");
        let expireat_past = app
            .feed_connection_bytes(
                &mut connection,
                &resp_command(&[b"EXPIREAT", b"past", b"1"]),
            )
            .expect("EXPIREAT past should execute");
        assert_that!(&expireat_past, eq(&vec![b":1\r\n".to_vec()]));

        let removed = app
            .feed_connection_bytes(&mut connection, &resp_command(&[b"GET", b"past"]))
            .expect("GET should execute");
        assert_that!(&removed, eq(&vec![b"$-1\r\n".to_vec()]));
    }

    #[rstest]
    fn resp_pexpireat_sets_future_expire_and_deletes_past_keys() {
        let mut app = ServerApp::new(RuntimeConfig::default());
        let mut connection = ServerApp::new_connection(ClientProtocol::Resp);

        let _ = app
            .feed_connection_bytes(&mut connection, &resp_command(&[b"SET", b"future", b"v"]))
            .expect("SET future should execute");
        let future_timestamp = (SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("system time should be after epoch")
            .as_millis()
            + 120_000)
            .to_string()
            .into_bytes();
        let pexpireat_future = app
            .feed_connection_bytes(
                &mut connection,
                &resp_command(&[b"PEXPIREAT", b"future", &future_timestamp]),
            )
            .expect("PEXPIREAT future should execute");
        assert_that!(&pexpireat_future, eq(&vec![b":1\r\n".to_vec()]));

        let pttl_future = app
            .feed_connection_bytes(&mut connection, &resp_command(&[b"PTTL", b"future"]))
            .expect("PTTL should execute");
        let pttl_value = parse_resp_integer(&pttl_future[0]);
        assert_that!(pttl_value > 0, eq(true));

        let _ = app
            .feed_connection_bytes(&mut connection, &resp_command(&[b"SET", b"past", b"v"]))
            .expect("SET past should execute");
        let pexpireat_past = app
            .feed_connection_bytes(
                &mut connection,
                &resp_command(&[b"PEXPIREAT", b"past", b"1"]),
            )
            .expect("PEXPIREAT past should execute");
        assert_that!(&pexpireat_past, eq(&vec![b":1\r\n".to_vec()]));

        let removed = app
            .feed_connection_bytes(&mut connection, &resp_command(&[b"GET", b"past"]))
            .expect("GET should execute");
        assert_that!(&removed, eq(&vec![b"$-1\r\n".to_vec()]));
    }

    #[rstest]
    fn resp_mget_preserves_key_order_across_shards() {
        let mut app = ServerApp::new(RuntimeConfig::default());
        let mut connection = ServerApp::new_connection(ClientProtocol::Resp);

        let first_key = b"user:1001".to_vec();
        let first_shard = app.core.resolve_shard_for_key(&first_key);
        let mut second_key = b"user:2002".to_vec();
        let mut second_shard = app.core.resolve_shard_for_key(&second_key);
        let mut suffix = 0_u32;
        while second_shard == first_shard {
            suffix += 1;
            second_key = format!("user:2002:{suffix}").into_bytes();
            second_shard = app.core.resolve_shard_for_key(&second_key);
        }

        let mset = resp_command(&[b"MSET", &first_key, b"alpha", &second_key, b"beta"]);
        let _ = app
            .feed_connection_bytes(&mut connection, &mset)
            .expect("MSET should execute");

        let mget = resp_command(&[b"MGET", &second_key, &first_key, b"missing"]);
        let reply = app
            .feed_connection_bytes(&mut connection, &mget)
            .expect("MGET should execute");
        let expected = vec![b"*3\r\n$4\r\nbeta\r\n$5\r\nalpha\r\n$-1\r\n".to_vec()];
        assert_that!(&reply, eq(&expected));
    }

    #[rstest]
    fn resp_connection_handles_partial_input() {
        let mut app = ServerApp::new(RuntimeConfig::default());
        let mut connection = ServerApp::new_connection(ClientProtocol::Resp);

        let partial_responses = app
            .feed_connection_bytes(&mut connection, b"*2\r\n$4\r\nECHO\r\n$5\r\nhe")
            .expect("partial input should not fail");
        assert_that!(partial_responses.is_empty(), eq(true));

        let final_responses = app
            .feed_connection_bytes(&mut connection, b"llo\r\n")
            .expect("completion bytes should execute pending command");
        let expected = vec![b"$5\r\nhello\r\n".to_vec()];
        assert_that!(&final_responses, eq(&expected));
    }

    #[rstest]
    fn memcache_connection_executes_set_then_get() {
        let mut app = ServerApp::new(RuntimeConfig::default());
        let mut connection = ServerApp::new_connection(ClientProtocol::Memcache);

        let set_responses = app
            .feed_connection_bytes(&mut connection, b"set user:42 0 0 5\r\nalice\r\n")
            .expect("memcache set should execute");
        let expected_set = vec![b"STORED\r\n".to_vec()];
        assert_that!(&set_responses, eq(&expected_set));

        let get_responses = app
            .feed_connection_bytes(&mut connection, b"get user:42\r\n")
            .expect("memcache get should execute");
        let expected_get = vec![b"VALUE user:42 0 5\r\nalice\r\nEND\r\n".to_vec()];
        assert_that!(&get_responses, eq(&expected_get));
    }

    #[rstest]
    fn memcache_set_handles_partial_value_payload() {
        let mut app = ServerApp::new(RuntimeConfig::default());
        let mut connection = ServerApp::new_connection(ClientProtocol::Memcache);

        let partial_responses = app
            .feed_connection_bytes(&mut connection, b"set cache:key 0 0 7\r\nru")
            .expect("partial memcache set should not fail");
        assert_that!(partial_responses.is_empty(), eq(true));

        let final_responses = app
            .feed_connection_bytes(&mut connection, b"sty42\r\n")
            .expect("payload completion should execute set");
        let expected = vec![b"STORED\r\n".to_vec()];
        assert_that!(&final_responses, eq(&expected));
    }

    #[rstest]
    fn resp_multi_exec_runs_queued_commands_in_order() {
        let mut app = ServerApp::new(RuntimeConfig::default());
        let mut connection = ServerApp::new_connection(ClientProtocol::Resp);

        let multi = app
            .feed_connection_bytes(&mut connection, b"*1\r\n$5\r\nMULTI\r\n")
            .expect("MULTI should succeed");
        assert_that!(&multi, eq(&vec![b"+OK\r\n".to_vec()]));

        let queued_set = app
            .feed_connection_bytes(
                &mut connection,
                b"*3\r\n$3\r\nSET\r\n$3\r\nfoo\r\n$3\r\nbar\r\n",
            )
            .expect("SET should be queued");
        assert_that!(&queued_set, eq(&vec![b"+QUEUED\r\n".to_vec()]));

        let queued_get = app
            .feed_connection_bytes(&mut connection, b"*2\r\n$3\r\nGET\r\n$3\r\nfoo\r\n")
            .expect("GET should be queued");
        assert_that!(&queued_get, eq(&vec![b"+QUEUED\r\n".to_vec()]));

        let exec = app
            .feed_connection_bytes(&mut connection, b"*1\r\n$4\r\nEXEC\r\n")
            .expect("EXEC should execute queued commands");
        let expected_exec = vec![b"*2\r\n+OK\r\n$3\r\nbar\r\n".to_vec()];
        assert_that!(&exec, eq(&expected_exec));
    }

    #[rstest]
    fn resp_exec_with_empty_queue_returns_empty_array() {
        let mut app = ServerApp::new(RuntimeConfig::default());
        let mut connection = ServerApp::new_connection(ClientProtocol::Resp);

        let multi = app
            .feed_connection_bytes(&mut connection, &resp_command(&[b"MULTI"]))
            .expect("MULTI should succeed");
        assert_that!(&multi, eq(&vec![b"+OK\r\n".to_vec()]));

        let exec = app
            .feed_connection_bytes(&mut connection, &resp_command(&[b"EXEC"]))
            .expect("EXEC should succeed");
        assert_that!(&exec, eq(&vec![b"*0\r\n".to_vec()]));
        assert_that!(app.replication.journal_entries().is_empty(), eq(true));
    }

    #[rstest]
    fn exec_plan_groups_commands_without_duplicate_shards_per_hop() {
        let mut app = ServerApp::new(RuntimeConfig::default());

        let first_key = b"plan:key:1".to_vec();
        let first_shard = app.core.resolve_shard_for_key(&first_key);
        let mut second_key = b"plan:key:2".to_vec();
        let mut second_shard = app.core.resolve_shard_for_key(&second_key);
        let mut suffix = 0_u32;
        while second_shard == first_shard {
            suffix = suffix.saturating_add(1);
            second_key = format!("plan:key:2:{suffix}").into_bytes();
            second_shard = app.core.resolve_shard_for_key(&second_key);
        }

        let queued = vec![
            CommandFrame::new("SET", vec![first_key.clone(), b"a".to_vec()]),
            CommandFrame::new("SET", vec![second_key.clone(), b"b".to_vec()]),
            CommandFrame::new("GET", vec![first_key.clone()]),
        ];
        let plan = app.build_exec_plan(&queued);

        assert_that!(plan.mode, eq(TransactionMode::LockAhead));
        assert_that!(plan.hops.len(), eq(2_usize));
        assert_that!(plan.hops[0].per_shard.len(), eq(2_usize));
        assert_that!(plan.hops[1].per_shard.len(), eq(1_usize));
        assert_that!(plan.hops[0].per_shard[0].0, eq(first_shard));
        assert_that!(plan.hops[0].per_shard[1].0, eq(second_shard));
        assert_that!(plan.hops[1].per_shard[0].0, eq(first_shard));

        let flattened = plan
            .hops
            .iter()
            .flat_map(|hop| hop.per_shard.iter().map(|(_, command)| command.clone()))
            .collect::<Vec<_>>();
        assert_that!(&flattened, eq(&queued));
    }

    #[rstest]
    fn exec_plan_selects_non_atomic_for_single_key_reads() {
        let mut app = ServerApp::new(RuntimeConfig::default());

        let first_key = b"plan:ro:1".to_vec();
        let first_shard = app.core.resolve_shard_for_key(&first_key);
        let mut second_key = b"plan:ro:2".to_vec();
        let mut second_shard = app.core.resolve_shard_for_key(&second_key);
        let mut suffix = 0_u32;
        while second_shard == first_shard {
            suffix = suffix.saturating_add(1);
            second_key = format!("plan:ro:2:{suffix}").into_bytes();
            second_shard = app.core.resolve_shard_for_key(&second_key);
        }

        let queued = vec![
            CommandFrame::new("GET", vec![first_key.clone()]),
            CommandFrame::new("TTL", vec![second_key.clone()]),
            CommandFrame::new("TYPE", vec![first_key.clone()]),
        ];
        let plan = app.build_exec_plan(&queued);

        assert_that!(plan.mode, eq(TransactionMode::NonAtomic));
        assert_that!(plan.hops.len(), eq(2_usize));
        assert_that!(plan.hops[0].per_shard.len(), eq(2_usize));
        assert_that!(plan.hops[1].per_shard.len(), eq(1_usize));

        let flattened = plan
            .hops
            .iter()
            .flat_map(|hop| hop.per_shard.iter().map(|(_, command)| command.clone()))
            .collect::<Vec<_>>();
        assert_that!(&flattened, eq(&queued));
    }

    #[rstest]
    fn exec_plan_falls_back_to_global_mode_for_non_key_commands() {
        let mut app = ServerApp::new(RuntimeConfig::default());
        let queued = vec![
            CommandFrame::new("PING", Vec::new()),
            CommandFrame::new("SET", vec![b"plan:key".to_vec(), b"value".to_vec()]),
            CommandFrame::new("TIME", Vec::new()),
        ];

        let plan = app.build_exec_plan(&queued);
        assert_that!(plan.mode, eq(TransactionMode::Global));
        assert_that!(plan.hops.len(), eq(3_usize));
        assert_that!(
            plan.hops.iter().all(|hop| hop.per_shard.len() == 1),
            eq(true)
        );

        let flattened = plan
            .hops
            .iter()
            .flat_map(|hop| hop.per_shard.iter().map(|(_, command)| command.clone()))
            .collect::<Vec<_>>();
        assert_that!(&flattened, eq(&queued));
    }

    #[rstest]
    fn exec_plan_dispatches_lockahead_commands_into_runtime_queues() {
        let mut app = ServerApp::new(RuntimeConfig::default());

        let first_key = b"plan:rt:1".to_vec();
        let first_shard = app.core.resolve_shard_for_key(&first_key);
        let mut second_key = b"plan:rt:2".to_vec();
        let mut second_shard = app.core.resolve_shard_for_key(&second_key);
        let mut suffix = 0_u32;
        while second_shard == first_shard {
            suffix = suffix.saturating_add(1);
            second_key = format!("plan:rt:2:{suffix}").into_bytes();
            second_shard = app.core.resolve_shard_for_key(&second_key);
        }

        let queued = vec![
            CommandFrame::new("SET", vec![first_key.clone(), b"a".to_vec()]),
            CommandFrame::new("SET", vec![second_key.clone(), b"b".to_vec()]),
        ];
        let plan = app.build_exec_plan(&queued);
        assert_that!(plan.mode, eq(TransactionMode::LockAhead));

        let replies = app.execute_transaction_plan(0, &plan);
        assert_that!(
            &replies,
            eq(&vec![
                CommandReply::SimpleString("OK".to_owned()),
                CommandReply::SimpleString("OK".to_owned()),
            ])
        );

        assert_that!(
            app.runtime
                .wait_for_processed_count(first_shard, 1, Duration::from_millis(200))
                .expect("wait should succeed"),
            eq(true)
        );
        assert_that!(
            app.runtime
                .wait_for_processed_count(second_shard, 1, Duration::from_millis(200))
                .expect("wait should succeed"),
            eq(true)
        );

        let first_runtime = app
            .runtime
            .drain_processed_for_shard(first_shard)
            .expect("drain should succeed");
        let second_runtime = app
            .runtime
            .drain_processed_for_shard(second_shard)
            .expect("drain should succeed");
        assert_that!(first_runtime.len(), eq(1_usize));
        assert_that!(second_runtime.len(), eq(1_usize));
        assert_that!(&first_runtime[0].command, eq(&queued[0]));
        assert_that!(&second_runtime[0].command, eq(&queued[1]));
    }

    #[rstest]
    fn exec_plan_non_atomic_enqueues_runtime_commands() {
        let mut app = ServerApp::new(RuntimeConfig::default());
        let key = b"plan:rt:readonly".to_vec();
        let shard = app.core.resolve_shard_for_key(&key);
        let command = CommandFrame::new("GET", vec![key.clone()]);
        let plan = TransactionPlan {
            txid: 1,
            mode: TransactionMode::NonAtomic,
            hops: vec![TransactionHop {
                per_shard: vec![(shard, command.clone())],
            }],
        };

        let replies = app.execute_transaction_plan(0, &plan);
        assert_that!(&replies, eq(&vec![CommandReply::Null]));
        let runtime = app
            .runtime
            .drain_processed_for_shard(shard)
            .expect("drain should succeed");
        assert_that!(runtime.len(), eq(1_usize));
        assert_that!(&runtime[0].command, eq(&command));
    }

    #[rstest]
    fn exec_plan_global_multikey_commands_dispatch_runtime_to_all_touched_shards() {
        let mut app = ServerApp::new(RuntimeConfig::default());
        let first_key = b"plan:rt:global:mset:1".to_vec();
        let first_shard = app.core.resolve_shard_for_key(&first_key);
        let mut second_key = b"plan:rt:global:mset:2".to_vec();
        let mut second_shard = app.core.resolve_shard_for_key(&second_key);
        let mut suffix = 0_u32;
        while second_shard == first_shard {
            suffix = suffix.saturating_add(1);
            second_key = format!("plan:rt:global:mset:2:{suffix}").into_bytes();
            second_shard = app.core.resolve_shard_for_key(&second_key);
        }

        let queued = vec![CommandFrame::new(
            "MSET",
            vec![first_key, b"a".to_vec(), second_key, b"b".to_vec()],
        )];
        let plan = app.build_exec_plan(&queued);
        assert_that!(plan.mode, eq(TransactionMode::Global));

        let replies = app.execute_transaction_plan(0, &plan);
        assert_that!(
            &replies,
            eq(&vec![CommandReply::SimpleString("OK".to_owned())])
        );
        assert_that!(
            app.runtime
                .wait_for_processed_count(first_shard, 1, Duration::from_millis(200))
                .expect("wait should succeed"),
            eq(true)
        );
        assert_that!(
            app.runtime
                .wait_for_processed_count(second_shard, 1, Duration::from_millis(200))
                .expect("wait should succeed"),
            eq(true)
        );

        let first_runtime = app
            .runtime
            .drain_processed_for_shard(first_shard)
            .expect("drain should succeed");
        let second_runtime = app
            .runtime
            .drain_processed_for_shard(second_shard)
            .expect("drain should succeed");
        assert_that!(first_runtime.len(), eq(1_usize));
        assert_that!(second_runtime.len(), eq(1_usize));
        assert_that!(&first_runtime[0].command, eq(&queued[0]));
        assert_that!(&second_runtime[0].command, eq(&queued[0]));
    }

    #[rstest]
    fn exec_plan_global_non_key_command_uses_planner_shard_hint() {
        let mut app = ServerApp::new(RuntimeConfig::default());
        let queued = vec![CommandFrame::new("PING", Vec::new())];
        let plan = app.build_exec_plan(&queued);
        assert_that!(plan.mode, eq(TransactionMode::Global));

        let replies = app.execute_transaction_plan(0, &plan);
        assert_that!(
            &replies,
            eq(&vec![CommandReply::SimpleString("PONG".to_owned())])
        );
        let shard_zero_runtime = app
            .runtime
            .drain_processed_for_shard(0)
            .expect("drain should succeed");
        assert_that!(shard_zero_runtime.len(), eq(1_usize));
        assert_that!(&shard_zero_runtime[0].command, eq(&queued[0]));

        for shard in 1_u16..app.config.shard_count.get() {
            let runtime = app
                .runtime
                .drain_processed_for_shard(shard)
                .expect("drain should succeed");
            assert_that!(runtime.is_empty(), eq(true));
        }
    }

    #[rstest]
    fn direct_mget_dispatches_runtime_to_each_touched_shard() {
        let mut app = ServerApp::new(RuntimeConfig::default());
        let first_key = b"direct:rt:mget:1".to_vec();
        let first_shard = app.core.resolve_shard_for_key(&first_key);
        let mut second_key = b"direct:rt:mget:2".to_vec();
        let mut second_shard = app.core.resolve_shard_for_key(&second_key);
        let mut suffix = 0_u32;
        while second_shard == first_shard {
            suffix = suffix.saturating_add(1);
            second_key = format!("direct:rt:mget:2:{suffix}").into_bytes();
            second_shard = app.core.resolve_shard_for_key(&second_key);
        }

        let frame = CommandFrame::new("MGET", vec![first_key, second_key]);
        let reply = app.execute_user_command(0, &frame, None);
        assert_that!(
            &reply,
            eq(&CommandReply::Array(vec![
                CommandReply::Null,
                CommandReply::Null
            ]))
        );
        assert_that!(
            app.runtime
                .wait_for_processed_count(first_shard, 1, Duration::from_millis(200))
                .expect("wait should succeed"),
            eq(true)
        );
        assert_that!(
            app.runtime
                .wait_for_processed_count(second_shard, 1, Duration::from_millis(200))
                .expect("wait should succeed"),
            eq(true)
        );

        let first_runtime = app
            .runtime
            .drain_processed_for_shard(first_shard)
            .expect("drain should succeed");
        let second_runtime = app
            .runtime
            .drain_processed_for_shard(second_shard)
            .expect("drain should succeed");
        assert_that!(first_runtime.len(), eq(1_usize));
        assert_that!(second_runtime.len(), eq(1_usize));
        assert_that!(&first_runtime[0].command, eq(&frame));
        assert_that!(&second_runtime[0].command, eq(&frame));
    }

    #[rstest]
    fn direct_mget_dispatches_runtime_once_when_keys_share_same_shard() {
        let mut app = ServerApp::new(RuntimeConfig::default());
        let first_key = b"direct:rt:same:1".to_vec();
        let shard = app.core.resolve_shard_for_key(&first_key);
        let mut second_key = b"direct:rt:same:2".to_vec();
        let mut second_shard = app.core.resolve_shard_for_key(&second_key);
        let mut suffix = 0_u32;
        while second_shard != shard {
            suffix = suffix.saturating_add(1);
            second_key = format!("direct:rt:same:2:{suffix}").into_bytes();
            second_shard = app.core.resolve_shard_for_key(&second_key);
        }

        let frame = CommandFrame::new("MGET", vec![first_key, second_key]);
        let _ = app.execute_user_command(0, &frame, None);
        let runtime = app
            .runtime
            .drain_processed_for_shard(shard)
            .expect("drain should succeed");
        assert_that!(runtime.len(), eq(1_usize));
        assert_that!(&runtime[0].command, eq(&frame));
    }

    #[rstest]
    fn direct_mset_with_odd_arity_does_not_dispatch_runtime() {
        let mut app = ServerApp::new(RuntimeConfig::default());
        let frame = CommandFrame::new(
            "MSET",
            vec![
                b"direct:rt:odd:1".to_vec(),
                b"a".to_vec(),
                b"orphan-key".to_vec(),
            ],
        );

        let reply = app.execute_user_command(0, &frame, None);
        assert_that!(
            &reply,
            eq(&CommandReply::Error(
                "wrong number of arguments for 'MSET' command".to_owned()
            ))
        );
        for shard in 0_u16..app.config.shard_count.get() {
            let runtime = app
                .runtime
                .drain_processed_for_shard(shard)
                .expect("drain should succeed");
            assert_that!(runtime.is_empty(), eq(true));
        }
    }

    #[rstest]
    fn direct_flushall_dispatches_runtime_to_all_shards() {
        let mut app = ServerApp::new(RuntimeConfig::default());
        let frame = CommandFrame::new("FLUSHALL", Vec::new());

        let reply = app.execute_user_command(0, &frame, None);
        assert_that!(&reply, eq(&CommandReply::SimpleString("OK".to_owned())));
        for shard in 0_u16..app.config.shard_count.get() {
            assert_that!(
                app.runtime
                    .wait_for_processed_count(shard, 1, Duration::from_millis(200))
                    .expect("wait should succeed"),
                eq(true)
            );
            let runtime = app
                .runtime
                .drain_processed_for_shard(shard)
                .expect("drain should succeed");
            assert_that!(runtime.len(), eq(1_usize));
            assert_that!(&runtime[0].command, eq(&frame));
        }
    }

    #[rstest]
    fn direct_ping_does_not_dispatch_runtime() {
        let mut app = ServerApp::new(RuntimeConfig::default());
        let frame = CommandFrame::new("PING", Vec::new());

        let reply = app.execute_user_command(0, &frame, None);
        assert_that!(&reply, eq(&CommandReply::SimpleString("PONG".to_owned())));
        for shard in 0_u16..app.config.shard_count.get() {
            let runtime = app
                .runtime
                .drain_processed_for_shard(shard)
                .expect("drain should succeed");
            assert_that!(runtime.is_empty(), eq(true));
        }
    }

    #[rstest]
    fn exec_plan_reports_runtime_dispatch_error_for_invalid_shard() {
        let mut app = ServerApp::new(RuntimeConfig::default());
        let plan = TransactionPlan {
            txid: 1,
            mode: TransactionMode::Global,
            hops: vec![TransactionHop {
                per_shard: vec![(u16::MAX, CommandFrame::new("PING", Vec::new()))],
            }],
        };

        let replies = app.execute_transaction_plan(0, &plan);
        assert_that!(
            &replies,
            eq(&vec![CommandReply::Error(
                "runtime dispatch failed: invalid runtime state: target shard is out of range"
                    .to_owned()
            )])
        );
    }

    #[rstest]
    fn exec_plan_aborts_whole_hop_when_runtime_dispatch_fails() {
        let mut app = ServerApp::new(RuntimeConfig::default());
        let key = b"plan:rt:abort".to_vec();
        let shard = app.core.resolve_shard_for_key(&key);
        let plan = TransactionPlan {
            txid: 1,
            mode: TransactionMode::Global,
            hops: vec![TransactionHop {
                per_shard: vec![
                    (u16::MAX, CommandFrame::new("PING", Vec::new())),
                    (
                        shard,
                        CommandFrame::new("SET", vec![key.clone(), b"value".to_vec()]),
                    ),
                ],
            }],
        };

        let replies = app.execute_transaction_plan(0, &plan);
        assert_that!(
            &replies,
            eq(&vec![
                CommandReply::Error(
                    "runtime dispatch failed: invalid runtime state: target shard is out of range"
                        .to_owned()
                ),
                CommandReply::Error("runtime dispatch failed: hop barrier aborted".to_owned()),
            ])
        );

        let value = app
            .core
            .execute_in_db(0, &CommandFrame::new("GET", vec![key]));
        assert_that!(&value, eq(&CommandReply::Null));
    }

    #[rstest]
    fn exec_plan_aborts_following_hops_after_runtime_dispatch_failure() {
        let mut app = ServerApp::new(RuntimeConfig::default());
        let key = b"plan:rt:skip-next-hop".to_vec();
        let shard = app.core.resolve_shard_for_key(&key);
        let plan = TransactionPlan {
            txid: 1,
            mode: TransactionMode::Global,
            hops: vec![
                TransactionHop {
                    per_shard: vec![(u16::MAX, CommandFrame::new("PING", Vec::new()))],
                },
                TransactionHop {
                    per_shard: vec![(
                        shard,
                        CommandFrame::new("SET", vec![key.clone(), b"value".to_vec()]),
                    )],
                },
            ],
        };

        let replies = app.execute_transaction_plan(0, &plan);
        assert_that!(
            &replies,
            eq(&vec![
                CommandReply::Error(
                    "runtime dispatch failed: invalid runtime state: target shard is out of range"
                        .to_owned()
                ),
                CommandReply::Error(
                    "runtime dispatch failed: transaction aborted after hop failure".to_owned()
                ),
            ])
        );

        let value = app
            .core
            .execute_in_db(0, &CommandFrame::new("GET", vec![key]));
        assert_that!(&value, eq(&CommandReply::Null));
    }

    #[rstest]
    fn exec_plan_keeps_following_hops_when_previous_command_error_is_not_runtime_failure() {
        let mut app = ServerApp::new(RuntimeConfig::default());
        let key = b"plan:rt:continue-after-command-error".to_vec();
        let key_shard = app.core.resolve_shard_for_key(&key);
        let plan = TransactionPlan {
            txid: 1,
            mode: TransactionMode::Global,
            hops: vec![
                TransactionHop {
                    per_shard: vec![(
                        0,
                        CommandFrame::new("PING", vec![b"a".to_vec(), b"b".to_vec()]),
                    )],
                },
                TransactionHop {
                    per_shard: vec![(
                        key_shard,
                        CommandFrame::new("SET", vec![key.clone(), b"value".to_vec()]),
                    )],
                },
            ],
        };

        let replies = app.execute_transaction_plan(0, &plan);
        assert_that!(
            &replies,
            eq(&vec![
                CommandReply::Error("wrong number of arguments for 'PING' command".to_owned()),
                CommandReply::SimpleString("OK".to_owned()),
            ])
        );

        let value = app
            .core
            .execute_in_db(0, &CommandFrame::new("GET", vec![key]));
        assert_that!(&value, eq(&CommandReply::BulkString(b"value".to_vec())));
    }

    #[rstest]
    fn exec_non_atomic_execution_rejects_non_single_key_read_only_commands() {
        let mut app = ServerApp::new(RuntimeConfig::default());
        let plan = TransactionPlan {
            txid: 1,
            mode: TransactionMode::NonAtomic,
            hops: vec![TransactionHop {
                per_shard: vec![(0, CommandFrame::new("PING", Vec::new()))],
            }],
        };

        let replies = app.execute_transaction_plan(0, &plan);
        assert_that!(
            &replies,
            eq(&vec![CommandReply::Error(
                "transaction planning error: NonAtomic mode requires read-only single-key commands"
                    .to_owned()
            )])
        );
    }

    #[rstest]
    fn resp_exec_read_only_queue_uses_non_mutating_path() {
        let mut app = ServerApp::new(RuntimeConfig::default());
        let mut connection = ServerApp::new_connection(ClientProtocol::Resp);

        let _ = app
            .feed_connection_bytes(&mut connection, &resp_command(&[b"SET", b"k", b"v"]))
            .expect("seed SET should succeed");
        assert_that!(app.replication.journal_entries().len(), eq(1_usize));

        let _ = app
            .feed_connection_bytes(&mut connection, &resp_command(&[b"MULTI"]))
            .expect("MULTI should succeed");
        let _ = app
            .feed_connection_bytes(&mut connection, &resp_command(&[b"GET", b"k"]))
            .expect("GET should queue");
        let _ = app
            .feed_connection_bytes(&mut connection, &resp_command(&[b"TYPE", b"k"]))
            .expect("TYPE should queue");

        let exec = app
            .feed_connection_bytes(&mut connection, &resp_command(&[b"EXEC"]))
            .expect("EXEC should succeed");
        let expected = vec![b"*2\r\n$1\r\nv\r\n+string\r\n".to_vec()];
        assert_that!(&exec, eq(&expected));

        // Read-only transaction must not create new journal records.
        assert_that!(app.replication.journal_entries().len(), eq(1_usize));
    }

    #[rstest]
    fn resp_discard_drops_queued_commands() {
        let mut app = ServerApp::new(RuntimeConfig::default());
        let mut connection = ServerApp::new_connection(ClientProtocol::Resp);

        let _ = app
            .feed_connection_bytes(&mut connection, b"*1\r\n$5\r\nMULTI\r\n")
            .expect("MULTI should succeed");
        let _ = app
            .feed_connection_bytes(
                &mut connection,
                b"*3\r\n$3\r\nSET\r\n$3\r\nfoo\r\n$3\r\nbar\r\n",
            )
            .expect("SET should be queued");
        let discard = app
            .feed_connection_bytes(&mut connection, b"*1\r\n$7\r\nDISCARD\r\n")
            .expect("DISCARD should succeed");
        assert_that!(&discard, eq(&vec![b"+OK\r\n".to_vec()]));

        let get = app
            .feed_connection_bytes(&mut connection, b"*2\r\n$3\r\nGET\r\n$3\r\nfoo\r\n")
            .expect("GET should run after discard");
        assert_that!(&get, eq(&vec![b"$-1\r\n".to_vec()]));
    }

    #[rstest]
    fn resp_exec_without_multi_returns_error() {
        let mut app = ServerApp::new(RuntimeConfig::default());
        let mut connection = ServerApp::new_connection(ClientProtocol::Resp);

        let exec = app
            .feed_connection_bytes(&mut connection, b"*1\r\n$4\r\nEXEC\r\n")
            .expect("EXEC command should parse");
        assert_that!(&exec, eq(&vec![b"-ERR EXEC without MULTI\r\n".to_vec()]));
    }

    #[rstest]
    fn resp_exec_without_multi_remains_error_even_when_watch_is_dirty() {
        let mut app = ServerApp::new(RuntimeConfig::default());
        let mut watch_conn = ServerApp::new_connection(ClientProtocol::Resp);
        let mut writer_conn = ServerApp::new_connection(ClientProtocol::Resp);

        let _ = app
            .feed_connection_bytes(&mut watch_conn, &resp_command(&[b"WATCH", b"key"]))
            .expect("WATCH should succeed");
        let _ = app
            .feed_connection_bytes(&mut writer_conn, &resp_command(&[b"SET", b"key", b"other"]))
            .expect("writer SET should succeed");

        let exec = app
            .feed_connection_bytes(&mut watch_conn, &resp_command(&[b"EXEC"]))
            .expect("EXEC should parse");
        assert_that!(&exec, eq(&vec![b"-ERR EXEC without MULTI\r\n".to_vec()]));
    }

    #[rstest]
    fn resp_execaborts_after_unknown_command_during_multi_queueing() {
        let mut app = ServerApp::new(RuntimeConfig::default());
        let mut connection = ServerApp::new_connection(ClientProtocol::Resp);

        let _ = app
            .feed_connection_bytes(&mut connection, &resp_command(&[b"MULTI"]))
            .expect("MULTI should succeed");
        let queue_error = app
            .feed_connection_bytes(&mut connection, &resp_command(&[b"NOPE"]))
            .expect("unknown command should parse");
        assert_that!(
            &queue_error,
            eq(&vec![b"-ERR unknown command 'NOPE'\r\n".to_vec()])
        );

        let exec = app
            .feed_connection_bytes(&mut connection, &resp_command(&[b"EXEC"]))
            .expect("EXEC should parse");
        assert_that!(
            &exec,
            eq(&vec![
                b"-ERR EXECABORT Transaction discarded because of previous errors\r\n".to_vec()
            ])
        );
    }

    #[rstest]
    fn resp_execaborts_after_core_arity_error_during_multi_queueing() {
        let mut app = ServerApp::new(RuntimeConfig::default());
        let mut connection = ServerApp::new_connection(ClientProtocol::Resp);

        let _ = app
            .feed_connection_bytes(&mut connection, &resp_command(&[b"MULTI"]))
            .expect("MULTI should succeed");
        let queue_error = app
            .feed_connection_bytes(&mut connection, &resp_command(&[b"GET"]))
            .expect("invalid GET arity should parse");
        assert_that!(
            &queue_error,
            eq(&vec![
                b"-ERR wrong number of arguments for 'GET' command\r\n".to_vec()
            ])
        );

        let exec = app
            .feed_connection_bytes(&mut connection, &resp_command(&[b"EXEC"]))
            .expect("EXEC should parse");
        assert_that!(
            &exec,
            eq(&vec![
                b"-ERR EXECABORT Transaction discarded because of previous errors\r\n".to_vec()
            ])
        );
    }

    #[rstest]
    fn resp_execaborts_after_server_command_arity_error_during_multi_queueing() {
        let mut app = ServerApp::new(RuntimeConfig::default());
        let mut connection = ServerApp::new_connection(ClientProtocol::Resp);

        let _ = app
            .feed_connection_bytes(&mut connection, &resp_command(&[b"MULTI"]))
            .expect("MULTI should succeed");
        let queue_error = app
            .feed_connection_bytes(&mut connection, &resp_command(&[b"WAIT", b"1"]))
            .expect("invalid WAIT arity should parse");
        assert_that!(
            &queue_error,
            eq(&vec![
                b"-ERR wrong number of arguments for 'WAIT' command\r\n".to_vec()
            ])
        );

        let exec = app
            .feed_connection_bytes(&mut connection, &resp_command(&[b"EXEC"]))
            .expect("EXEC should parse");
        assert_that!(
            &exec,
            eq(&vec![
                b"-ERR EXECABORT Transaction discarded because of previous errors\r\n".to_vec()
            ])
        );
    }

    #[rstest]
    fn resp_execaborts_after_replconf_syntax_error_during_multi_queueing() {
        let mut app = ServerApp::new(RuntimeConfig::default());
        let mut connection = ServerApp::new_connection(ClientProtocol::Resp);

        let _ = app
            .feed_connection_bytes(&mut connection, &resp_command(&[b"MULTI"]))
            .expect("MULTI should succeed");
        let queue_error = app
            .feed_connection_bytes(&mut connection, &resp_command(&[b"REPLCONF", b"ACK"]))
            .expect("invalid REPLCONF shape should parse");
        assert_that!(&queue_error, eq(&vec![b"-ERR syntax error\r\n".to_vec()]));

        let exec = app
            .feed_connection_bytes(&mut connection, &resp_command(&[b"EXEC"]))
            .expect("EXEC should parse");
        assert_that!(
            &exec,
            eq(&vec![
                b"-ERR EXECABORT Transaction discarded because of previous errors\r\n".to_vec()
            ])
        );
    }

    #[rstest]
    fn resp_watch_aborts_exec_when_watched_key_changes() {
        let mut app = ServerApp::new(RuntimeConfig::default());
        let mut watch_conn = ServerApp::new_connection(ClientProtocol::Resp);
        let mut writer_conn = ServerApp::new_connection(ClientProtocol::Resp);

        let _ = app
            .feed_connection_bytes(
                &mut watch_conn,
                b"*3\r\n$3\r\nSET\r\n$3\r\nkey\r\n$3\r\nold\r\n",
            )
            .expect("seed SET should succeed");
        let _ = app
            .feed_connection_bytes(&mut watch_conn, b"*2\r\n$5\r\nWATCH\r\n$3\r\nkey\r\n")
            .expect("WATCH should succeed");
        let _ = app
            .feed_connection_bytes(&mut watch_conn, b"*1\r\n$5\r\nMULTI\r\n")
            .expect("MULTI should succeed");
        let _ = app
            .feed_connection_bytes(
                &mut watch_conn,
                b"*3\r\n$3\r\nSET\r\n$3\r\nkey\r\n$4\r\nmine\r\n",
            )
            .expect("queued SET should succeed");
        let _ = app
            .feed_connection_bytes(
                &mut writer_conn,
                b"*3\r\n$3\r\nSET\r\n$3\r\nkey\r\n$5\r\nother\r\n",
            )
            .expect("concurrent SET should succeed");

        let exec = app
            .feed_connection_bytes(&mut watch_conn, b"*1\r\n$4\r\nEXEC\r\n")
            .expect("EXEC should parse");
        assert_that!(&exec, eq(&vec![b"*-1\r\n".to_vec()]));

        let check = app
            .feed_connection_bytes(&mut watch_conn, b"*2\r\n$3\r\nGET\r\n$3\r\nkey\r\n")
            .expect("GET should succeed");
        assert_that!(&check, eq(&vec![b"$5\r\nother\r\n".to_vec()]));
    }

    #[rstest]
    fn resp_watch_aborts_exec_when_flushdb_deletes_watched_key() {
        let mut app = ServerApp::new(RuntimeConfig::default());
        let mut watch_conn = ServerApp::new_connection(ClientProtocol::Resp);
        let mut writer_conn = ServerApp::new_connection(ClientProtocol::Resp);

        let _ = app
            .feed_connection_bytes(
                &mut watch_conn,
                &resp_command(&[b"SET", b"watch:key", b"v"]),
            )
            .expect("seed SET should succeed");
        let _ = app
            .feed_connection_bytes(&mut watch_conn, &resp_command(&[b"WATCH", b"watch:key"]))
            .expect("WATCH should succeed");
        let _ = app
            .feed_connection_bytes(&mut writer_conn, &resp_command(&[b"FLUSHDB"]))
            .expect("FLUSHDB should succeed");

        let _ = app
            .feed_connection_bytes(&mut watch_conn, &resp_command(&[b"MULTI"]))
            .expect("MULTI should succeed");
        let _ = app
            .feed_connection_bytes(
                &mut watch_conn,
                &resp_command(&[b"SET", b"watch:key", b"mine"]),
            )
            .expect("SET should queue");

        let exec = app
            .feed_connection_bytes(&mut watch_conn, &resp_command(&[b"EXEC"]))
            .expect("EXEC should parse");
        assert_that!(&exec, eq(&vec![b"*-1\r\n".to_vec()]));
    }

    #[rstest]
    fn resp_execaborts_after_watch_or_unwatch_error_inside_multi() {
        let mut app = ServerApp::new(RuntimeConfig::default());
        let mut watch_conn = ServerApp::new_connection(ClientProtocol::Resp);

        let _ = app
            .feed_connection_bytes(&mut watch_conn, &resp_command(&[b"MULTI"]))
            .expect("MULTI should succeed");
        let watch_error = app
            .feed_connection_bytes(&mut watch_conn, &resp_command(&[b"WATCH", b"k"]))
            .expect("WATCH should parse");
        assert_that!(
            &watch_error,
            eq(&vec![
                b"-ERR WATCH inside MULTI is not allowed\r\n".to_vec()
            ])
        );
        let exec_after_watch_error = app
            .feed_connection_bytes(&mut watch_conn, &resp_command(&[b"EXEC"]))
            .expect("EXEC should parse");
        assert_that!(
            &exec_after_watch_error,
            eq(&vec![
                b"-ERR EXECABORT Transaction discarded because of previous errors\r\n".to_vec()
            ])
        );

        let mut unwatch_conn = ServerApp::new_connection(ClientProtocol::Resp);
        let _ = app
            .feed_connection_bytes(&mut unwatch_conn, &resp_command(&[b"MULTI"]))
            .expect("MULTI should succeed");
        let unwatch_error = app
            .feed_connection_bytes(&mut unwatch_conn, &resp_command(&[b"UNWATCH"]))
            .expect("UNWATCH should parse");
        assert_that!(
            &unwatch_error,
            eq(&vec![
                b"-ERR UNWATCH inside MULTI is not allowed\r\n".to_vec()
            ])
        );
        let exec_after_unwatch_error = app
            .feed_connection_bytes(&mut unwatch_conn, &resp_command(&[b"EXEC"]))
            .expect("EXEC should parse");
        assert_that!(
            &exec_after_unwatch_error,
            eq(&vec![
                b"-ERR EXECABORT Transaction discarded because of previous errors\r\n".to_vec()
            ])
        );
    }

    #[rstest]
    fn resp_watch_exec_succeeds_when_key_unchanged() {
        let mut app = ServerApp::new(RuntimeConfig::default());
        let mut connection = ServerApp::new_connection(ClientProtocol::Resp);

        let _ = app
            .feed_connection_bytes(&mut connection, b"*2\r\n$5\r\nWATCH\r\n$3\r\nkey\r\n")
            .expect("WATCH should succeed");
        let _ = app
            .feed_connection_bytes(&mut connection, b"*1\r\n$5\r\nMULTI\r\n")
            .expect("MULTI should succeed");
        let _ = app
            .feed_connection_bytes(
                &mut connection,
                b"*3\r\n$3\r\nSET\r\n$3\r\nkey\r\n$3\r\nval\r\n",
            )
            .expect("SET should queue");

        let exec = app
            .feed_connection_bytes(&mut connection, b"*1\r\n$4\r\nEXEC\r\n")
            .expect("EXEC should succeed");
        assert_that!(&exec, eq(&vec![b"*1\r\n+OK\r\n".to_vec()]));
    }

    #[rstest]
    fn resp_unwatch_clears_watch_set() {
        let mut app = ServerApp::new(RuntimeConfig::default());
        let mut watch_conn = ServerApp::new_connection(ClientProtocol::Resp);
        let mut writer_conn = ServerApp::new_connection(ClientProtocol::Resp);

        let _ = app
            .feed_connection_bytes(&mut watch_conn, b"*2\r\n$5\r\nWATCH\r\n$3\r\nkey\r\n")
            .expect("WATCH should succeed");
        let _ = app
            .feed_connection_bytes(&mut watch_conn, b"*1\r\n$7\r\nUNWATCH\r\n")
            .expect("UNWATCH should succeed");
        let _ = app
            .feed_connection_bytes(
                &mut writer_conn,
                b"*3\r\n$3\r\nSET\r\n$3\r\nkey\r\n$5\r\nother\r\n",
            )
            .expect("external SET should succeed");
        let _ = app
            .feed_connection_bytes(&mut watch_conn, b"*1\r\n$5\r\nMULTI\r\n")
            .expect("MULTI should succeed");
        let _ = app
            .feed_connection_bytes(
                &mut watch_conn,
                b"*3\r\n$3\r\nSET\r\n$3\r\nkey\r\n$4\r\nmine\r\n",
            )
            .expect("SET should queue");

        let exec = app
            .feed_connection_bytes(&mut watch_conn, b"*1\r\n$4\r\nEXEC\r\n")
            .expect("EXEC should run queued SET");
        assert_that!(&exec, eq(&vec![b"*1\r\n+OK\r\n".to_vec()]));
    }

    #[rstest]
    fn resp_exec_rejects_watch_and_exec_across_databases() {
        let mut app = ServerApp::new(RuntimeConfig::default());
        let mut connection = ServerApp::new_connection(ClientProtocol::Resp);

        let _ = app
            .feed_connection_bytes(&mut connection, &resp_command(&[b"WATCH", b"cross:key"]))
            .expect("WATCH should succeed in DB 0");
        let _ = app
            .feed_connection_bytes(&mut connection, &resp_command(&[b"SELECT", b"1"]))
            .expect("SELECT 1 should succeed");
        let _ = app
            .feed_connection_bytes(&mut connection, &resp_command(&[b"MULTI"]))
            .expect("MULTI should succeed");
        let queued = app
            .feed_connection_bytes(
                &mut connection,
                &resp_command(&[b"SET", b"cross:key", b"db1-value"]),
            )
            .expect("SET should queue");
        assert_that!(&queued, eq(&vec![b"+QUEUED\r\n".to_vec()]));

        let exec = app
            .feed_connection_bytes(&mut connection, &resp_command(&[b"EXEC"]))
            .expect("EXEC should parse");
        assert_that!(
            &exec,
            eq(&vec![
                b"-ERR Dragonfly does not allow WATCH and EXEC on different databases\r\n".to_vec()
            ])
        );

        let multi_again = app
            .feed_connection_bytes(&mut connection, &resp_command(&[b"MULTI"]))
            .expect("MULTI should succeed after EXEC failure cleanup");
        assert_that!(&multi_again, eq(&vec![b"+OK\r\n".to_vec()]));
    }

    #[rstest]
    fn journal_records_only_successful_write_commands() {
        let mut app = ServerApp::new(RuntimeConfig::default());
        let mut connection = ServerApp::new_connection(ClientProtocol::Resp);

        let _ = app
            .feed_connection_bytes(
                &mut connection,
                b"*3\r\n$3\r\nSET\r\n$3\r\nfoo\r\n$3\r\nbar\r\n",
            )
            .expect("SET should succeed");
        let _ = app
            .feed_connection_bytes(&mut connection, b"*2\r\n$3\r\nGET\r\n$3\r\nfoo\r\n")
            .expect("GET should succeed");

        assert_that!(app.replication.journal_entries().len(), eq(1_usize));
        assert_that!(
            app.replication.journal_entries()[0].op,
            eq(JournalOp::Command),
        );
    }

    #[rstest]
    fn journal_records_set_only_when_mutation_happens_with_conditions_and_get() {
        let mut app = ServerApp::new(RuntimeConfig::default());
        let mut connection = ServerApp::new_connection(ClientProtocol::Resp);

        let _ = app
            .feed_connection_bytes(
                &mut connection,
                &resp_command(&[b"SET", b"k", b"v1", b"NX"]),
            )
            .expect("SET NX should succeed");
        let _ = app
            .feed_connection_bytes(
                &mut connection,
                &resp_command(&[b"SET", b"k", b"v2", b"NX"]),
            )
            .expect("SET NX should execute");
        let _ = app
            .feed_connection_bytes(
                &mut connection,
                &resp_command(&[b"SET", b"k", b"ignored", b"NX", b"GET"]),
            )
            .expect("SET NX GET should execute");
        let _ = app
            .feed_connection_bytes(
                &mut connection,
                &resp_command(&[b"SET", b"k", b"v2", b"XX", b"GET"]),
            )
            .expect("SET XX GET should execute");
        let _ = app
            .feed_connection_bytes(
                &mut connection,
                &resp_command(&[b"SET", b"missing", b"v", b"XX", b"GET"]),
            )
            .expect("SET XX GET on missing key should execute");
        let _ = app
            .feed_connection_bytes(
                &mut connection,
                &resp_command(&[b"SET", b"fresh", b"v", b"GET"]),
            )
            .expect("SET GET should execute");

        let entries = app.replication.journal_entries();
        assert_that!(entries.len(), eq(3_usize));
        assert_that!(
            String::from_utf8_lossy(&entries[0].payload).contains("SET"),
            eq(true)
        );
        assert_that!(
            String::from_utf8_lossy(&entries[1].payload).contains("SET"),
            eq(true)
        );
        assert_that!(
            String::from_utf8_lossy(&entries[2].payload).contains("SET"),
            eq(true)
        );
    }

    #[rstest]
    fn journal_records_setex_only_for_successful_updates() {
        let mut app = ServerApp::new(RuntimeConfig::default());
        let mut connection = ServerApp::new_connection(ClientProtocol::Resp);

        let _ = app
            .feed_connection_bytes(
                &mut connection,
                &resp_command(&[b"SETEX", b"k", b"10", b"v"]),
            )
            .expect("SETEX should succeed");
        let _ = app
            .feed_connection_bytes(
                &mut connection,
                &resp_command(&[b"SETEX", b"k", b"0", b"v"]),
            )
            .expect("SETEX should parse");

        let entries = app.replication.journal_entries();
        assert_that!(entries.len(), eq(1_usize));
        assert_that!(entries[0].op, eq(JournalOp::Command));
        assert_that!(
            String::from_utf8_lossy(&entries[0].payload).contains("SETEX"),
            eq(true)
        );
    }

    #[rstest]
    fn journal_records_psetex_only_for_successful_updates() {
        let mut app = ServerApp::new(RuntimeConfig::default());
        let mut connection = ServerApp::new_connection(ClientProtocol::Resp);

        let _ = app
            .feed_connection_bytes(
                &mut connection,
                &resp_command(&[b"PSETEX", b"k", b"1500", b"v"]),
            )
            .expect("PSETEX should succeed");
        let _ = app
            .feed_connection_bytes(
                &mut connection,
                &resp_command(&[b"PSETEX", b"k", b"0", b"v"]),
            )
            .expect("PSETEX should parse");

        let entries = app.replication.journal_entries();
        assert_that!(entries.len(), eq(1_usize));
        assert_that!(entries[0].op, eq(JournalOp::Command));
        assert_that!(
            String::from_utf8_lossy(&entries[0].payload).contains("PSETEX"),
            eq(true)
        );
    }

    #[rstest]
    fn journal_records_pexpire_as_command_or_expired_by_argument() {
        let mut app = ServerApp::new(RuntimeConfig::default());
        let mut connection = ServerApp::new_connection(ClientProtocol::Resp);

        let _ = app
            .feed_connection_bytes(&mut connection, &resp_command(&[b"SET", b"k", b"v"]))
            .expect("SET should succeed");
        let _ = app
            .feed_connection_bytes(&mut connection, &resp_command(&[b"PEXPIRE", b"k", b"1000"]))
            .expect("PEXPIRE with positive timeout should succeed");
        let _ = app
            .feed_connection_bytes(&mut connection, &resp_command(&[b"SET", b"k", b"v"]))
            .expect("SET should recreate key");
        let _ = app
            .feed_connection_bytes(&mut connection, &resp_command(&[b"PEXPIRE", b"k", b"0"]))
            .expect("PEXPIRE with zero timeout should succeed");

        let entries = app.replication.journal_entries();
        assert_that!(entries.len(), eq(4_usize));
        assert_that!(entries[1].op, eq(JournalOp::Command));
        assert_that!(entries[3].op, eq(JournalOp::Expired));
    }

    #[rstest]
    fn journal_records_expire_options_only_when_update_is_applied() {
        let mut app = ServerApp::new(RuntimeConfig::default());
        let mut connection = ServerApp::new_connection(ClientProtocol::Resp);

        let _ = app
            .feed_connection_bytes(&mut connection, &resp_command(&[b"SET", b"k", b"v"]))
            .expect("SET should succeed");
        let _ = app
            .feed_connection_bytes(
                &mut connection,
                &resp_command(&[b"EXPIRE", b"k", b"10", b"GT"]),
            )
            .expect("EXPIRE GT should execute");
        let _ = app
            .feed_connection_bytes(
                &mut connection,
                &resp_command(&[b"EXPIRE", b"k", b"10", b"LT"]),
            )
            .expect("EXPIRE LT should execute");
        let _ = app
            .feed_connection_bytes(
                &mut connection,
                &resp_command(&[b"EXPIRE", b"k", b"20", b"NX"]),
            )
            .expect("EXPIRE NX should execute");
        let _ = app
            .feed_connection_bytes(
                &mut connection,
                &resp_command(&[b"EXPIRE", b"k", b"0", b"LT"]),
            )
            .expect("EXPIRE LT delete should execute");

        let entries = app.replication.journal_entries();
        assert_that!(entries.len(), eq(3_usize));
        assert_that!(entries[1].op, eq(JournalOp::Command));
        assert_that!(entries[2].op, eq(JournalOp::Expired));
    }

    #[rstest]
    fn journal_records_pexpireat_as_command_or_expired_by_timestamp() {
        let mut app = ServerApp::new(RuntimeConfig::default());
        let mut connection = ServerApp::new_connection(ClientProtocol::Resp);

        let _ = app
            .feed_connection_bytes(&mut connection, &resp_command(&[b"SET", b"k", b"v"]))
            .expect("SET should succeed");
        let future_timestamp = (SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("system time should be after epoch")
            .as_millis()
            + 120_000)
            .to_string()
            .into_bytes();
        let _ = app
            .feed_connection_bytes(
                &mut connection,
                &resp_command(&[b"PEXPIREAT", b"k", &future_timestamp]),
            )
            .expect("PEXPIREAT future should succeed");
        let _ = app
            .feed_connection_bytes(&mut connection, &resp_command(&[b"SET", b"k", b"v"]))
            .expect("SET should recreate key");
        let _ = app
            .feed_connection_bytes(&mut connection, &resp_command(&[b"PEXPIREAT", b"k", b"1"]))
            .expect("PEXPIREAT past should succeed");

        let entries = app.replication.journal_entries();
        assert_that!(entries.len(), eq(4_usize));
        assert_that!(entries[1].op, eq(JournalOp::Command));
        assert_that!(entries[3].op, eq(JournalOp::Expired));
    }

    #[rstest]
    fn journal_records_mset_as_single_write_command() {
        let mut app = ServerApp::new(RuntimeConfig::default());
        let mut connection = ServerApp::new_connection(ClientProtocol::Resp);

        let mset = resp_command(&[b"MSET", b"a", b"1", b"b", b"2"]);
        let _ = app
            .feed_connection_bytes(&mut connection, &mset)
            .expect("MSET should succeed");

        let entries = app.replication.journal_entries();
        assert_that!(entries.len(), eq(1_usize));
        assert_that!(entries[0].op, eq(JournalOp::Command));
        assert_that!(
            String::from_utf8_lossy(&entries[0].payload).contains("MSET"),
            eq(true)
        );
    }

    #[rstest]
    fn journal_records_msetnx_only_on_successful_insert_batch() {
        let mut app = ServerApp::new(RuntimeConfig::default());
        let mut connection = ServerApp::new_connection(ClientProtocol::Resp);

        let _ = app
            .feed_connection_bytes(
                &mut connection,
                &resp_command(&[b"MSETNX", b"a", b"1", b"b", b"2"]),
            )
            .expect("first MSETNX should succeed");
        let _ = app
            .feed_connection_bytes(
                &mut connection,
                &resp_command(&[b"MSETNX", b"a", b"x", b"c", b"3"]),
            )
            .expect("second MSETNX should succeed");

        let entries = app.replication.journal_entries();
        assert_that!(entries.len(), eq(1_usize));
        assert_that!(
            String::from_utf8_lossy(&entries[0].payload).contains("MSETNX"),
            eq(true)
        );
    }

    #[rstest]
    fn journal_records_rename_family_only_for_effective_mutations() {
        let mut app = ServerApp::new(RuntimeConfig::default());
        let mut connection = ServerApp::new_connection(ClientProtocol::Resp);

        let _ = app
            .feed_connection_bytes(&mut connection, &resp_command(&[b"SET", b"src", b"v"]))
            .expect("SET should succeed");
        let _ = app
            .feed_connection_bytes(
                &mut connection,
                &resp_command(&[b"RENAMENX", b"src", b"dst"]),
            )
            .expect("RENAMENX success should execute");
        let _ = app
            .feed_connection_bytes(
                &mut connection,
                &resp_command(&[b"RENAMENX", b"dst", b"dst"]),
            )
            .expect("RENAMENX same key should execute");
        let _ = app
            .feed_connection_bytes(&mut connection, &resp_command(&[b"RENAME", b"dst", b"dst"]))
            .expect("RENAME same key should execute");

        let entries = app.replication.journal_entries();
        assert_that!(entries.len(), eq(2_usize));
        assert_that!(entries[0].op, eq(JournalOp::Command));
        assert_that!(entries[1].op, eq(JournalOp::Command));
        assert_that!(
            String::from_utf8_lossy(&entries[1].payload).contains("RENAMENX"),
            eq(true)
        );
    }

    #[rstest]
    fn journal_records_copy_only_when_destination_is_written() {
        let mut app = ServerApp::new(RuntimeConfig::default());
        let mut connection = ServerApp::new_connection(ClientProtocol::Resp);

        let _ = app
            .feed_connection_bytes(&mut connection, &resp_command(&[b"SET", b"src", b"v1"]))
            .expect("SET should succeed");
        let _ = app
            .feed_connection_bytes(
                &mut connection,
                &resp_command(&[b"COPY", b"missing", b"dst"]),
            )
            .expect("COPY missing should execute");
        let _ = app
            .feed_connection_bytes(&mut connection, &resp_command(&[b"COPY", b"src", b"dst"]))
            .expect("COPY success should execute");
        let _ = app
            .feed_connection_bytes(&mut connection, &resp_command(&[b"COPY", b"src", b"dst"]))
            .expect("COPY blocked should execute");
        let _ = app
            .feed_connection_bytes(
                &mut connection,
                &resp_command(&[b"COPY", b"src", b"dst", b"REPLACE"]),
            )
            .expect("COPY REPLACE should execute");

        let entries = app.replication.journal_entries();
        assert_that!(entries.len(), eq(3_usize));
        assert_that!(
            String::from_utf8_lossy(&entries[1].payload).contains("COPY"),
            eq(true)
        );
        assert_that!(
            String::from_utf8_lossy(&entries[2].payload).contains("COPY"),
            eq(true)
        );
    }

    #[rstest]
    fn journal_records_del_only_when_keyspace_changes() {
        let mut app = ServerApp::new(RuntimeConfig::default());
        let mut connection = ServerApp::new_connection(ClientProtocol::Resp);

        let _ = app
            .feed_connection_bytes(&mut connection, &resp_command(&[b"DEL", b"missing"]))
            .expect("DEL missing should succeed");
        let _ = app
            .feed_connection_bytes(&mut connection, &resp_command(&[b"SET", b"k", b"v"]))
            .expect("SET should succeed");
        let _ = app
            .feed_connection_bytes(&mut connection, &resp_command(&[b"DEL", b"k"]))
            .expect("DEL existing should succeed");

        let entries = app.replication.journal_entries();
        assert_that!(entries.len(), eq(2_usize));
        assert_that!(
            String::from_utf8_lossy(&entries[1].payload).contains("DEL"),
            eq(true)
        );
    }

    #[rstest]
    fn journal_records_unlink_only_when_keyspace_changes() {
        let mut app = ServerApp::new(RuntimeConfig::default());
        let mut connection = ServerApp::new_connection(ClientProtocol::Resp);

        let _ = app
            .feed_connection_bytes(&mut connection, &resp_command(&[b"UNLINK", b"missing"]))
            .expect("UNLINK missing should succeed");
        let _ = app
            .feed_connection_bytes(&mut connection, &resp_command(&[b"SET", b"k", b"v"]))
            .expect("SET should succeed");
        let _ = app
            .feed_connection_bytes(&mut connection, &resp_command(&[b"UNLINK", b"k"]))
            .expect("UNLINK existing should succeed");

        let entries = app.replication.journal_entries();
        assert_that!(entries.len(), eq(2_usize));
        assert_that!(
            String::from_utf8_lossy(&entries[1].payload).contains("UNLINK"),
            eq(true)
        );
    }

    #[rstest]
    fn journal_records_move_only_when_transfer_happens() {
        let mut app = ServerApp::new(RuntimeConfig::default());
        let mut connection = ServerApp::new_connection(ClientProtocol::Resp);

        let _ = app
            .feed_connection_bytes(&mut connection, &resp_command(&[b"MOVE", b"missing", b"1"]))
            .expect("MOVE missing should succeed");
        let _ = app
            .feed_connection_bytes(&mut connection, &resp_command(&[b"SET", b"k", b"v"]))
            .expect("SET should succeed");
        let _ = app
            .feed_connection_bytes(&mut connection, &resp_command(&[b"MOVE", b"k", b"1"]))
            .expect("MOVE existing should succeed");

        let entries = app.replication.journal_entries();
        assert_that!(entries.len(), eq(2_usize));
        assert_that!(
            String::from_utf8_lossy(&entries[1].payload).contains("MOVE"),
            eq(true)
        );
    }

    #[rstest]
    fn journal_records_persist_only_when_it_changes_expiry() {
        let mut app = ServerApp::new(RuntimeConfig::default());
        let mut connection = ServerApp::new_connection(ClientProtocol::Resp);

        let _ = app
            .feed_connection_bytes(&mut connection, &resp_command(&[b"SET", b"k", b"v"]))
            .expect("SET should succeed");
        let _ = app
            .feed_connection_bytes(&mut connection, &resp_command(&[b"PERSIST", b"k"]))
            .expect("PERSIST without expiry should succeed");
        let _ = app
            .feed_connection_bytes(&mut connection, &resp_command(&[b"EXPIRE", b"k", b"10"]))
            .expect("EXPIRE should succeed");
        let _ = app
            .feed_connection_bytes(&mut connection, &resp_command(&[b"PERSIST", b"k"]))
            .expect("PERSIST should clear expiry");

        let entries = app.replication.journal_entries();
        assert_that!(entries.len(), eq(3_usize));
        assert_that!(
            String::from_utf8_lossy(&entries[2].payload).contains("PERSIST"),
            eq(true)
        );
    }

    #[rstest]
    fn journal_records_incr_family_only_on_success() {
        let mut app = ServerApp::new(RuntimeConfig::default());
        let mut connection = ServerApp::new_connection(ClientProtocol::Resp);

        let _ = app
            .feed_connection_bytes(&mut connection, &resp_command(&[b"INCR", b"counter"]))
            .expect("INCR should succeed");
        let _ = app
            .feed_connection_bytes(
                &mut connection,
                &resp_command(&[b"INCRBY", b"counter", b"2"]),
            )
            .expect("INCRBY should succeed");
        let _ = app
            .feed_connection_bytes(&mut connection, &resp_command(&[b"SET", b"bad", b"abc"]))
            .expect("SET should succeed");
        let _ = app
            .feed_connection_bytes(&mut connection, &resp_command(&[b"INCR", b"bad"]))
            .expect("INCR bad should parse");

        let entries = app.replication.journal_entries();
        assert_that!(entries.len(), eq(3_usize));
        assert_that!(
            String::from_utf8_lossy(&entries[0].payload).contains("INCR"),
            eq(true)
        );
        assert_that!(
            String::from_utf8_lossy(&entries[1].payload).contains("INCRBY"),
            eq(true)
        );
    }

    #[rstest]
    fn journal_records_setnx_only_when_insert_happens() {
        let mut app = ServerApp::new(RuntimeConfig::default());
        let mut connection = ServerApp::new_connection(ClientProtocol::Resp);

        let _ = app
            .feed_connection_bytes(&mut connection, &resp_command(&[b"SETNX", b"k", b"v1"]))
            .expect("first SETNX should succeed");
        let _ = app
            .feed_connection_bytes(&mut connection, &resp_command(&[b"SETNX", b"k", b"v2"]))
            .expect("second SETNX should succeed");

        let entries = app.replication.journal_entries();
        assert_that!(entries.len(), eq(1_usize));
        assert_that!(
            String::from_utf8_lossy(&entries[0].payload).contains("SETNX"),
            eq(true)
        );
    }

    #[rstest]
    fn journal_records_getset_and_getdel_only_when_key_is_deleted() {
        let mut app = ServerApp::new(RuntimeConfig::default());
        let mut connection = ServerApp::new_connection(ClientProtocol::Resp);

        let _ = app
            .feed_connection_bytes(&mut connection, &resp_command(&[b"GETSET", b"k", b"v1"]))
            .expect("first GETSET should succeed");
        let _ = app
            .feed_connection_bytes(&mut connection, &resp_command(&[b"GETSET", b"k", b"v2"]))
            .expect("second GETSET should succeed");
        let _ = app
            .feed_connection_bytes(&mut connection, &resp_command(&[b"GETDEL", b"k"]))
            .expect("GETDEL existing key should succeed");
        let _ = app
            .feed_connection_bytes(&mut connection, &resp_command(&[b"GETDEL", b"k"]))
            .expect("GETDEL missing key should succeed");

        let entries = app.replication.journal_entries();
        assert_that!(entries.len(), eq(3_usize));
        assert_that!(
            String::from_utf8_lossy(&entries[0].payload).contains("GETSET"),
            eq(true)
        );
        assert_that!(
            String::from_utf8_lossy(&entries[1].payload).contains("GETSET"),
            eq(true)
        );
        assert_that!(
            String::from_utf8_lossy(&entries[2].payload).contains("GETDEL"),
            eq(true)
        );
    }

    #[rstest]
    fn journal_records_append_writes() {
        let mut app = ServerApp::new(RuntimeConfig::default());
        let mut connection = ServerApp::new_connection(ClientProtocol::Resp);

        let _ = app
            .feed_connection_bytes(&mut connection, &resp_command(&[b"APPEND", b"k", b"he"]))
            .expect("first APPEND should succeed");
        let _ = app
            .feed_connection_bytes(&mut connection, &resp_command(&[b"APPEND", b"k", b"llo"]))
            .expect("second APPEND should succeed");

        let entries = app.replication.journal_entries();
        assert_that!(entries.len(), eq(2_usize));
        assert_that!(
            String::from_utf8_lossy(&entries[0].payload).contains("APPEND"),
            eq(true)
        );
        assert_that!(
            String::from_utf8_lossy(&entries[1].payload).contains("APPEND"),
            eq(true)
        );
    }

    #[rstest]
    fn journal_records_setrange_only_when_payload_is_non_empty() {
        let mut app = ServerApp::new(RuntimeConfig::default());
        let mut connection = ServerApp::new_connection(ClientProtocol::Resp);

        let _ = app
            .feed_connection_bytes(
                &mut connection,
                &resp_command(&[b"SETRANGE", b"k", b"0", b""]),
            )
            .expect("SETRANGE with empty payload should succeed");
        let _ = app
            .feed_connection_bytes(
                &mut connection,
                &resp_command(&[b"SETRANGE", b"k", b"0", b"xy"]),
            )
            .expect("SETRANGE with payload should succeed");

        let entries = app.replication.journal_entries();
        assert_that!(entries.len(), eq(1_usize));
        assert_that!(
            String::from_utf8_lossy(&entries[0].payload).contains("SETRANGE"),
            eq(true)
        );
    }

    #[rstest]
    fn journal_records_flush_commands() {
        let mut app = ServerApp::new(RuntimeConfig::default());
        let mut connection = ServerApp::new_connection(ClientProtocol::Resp);

        let _ = app
            .feed_connection_bytes(&mut connection, &resp_command(&[b"SET", b"k", b"v"]))
            .expect("SET should succeed");
        let _ = app
            .feed_connection_bytes(&mut connection, &resp_command(&[b"FLUSHDB"]))
            .expect("FLUSHDB should succeed");
        let _ = app
            .feed_connection_bytes(&mut connection, &resp_command(&[b"FLUSHALL"]))
            .expect("FLUSHALL should succeed");

        let entries = app.replication.journal_entries();
        assert_that!(entries.len(), eq(3_usize));
        assert_that!(
            String::from_utf8_lossy(&entries[1].payload).contains("FLUSHDB"),
            eq(true)
        );
        assert_that!(
            String::from_utf8_lossy(&entries[2].payload).contains("FLUSHALL"),
            eq(true)
        );
    }

    #[rstest]
    fn journal_uses_same_txid_for_exec_batch_entries() {
        let mut app = ServerApp::new(RuntimeConfig::default());
        let mut connection = ServerApp::new_connection(ClientProtocol::Resp);

        let _ = app
            .feed_connection_bytes(&mut connection, b"*1\r\n$5\r\nMULTI\r\n")
            .expect("MULTI should succeed");
        let _ = app
            .feed_connection_bytes(
                &mut connection,
                b"*3\r\n$3\r\nSET\r\n$3\r\nfoo\r\n$3\r\nbar\r\n",
            )
            .expect("SET should queue");
        let _ = app
            .feed_connection_bytes(
                &mut connection,
                b"*3\r\n$6\r\nEXPIRE\r\n$3\r\nfoo\r\n$1\r\n0\r\n",
            )
            .expect("EXPIRE should queue");
        let _ = app
            .feed_connection_bytes(&mut connection, b"*1\r\n$4\r\nEXEC\r\n")
            .expect("EXEC should succeed");

        let entries = app.replication.journal_entries();
        assert_that!(entries.len(), eq(2_usize));
        assert_that!(entries[0].txid, eq(entries[1].txid));
        assert_that!(entries[1].op, eq(JournalOp::Expired));
    }

    #[rstest]
    fn resp_select_switches_logical_db_namespace() {
        let mut app = ServerApp::new(RuntimeConfig::default());
        let mut connection = ServerApp::new_connection(ClientProtocol::Resp);

        let _ = app
            .feed_connection_bytes(
                &mut connection,
                b"*3\r\n$3\r\nSET\r\n$6\r\nshared\r\n$3\r\ndb0\r\n",
            )
            .expect("SET in DB 0 should succeed");

        let select_db1 = app
            .feed_connection_bytes(&mut connection, b"*2\r\n$6\r\nSELECT\r\n$1\r\n1\r\n")
            .expect("SELECT 1 should succeed");
        assert_that!(&select_db1, eq(&vec![b"+OK\r\n".to_vec()]));

        let get_in_db1 = app
            .feed_connection_bytes(&mut connection, b"*2\r\n$3\r\nGET\r\n$6\r\nshared\r\n")
            .expect("GET in DB 1 should succeed");
        assert_that!(&get_in_db1, eq(&vec![b"$-1\r\n".to_vec()]));

        let _ = app
            .feed_connection_bytes(
                &mut connection,
                b"*3\r\n$3\r\nSET\r\n$6\r\nshared\r\n$3\r\ndb1\r\n",
            )
            .expect("SET in DB 1 should succeed");

        let select_db0 = app
            .feed_connection_bytes(&mut connection, b"*2\r\n$6\r\nSELECT\r\n$1\r\n0\r\n")
            .expect("SELECT 0 should succeed");
        assert_that!(&select_db0, eq(&vec![b"+OK\r\n".to_vec()]));

        let get_in_db0 = app
            .feed_connection_bytes(&mut connection, b"*2\r\n$3\r\nGET\r\n$6\r\nshared\r\n")
            .expect("GET in DB 0 should succeed");
        assert_that!(&get_in_db0, eq(&vec![b"$3\r\ndb0\r\n".to_vec()]));
    }

    #[rstest]
    fn resp_flushdb_clears_only_selected_database() {
        let mut app = ServerApp::new(RuntimeConfig::default());
        let mut connection = ServerApp::new_connection(ClientProtocol::Resp);

        let _ = app
            .feed_connection_bytes(&mut connection, &resp_command(&[b"SET", b"k", b"db0"]))
            .expect("SET in DB 0 should succeed");
        let _ = app
            .feed_connection_bytes(&mut connection, &resp_command(&[b"SELECT", b"1"]))
            .expect("SELECT 1 should succeed");
        let _ = app
            .feed_connection_bytes(&mut connection, &resp_command(&[b"SET", b"k", b"db1"]))
            .expect("SET in DB 1 should succeed");

        let flushdb = app
            .feed_connection_bytes(&mut connection, &resp_command(&[b"FLUSHDB"]))
            .expect("FLUSHDB should execute");
        assert_that!(&flushdb, eq(&vec![b"+OK\r\n".to_vec()]));

        let get_in_db1 = app
            .feed_connection_bytes(&mut connection, &resp_command(&[b"GET", b"k"]))
            .expect("GET in DB 1 should execute");
        assert_that!(&get_in_db1, eq(&vec![b"$-1\r\n".to_vec()]));

        let _ = app
            .feed_connection_bytes(&mut connection, &resp_command(&[b"SELECT", b"0"]))
            .expect("SELECT 0 should succeed");
        let get_in_db0 = app
            .feed_connection_bytes(&mut connection, &resp_command(&[b"GET", b"k"]))
            .expect("GET in DB 0 should execute");
        assert_that!(&get_in_db0, eq(&vec![b"$3\r\ndb0\r\n".to_vec()]));
    }

    #[rstest]
    fn resp_flushall_clears_all_databases() {
        let mut app = ServerApp::new(RuntimeConfig::default());
        let mut connection = ServerApp::new_connection(ClientProtocol::Resp);

        let _ = app
            .feed_connection_bytes(&mut connection, &resp_command(&[b"SET", b"k0", b"v0"]))
            .expect("SET in DB 0 should succeed");
        let _ = app
            .feed_connection_bytes(&mut connection, &resp_command(&[b"SELECT", b"1"]))
            .expect("SELECT 1 should succeed");
        let _ = app
            .feed_connection_bytes(&mut connection, &resp_command(&[b"SET", b"k1", b"v1"]))
            .expect("SET in DB 1 should succeed");

        let flushall = app
            .feed_connection_bytes(&mut connection, &resp_command(&[b"FLUSHALL"]))
            .expect("FLUSHALL should execute");
        assert_that!(&flushall, eq(&vec![b"+OK\r\n".to_vec()]));

        let get_in_db1 = app
            .feed_connection_bytes(&mut connection, &resp_command(&[b"GET", b"k1"]))
            .expect("GET in DB 1 should execute");
        assert_that!(&get_in_db1, eq(&vec![b"$-1\r\n".to_vec()]));

        let _ = app
            .feed_connection_bytes(&mut connection, &resp_command(&[b"SELECT", b"0"]))
            .expect("SELECT 0 should succeed");
        let get_in_db0 = app
            .feed_connection_bytes(&mut connection, &resp_command(&[b"GET", b"k0"]))
            .expect("GET in DB 0 should execute");
        assert_that!(&get_in_db0, eq(&vec![b"$-1\r\n".to_vec()]));
    }

    #[rstest]
    fn resp_flush_commands_are_rejected_inside_multi() {
        let mut app = ServerApp::new(RuntimeConfig::default());

        let mut flushall_conn = ServerApp::new_connection(ClientProtocol::Resp);
        let _ = app
            .feed_connection_bytes(&mut flushall_conn, &resp_command(&[b"MULTI"]))
            .expect("MULTI should succeed");
        let flushall = app
            .feed_connection_bytes(&mut flushall_conn, &resp_command(&[b"FLUSHALL"]))
            .expect("FLUSHALL should parse");
        assert_that!(
            &flushall,
            eq(&vec![
                b"-ERR 'FLUSHALL' not allowed inside a transaction\r\n".to_vec()
            ])
        );
        let flushall_exec = app
            .feed_connection_bytes(&mut flushall_conn, &resp_command(&[b"EXEC"]))
            .expect("EXEC should parse");
        assert_that!(
            &flushall_exec,
            eq(&vec![
                b"-ERR EXECABORT Transaction discarded because of previous errors\r\n".to_vec()
            ])
        );

        let mut flushdb_conn = ServerApp::new_connection(ClientProtocol::Resp);
        let _ = app
            .feed_connection_bytes(&mut flushdb_conn, &resp_command(&[b"MULTI"]))
            .expect("MULTI should succeed");
        let flushdb = app
            .feed_connection_bytes(&mut flushdb_conn, &resp_command(&[b"FLUSHDB"]))
            .expect("FLUSHDB should parse");
        assert_that!(
            &flushdb,
            eq(&vec![
                b"-ERR 'FLUSHDB' not allowed inside a transaction\r\n".to_vec()
            ])
        );
        let flushdb_exec = app
            .feed_connection_bytes(&mut flushdb_conn, &resp_command(&[b"EXEC"]))
            .expect("EXEC should parse");
        assert_that!(
            &flushdb_exec,
            eq(&vec![
                b"-ERR EXECABORT Transaction discarded because of previous errors\r\n".to_vec()
            ])
        );
    }

    #[rstest]
    fn resp_select_is_rejected_while_multi_is_open() {
        let mut app = ServerApp::new(RuntimeConfig::default());
        let mut connection = ServerApp::new_connection(ClientProtocol::Resp);

        let _ = app
            .feed_connection_bytes(&mut connection, b"*1\r\n$5\r\nMULTI\r\n")
            .expect("MULTI should succeed");
        let select = app
            .feed_connection_bytes(&mut connection, b"*2\r\n$6\r\nSELECT\r\n$1\r\n1\r\n")
            .expect("SELECT should parse");
        assert_that!(
            &select,
            eq(&vec![b"-ERR SELECT is not allowed in MULTI\r\n".to_vec()])
        );
    }

    #[rstest]
    fn resp_execaborts_after_select_error_inside_multi() {
        let mut app = ServerApp::new(RuntimeConfig::default());
        let mut connection = ServerApp::new_connection(ClientProtocol::Resp);

        let _ = app
            .feed_connection_bytes(&mut connection, &resp_command(&[b"MULTI"]))
            .expect("MULTI should succeed");
        let select = app
            .feed_connection_bytes(&mut connection, &resp_command(&[b"SELECT", b"1"]))
            .expect("SELECT should parse");
        assert_that!(
            &select,
            eq(&vec![b"-ERR SELECT is not allowed in MULTI\r\n".to_vec()])
        );

        let exec = app
            .feed_connection_bytes(&mut connection, &resp_command(&[b"EXEC"]))
            .expect("EXEC should parse");
        assert_that!(
            &exec,
            eq(&vec![
                b"-ERR EXECABORT Transaction discarded because of previous errors\r\n".to_vec()
            ])
        );
    }

    #[rstest]
    fn resp_execaborts_after_exec_arity_error_inside_multi() {
        let mut app = ServerApp::new(RuntimeConfig::default());
        let mut connection = ServerApp::new_connection(ClientProtocol::Resp);

        let _ = app
            .feed_connection_bytes(&mut connection, &resp_command(&[b"MULTI"]))
            .expect("MULTI should succeed");
        let exec_arity_error = app
            .feed_connection_bytes(&mut connection, &resp_command(&[b"EXEC", b"extra"]))
            .expect("EXEC should parse");
        assert_that!(
            &exec_arity_error,
            eq(&vec![
                b"-ERR wrong number of arguments for 'EXEC' command\r\n".to_vec()
            ])
        );

        let exec_after_error = app
            .feed_connection_bytes(&mut connection, &resp_command(&[b"EXEC"]))
            .expect("EXEC should parse");
        assert_that!(
            &exec_after_error,
            eq(&vec![
                b"-ERR EXECABORT Transaction discarded because of previous errors\r\n".to_vec()
            ])
        );
    }

    #[rstest]
    fn resp_execaborts_after_discard_arity_error_inside_multi() {
        let mut app = ServerApp::new(RuntimeConfig::default());
        let mut connection = ServerApp::new_connection(ClientProtocol::Resp);

        let _ = app
            .feed_connection_bytes(&mut connection, &resp_command(&[b"MULTI"]))
            .expect("MULTI should succeed");
        let discard_arity_error = app
            .feed_connection_bytes(&mut connection, &resp_command(&[b"DISCARD", b"extra"]))
            .expect("DISCARD should parse");
        assert_that!(
            &discard_arity_error,
            eq(&vec![
                b"-ERR wrong number of arguments for 'DISCARD' command\r\n".to_vec()
            ])
        );

        let exec_after_error = app
            .feed_connection_bytes(&mut connection, &resp_command(&[b"EXEC"]))
            .expect("EXEC should parse");
        assert_that!(
            &exec_after_error,
            eq(&vec![
                b"-ERR EXECABORT Transaction discarded because of previous errors\r\n".to_vec()
            ])
        );
    }

    #[rstest]
    fn resp_role_reports_master_with_empty_replica_list() {
        let mut app = ServerApp::new(RuntimeConfig::default());
        let mut connection = ServerApp::new_connection(ClientProtocol::Resp);

        let reply = app
            .feed_connection_bytes(&mut connection, &resp_command(&[b"ROLE"]))
            .expect("ROLE should succeed");
        assert_that!(&reply, eq(&vec![b"*2\r\n$6\r\nmaster\r\n*0\r\n".to_vec()]));
    }

    #[rstest]
    fn resp_info_replication_reports_master_offsets() {
        let mut app = ServerApp::new(RuntimeConfig::default());
        let mut connection = ServerApp::new_connection(ClientProtocol::Resp);

        let _ = app
            .feed_connection_bytes(
                &mut connection,
                &resp_command(&[b"SET", b"info:key", b"info:value"]),
            )
            .expect("SET should succeed");

        let reply = app
            .feed_connection_bytes(&mut connection, &resp_command(&[b"INFO", b"REPLICATION"]))
            .expect("INFO REPLICATION should succeed");
        assert_that!(reply.len(), eq(1_usize));
        let body = decode_resp_bulk_payload(&reply[0]);
        assert_that!(body.contains("# Replication\r\n"), eq(true));
        assert_that!(body.contains("role:master\r\n"), eq(true));
        assert_that!(body.contains("connected_slaves:0\r\n"), eq(true));
        assert_that!(body.contains("master_replid:"), eq(true));
        assert_that!(body.contains("master_repl_offset:1\r\n"), eq(true));
        assert_that!(body.contains("last_ack_lsn:0\r\n"), eq(true));
    }

    #[rstest]
    fn resp_info_supports_multiple_sections_and_includes_persistence() {
        let mut app = ServerApp::new(RuntimeConfig::default());
        let mut connection = ServerApp::new_connection(ClientProtocol::Resp);

        let _ = app
            .feed_connection_bytes(
                &mut connection,
                &resp_command(&[b"SET", b"info:multi:key", b"value"]),
            )
            .expect("SET should succeed");

        let reply = app
            .feed_connection_bytes(
                &mut connection,
                &resp_command(&[b"INFO", b"REPLICATION", b"PERSISTENCE"]),
            )
            .expect("INFO should succeed");
        assert_that!(reply.len(), eq(1_usize));
        let body = decode_resp_bulk_payload(&reply[0]);
        assert_that!(body.contains("# Replication\r\n"), eq(true));
        assert_that!(body.contains("# Persistence\r\n"), eq(true));
        assert_that!(body.contains("loading:0\r\n"), eq(true));
    }

    #[rstest]
    fn resp_info_ignores_unknown_sections_when_valid_section_is_present() {
        let mut app = ServerApp::new(RuntimeConfig::default());
        let mut connection = ServerApp::new_connection(ClientProtocol::Resp);

        let reply = app
            .feed_connection_bytes(
                &mut connection,
                &resp_command(&[b"INFO", b"REPLICATION", b"INVALIDSECTION"]),
            )
            .expect("INFO should succeed");
        assert_that!(reply.len(), eq(1_usize));
        let body = decode_resp_bulk_payload(&reply[0]);
        assert_that!(body.contains("# Replication\r\n"), eq(true));
        assert_that!(body.contains("INVALIDSECTION"), eq(false));
    }

    #[rstest]
    fn resp_replconf_ack_is_silent_and_tracks_ack_lsn() {
        let mut app = ServerApp::new(RuntimeConfig::default());
        let mut connection = ServerApp::new_connection(ClientProtocol::Resp);

        let _ = app
            .feed_connection_bytes(
                &mut connection,
                &resp_command(&[b"REPLCONF", b"LISTENING-PORT", b"7001"]),
            )
            .expect("REPLCONF LISTENING-PORT should succeed");
        let _ = app
            .feed_connection_bytes(
                &mut connection,
                &resp_command(&[b"SET", b"ack:key", b"ack:value"]),
            )
            .expect("SET should succeed");

        let ack_reply = app
            .feed_connection_bytes(
                &mut connection,
                &resp_command(&[b"REPLCONF", b"ACK", b"999"]),
            )
            .expect("REPLCONF ACK should parse");
        assert_that!(&ack_reply, eq(&Vec::<Vec<u8>>::new()));
        assert_that!(app.replication.last_acked_lsn(), eq(1_u64));

        let info = app
            .feed_connection_bytes(&mut connection, &resp_command(&[b"INFO", b"REPLICATION"]))
            .expect("INFO REPLICATION should succeed");
        let body = decode_resp_bulk_payload(&info[0]);
        assert_that!(body.contains("last_ack_lsn:1\r\n"), eq(true));
    }

    #[rstest]
    fn resp_replconf_ack_without_registered_endpoint_is_ignored_silently() {
        let mut app = ServerApp::new(RuntimeConfig::default());
        let mut connection = ServerApp::new_connection(ClientProtocol::Resp);

        let _ = app
            .feed_connection_bytes(
                &mut connection,
                &resp_command(&[b"SET", b"ack:key", b"ack:value"]),
            )
            .expect("SET should succeed");

        let ack_reply = app
            .feed_connection_bytes(
                &mut connection,
                &resp_command(&[b"REPLCONF", b"ACK", b"999"]),
            )
            .expect("REPLCONF ACK should parse");
        assert_that!(&ack_reply, eq(&Vec::<Vec<u8>>::new()));
        assert_that!(app.replication.last_acked_lsn(), eq(0_u64));
    }

    #[rstest]
    fn resp_replconf_ack_requires_standalone_pair() {
        let mut app = ServerApp::new(RuntimeConfig::default());
        let mut connection = ServerApp::new_connection(ClientProtocol::Resp);

        let reply = app
            .feed_connection_bytes(
                &mut connection,
                &resp_command(&[b"REPLCONF", b"ACK", b"1", b"LISTENING-PORT", b"7001"]),
            )
            .expect("invalid REPLCONF ACK shape should parse");
        assert_that!(&reply, eq(&vec![b"-ERR syntax error\r\n".to_vec()]));

        let info = app
            .feed_connection_bytes(&mut connection, &resp_command(&[b"INFO", b"REPLICATION"]))
            .expect("INFO REPLICATION should succeed");
        let body = decode_resp_bulk_payload(&info[0]);
        assert_that!(body.contains("connected_slaves:0\r\n"), eq(true));
    }

    #[rstest]
    fn resp_replconf_client_id_is_stored_and_requires_standalone_pair() {
        let mut app = ServerApp::new(RuntimeConfig::default());
        let mut connection = ServerApp::new_connection(ClientProtocol::Resp);

        let ok_reply = app
            .feed_connection_bytes(
                &mut connection,
                &resp_command(&[b"REPLCONF", b"CLIENT-ID", b"replica-1"]),
            )
            .expect("REPLCONF CLIENT-ID should succeed");
        assert_that!(&ok_reply, eq(&vec![b"+OK\r\n".to_vec()]));
        assert_that!(
            &connection.replica_client_id,
            eq(&Some("replica-1".to_owned()))
        );

        let error_reply = app
            .feed_connection_bytes(
                &mut connection,
                &resp_command(&[
                    b"REPLCONF",
                    b"CLIENT-ID",
                    b"replica-2",
                    b"LISTENING-PORT",
                    b"7001",
                ]),
            )
            .expect("invalid REPLCONF CLIENT-ID shape should parse");
        assert_that!(&error_reply, eq(&vec![b"-ERR syntax error\r\n".to_vec()]));
    }

    #[rstest]
    fn resp_replconf_client_version_requires_integer_and_standalone_pair() {
        let mut app = ServerApp::new(RuntimeConfig::default());
        let mut connection = ServerApp::new_connection(ClientProtocol::Resp);

        let bad_int_reply = app
            .feed_connection_bytes(
                &mut connection,
                &resp_command(&[b"REPLCONF", b"CLIENT-VERSION", b"abc"]),
            )
            .expect("invalid REPLCONF CLIENT-VERSION should parse");
        assert_that!(
            &bad_int_reply,
            eq(&vec![
                b"-ERR value is not an integer or out of range\r\n".to_vec()
            ])
        );

        let mixed_reply = app
            .feed_connection_bytes(
                &mut connection,
                &resp_command(&[
                    b"REPLCONF",
                    b"CLIENT-VERSION",
                    b"1",
                    b"LISTENING-PORT",
                    b"7001",
                ]),
            )
            .expect("invalid REPLCONF CLIENT-VERSION shape should parse");
        assert_that!(&mixed_reply, eq(&vec![b"-ERR syntax error\r\n".to_vec()]));

        let ok_reply = app
            .feed_connection_bytes(
                &mut connection,
                &resp_command(&[b"REPLCONF", b"CLIENT-VERSION", b"3"]),
            )
            .expect("REPLCONF CLIENT-VERSION should succeed");
        assert_that!(&ok_reply, eq(&vec![b"+OK\r\n".to_vec()]));
        assert_that!(&connection.replica_client_version, eq(&Some(3_u64)));
    }

    #[rstest]
    fn resp_wait_reports_zero_without_replica_acks() {
        let mut app = ServerApp::new(RuntimeConfig::default());
        let mut connection = ServerApp::new_connection(ClientProtocol::Resp);

        let wait = app
            .feed_connection_bytes(&mut connection, &resp_command(&[b"WAIT", b"1", b"0"]))
            .expect("WAIT should execute");
        assert_that!(&wait, eq(&vec![b":0\r\n".to_vec()]));
    }

    #[rstest]
    fn resp_wait_counts_replicas_after_ack_reaches_current_offset() {
        let mut app = ServerApp::new(RuntimeConfig::default());
        let mut connection = ServerApp::new_connection(ClientProtocol::Resp);

        let _ = app
            .feed_connection_bytes(
                &mut connection,
                &resp_command(&[b"REPLCONF", b"LISTENING-PORT", b"7001"]),
            )
            .expect("REPLCONF LISTENING-PORT should succeed");
        let _ = app
            .feed_connection_bytes(
                &mut connection,
                &resp_command(&[b"SET", b"wait:key", b"v1"]),
            )
            .expect("SET should succeed");

        let wait_before_ack = app
            .feed_connection_bytes(&mut connection, &resp_command(&[b"WAIT", b"1", b"0"]))
            .expect("WAIT should execute");
        assert_that!(&wait_before_ack, eq(&vec![b":0\r\n".to_vec()]));

        let _ = app
            .feed_connection_bytes(&mut connection, &resp_command(&[b"REPLCONF", b"ACK", b"1"]))
            .expect("REPLCONF ACK should parse");

        let wait_after_ack = app
            .feed_connection_bytes(&mut connection, &resp_command(&[b"WAIT", b"1", b"0"]))
            .expect("WAIT should execute");
        assert_that!(&wait_after_ack, eq(&vec![b":1\r\n".to_vec()]));
    }

    #[rstest]
    fn resp_wait_counts_only_replicas_that_acked_target_offset() {
        let mut app = ServerApp::new(RuntimeConfig::default());
        let mut replica1 = ServerApp::new_connection(ClientProtocol::Resp);
        let mut replica2 = ServerApp::new_connection(ClientProtocol::Resp);
        let mut client = ServerApp::new_connection(ClientProtocol::Resp);

        let _ = app
            .feed_connection_bytes(
                &mut replica1,
                &resp_command(&[b"REPLCONF", b"LISTENING-PORT", b"7001"]),
            )
            .expect("replica1 REPLCONF LISTENING-PORT should succeed");
        let _ = app
            .feed_connection_bytes(
                &mut replica2,
                &resp_command(&[b"REPLCONF", b"LISTENING-PORT", b"7002"]),
            )
            .expect("replica2 REPLCONF LISTENING-PORT should succeed");

        let _ = app
            .feed_connection_bytes(&mut client, &resp_command(&[b"SET", b"wait:key", b"value"]))
            .expect("SET should succeed");

        let _ = app
            .feed_connection_bytes(&mut replica1, &resp_command(&[b"REPLCONF", b"ACK", b"1"]))
            .expect("replica1 ACK should parse");

        let wait_one = app
            .feed_connection_bytes(&mut client, &resp_command(&[b"WAIT", b"2", b"0"]))
            .expect("WAIT should execute");
        assert_that!(&wait_one, eq(&vec![b":1\r\n".to_vec()]));

        let _ = app
            .feed_connection_bytes(&mut replica2, &resp_command(&[b"REPLCONF", b"ACK", b"1"]))
            .expect("replica2 ACK should parse");

        let wait_two = app
            .feed_connection_bytes(&mut client, &resp_command(&[b"WAIT", b"2", b"0"]))
            .expect("WAIT should execute");
        assert_that!(&wait_two, eq(&vec![b":2\r\n".to_vec()]));
    }

    #[rstest]
    fn resp_wait_reports_actual_acked_replica_count_even_when_requested_is_lower() {
        let mut app = ServerApp::new(RuntimeConfig::default());
        let mut replica1 = ServerApp::new_connection(ClientProtocol::Resp);
        let mut replica2 = ServerApp::new_connection(ClientProtocol::Resp);
        let mut client = ServerApp::new_connection(ClientProtocol::Resp);

        let _ = app
            .feed_connection_bytes(
                &mut replica1,
                &resp_command(&[b"REPLCONF", b"LISTENING-PORT", b"7001"]),
            )
            .expect("replica1 REPLCONF LISTENING-PORT should succeed");
        let _ = app
            .feed_connection_bytes(
                &mut replica2,
                &resp_command(&[b"REPLCONF", b"LISTENING-PORT", b"7002"]),
            )
            .expect("replica2 REPLCONF LISTENING-PORT should succeed");
        let _ = app
            .feed_connection_bytes(&mut client, &resp_command(&[b"SET", b"wait:key", b"value"]))
            .expect("SET should succeed");
        let _ = app
            .feed_connection_bytes(&mut replica1, &resp_command(&[b"REPLCONF", b"ACK", b"1"]))
            .expect("replica1 ACK should parse");
        let _ = app
            .feed_connection_bytes(&mut replica2, &resp_command(&[b"REPLCONF", b"ACK", b"1"]))
            .expect("replica2 ACK should parse");

        let wait = app
            .feed_connection_bytes(&mut client, &resp_command(&[b"WAIT", b"1", b"0"]))
            .expect("WAIT should execute");
        assert_that!(&wait, eq(&vec![b":2\r\n".to_vec()]));
    }

    #[rstest]
    fn resp_wait_does_not_reuse_ack_after_replica_re_registers() {
        let mut app = ServerApp::new(RuntimeConfig::default());
        let mut replica = ServerApp::new_connection(ClientProtocol::Resp);
        let mut client = ServerApp::new_connection(ClientProtocol::Resp);

        let _ = app
            .feed_connection_bytes(
                &mut replica,
                &resp_command(&[b"REPLCONF", b"LISTENING-PORT", b"7001"]),
            )
            .expect("replica REPLCONF LISTENING-PORT should succeed");
        let _ = app
            .feed_connection_bytes(&mut client, &resp_command(&[b"SET", b"wait:key", b"value"]))
            .expect("SET should succeed");
        let _ = app
            .feed_connection_bytes(&mut replica, &resp_command(&[b"REPLCONF", b"ACK", b"1"]))
            .expect("replica ACK should parse");

        let wait_after_ack = app
            .feed_connection_bytes(&mut client, &resp_command(&[b"WAIT", b"1", b"0"]))
            .expect("WAIT should execute");
        assert_that!(&wait_after_ack, eq(&vec![b":1\r\n".to_vec()]));

        let _ = app
            .feed_connection_bytes(
                &mut replica,
                &resp_command(&[b"REPLCONF", b"LISTENING-PORT", b"7001"]),
            )
            .expect("replica REPLCONF LISTENING-PORT re-registration should succeed");

        let wait_after_reregister = app
            .feed_connection_bytes(&mut client, &resp_command(&[b"WAIT", b"1", b"0"]))
            .expect("WAIT should execute");
        assert_that!(&wait_after_reregister, eq(&vec![b":0\r\n".to_vec()]));
    }

    #[rstest]
    fn resp_info_replication_recomputes_last_ack_after_replica_reregister() {
        let mut app = ServerApp::new(RuntimeConfig::default());
        let mut replica1 = ServerApp::new_connection(ClientProtocol::Resp);
        let mut replica2 = ServerApp::new_connection(ClientProtocol::Resp);
        let mut client = ServerApp::new_connection(ClientProtocol::Resp);

        let _ = app
            .feed_connection_bytes(
                &mut replica1,
                &resp_command(&[b"REPLCONF", b"LISTENING-PORT", b"7001"]),
            )
            .expect("replica1 REPLCONF LISTENING-PORT should succeed");
        let _ = app
            .feed_connection_bytes(
                &mut replica2,
                &resp_command(&[b"REPLCONF", b"LISTENING-PORT", b"7002"]),
            )
            .expect("replica2 REPLCONF LISTENING-PORT should succeed");
        let _ = app
            .feed_connection_bytes(
                &mut client,
                &resp_command(&[b"SET", b"info:ack:key1", b"value1"]),
            )
            .expect("first SET should succeed");
        let _ = app
            .feed_connection_bytes(&mut replica1, &resp_command(&[b"REPLCONF", b"ACK", b"1"]))
            .expect("replica1 first ACK should parse");
        let _ = app
            .feed_connection_bytes(&mut replica2, &resp_command(&[b"REPLCONF", b"ACK", b"1"]))
            .expect("replica2 first ACK should parse");
        let _ = app
            .feed_connection_bytes(
                &mut client,
                &resp_command(&[b"SET", b"info:ack:key2", b"value2"]),
            )
            .expect("second SET should succeed");
        let _ = app
            .feed_connection_bytes(&mut replica1, &resp_command(&[b"REPLCONF", b"ACK", b"2"]))
            .expect("replica1 second ACK should parse");

        let before = app
            .feed_connection_bytes(&mut client, &resp_command(&[b"INFO", b"REPLICATION"]))
            .expect("INFO REPLICATION should succeed");
        let before_body = decode_resp_bulk_payload(&before[0]);
        assert_that!(before_body.contains("last_ack_lsn:2\r\n"), eq(true));

        let _ = app
            .feed_connection_bytes(
                &mut replica1,
                &resp_command(&[b"REPLCONF", b"LISTENING-PORT", b"7001"]),
            )
            .expect("replica1 REPLCONF LISTENING-PORT re-registration should succeed");

        let after = app
            .feed_connection_bytes(&mut client, &resp_command(&[b"INFO", b"REPLICATION"]))
            .expect("INFO REPLICATION should succeed");
        let after_body = decode_resp_bulk_payload(&after[0]);
        assert_that!(after_body.contains("last_ack_lsn:1\r\n"), eq(true));
    }

    #[rstest]
    fn resp_info_replication_reports_per_replica_state_and_lag() {
        let mut app = ServerApp::new(RuntimeConfig::default());
        let mut replica1 = ServerApp::new_connection(ClientProtocol::Resp);
        let mut replica2 = ServerApp::new_connection(ClientProtocol::Resp);
        let mut client = ServerApp::new_connection(ClientProtocol::Resp);

        let _ = app
            .feed_connection_bytes(
                &mut replica1,
                &resp_command(&[
                    b"REPLCONF",
                    b"LISTENING-PORT",
                    b"7001",
                    b"IP-ADDRESS",
                    b"10.0.0.1",
                ]),
            )
            .expect("replica1 REPLCONF endpoint registration should succeed");
        let _ = app
            .feed_connection_bytes(
                &mut replica2,
                &resp_command(&[
                    b"REPLCONF",
                    b"LISTENING-PORT",
                    b"7002",
                    b"IP-ADDRESS",
                    b"10.0.0.2",
                ]),
            )
            .expect("replica2 REPLCONF endpoint registration should succeed");

        let _ = app
            .feed_connection_bytes(
                &mut client,
                &resp_command(&[b"SET", b"info:replica:key", b"value"]),
            )
            .expect("SET should succeed");
        let _ = app
            .feed_connection_bytes(&mut replica1, &resp_command(&[b"REPLCONF", b"ACK", b"1"]))
            .expect("replica1 ACK should parse");

        let info = app
            .feed_connection_bytes(&mut client, &resp_command(&[b"INFO", b"REPLICATION"]))
            .expect("INFO REPLICATION should succeed");
        let body = decode_resp_bulk_payload(&info[0]);
        assert_that!(body.contains("connected_slaves:2\r\n"), eq(true));
        assert_that!(
            body.contains("slave0:ip=10.0.0.1,port=7001,state=stable_sync,lag=0\r\n"),
            eq(true)
        );
        assert_that!(
            body.contains("slave1:ip=10.0.0.2,port=7002,state=preparation,lag=0\r\n"),
            eq(true)
        );
    }

    #[rstest]
    fn resp_replconf_capa_dragonfly_returns_handshake_array() {
        let mut app = ServerApp::new(RuntimeConfig::default());
        let mut connection = ServerApp::new_connection(ClientProtocol::Resp);

        let reply = app
            .feed_connection_bytes(
                &mut connection,
                &resp_command(&[b"REPLCONF", b"CAPA", b"dragonfly"]),
            )
            .expect("REPLCONF CAPA dragonfly should succeed");
        assert_that!(reply.len(), eq(1_usize));

        let payload = std::str::from_utf8(&reply[0]).expect("payload must be UTF-8");
        assert_that!(payload.starts_with("*4\r\n$40\r\n"), eq(true));
        assert_that!(payload.contains("\r\n$5\r\nSYNC1\r\n"), eq(true));
        let shard_and_version = format!(":{}\r\n:1\r\n", app.config.shard_count.get());
        assert_that!(payload.contains(&shard_and_version), eq(true));
    }

    #[rstest]
    fn resp_replconf_capa_dragonfly_allocates_monotonic_sync_ids() {
        let mut app = ServerApp::new(RuntimeConfig::default());
        let mut connection = ServerApp::new_connection(ClientProtocol::Resp);

        let first = app
            .feed_connection_bytes(
                &mut connection,
                &resp_command(&[b"REPLCONF", b"CAPA", b"dragonfly"]),
            )
            .expect("first REPLCONF CAPA dragonfly should succeed");
        let second = app
            .feed_connection_bytes(
                &mut connection,
                &resp_command(&[b"REPLCONF", b"CAPA", b"dragonfly"]),
            )
            .expect("second REPLCONF CAPA dragonfly should succeed");

        let first_sync_id = extract_sync_id_from_capa_reply(&first[0]);
        let second_sync_id = extract_sync_id_from_capa_reply(&second[0]);
        assert_that!(first_sync_id.as_str(), eq("SYNC1"));
        assert_that!(second_sync_id.as_str(), eq("SYNC2"));
    }

    #[rstest]
    fn resp_dfly_flow_returns_partial_when_lsn_is_available() {
        let mut app = ServerApp::new(RuntimeConfig::default());
        let mut connection = ServerApp::new_connection(ClientProtocol::Resp);

        let handshake = app
            .feed_connection_bytes(
                &mut connection,
                &resp_command(&[b"REPLCONF", b"CAPA", b"dragonfly"]),
            )
            .expect("REPLCONF CAPA dragonfly should succeed");
        let sync_id = extract_sync_id_from_capa_reply(&handshake[0]).into_bytes();
        let master_id = app.replication.master_replid().as_bytes().to_vec();

        let _ = app
            .feed_connection_bytes(
                &mut connection,
                &resp_command(&[b"SET", b"flow:key", b"flow:value"]),
            )
            .expect("SET should succeed");

        let flow_reply = app
            .feed_connection_bytes(
                &mut connection,
                &resp_command(&[b"DFLY", b"FLOW", &master_id, &sync_id, b"0", b"0"]),
            )
            .expect("DFLY FLOW should succeed");
        let (sync_type, eof_token) = extract_dfly_flow_reply(&flow_reply[0]);
        assert_that!(sync_type.as_str(), eq("PARTIAL"));
        assert_that!(eof_token.len(), eq(40_usize));
    }

    #[rstest]
    fn resp_dfly_flow_returns_full_when_partial_cursor_is_stale() {
        let mut app = ServerApp::new(RuntimeConfig::default());
        app.replication.journal = InMemoryJournal::with_backlog(1);
        let mut connection = ServerApp::new_connection(ClientProtocol::Resp);

        let handshake = app
            .feed_connection_bytes(
                &mut connection,
                &resp_command(&[b"REPLCONF", b"CAPA", b"dragonfly"]),
            )
            .expect("REPLCONF CAPA dragonfly should succeed");
        let sync_id = extract_sync_id_from_capa_reply(&handshake[0]).into_bytes();
        let master_id = app.replication.master_replid().as_bytes().to_vec();

        let _ = app
            .feed_connection_bytes(
                &mut connection,
                &resp_command(&[b"SET", b"flow:key", b"v1"]),
            )
            .expect("first SET should succeed");
        let _ = app
            .feed_connection_bytes(
                &mut connection,
                &resp_command(&[b"SET", b"flow:key", b"v2"]),
            )
            .expect("second SET should succeed");

        let flow_reply = app
            .feed_connection_bytes(
                &mut connection,
                &resp_command(&[b"DFLY", b"FLOW", &master_id, &sync_id, b"0", b"0"]),
            )
            .expect("DFLY FLOW should succeed");
        let (sync_type, _) = extract_dfly_flow_reply(&flow_reply[0]);
        assert_that!(sync_type.as_str(), eq("FULL"));
    }

    #[rstest]
    fn resp_dfly_flow_full_mode_does_not_store_partial_start_offset() {
        let mut app = ServerApp::new(RuntimeConfig::default());
        app.replication.journal = InMemoryJournal::with_backlog(1);
        let mut connection = ServerApp::new_connection(ClientProtocol::Resp);

        let handshake = app
            .feed_connection_bytes(
                &mut connection,
                &resp_command(&[b"REPLCONF", b"CAPA", b"dragonfly"]),
            )
            .expect("REPLCONF CAPA dragonfly should succeed");
        let sync_id_text = extract_sync_id_from_capa_reply(&handshake[0]);
        let sync_id = sync_id_text.as_bytes().to_vec();
        let master_id = app.replication.master_replid().as_bytes().to_vec();

        let _ = app
            .feed_connection_bytes(
                &mut connection,
                &resp_command(&[b"SET", b"flow:stale:key", b"v1"]),
            )
            .expect("first SET should succeed");
        let _ = app
            .feed_connection_bytes(
                &mut connection,
                &resp_command(&[b"SET", b"flow:stale:key", b"v2"]),
            )
            .expect("second SET should succeed");

        let flow_reply = app
            .feed_connection_bytes(
                &mut connection,
                &resp_command(&[b"DFLY", b"FLOW", &master_id, &sync_id, b"0", b"0"]),
            )
            .expect("DFLY FLOW should succeed");
        let (sync_type, _) = extract_dfly_flow_reply(&flow_reply[0]);
        assert_that!(sync_type.as_str(), eq("FULL"));

        let flow = app
            .replication
            .state
            .sync_flow(&sync_id_text, 0)
            .expect("flow must be registered");
        assert_that!(flow.start_offset.is_none(), eq(true));
    }

    #[rstest]
    fn resp_dfly_sync_and_startstable_update_replica_role_state() {
        let mut app = ServerApp::new(RuntimeConfig::default());
        let mut connection = ServerApp::new_connection(ClientProtocol::Resp);

        let _ = app
            .feed_connection_bytes(
                &mut connection,
                &resp_command(&[b"REPLCONF", b"LISTENING-PORT", b"7001"]),
            )
            .expect("REPLCONF LISTENING-PORT should succeed");

        let handshake = app
            .feed_connection_bytes(
                &mut connection,
                &resp_command(&[b"REPLCONF", b"CAPA", b"dragonfly"]),
            )
            .expect("REPLCONF CAPA dragonfly should succeed");
        let sync_id = extract_sync_id_from_capa_reply(&handshake[0]).into_bytes();
        let master_id = app.replication.master_replid().as_bytes().to_vec();
        for flow_id in 0..usize::from(app.config.shard_count.get()) {
            let flow_id_text = flow_id.to_string().into_bytes();
            let flow_reply = app
                .feed_connection_bytes(
                    &mut connection,
                    &resp_command(&[b"DFLY", b"FLOW", &master_id, &sync_id, &flow_id_text]),
                )
                .expect("DFLY FLOW should succeed");
            let (sync_type, eof_token) = extract_dfly_flow_reply(&flow_reply[0]);
            assert_that!(sync_type.as_str(), eq("FULL"));
            assert_that!(eof_token.len(), eq(40_usize));
        }

        let sync_reply = app
            .feed_connection_bytes(
                &mut connection,
                &resp_command(&[b"DFLY", b"SYNC", &sync_id]),
            )
            .expect("DFLY SYNC should succeed");
        assert_that!(&sync_reply, eq(&vec![b"+OK\r\n".to_vec()]));

        let role_after_sync = app
            .feed_connection_bytes(&mut connection, &resp_command(&[b"ROLE"]))
            .expect("ROLE should succeed");
        let role_payload = std::str::from_utf8(&role_after_sync[0]).expect("ROLE is UTF-8");
        assert_that!(role_payload.contains("full_sync"), eq(true));

        let stable_reply = app
            .feed_connection_bytes(
                &mut connection,
                &resp_command(&[b"DFLY", b"STARTSTABLE", &sync_id]),
            )
            .expect("DFLY STARTSTABLE should succeed");
        assert_that!(&stable_reply, eq(&vec![b"+OK\r\n".to_vec()]));

        let role_after_stable = app
            .feed_connection_bytes(&mut connection, &resp_command(&[b"ROLE"]))
            .expect("ROLE should succeed");
        let role_payload = std::str::from_utf8(&role_after_stable[0]).expect("ROLE is UTF-8");
        assert_that!(role_payload.contains("stable_sync"), eq(true));
    }

    #[rstest]
    fn resp_dfly_sync_requires_all_flows_registered() {
        let mut app = ServerApp::new(RuntimeConfig::default());
        let mut connection = ServerApp::new_connection(ClientProtocol::Resp);

        let handshake = app
            .feed_connection_bytes(
                &mut connection,
                &resp_command(&[b"REPLCONF", b"CAPA", b"dragonfly"]),
            )
            .expect("REPLCONF CAPA dragonfly should succeed");
        let sync_id = extract_sync_id_from_capa_reply(&handshake[0]).into_bytes();
        let master_id = app.replication.master_replid().as_bytes().to_vec();

        let flow_reply = app
            .feed_connection_bytes(
                &mut connection,
                &resp_command(&[b"DFLY", b"FLOW", &master_id, &sync_id, b"0"]),
            )
            .expect("first DFLY FLOW should succeed");
        let (sync_type, _) = extract_dfly_flow_reply(&flow_reply[0]);
        assert_that!(sync_type.as_str(), eq("FULL"));

        let sync_reply = app
            .feed_connection_bytes(
                &mut connection,
                &resp_command(&[b"DFLY", b"SYNC", &sync_id]),
            )
            .expect("DFLY SYNC should parse");
        assert_that!(&sync_reply, eq(&vec![b"-ERR invalid state\r\n".to_vec()]));
    }

    #[rstest]
    fn resp_dfly_replicaoffset_reports_offsets_for_all_shards() {
        let mut app = ServerApp::new(RuntimeConfig::default());
        let mut connection = ServerApp::new_connection(ClientProtocol::Resp);

        let _ = app
            .feed_connection_bytes(&mut connection, &resp_command(&[b"SET", b"ro:key", b"v1"]))
            .expect("first SET should succeed");
        let _ = app
            .feed_connection_bytes(&mut connection, &resp_command(&[b"SET", b"ro:key", b"v2"]))
            .expect("second SET should succeed");

        let reply = app
            .feed_connection_bytes(&mut connection, &resp_command(&[b"DFLY", b"REPLICAOFFSET"]))
            .expect("DFLY REPLICAOFFSET should execute");
        let expected_entry = format!(":{}\r\n", app.replication.replication_offset());
        let expected = format!(
            "*{}\r\n{}",
            app.config.shard_count.get(),
            expected_entry.repeat(usize::from(app.config.shard_count.get()))
        )
        .into_bytes();
        assert_that!(&reply, eq(&vec![expected]));
    }

    #[rstest]
    fn resp_dfly_flow_rejects_after_session_leaves_preparation() {
        let mut app = ServerApp::new(RuntimeConfig::default());
        let mut connection = ServerApp::new_connection(ClientProtocol::Resp);

        let handshake = app
            .feed_connection_bytes(
                &mut connection,
                &resp_command(&[b"REPLCONF", b"CAPA", b"dragonfly"]),
            )
            .expect("REPLCONF CAPA dragonfly should succeed");
        let sync_id = extract_sync_id_from_capa_reply(&handshake[0]).into_bytes();
        let master_id = app.replication.master_replid().as_bytes().to_vec();

        for flow_id in 0..usize::from(app.config.shard_count.get()) {
            let flow_id_text = flow_id.to_string().into_bytes();
            let _ = app
                .feed_connection_bytes(
                    &mut connection,
                    &resp_command(&[b"DFLY", b"FLOW", &master_id, &sync_id, &flow_id_text]),
                )
                .expect("DFLY FLOW should succeed");
        }
        let _ = app
            .feed_connection_bytes(
                &mut connection,
                &resp_command(&[b"DFLY", b"SYNC", &sync_id]),
            )
            .expect("DFLY SYNC should succeed");

        let replay_flow = app
            .feed_connection_bytes(
                &mut connection,
                &resp_command(&[b"DFLY", b"FLOW", &master_id, &sync_id, b"0"]),
            )
            .expect("DFLY FLOW should parse");
        assert_that!(&replay_flow, eq(&vec![b"-ERR invalid state\r\n".to_vec()]));
    }

    #[rstest]
    fn resp_dfly_flow_accepts_master_lsn_vector_argument() {
        let mut app = ServerApp::new(RuntimeConfig::default());
        let mut connection = ServerApp::new_connection(ClientProtocol::Resp);

        let handshake = app
            .feed_connection_bytes(
                &mut connection,
                &resp_command(&[b"REPLCONF", b"CAPA", b"dragonfly"]),
            )
            .expect("REPLCONF CAPA dragonfly should succeed");
        let sync_id = extract_sync_id_from_capa_reply(&handshake[0]).into_bytes();
        let master_id = app.replication.master_replid().as_bytes().to_vec();

        let _ = app
            .feed_connection_bytes(
                &mut connection,
                &resp_command(&[b"SET", b"vector:key", b"vector:value"]),
            )
            .expect("SET should succeed");

        let lsn_vec = vec!["0"; usize::from(app.config.shard_count.get())]
            .join("-")
            .into_bytes();
        let flow_reply = app
            .feed_connection_bytes(
                &mut connection,
                &resp_command(&[
                    b"DFLY",
                    b"FLOW",
                    &master_id,
                    &sync_id,
                    b"1",
                    b"LASTMASTER",
                    &lsn_vec,
                ]),
            )
            .expect("DFLY FLOW should succeed");
        let (sync_type, _) = extract_dfly_flow_reply(&flow_reply[0]);
        assert_that!(sync_type.as_str(), eq("PARTIAL"));
    }

    #[rstest]
    fn resp_dfly_flow_rejects_invalid_lastmaster_marker() {
        let mut app = ServerApp::new(RuntimeConfig::default());
        let mut connection = ServerApp::new_connection(ClientProtocol::Resp);

        let handshake = app
            .feed_connection_bytes(
                &mut connection,
                &resp_command(&[b"REPLCONF", b"CAPA", b"dragonfly"]),
            )
            .expect("REPLCONF CAPA dragonfly should succeed");
        let sync_id = extract_sync_id_from_capa_reply(&handshake[0]).into_bytes();
        let master_id = app.replication.master_replid().as_bytes().to_vec();
        let lsn_vec = vec!["0"; usize::from(app.config.shard_count.get())]
            .join("-")
            .into_bytes();

        let flow_reply = app
            .feed_connection_bytes(
                &mut connection,
                &resp_command(&[
                    b"DFLY",
                    b"FLOW",
                    &master_id,
                    &sync_id,
                    b"0",
                    b"BADMARKER",
                    &lsn_vec,
                ]),
            )
            .expect("DFLY FLOW should parse");
        assert_that!(&flow_reply, eq(&vec![b"-ERR syntax error\r\n".to_vec()]));
    }

    #[rstest]
    fn resp_dfly_flow_rejects_duplicate_flow_registration() {
        let mut app = ServerApp::new(RuntimeConfig::default());
        let mut connection = ServerApp::new_connection(ClientProtocol::Resp);

        let handshake = app
            .feed_connection_bytes(
                &mut connection,
                &resp_command(&[b"REPLCONF", b"CAPA", b"dragonfly"]),
            )
            .expect("REPLCONF CAPA dragonfly should succeed");
        let sync_id = extract_sync_id_from_capa_reply(&handshake[0]).into_bytes();
        let master_id = app.replication.master_replid().as_bytes().to_vec();
        let first = app
            .feed_connection_bytes(
                &mut connection,
                &resp_command(&[b"DFLY", b"FLOW", &master_id, &sync_id, b"0"]),
            )
            .expect("first DFLY FLOW should succeed");
        let (sync_type, _) = extract_dfly_flow_reply(&first[0]);
        assert_that!(sync_type.as_str(), eq("FULL"));

        let second = app
            .feed_connection_bytes(
                &mut connection,
                &resp_command(&[b"DFLY", b"FLOW", &master_id, &sync_id, b"0"]),
            )
            .expect("second DFLY FLOW should parse");
        assert_that!(&second, eq(&vec![b"-ERR invalid state\r\n".to_vec()]));
    }

    #[rstest]
    fn resp_replconf_registration_and_psync_update_role_replica_state() {
        let mut app = ServerApp::new(RuntimeConfig::default());
        let mut connection = ServerApp::new_connection(ClientProtocol::Resp);

        let register = app
            .feed_connection_bytes(
                &mut connection,
                &resp_command(&[
                    b"REPLCONF",
                    b"LISTENING-PORT",
                    b"7001",
                    b"IP-ADDRESS",
                    b"10.0.0.2",
                ]),
            )
            .expect("REPLCONF LISTENING-PORT/IP-ADDRESS should succeed");
        assert_that!(&register, eq(&vec![b"+OK\r\n".to_vec()]));

        let role_after_register = app
            .feed_connection_bytes(&mut connection, &resp_command(&[b"ROLE"]))
            .expect("ROLE should succeed");
        let role_payload = std::str::from_utf8(&role_after_register[0]).expect("ROLE is UTF-8");
        assert_that!(role_payload.contains("10.0.0.2"), eq(true));
        assert_that!(role_payload.contains("7001"), eq(true));
        assert_that!(role_payload.contains("preparation"), eq(true));

        let _ = app
            .feed_connection_bytes(&mut connection, &resp_command(&[b"PSYNC", b"?", b"-1"]))
            .expect("PSYNC should succeed");

        let role_after_fullsync = app
            .feed_connection_bytes(&mut connection, &resp_command(&[b"ROLE"]))
            .expect("ROLE should succeed after PSYNC");
        let role_payload = std::str::from_utf8(&role_after_fullsync[0]).expect("ROLE is UTF-8");
        assert_that!(role_payload.contains("full_sync"), eq(true));

        let ack_reply = app
            .feed_connection_bytes(&mut connection, &resp_command(&[b"REPLCONF", b"ACK", b"0"]))
            .expect("REPLCONF ACK should parse");
        assert_that!(&ack_reply, eq(&Vec::<Vec<u8>>::new()));

        let role_after_ack = app
            .feed_connection_bytes(&mut connection, &resp_command(&[b"ROLE"]))
            .expect("ROLE should succeed after ACK");
        let role_payload = std::str::from_utf8(&role_after_ack[0]).expect("ROLE is UTF-8");
        assert_that!(role_payload.contains("stable_sync"), eq(true));

        let info = app
            .feed_connection_bytes(&mut connection, &resp_command(&[b"INFO", b"REPLICATION"]))
            .expect("INFO REPLICATION should succeed");
        let body = decode_resp_bulk_payload(&info[0]);
        assert_that!(body.contains("connected_slaves:1\r\n"), eq(true));
    }

    #[rstest]
    fn resp_replconf_endpoint_identity_update_replaces_stale_endpoint_row() {
        let mut app = ServerApp::new(RuntimeConfig::default());
        let mut replica = ServerApp::new_connection(ClientProtocol::Resp);
        let mut client = ServerApp::new_connection(ClientProtocol::Resp);

        let _ = app
            .feed_connection_bytes(
                &mut replica,
                &resp_command(&[b"REPLCONF", b"LISTENING-PORT", b"7001"]),
            )
            .expect("replica REPLCONF LISTENING-PORT should succeed");
        let _ = app
            .feed_connection_bytes(
                &mut client,
                &resp_command(&[b"SET", b"replconf:identity:key", b"value"]),
            )
            .expect("SET should succeed");
        let _ = app
            .feed_connection_bytes(&mut replica, &resp_command(&[b"REPLCONF", b"ACK", b"1"]))
            .expect("replica ACK should parse");
        assert_that!(app.replication.last_acked_lsn(), eq(1_u64));

        let _ = app
            .feed_connection_bytes(
                &mut replica,
                &resp_command(&[b"REPLCONF", b"IP-ADDRESS", b"10.0.0.8"]),
            )
            .expect("replica REPLCONF IP-ADDRESS should succeed");

        let info = app
            .feed_connection_bytes(&mut client, &resp_command(&[b"INFO", b"REPLICATION"]))
            .expect("INFO REPLICATION should succeed");
        let body = decode_resp_bulk_payload(&info[0]);
        assert_that!(body.contains("connected_slaves:1\r\n"), eq(true));
        assert_that!(
            body.contains("slave0:ip=10.0.0.8,port=7001,state=preparation,lag=0\r\n"),
            eq(true)
        );
        assert_that!(body.contains("127.0.0.1"), eq(false));

        let wait = app
            .feed_connection_bytes(&mut client, &resp_command(&[b"WAIT", b"1", b"0"]))
            .expect("WAIT should execute");
        assert_that!(&wait, eq(&vec![b":0\r\n".to_vec()]));
    }

    #[rstest]
    fn disconnect_connection_unregisters_replica_endpoint_and_ack_progress() {
        let mut app = ServerApp::new(RuntimeConfig::default());
        let mut replica = ServerApp::new_connection(ClientProtocol::Resp);
        let mut client = ServerApp::new_connection(ClientProtocol::Resp);

        let _ = app
            .feed_connection_bytes(
                &mut replica,
                &resp_command(&[b"REPLCONF", b"LISTENING-PORT", b"7001"]),
            )
            .expect("replica REPLCONF LISTENING-PORT should succeed");
        let _ = app
            .feed_connection_bytes(
                &mut client,
                &resp_command(&[b"SET", b"disconnect:replica:key", b"value"]),
            )
            .expect("SET should succeed");
        let _ = app
            .feed_connection_bytes(&mut replica, &resp_command(&[b"REPLCONF", b"ACK", b"1"]))
            .expect("replica ACK should parse");
        assert_that!(app.replication.last_acked_lsn(), eq(1_u64));

        app.disconnect_connection(&mut replica);

        let info = app
            .feed_connection_bytes(&mut client, &resp_command(&[b"INFO", b"REPLICATION"]))
            .expect("INFO REPLICATION should succeed");
        let body = decode_resp_bulk_payload(&info[0]);
        assert_that!(body.contains("connected_slaves:0\r\n"), eq(true));
        assert_that!(body.contains("last_ack_lsn:0\r\n"), eq(true));

        let wait = app
            .feed_connection_bytes(&mut client, &resp_command(&[b"WAIT", b"1", b"0"]))
            .expect("WAIT should execute");
        assert_that!(&wait, eq(&vec![b":0\r\n".to_vec()]));
    }

    #[rstest]
    fn resp_psync_with_unknown_replid_returns_full_resync_header() {
        let mut app = ServerApp::new(RuntimeConfig::default());
        let mut connection = ServerApp::new_connection(ClientProtocol::Resp);

        let reply = app
            .feed_connection_bytes(&mut connection, &resp_command(&[b"PSYNC", b"?", b"-1"]))
            .expect("PSYNC should succeed");
        let expected =
            format!("+FULLRESYNC {} 0\r\n", app.replication.master_replid()).into_bytes();
        assert_that!(&reply, eq(&vec![expected]));
    }

    #[rstest]
    fn resp_psync_returns_continue_when_offset_is_available() {
        let mut app = ServerApp::new(RuntimeConfig::default());
        let mut connection = ServerApp::new_connection(ClientProtocol::Resp);

        let _ = app
            .feed_connection_bytes(
                &mut connection,
                &resp_command(&[b"SET", b"psync:key", b"psync:value"]),
            )
            .expect("SET should succeed");

        let replid = app.replication.master_replid().as_bytes().to_vec();
        let reply = app
            .feed_connection_bytes(&mut connection, &resp_command(&[b"PSYNC", &replid, b"0"]))
            .expect("PSYNC should succeed");
        assert_that!(&reply, eq(&vec![b"+CONTINUE\r\n".to_vec()]));
    }

    #[rstest]
    fn resp_psync_falls_back_to_full_resync_when_backlog_is_stale() {
        let mut app = ServerApp::new(RuntimeConfig::default());
        app.replication.journal = InMemoryJournal::with_backlog(1);
        let mut connection = ServerApp::new_connection(ClientProtocol::Resp);

        let _ = app
            .feed_connection_bytes(
                &mut connection,
                &resp_command(&[b"SET", b"stale:key", b"v1"]),
            )
            .expect("first SET should succeed");
        let _ = app
            .feed_connection_bytes(
                &mut connection,
                &resp_command(&[b"SET", b"stale:key", b"v2"]),
            )
            .expect("second SET should succeed");

        let replid = app.replication.master_replid().as_bytes().to_vec();
        let reply = app
            .feed_connection_bytes(&mut connection, &resp_command(&[b"PSYNC", &replid, b"0"]))
            .expect("PSYNC should succeed");
        let expected =
            format!("+FULLRESYNC {} 2\r\n", app.replication.master_replid()).into_bytes();
        assert_that!(&reply, eq(&vec![expected]));
    }

    #[rstest]
    fn server_snapshot_roundtrip_restores_data_across_databases() {
        let mut source = ServerApp::new(RuntimeConfig::default());
        let mut source_connection = ServerApp::new_connection(ClientProtocol::Resp);

        let _ = source
            .feed_connection_bytes(
                &mut source_connection,
                b"*3\r\n$3\r\nSET\r\n$6\r\nshared\r\n$3\r\ndb0\r\n",
            )
            .expect("db0 SET should succeed");
        let _ = source
            .feed_connection_bytes(&mut source_connection, b"*2\r\n$6\r\nSELECT\r\n$1\r\n2\r\n")
            .expect("SELECT 2 should succeed");
        let _ = source
            .feed_connection_bytes(
                &mut source_connection,
                b"*3\r\n$3\r\nSET\r\n$6\r\nshared\r\n$3\r\ndb2\r\n",
            )
            .expect("db2 SET should succeed");

        let snapshot_bytes = source
            .create_snapshot_bytes()
            .expect("snapshot export should succeed");

        let mut restored = ServerApp::new(RuntimeConfig::default());
        restored
            .load_snapshot_bytes(&snapshot_bytes)
            .expect("snapshot import should succeed");

        let mut restored_connection = ServerApp::new_connection(ClientProtocol::Resp);
        let db0_get = restored
            .feed_connection_bytes(
                &mut restored_connection,
                b"*2\r\n$3\r\nGET\r\n$6\r\nshared\r\n",
            )
            .expect("db0 GET should succeed");
        assert_that!(&db0_get, eq(&vec![b"$3\r\ndb0\r\n".to_vec()]));

        let _ = restored
            .feed_connection_bytes(
                &mut restored_connection,
                b"*2\r\n$6\r\nSELECT\r\n$1\r\n2\r\n",
            )
            .expect("SELECT 2 should succeed after restore");
        let db2_get = restored
            .feed_connection_bytes(
                &mut restored_connection,
                b"*2\r\n$3\r\nGET\r\n$6\r\nshared\r\n",
            )
            .expect("db2 GET should succeed");
        assert_that!(&db2_get, eq(&vec![b"$3\r\ndb2\r\n".to_vec()]));
    }

    #[rstest]
    fn server_snapshot_load_rejects_malformed_payload() {
        let mut app = ServerApp::new(RuntimeConfig::default());
        let error = app
            .load_snapshot_bytes(b"not-a-snapshot")
            .expect_err("invalid bytes should fail");

        let DflyError::Protocol(message) = error else {
            panic!("expected protocol error");
        };
        assert_that!(message.contains("snapshot payload error"), eq(true));
    }

    #[rstest]
    fn resp_cluster_keyslot_returns_redis_slot() {
        let config = RuntimeConfig {
            cluster_mode: ClusterMode::Emulated,
            ..RuntimeConfig::default()
        };
        let mut app = ServerApp::new(config);
        let mut connection = ServerApp::new_connection(ClientProtocol::Resp);

        let key = b"user:{42}:meta";
        let command = resp_command(&[b"CLUSTER", b"KEYSLOT", key]);
        let reply = app
            .feed_connection_bytes(&mut connection, &command)
            .expect("CLUSTER KEYSLOT should execute");
        let expected = vec![format!(":{}\r\n", key_slot(key)).into_bytes()];
        assert_that!(&reply, eq(&expected));
    }

    #[rstest]
    fn resp_cluster_myid_returns_local_node_id() {
        let config = RuntimeConfig {
            cluster_mode: ClusterMode::Emulated,
            ..RuntimeConfig::default()
        };
        let mut app = ServerApp::new(config);
        let mut connection = ServerApp::new_connection(ClientProtocol::Resp);

        let reply = app
            .feed_connection_bytes(&mut connection, &resp_command(&[b"CLUSTER", b"MYID"]))
            .expect("CLUSTER MYID should execute");
        assert_that!(reply.len(), eq(1_usize));
        let body = decode_resp_bulk_payload(&reply[0]);
        assert_that!(body.as_str(), eq(app.cluster.node_id.as_str()));
    }

    #[rstest]
    fn resp_cluster_commands_are_rejected_when_cluster_mode_is_disabled() {
        let mut app = ServerApp::new(RuntimeConfig::default());
        let mut connection = ServerApp::new_connection(ClientProtocol::Resp);

        let reply = app
            .feed_connection_bytes(&mut connection, &resp_command(&[b"CLUSTER", b"INFO"]))
            .expect("CLUSTER INFO should parse");
        assert_that!(
            &reply,
            eq(&vec![
                b"-ERR Cluster is disabled. Use --cluster_mode=yes to enable.\r\n".to_vec()
            ])
        );
    }

    #[rstest]
    fn resp_readonly_returns_ok() {
        let mut app = ServerApp::new(RuntimeConfig::default());
        let mut connection = ServerApp::new_connection(ClientProtocol::Resp);

        let reply = app
            .feed_connection_bytes(&mut connection, &resp_command(&[b"READONLY"]))
            .expect("READONLY should execute");
        assert_that!(&reply, eq(&vec![b"+OK\r\n".to_vec()]));
    }

    #[rstest]
    fn resp_readwrite_requires_cluster_emulated_mode() {
        let mut disabled = ServerApp::new(RuntimeConfig::default());
        let mut disabled_connection = ServerApp::new_connection(ClientProtocol::Resp);
        let disabled_reply = disabled
            .feed_connection_bytes(&mut disabled_connection, &resp_command(&[b"READWRITE"]))
            .expect("READWRITE should parse");
        assert_that!(
            &disabled_reply,
            eq(&vec![
                b"-ERR Cluster is disabled. Use --cluster_mode=yes to enable.\r\n".to_vec()
            ])
        );

        let config = RuntimeConfig {
            cluster_mode: ClusterMode::Emulated,
            ..RuntimeConfig::default()
        };
        let mut emulated = ServerApp::new(config);
        let mut emulated_connection = ServerApp::new_connection(ClientProtocol::Resp);
        let emulated_reply = emulated
            .feed_connection_bytes(&mut emulated_connection, &resp_command(&[b"READWRITE"]))
            .expect("READWRITE should execute in emulated mode");
        assert_that!(&emulated_reply, eq(&vec![b"+OK\r\n".to_vec()]));
    }

    #[rstest]
    fn resp_cluster_help_returns_help_entries() {
        let config = RuntimeConfig {
            cluster_mode: ClusterMode::Emulated,
            ..RuntimeConfig::default()
        };
        let mut app = ServerApp::new(config);
        let mut connection = ServerApp::new_connection(ClientProtocol::Resp);

        let reply = app
            .feed_connection_bytes(&mut connection, &resp_command(&[b"CLUSTER", b"HELP"]))
            .expect("CLUSTER HELP should execute");
        assert_that!(reply.len(), eq(1_usize));
        let payload = std::str::from_utf8(&reply[0]).expect("payload must be UTF-8");
        assert_that!(payload.starts_with("*11\r\n"), eq(true));
        assert_that!(payload.contains("$5\r\nSLOTS\r\n"), eq(true));
        assert_that!(payload.contains("$5\r\nNODES\r\n"), eq(true));
        assert_that!(payload.contains("$4\r\nINFO\r\n"), eq(true));
        assert_that!(payload.contains("$4\r\nHELP\r\n"), eq(true));
    }

    #[rstest]
    fn resp_cluster_shards_reports_single_master_descriptor() {
        let config = RuntimeConfig {
            cluster_mode: ClusterMode::Emulated,
            ..RuntimeConfig::default()
        };
        let mut app = ServerApp::new(config);
        app.cluster.set_owned_ranges(vec![
            SlotRange { start: 0, end: 99 },
            SlotRange {
                start: 200,
                end: 300,
            },
        ]);
        let mut connection = ServerApp::new_connection(ClientProtocol::Resp);

        let reply = app
            .feed_connection_bytes(&mut connection, &resp_command(&[b"CLUSTER", b"SHARDS"]))
            .expect("CLUSTER SHARDS should execute");
        assert_that!(reply.len(), eq(1_usize));

        let payload = std::str::from_utf8(&reply[0]).expect("payload must be UTF-8");
        assert_that!(payload.starts_with("*1\r\n*4\r\n$5\r\nslots\r\n"), eq(true));
        assert_that!(payload.contains(":0\r\n:99\r\n:200\r\n:300\r\n"), eq(true));
        assert_that!(payload.contains("$5\r\nnodes\r\n"), eq(true));
        assert_that!(payload.contains(&app.cluster.node_id), eq(true));
        assert_that!(payload.contains("$6\r\nmaster\r\n"), eq(true));
        assert_that!(payload.contains("$6\r\nonline\r\n"), eq(true));
    }

    #[rstest]
    fn resp_cluster_slots_returns_owned_ranges() {
        let config = RuntimeConfig {
            cluster_mode: ClusterMode::Emulated,
            ..RuntimeConfig::default()
        };
        let mut app = ServerApp::new(config);
        app.cluster.set_owned_ranges(vec![
            SlotRange { start: 0, end: 99 },
            SlotRange {
                start: 200,
                end: 300,
            },
        ]);
        let mut connection = ServerApp::new_connection(ClientProtocol::Resp);

        let command = resp_command(&[b"CLUSTER", b"SLOTS"]);
        let reply = app
            .feed_connection_bytes(&mut connection, &command)
            .expect("CLUSTER SLOTS should execute");
        let expected = vec![
            b"*2\r\n*3\r\n:0\r\n:99\r\n*2\r\n$9\r\n127.0.0.1\r\n:7000\r\n*3\r\n:200\r\n:300\r\n*2\r\n$9\r\n127.0.0.1\r\n:7000\r\n".to_vec(),
        ];
        assert_that!(&reply, eq(&expected));
    }

    #[rstest]
    fn resp_cluster_info_reports_slot_summary() {
        let config = RuntimeConfig {
            cluster_mode: ClusterMode::Emulated,
            ..RuntimeConfig::default()
        };
        let mut app = ServerApp::new(config);
        app.cluster.set_owned_ranges(vec![
            SlotRange { start: 0, end: 99 },
            SlotRange {
                start: 200,
                end: 300,
            },
        ]);
        let mut connection = ServerApp::new_connection(ClientProtocol::Resp);

        let reply = app
            .feed_connection_bytes(&mut connection, &resp_command(&[b"CLUSTER", b"INFO"]))
            .expect("CLUSTER INFO should execute");
        assert_that!(reply.len(), eq(1_usize));
        let body = decode_resp_bulk_payload(&reply[0]);
        assert_that!(body.contains("cluster_state:ok\r\n"), eq(true));
        assert_that!(body.contains("cluster_slots_assigned:201\r\n"), eq(true));
        assert_that!(body.contains("cluster_known_nodes:1\r\n"), eq(true));
    }

    #[rstest]
    fn resp_cluster_nodes_reports_myself_master_topology() {
        let config = RuntimeConfig {
            cluster_mode: ClusterMode::Emulated,
            ..RuntimeConfig::default()
        };
        let mut app = ServerApp::new(config);
        app.cluster.set_owned_ranges(vec![
            SlotRange { start: 0, end: 99 },
            SlotRange {
                start: 200,
                end: 300,
            },
        ]);
        let mut connection = ServerApp::new_connection(ClientProtocol::Resp);

        let reply = app
            .feed_connection_bytes(&mut connection, &resp_command(&[b"CLUSTER", b"NODES"]))
            .expect("CLUSTER NODES should execute");
        let body = decode_resp_bulk_payload(&reply[0]);
        assert_that!(body.contains("myself,master"), eq(true));
        assert_that!(body.contains(&app.cluster.node_id), eq(true));
        assert_that!(body.contains("0-99 200-300"), eq(true));
    }

    #[rstest]
    fn cluster_mode_rejects_crossslot_mset() {
        let config = RuntimeConfig {
            cluster_mode: ClusterMode::Emulated,
            ..RuntimeConfig::default()
        };
        let mut app = ServerApp::new(config);
        let mut connection = ServerApp::new_connection(ClientProtocol::Resp);

        let first_key = b"acct:{100}".to_vec();
        let mut second_key = b"acct:{200}".to_vec();
        let mut suffix = 0_u32;
        while key_slot(&second_key) == key_slot(&first_key) {
            suffix += 1;
            second_key = format!("acct:{{200}}:{suffix}").into_bytes();
        }

        let request = resp_command(&[b"MSET", &first_key, b"v1", &second_key, b"v2"]);
        let reply = app
            .feed_connection_bytes(&mut connection, &request)
            .expect("MSET should parse");
        assert_that!(
            &reply,
            eq(&vec![
                b"-ERR CROSSSLOT Keys in request don't hash to the same slot\r\n".to_vec()
            ])
        );
    }

    #[rstest]
    fn cluster_mode_redirects_unowned_single_key_command_with_moved() {
        let config = RuntimeConfig {
            cluster_mode: ClusterMode::Emulated,
            ..RuntimeConfig::default()
        };
        let mut app = ServerApp::new(config);
        let mut connection = ServerApp::new_connection(ClientProtocol::Resp);

        let key = b"user:{42}";
        let slot = key_slot(key);
        let owned = if slot == 0 {
            SlotRange { start: 1, end: 100 }
        } else {
            SlotRange {
                start: 0,
                end: slot - 1,
            }
        };
        app.cluster.set_owned_ranges(vec![owned]);

        let command = resp_command(&[b"GET", key]);
        let reply = app
            .feed_connection_bytes(&mut connection, &command)
            .expect("GET should parse");
        let expected = vec![format!("-MOVED {slot} 127.0.0.1:7000\r\n").into_bytes()];
        assert_that!(&reply, eq(&expected));
    }

    #[rstest]
    fn dfly_save_then_load_restores_snapshot_file() {
        let path = unique_test_snapshot_path("dfly-save-load");
        let path_bytes = path.to_string_lossy().as_bytes().to_vec();

        let mut source = ServerApp::new(RuntimeConfig::default());
        let mut source_conn = ServerApp::new_connection(ClientProtocol::Resp);
        let _ = source
            .feed_connection_bytes(
                &mut source_conn,
                b"*3\r\n$3\r\nSET\r\n$3\r\nfoo\r\n$3\r\nbar\r\n",
            )
            .expect("seed SET should succeed");

        let save = resp_command(&[b"DFLY", b"SAVE", &path_bytes]);
        let save_reply = source
            .feed_connection_bytes(&mut source_conn, &save)
            .expect("DFLY SAVE should execute");
        assert_that!(&save_reply, eq(&vec![b"+OK\r\n".to_vec()]));

        let mut restored = ServerApp::new(RuntimeConfig::default());
        let mut restored_conn = ServerApp::new_connection(ClientProtocol::Resp);
        let load = resp_command(&[b"DFLY", b"LOAD", &path_bytes]);
        let load_reply = restored
            .feed_connection_bytes(&mut restored_conn, &load)
            .expect("DFLY LOAD should execute");
        assert_that!(&load_reply, eq(&vec![b"+OK\r\n".to_vec()]));

        let get_reply = restored
            .feed_connection_bytes(&mut restored_conn, b"*2\r\n$3\r\nGET\r\n$3\r\nfoo\r\n")
            .expect("GET after LOAD should succeed");
        assert_that!(&get_reply, eq(&vec![b"$3\r\nbar\r\n".to_vec()]));

        let _ = std::fs::remove_file(path);
    }

    #[rstest]
    fn resp_watch_aborts_exec_when_dfly_load_creates_watched_key() {
        let path = unique_test_snapshot_path("watch-load-abort");
        let path_bytes = path.to_string_lossy().as_bytes().to_vec();

        let mut source = ServerApp::new(RuntimeConfig::default());
        let mut source_conn = ServerApp::new_connection(ClientProtocol::Resp);
        let _ = source
            .feed_connection_bytes(
                &mut source_conn,
                &resp_command(&[b"SET", b"load:watch:key", b"seed"]),
            )
            .expect("seed key should be created for snapshot");
        let _ = source
            .feed_connection_bytes(
                &mut source_conn,
                &resp_command(&[b"DFLY", b"SAVE", &path_bytes]),
            )
            .expect("DFLY SAVE should succeed");

        let mut app = ServerApp::new(RuntimeConfig::default());
        let mut watch_conn = ServerApp::new_connection(ClientProtocol::Resp);
        let mut admin_conn = ServerApp::new_connection(ClientProtocol::Resp);

        let _ = app
            .feed_connection_bytes(
                &mut watch_conn,
                &resp_command(&[b"WATCH", b"load:watch:key"]),
            )
            .expect("WATCH should succeed");
        let load_reply = app
            .feed_connection_bytes(
                &mut admin_conn,
                &resp_command(&[b"DFLY", b"LOAD", &path_bytes]),
            )
            .expect("DFLY LOAD should execute");
        assert_that!(&load_reply, eq(&vec![b"+OK\r\n".to_vec()]));

        let _ = app
            .feed_connection_bytes(&mut watch_conn, &resp_command(&[b"MULTI"]))
            .expect("MULTI should succeed");
        let _ = app
            .feed_connection_bytes(
                &mut watch_conn,
                &resp_command(&[b"SET", b"load:watch:key", b"mine"]),
            )
            .expect("SET should queue");

        let exec_reply = app
            .feed_connection_bytes(&mut watch_conn, &resp_command(&[b"EXEC"]))
            .expect("EXEC should parse");
        assert_that!(&exec_reply, eq(&vec![b"*-1\r\n".to_vec()]));

        let _ = std::fs::remove_file(path);
    }

    #[rstest]
    fn dfly_load_reports_missing_file_error() {
        let mut app = ServerApp::new(RuntimeConfig::default());
        let mut conn = ServerApp::new_connection(ClientProtocol::Resp);
        let path = unique_test_snapshot_path("dfly-missing");
        let path_bytes = path.to_string_lossy().as_bytes().to_vec();

        let load = resp_command(&[b"DFLY", b"LOAD", &path_bytes]);
        let reply = app
            .feed_connection_bytes(&mut conn, &load)
            .expect("DFLY LOAD should parse");
        assert_that!(
            reply[0].starts_with(b"-ERR DFLY LOAD failed: io error:"),
            eq(true)
        );
    }

    #[rstest]
    fn journal_replay_restores_state_in_fresh_server() {
        let mut source = ServerApp::new(RuntimeConfig::default());
        let mut source_connection = ServerApp::new_connection(ClientProtocol::Resp);

        let _ = source
            .feed_connection_bytes(
                &mut source_connection,
                b"*3\r\n$3\r\nSET\r\n$3\r\nfoo\r\n$3\r\nbar\r\n",
            )
            .expect("SET should succeed");
        let _ = source
            .feed_connection_bytes(&mut source_connection, b"*2\r\n$6\r\nSELECT\r\n$1\r\n1\r\n")
            .expect("SELECT should succeed");
        let _ = source
            .feed_connection_bytes(
                &mut source_connection,
                b"*3\r\n$3\r\nSET\r\n$3\r\nfoo\r\n$3\r\nbaz\r\n",
            )
            .expect("SET should succeed");

        let entries = source.replication.journal_entries();

        let mut restored = ServerApp::new(RuntimeConfig::default());
        for entry in entries {
            restored.replication.append_journal(entry);
        }
        let applied = restored
            .recover_from_replication_journal()
            .expect("journal replay should succeed");
        assert_that!(applied, eq(2_usize));

        let mut connection = ServerApp::new_connection(ClientProtocol::Resp);
        let db0 = restored
            .feed_connection_bytes(&mut connection, b"*2\r\n$3\r\nGET\r\n$3\r\nfoo\r\n")
            .expect("GET in db0 should succeed");
        assert_that!(&db0, eq(&vec![b"$3\r\nbar\r\n".to_vec()]));

        let _ = restored
            .feed_connection_bytes(&mut connection, b"*2\r\n$6\r\nSELECT\r\n$1\r\n1\r\n")
            .expect("SELECT should succeed");
        let db1 = restored
            .feed_connection_bytes(&mut connection, b"*2\r\n$3\r\nGET\r\n$3\r\nfoo\r\n")
            .expect("GET in db1 should succeed");
        assert_that!(&db1, eq(&vec![b"$3\r\nbaz\r\n".to_vec()]));
    }

    #[rstest]
    fn journal_replay_from_lsn_applies_only_suffix() {
        let mut source = ServerApp::new(RuntimeConfig::default());
        let mut source_connection = ServerApp::new_connection(ClientProtocol::Resp);

        let _ = source
            .feed_connection_bytes(
                &mut source_connection,
                b"*3\r\n$3\r\nSET\r\n$3\r\nfoo\r\n$3\r\nold\r\n",
            )
            .expect("first SET should succeed");
        let start_lsn = source.replication.journal_lsn();
        let _ = source
            .feed_connection_bytes(
                &mut source_connection,
                b"*3\r\n$3\r\nSET\r\n$3\r\nfoo\r\n$3\r\nnew\r\n",
            )
            .expect("second SET should succeed");

        let entries = source.replication.journal_entries();
        let mut restored = ServerApp::new(RuntimeConfig::default());
        for entry in entries {
            restored.replication.append_journal(entry);
        }

        let applied = restored
            .recover_from_replication_journal_from_lsn(start_lsn)
            .expect("journal suffix replay should succeed");
        assert_that!(applied, eq(1_usize));

        let mut connection = ServerApp::new_connection(ClientProtocol::Resp);
        let get_reply = restored
            .feed_connection_bytes(&mut connection, b"*2\r\n$3\r\nGET\r\n$3\r\nfoo\r\n")
            .expect("GET should succeed");
        assert_that!(&get_reply, eq(&vec![b"$3\r\nnew\r\n".to_vec()]));
    }

    #[rstest]
    fn journal_replay_from_lsn_rejects_stale_cursor() {
        let mut app = ServerApp::new(RuntimeConfig::default());
        app.replication.journal = InMemoryJournal::with_backlog(1);
        app.replication.append_journal(JournalEntry {
            txid: 1,
            db: 0,
            op: JournalOp::Command,
            payload: resp_command(&[b"SET", b"a", b"1"]),
        });
        app.replication.append_journal(JournalEntry {
            txid: 2,
            db: 0,
            op: JournalOp::Command,
            payload: resp_command(&[b"SET", b"a", b"2"]),
        });

        let error = app
            .recover_from_replication_journal_from_lsn(1)
            .expect_err("stale backlog cursor must fail");
        let DflyError::Protocol(message) = error else {
            panic!("expected protocol error");
        };
        assert_that!(message.contains("not available"), eq(true));
    }

    #[rstest]
    fn journal_replay_dispatches_runtime_for_single_key_command() {
        let mut app = ServerApp::new(RuntimeConfig::default());
        let key = b"journal:rt:set".to_vec();
        let shard = app.core.resolve_shard_for_key(&key);
        let payload = resp_command(&[b"SET", &key, b"value"]);
        app.replication.append_journal(JournalEntry {
            txid: 1,
            db: 0,
            op: JournalOp::Command,
            payload,
        });

        let applied = app
            .recover_from_replication_journal()
            .expect("journal replay should succeed");
        assert_that!(applied, eq(1_usize));
        assert_that!(
            app.runtime
                .wait_for_processed_count(shard, 1, Duration::from_millis(200))
                .expect("wait should succeed"),
            eq(true)
        );
        let runtime = app
            .runtime
            .drain_processed_for_shard(shard)
            .expect("drain should succeed");
        assert_that!(runtime.len(), eq(1_usize));
        assert_that!(&runtime[0].command.name, eq(&"SET".to_owned()));
    }

    #[rstest]
    fn journal_replay_dispatches_runtime_to_all_touched_shards_for_multikey_command() {
        let mut app = ServerApp::new(RuntimeConfig::default());
        let first_key = b"journal:rt:mset:1".to_vec();
        let first_shard = app.core.resolve_shard_for_key(&first_key);
        let mut second_key = b"journal:rt:mset:2".to_vec();
        let mut second_shard = app.core.resolve_shard_for_key(&second_key);
        let mut suffix = 0_u32;
        while second_shard == first_shard {
            suffix = suffix.saturating_add(1);
            second_key = format!("journal:rt:mset:2:{suffix}").into_bytes();
            second_shard = app.core.resolve_shard_for_key(&second_key);
        }

        let payload = resp_command(&[b"MSET", &first_key, b"a", &second_key, b"b"]);
        app.replication.append_journal(JournalEntry {
            txid: 1,
            db: 0,
            op: JournalOp::Command,
            payload,
        });

        let applied = app
            .recover_from_replication_journal()
            .expect("journal replay should succeed");
        assert_that!(applied, eq(1_usize));
        assert_that!(
            app.runtime
                .wait_for_processed_count(first_shard, 1, Duration::from_millis(200))
                .expect("wait should succeed"),
            eq(true)
        );
        assert_that!(
            app.runtime
                .wait_for_processed_count(second_shard, 1, Duration::from_millis(200))
                .expect("wait should succeed"),
            eq(true)
        );
        let first_runtime = app
            .runtime
            .drain_processed_for_shard(first_shard)
            .expect("drain should succeed");
        let second_runtime = app
            .runtime
            .drain_processed_for_shard(second_shard)
            .expect("drain should succeed");
        assert_that!(first_runtime.len(), eq(1_usize));
        assert_that!(second_runtime.len(), eq(1_usize));
        assert_that!(&first_runtime[0].command.name, eq(&"MSET".to_owned()));
        assert_that!(&second_runtime[0].command.name, eq(&"MSET".to_owned()));
    }

    #[rstest]
    fn journal_replay_rejects_malformed_payload() {
        let mut app = ServerApp::new(RuntimeConfig::default());
        app.replication.append_journal(JournalEntry {
            txid: 1,
            db: 0,
            op: JournalOp::Command,
            payload: b"not-resp".to_vec(),
        });

        let error = app
            .recover_from_replication_journal()
            .expect_err("invalid journal payload must fail");
        let DflyError::Protocol(message) = error else {
            panic!("expected protocol error");
        };
        assert_that!(message.contains("RESP"), eq(true));
    }

    #[rstest]
    fn journal_replay_skips_ping_and_lsn_entries() {
        let mut app = ServerApp::new(RuntimeConfig::default());
        app.replication.append_journal(JournalEntry {
            txid: 1,
            db: 0,
            op: JournalOp::Ping,
            payload: Vec::new(),
        });
        app.replication.append_journal(JournalEntry {
            txid: 2,
            db: 0,
            op: JournalOp::Lsn,
            payload: Vec::new(),
        });

        let applied = app
            .recover_from_replication_journal()
            .expect("non-command entries should be skipped");
        assert_that!(applied, eq(0_usize));
    }

    fn resp_command(parts: &[&[u8]]) -> Vec<u8> {
        let mut payload = format!("*{}\r\n", parts.len()).into_bytes();
        for part in parts {
            payload.extend_from_slice(format!("${}\r\n", part.len()).as_bytes());
            payload.extend_from_slice(part);
            payload.extend_from_slice(b"\r\n");
        }
        payload
    }

    fn decode_resp_bulk_payload(frame: &[u8]) -> String {
        assert_that!(frame.first(), eq(Some(&b'$')));

        let Some(header_end) = frame.windows(2).position(|window| window == b"\r\n") else {
            panic!("RESP bulk string must contain a header terminator");
        };
        let header = std::str::from_utf8(&frame[1..header_end]).expect("header must be UTF-8");
        let payload_len = header
            .parse::<usize>()
            .expect("header must encode bulk payload length");

        let payload_start = header_end + 2;
        let payload_end = payload_start + payload_len;
        std::str::from_utf8(&frame[payload_start..payload_end])
            .expect("payload must be UTF-8")
            .to_owned()
    }

    fn parse_resp_integer(frame: &[u8]) -> i64 {
        assert_that!(frame.first(), eq(Some(&b':')));
        assert_that!(frame.ends_with(b"\r\n"), eq(true));

        let number = std::str::from_utf8(&frame[1..frame.len() - 2])
            .expect("RESP integer payload must be UTF-8");
        number
            .parse::<i64>()
            .expect("RESP integer payload must parse")
    }

    fn extract_sync_id_from_capa_reply(frame: &[u8]) -> String {
        let text = std::str::from_utf8(frame).expect("payload must be UTF-8");
        let lines = text.split("\r\n").collect::<Vec<_>>();
        let Some(sync_id) = lines.get(4) else {
            panic!("REPLCONF CAPA reply must contain sync id as second bulk string");
        };
        (*sync_id).to_owned()
    }

    fn extract_dfly_flow_reply(frame: &[u8]) -> (String, String) {
        let text = std::str::from_utf8(frame).expect("payload must be UTF-8");
        let lines = text.split("\r\n").collect::<Vec<_>>();
        let Some(sync_type) = lines.get(1).and_then(|line| line.strip_prefix('+')) else {
            panic!("DFLY FLOW reply must contain sync type as first simple string");
        };
        let Some(eof_token) = lines.get(2).and_then(|line| line.strip_prefix('+')) else {
            panic!("DFLY FLOW reply must contain eof token as second simple string");
        };
        (sync_type.to_owned(), eof_token.to_owned())
    }

    fn unique_test_snapshot_path(tag: &str) -> PathBuf {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_or(0_u128, |duration| duration.as_nanos());
        std::env::temp_dir().join(format!("dragonfly-rs-{tag}-{nanos}.snapshot"))
    }
}
