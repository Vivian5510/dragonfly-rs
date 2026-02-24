//! Process composition root for `dfly-server`.

use dfly_cluster::ClusterModule;
use dfly_cluster::slot::key_slot;
use dfly_common::config::{ClusterMode, RuntimeConfig};
use dfly_common::error::DflyError;
use dfly_common::error::DflyResult;
use dfly_common::ids::TxId;
use dfly_core::CoreModule;
use dfly_core::command::{CommandFrame, CommandReply};
use dfly_facade::FacadeModule;
use dfly_facade::connection::{ConnectionContext, ConnectionState};
use dfly_facade::protocol::{ClientProtocol, ParseStatus, parse_next_command};
use dfly_replication::ReplicationModule;
use dfly_replication::journal::{JournalEntry, JournalOp};
use dfly_search::SearchModule;
use dfly_storage::StorageModule;
use dfly_tiering::TieringModule;
use dfly_transaction::TransactionModule;
use dfly_transaction::plan::{TransactionHop, TransactionMode, TransactionPlan};
use dfly_transaction::scheduler::TransactionScheduler;
use dfly_transaction::session::TransactionSession;

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
        let entries = self.replication.journal_entries().to_vec();
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
                    let queued = connection.transaction.queue_command(frame);
                    if queued {
                        responses
                            .push(CommandReply::SimpleString("QUEUED".to_owned()).to_resp_bytes());
                    } else {
                        responses.push(
                            CommandReply::Error("transaction queue is unavailable".to_owned())
                                .to_resp_bytes(),
                        );
                    }
                    continue;
                }
            }

            let db = connection.parser.context.db_index;
            let reply = self.execute_user_command(db, &frame, None);

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
        if frame.args.len() != 1 {
            return Some(wrong_arity("SELECT"));
        }
        if connection.transaction.in_multi() {
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
        if !frame.args.is_empty() {
            return wrong_arity("MULTI");
        }
        if connection.transaction.begin_multi() {
            return CommandReply::SimpleString("OK".to_owned()).to_resp_bytes();
        }
        CommandReply::Error("MULTI calls can not be nested".to_owned()).to_resp_bytes()
    }

    fn handle_discard(connection: &mut ServerConnection, frame: &CommandFrame) -> Vec<u8> {
        if !frame.args.is_empty() {
            return wrong_arity("DISCARD");
        }
        if connection.transaction.discard() {
            return CommandReply::SimpleString("OK".to_owned()).to_resp_bytes();
        }
        CommandReply::Error("DISCARD without MULTI".to_owned()).to_resp_bytes()
    }

    fn handle_exec(&mut self, connection: &mut ServerConnection, frame: &CommandFrame) -> Vec<u8> {
        if !frame.args.is_empty() {
            return wrong_arity("EXEC");
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

        let plan = self.build_exec_plan(&queued_commands);
        if let Err(error) = self.transaction.scheduler.schedule(&plan) {
            return CommandReply::Error(format!("transaction scheduling failed: {error}"))
                .to_resp_bytes();
        }

        let replies = queued_commands
            .into_iter()
            .map(|queued| {
                let db = connection.parser.context.db_index;
                self.execute_user_command(db, &queued, Some(plan.txid))
            })
            .collect::<Vec<_>>();
        CommandReply::Array(replies).to_resp_bytes()
    }

    fn handle_watch(&mut self, connection: &mut ServerConnection, frame: &CommandFrame) -> Vec<u8> {
        if frame.args.is_empty() {
            return wrong_arity("WATCH");
        }
        if connection.transaction.in_multi() {
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
        if !frame.args.is_empty() {
            return wrong_arity("UNWATCH");
        }
        if connection.transaction.in_multi() {
            return CommandReply::Error("UNWATCH inside MULTI is not allowed".to_owned())
                .to_resp_bytes();
        }

        connection.transaction.unwatch();
        CommandReply::SimpleString("OK".to_owned()).to_resp_bytes()
    }

    fn build_exec_plan(&mut self, queued_commands: &[CommandFrame]) -> TransactionPlan {
        let txid = self.allocate_txid();

        // This learning-path plan keeps one command per hop to make scheduling boundaries
        // explicit. Later units can merge compatible commands into fewer coordinator hops.
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

    fn execute_user_command(
        &mut self,
        db: u16,
        frame: &CommandFrame,
        txid: Option<TxId>,
    ) -> CommandReply {
        let reply = self.execute_command_without_side_effects(db, frame);
        self.maybe_append_journal_for_command(txid, db, frame, &reply);
        reply
    }

    fn execute_command_without_side_effects(
        &mut self,
        db: u16,
        frame: &CommandFrame,
    ) -> CommandReply {
        match frame.name.as_str() {
            "CLUSTER" => Self::execute_cluster(frame),
            "MGET" => self.execute_mget(db, frame),
            "MSET" => self.execute_mset(db, frame),
            _ => self.core.execute_in_db(db, frame),
        }
    }

    fn execute_cluster(frame: &CommandFrame) -> CommandReply {
        let Some(subcommand_raw) = frame.args.first() else {
            return CommandReply::Error(
                "wrong number of arguments for 'CLUSTER' command".to_owned(),
            );
        };
        let Ok(subcommand) = std::str::from_utf8(subcommand_raw) else {
            return CommandReply::Error("CLUSTER subcommand must be valid UTF-8".to_owned());
        };

        match subcommand.to_ascii_uppercase().as_str() {
            "KEYSLOT" => Self::execute_cluster_keyslot(frame),
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

    fn execute_mget(&mut self, db: u16, frame: &CommandFrame) -> CommandReply {
        if frame.args.is_empty() {
            return CommandReply::Error("wrong number of arguments for 'MGET' command".to_owned());
        }
        if let Some(error_reply) =
            self.ensure_cluster_single_slot(frame.args.iter().map(Vec::as_slice))
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
        if let Some(error_reply) = self
            .ensure_cluster_single_slot(frame.args.chunks_exact(2).map(|pair| pair[0].as_slice()))
        {
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

    fn ensure_cluster_single_slot<'a, I>(&self, keys: I) -> Option<CommandReply>
    where
        I: IntoIterator<Item = &'a [u8]>,
    {
        if self.cluster.mode == ClusterMode::Disabled {
            return None;
        }

        let mut key_iter = keys.into_iter();
        let first_key = key_iter.next()?;
        let first_slot = key_slot(first_key);
        if key_iter.any(|key| key_slot(key) != first_slot) {
            return Some(CommandReply::Error(
                "CROSSSLOT Keys in request don't hash to the same slot".to_owned(),
            ));
        }
        None
    }

    fn replay_journal_entries(&mut self, entries: &[JournalEntry]) -> DflyResult<usize> {
        let mut applied = 0_usize;
        for entry in entries {
            if matches!(entry.op, JournalOp::Ping | JournalOp::Lsn) {
                continue;
            }

            let frame = parse_journal_command_frame(&entry.payload)?;
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
}

/// Builds standard wrong-arity RESP error for one command.
fn wrong_arity(command_name: &str) -> Vec<u8> {
    CommandReply::Error(format!(
        "wrong number of arguments for '{command_name}' command"
    ))
    .to_resp_bytes()
}

/// Returns journal op kind when the command mutated keyspace state.
fn journal_op_for_command(frame: &CommandFrame, reply: &CommandReply) -> Option<JournalOp> {
    match (frame.name.as_str(), reply) {
        ("SET", CommandReply::SimpleString(ok)) if ok == "OK" => Some(JournalOp::Command),
        ("MSET", CommandReply::SimpleString(ok)) if ok == "OK" => Some(JournalOp::Command),
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

fn expire_is_delete(frame: &CommandFrame) -> bool {
    frame
        .args
        .get(1)
        .and_then(|seconds| std::str::from_utf8(seconds).ok())
        .and_then(|seconds| seconds.parse::<i64>().ok())
        .is_some_and(|seconds| seconds <= 0)
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

    println!("{}", app.startup_summary());
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::ServerApp;
    use dfly_cluster::slot::key_slot;
    use dfly_common::config::{ClusterMode, RuntimeConfig};
    use dfly_common::error::DflyError;
    use dfly_facade::protocol::ClientProtocol;
    use dfly_replication::journal::{JournalEntry, JournalOp};
    use googletest::prelude::*;
    use rstest::rstest;

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
        let mut app = ServerApp::new(RuntimeConfig::default());
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

        let entries = source.replication.journal_entries().to_vec();

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
}
