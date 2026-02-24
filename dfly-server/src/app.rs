//! Process composition root for `dfly-server`.

use dfly_cluster::ClusterModule;
use dfly_common::config::RuntimeConfig;
use dfly_common::error::DflyResult;
use dfly_common::ids::TxId;
use dfly_core::CoreModule;
use dfly_core::command::{CommandFrame, CommandReply};
use dfly_facade::FacadeModule;
use dfly_facade::connection::{ConnectionContext, ConnectionState};
use dfly_facade::protocol::ClientProtocol;
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
            let reply = self.core.execute_in_db(db, &frame);
            self.maybe_append_journal_for_command(None, db, &frame, &reply);

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
                let reply = self.core.execute_in_db(db, &queued);
                self.maybe_append_journal_for_command(Some(plan.txid), db, &queued, &reply);
                reply
            })
            .collect::<Vec<_>>();
        encode_resp_array(&replies)
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
        (_, CommandReply::Null) => b"END\r\n".to_vec(),
        (_, CommandReply::Error(message)) => format!("ERROR {message}\r\n").into_bytes(),
    }
}

/// Encodes `EXEC` result as one RESP array.
fn encode_resp_array(replies: &[CommandReply]) -> Vec<u8> {
    let mut output = format!("*{}\r\n", replies.len()).into_bytes();
    for reply in replies {
        output.extend_from_slice(&reply.to_resp_bytes());
    }
    output
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

    println!("{}", app.startup_summary());
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::ServerApp;
    use dfly_common::config::RuntimeConfig;
    use dfly_facade::protocol::ClientProtocol;
    use dfly_replication::journal::JournalOp;
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
}
