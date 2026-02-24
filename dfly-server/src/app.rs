//! Process composition root for `dfly-server`.

use dfly_cluster::ClusterModule;
use dfly_common::config::RuntimeConfig;
use dfly_common::error::DflyResult;
use dfly_core::CoreModule;
use dfly_core::command::CommandFrame;
use dfly_facade::FacadeModule;
use dfly_facade::connection::{ConnectionContext, ConnectionState};
use dfly_facade::protocol::ClientProtocol;
use dfly_replication::ReplicationModule;
use dfly_search::SearchModule;
use dfly_storage::StorageModule;
use dfly_tiering::TieringModule;
use dfly_transaction::TransactionModule;

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
}

impl ServerApp {
    /// Creates a process composition from runtime config.
    #[must_use]
    pub fn new(config: RuntimeConfig) -> Self {
        let facade = FacadeModule::from_config(&config);
        let core = CoreModule::new(config.shard_count);
        let transaction = TransactionModule::new();
        let storage = StorageModule::new();
        let replication = ReplicationModule::new(false);
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
            let reply = self.core.execute(&frame);

            // Unit 1 keeps protocol-specific encoding at the facade edge. This mirrors Dragonfly
            // where execution result is translated back to client protocol right before writeback.
            let encoded =
                encode_reply_for_protocol(connection.parser.context.protocol, &frame, reply);
            responses.push(encoded);
        }

        Ok(responses)
    }
}

/// Per-client state stored by the server.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ServerConnection {
    /// Incremental parser and connection metadata.
    pub parser: ConnectionState,
}

/// Encodes a canonical reply according to the target client protocol.
fn encode_reply_for_protocol(
    protocol: ClientProtocol,
    frame: &CommandFrame,
    reply: dfly_core::command::CommandReply,
) -> Vec<u8> {
    match protocol {
        ClientProtocol::Resp => reply.to_resp_bytes(),
        ClientProtocol::Memcache => encode_memcache_reply(frame, reply),
    }
}

/// Encodes replies in memcache text protocol style for supported command subset.
fn encode_memcache_reply(frame: &CommandFrame, reply: dfly_core::command::CommandReply) -> Vec<u8> {
    match (frame.name.as_str(), reply) {
        ("SET", dfly_core::command::CommandReply::SimpleString(ok)) if ok == "OK" => {
            b"STORED\r\n".to_vec()
        }
        ("GET", dfly_core::command::CommandReply::BulkString(payload)) => {
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
        (_, dfly_core::command::CommandReply::SimpleString(message)) => {
            format!("{message}\r\n").into_bytes()
        }
        (_, dfly_core::command::CommandReply::BulkString(payload)) => {
            let mut output = payload;
            output.extend_from_slice(b"\r\n");
            output
        }
        (_, dfly_core::command::CommandReply::Null) => b"END\r\n".to_vec(),
        (_, dfly_core::command::CommandReply::Error(message)) => {
            format!("ERROR {message}\r\n").into_bytes()
        }
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

    println!("{}", app.startup_summary());
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::ServerApp;
    use dfly_common::config::RuntimeConfig;
    use dfly_facade::protocol::ClientProtocol;
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
}
