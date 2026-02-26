use super::ServerApp;
use crate::ingress::ingress_connection_bytes;
use dfly_cluster::slot::{SlotRange, key_slot};
use dfly_common::config::{ClusterMode, RuntimeConfig};
use dfly_common::error::DflyError;
use dfly_core::command::{CommandFrame, CommandReply};
use dfly_facade::protocol::ClientProtocol;
use dfly_replication::journal::InMemoryJournal;
use dfly_transaction::plan::{TransactionHop, TransactionMode, TransactionPlan};
use dfly_transaction::scheduler::TransactionScheduler;
use googletest::prelude::*;
use rstest::rstest;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

mod testkit;
mod cluster_snapshot;
mod journal;
mod tx;
mod repl;
mod runtime;
mod basic;
use testkit::{
    decode_resp_bulk_payload, extract_dfly_flow_reply, extract_sync_id_from_capa_reply,
    parse_resp_integer, resp_command, unique_test_snapshot_path,
};
#[rstest]
fn memcache_connection_executes_set_then_get() {
    let mut app = ServerApp::new(RuntimeConfig::default());
    let mut connection = ServerApp::new_connection(ClientProtocol::Memcache);

    let set_responses =
        ingress_connection_bytes(&mut app, &mut connection, b"set user:42 0 0 5\r\nalice\r\n")
            .expect("memcache set should execute");
    let expected_set = vec![b"STORED\r\n".to_vec()];
    assert_that!(&set_responses, eq(&expected_set));

    let get_responses = ingress_connection_bytes(&mut app, &mut connection, b"get user:42\r\n")
        .expect("memcache get should execute");
    let expected_get = vec![b"VALUE user:42 0 5\r\nalice\r\nEND\r\n".to_vec()];
    assert_that!(&get_responses, eq(&expected_get));
}

#[rstest]
fn memcache_set_handles_partial_value_payload() {
    let mut app = ServerApp::new(RuntimeConfig::default());
    let mut connection = ServerApp::new_connection(ClientProtocol::Memcache);

    let partial_responses =
        ingress_connection_bytes(&mut app, &mut connection, b"set cache:key 0 0 7\r\nru")
            .expect("partial memcache set should not fail");
    assert_that!(partial_responses.is_empty(), eq(true));

    let final_responses = ingress_connection_bytes(&mut app, &mut connection, b"sty42\r\n")
        .expect("payload completion should execute set");
    let expected = vec![b"STORED\r\n".to_vec()];
    assert_that!(&final_responses, eq(&expected));
}

#[rstest]
fn resp_multi_exec_runs_queued_commands_in_order() {
    let mut app = ServerApp::new(RuntimeConfig::default());
    let mut connection = ServerApp::new_connection(ClientProtocol::Resp);

    let multi = ingress_connection_bytes(&mut app, &mut connection, b"*1\r\n$5\r\nMULTI\r\n")
        .expect("MULTI should succeed");
    assert_that!(&multi, eq(&vec![b"+OK\r\n".to_vec()]));

    let queued_set = ingress_connection_bytes(
        &mut app,
        &mut connection,
        b"*3\r\n$3\r\nSET\r\n$3\r\nfoo\r\n$3\r\nbar\r\n",
    )
    .expect("SET should be queued");
    assert_that!(&queued_set, eq(&vec![b"+QUEUED\r\n".to_vec()]));

    let queued_get = ingress_connection_bytes(
        &mut app,
        &mut connection,
        b"*2\r\n$3\r\nGET\r\n$3\r\nfoo\r\n",
    )
    .expect("GET should be queued");
    assert_that!(&queued_get, eq(&vec![b"+QUEUED\r\n".to_vec()]));

    let exec = ingress_connection_bytes(&mut app, &mut connection, b"*1\r\n$4\r\nEXEC\r\n")
        .expect("EXEC should execute queued commands");
    let expected_exec = vec![b"*2\r\n+OK\r\n$3\r\nbar\r\n".to_vec()];
    assert_that!(&exec, eq(&expected_exec));
}

#[rstest]
fn resp_exec_with_empty_queue_returns_empty_array() {
    let mut app = ServerApp::new(RuntimeConfig::default());
    let mut connection = ServerApp::new_connection(ClientProtocol::Resp);

    let multi = ingress_connection_bytes(&mut app, &mut connection, &resp_command(&[b"MULTI"]))
        .expect("MULTI should succeed");
    assert_that!(&multi, eq(&vec![b"+OK\r\n".to_vec()]));

    let exec = ingress_connection_bytes(&mut app, &mut connection, &resp_command(&[b"EXEC"]))
        .expect("EXEC should succeed");
    assert_that!(&exec, eq(&vec![b"*0\r\n".to_vec()]));
    assert_that!(
        app.replication_guard_with_latest_journal()
            .journal_entries()
            .is_empty(),
        eq(true)
    );
}

#[rstest]
fn resp_select_switches_logical_db_namespace() {
    let mut app = ServerApp::new(RuntimeConfig::default());
    let mut connection = ServerApp::new_connection(ClientProtocol::Resp);

    let _ = ingress_connection_bytes(
        &mut app,
        &mut connection,
        b"*3\r\n$3\r\nSET\r\n$6\r\nshared\r\n$3\r\ndb0\r\n",
    )
    .expect("SET in DB 0 should succeed");

    let select_db1 = ingress_connection_bytes(
        &mut app,
        &mut connection,
        b"*2\r\n$6\r\nSELECT\r\n$1\r\n1\r\n",
    )
    .expect("SELECT 1 should succeed");
    assert_that!(&select_db1, eq(&vec![b"+OK\r\n".to_vec()]));

    let get_in_db1 = ingress_connection_bytes(
        &mut app,
        &mut connection,
        b"*2\r\n$3\r\nGET\r\n$6\r\nshared\r\n",
    )
    .expect("GET in DB 1 should succeed");
    assert_that!(&get_in_db1, eq(&vec![b"$-1\r\n".to_vec()]));

    let _ = ingress_connection_bytes(
        &mut app,
        &mut connection,
        b"*3\r\n$3\r\nSET\r\n$6\r\nshared\r\n$3\r\ndb1\r\n",
    )
    .expect("SET in DB 1 should succeed");

    let select_db0 = ingress_connection_bytes(
        &mut app,
        &mut connection,
        b"*2\r\n$6\r\nSELECT\r\n$1\r\n0\r\n",
    )
    .expect("SELECT 0 should succeed");
    assert_that!(&select_db0, eq(&vec![b"+OK\r\n".to_vec()]));

    let get_in_db0 = ingress_connection_bytes(
        &mut app,
        &mut connection,
        b"*2\r\n$3\r\nGET\r\n$6\r\nshared\r\n",
    )
    .expect("GET in DB 0 should succeed");
    assert_that!(&get_in_db0, eq(&vec![b"$3\r\ndb0\r\n".to_vec()]));
}

#[rstest]
fn resp_flushdb_clears_only_selected_database() {
    let mut app = ServerApp::new(RuntimeConfig::default());
    let mut connection = ServerApp::new_connection(ClientProtocol::Resp);

    let _ = ingress_connection_bytes(
        &mut app,
        &mut connection,
        &resp_command(&[b"SET", b"k", b"db0"]),
    )
    .expect("SET in DB 0 should succeed");
    let _ = ingress_connection_bytes(&mut app, &mut connection, &resp_command(&[b"SELECT", b"1"]))
        .expect("SELECT 1 should succeed");
    let _ = ingress_connection_bytes(
        &mut app,
        &mut connection,
        &resp_command(&[b"SET", b"k", b"db1"]),
    )
    .expect("SET in DB 1 should succeed");

    let flushdb = ingress_connection_bytes(&mut app, &mut connection, &resp_command(&[b"FLUSHDB"]))
        .expect("FLUSHDB should execute");
    assert_that!(&flushdb, eq(&vec![b"+OK\r\n".to_vec()]));

    let get_in_db1 =
        ingress_connection_bytes(&mut app, &mut connection, &resp_command(&[b"GET", b"k"]))
            .expect("GET in DB 1 should execute");
    assert_that!(&get_in_db1, eq(&vec![b"$-1\r\n".to_vec()]));

    let _ = ingress_connection_bytes(&mut app, &mut connection, &resp_command(&[b"SELECT", b"0"]))
        .expect("SELECT 0 should succeed");
    let get_in_db0 =
        ingress_connection_bytes(&mut app, &mut connection, &resp_command(&[b"GET", b"k"]))
            .expect("GET in DB 0 should execute");
    assert_that!(&get_in_db0, eq(&vec![b"$3\r\ndb0\r\n".to_vec()]));
}

#[rstest]
fn resp_flushall_clears_all_databases() {
    let mut app = ServerApp::new(RuntimeConfig::default());
    let mut connection = ServerApp::new_connection(ClientProtocol::Resp);

    let _ = ingress_connection_bytes(
        &mut app,
        &mut connection,
        &resp_command(&[b"SET", b"k0", b"v0"]),
    )
    .expect("SET in DB 0 should succeed");
    let _ = ingress_connection_bytes(&mut app, &mut connection, &resp_command(&[b"SELECT", b"1"]))
        .expect("SELECT 1 should succeed");
    let _ = ingress_connection_bytes(
        &mut app,
        &mut connection,
        &resp_command(&[b"SET", b"k1", b"v1"]),
    )
    .expect("SET in DB 1 should succeed");

    let flushall =
        ingress_connection_bytes(&mut app, &mut connection, &resp_command(&[b"FLUSHALL"]))
            .expect("FLUSHALL should execute");
    assert_that!(&flushall, eq(&vec![b"+OK\r\n".to_vec()]));

    let get_in_db1 =
        ingress_connection_bytes(&mut app, &mut connection, &resp_command(&[b"GET", b"k1"]))
            .expect("GET in DB 1 should execute");
    assert_that!(&get_in_db1, eq(&vec![b"$-1\r\n".to_vec()]));

    let _ = ingress_connection_bytes(&mut app, &mut connection, &resp_command(&[b"SELECT", b"0"]))
        .expect("SELECT 0 should succeed");
    let get_in_db0 =
        ingress_connection_bytes(&mut app, &mut connection, &resp_command(&[b"GET", b"k0"]))
            .expect("GET in DB 0 should execute");
    assert_that!(&get_in_db0, eq(&vec![b"$-1\r\n".to_vec()]));
}

#[rstest]
fn resp_flush_commands_are_rejected_inside_multi() {
    let mut app = ServerApp::new(RuntimeConfig::default());

    let mut flushall_conn = ServerApp::new_connection(ClientProtocol::Resp);
    let _ = ingress_connection_bytes(&mut app, &mut flushall_conn, &resp_command(&[b"MULTI"]))
        .expect("MULTI should succeed");
    let flushall =
        ingress_connection_bytes(&mut app, &mut flushall_conn, &resp_command(&[b"FLUSHALL"]))
            .expect("FLUSHALL should parse");
    assert_that!(
        &flushall,
        eq(&vec![
            b"-ERR 'FLUSHALL' not allowed inside a transaction\r\n".to_vec()
        ])
    );
    let flushall_exec =
        ingress_connection_bytes(&mut app, &mut flushall_conn, &resp_command(&[b"EXEC"]))
            .expect("EXEC should parse");
    assert_that!(
        &flushall_exec,
        eq(&vec![
            b"-ERR EXECABORT Transaction discarded because of previous errors\r\n".to_vec()
        ])
    );

    let mut flushdb_conn = ServerApp::new_connection(ClientProtocol::Resp);
    let _ = ingress_connection_bytes(&mut app, &mut flushdb_conn, &resp_command(&[b"MULTI"]))
        .expect("MULTI should succeed");
    let flushdb =
        ingress_connection_bytes(&mut app, &mut flushdb_conn, &resp_command(&[b"FLUSHDB"]))
            .expect("FLUSHDB should parse");
    assert_that!(
        &flushdb,
        eq(&vec![
            b"-ERR 'FLUSHDB' not allowed inside a transaction\r\n".to_vec()
        ])
    );
    let flushdb_exec =
        ingress_connection_bytes(&mut app, &mut flushdb_conn, &resp_command(&[b"EXEC"]))
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

    let _ = ingress_connection_bytes(&mut app, &mut connection, b"*1\r\n$5\r\nMULTI\r\n")
        .expect("MULTI should succeed");
    let select = ingress_connection_bytes(
        &mut app,
        &mut connection,
        b"*2\r\n$6\r\nSELECT\r\n$1\r\n1\r\n",
    )
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

    let _ = ingress_connection_bytes(&mut app, &mut connection, &resp_command(&[b"MULTI"]))
        .expect("MULTI should succeed");
    let select =
        ingress_connection_bytes(&mut app, &mut connection, &resp_command(&[b"SELECT", b"1"]))
            .expect("SELECT should parse");
    assert_that!(
        &select,
        eq(&vec![b"-ERR SELECT is not allowed in MULTI\r\n".to_vec()])
    );

    let exec = ingress_connection_bytes(&mut app, &mut connection, &resp_command(&[b"EXEC"]))
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

    let _ = ingress_connection_bytes(&mut app, &mut connection, &resp_command(&[b"MULTI"]))
        .expect("MULTI should succeed");
    let exec_arity_error = ingress_connection_bytes(
        &mut app,
        &mut connection,
        &resp_command(&[b"EXEC", b"extra"]),
    )
    .expect("EXEC should parse");
    assert_that!(
        &exec_arity_error,
        eq(&vec![
            b"-ERR wrong number of arguments for 'EXEC' command\r\n".to_vec()
        ])
    );

    let exec_after_error =
        ingress_connection_bytes(&mut app, &mut connection, &resp_command(&[b"EXEC"]))
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

    let _ = ingress_connection_bytes(&mut app, &mut connection, &resp_command(&[b"MULTI"]))
        .expect("MULTI should succeed");
    let discard_arity_error = ingress_connection_bytes(
        &mut app,
        &mut connection,
        &resp_command(&[b"DISCARD", b"extra"]),
    )
    .expect("DISCARD should parse");
    assert_that!(
        &discard_arity_error,
        eq(&vec![
            b"-ERR wrong number of arguments for 'DISCARD' command\r\n".to_vec()
        ])
    );

    let exec_after_error =
        ingress_connection_bytes(&mut app, &mut connection, &resp_command(&[b"EXEC"]))
            .expect("EXEC should parse");
    assert_that!(
        &exec_after_error,
        eq(&vec![
            b"-ERR EXECABORT Transaction discarded because of previous errors\r\n".to_vec()
        ])
    );
}
