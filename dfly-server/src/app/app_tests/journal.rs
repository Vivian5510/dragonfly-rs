use super::*;
use dfly_replication::journal::JournalOp;
use std::time::{SystemTime, UNIX_EPOCH};

#[rstest]
fn journal_records_only_successful_write_commands() {
    let mut app = ServerApp::new(RuntimeConfig::default());
    let mut connection = ServerApp::new_connection(ClientProtocol::Resp);

    let _ = ingress_connection_bytes(
        &mut app,
        &mut connection,
        b"*3\r\n$3\r\nSET\r\n$3\r\nfoo\r\n$3\r\nbar\r\n",
    )
    .expect("SET should succeed");
    let _ = ingress_connection_bytes(
        &mut app,
        &mut connection,
        b"*2\r\n$3\r\nGET\r\n$3\r\nfoo\r\n",
    )
    .expect("GET should succeed");

    assert_that!(
        app.replication_guard_with_latest_journal()
            .journal_entries()
            .len(),
        eq(1_usize)
    );
    assert_that!(
        app.replication_guard_with_latest_journal()
            .journal_entries()[0]
            .op,
        eq(JournalOp::Command),
    );
}

#[rstest]
fn journal_records_set_only_when_mutation_happens_with_conditions_and_get() {
    let mut app = ServerApp::new(RuntimeConfig::default());
    let mut connection = ServerApp::new_connection(ClientProtocol::Resp);

    let _ = ingress_connection_bytes(
        &mut app,
        &mut connection,
        &resp_command(&[b"SET", b"k", b"v1", b"NX"]),
    )
    .expect("SET NX should succeed");
    let _ = ingress_connection_bytes(
        &mut app,
        &mut connection,
        &resp_command(&[b"SET", b"k", b"v2", b"NX"]),
    )
    .expect("SET NX should execute");
    let _ = ingress_connection_bytes(
        &mut app,
        &mut connection,
        &resp_command(&[b"SET", b"k", b"ignored", b"NX", b"GET"]),
    )
    .expect("SET NX GET should execute");
    let _ = ingress_connection_bytes(
        &mut app,
        &mut connection,
        &resp_command(&[b"SET", b"k", b"v2", b"XX", b"GET"]),
    )
    .expect("SET XX GET should execute");
    let _ = ingress_connection_bytes(
        &mut app,
        &mut connection,
        &resp_command(&[b"SET", b"missing", b"v", b"XX", b"GET"]),
    )
    .expect("SET XX GET on missing key should execute");
    let _ = ingress_connection_bytes(
        &mut app,
        &mut connection,
        &resp_command(&[b"SET", b"fresh", b"v", b"GET"]),
    )
    .expect("SET GET should execute");

    let entries = app
        .replication_guard_with_latest_journal()
        .journal_entries();
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

    let _ = ingress_connection_bytes(
        &mut app,
        &mut connection,
        &resp_command(&[b"SETEX", b"k", b"10", b"v"]),
    )
    .expect("SETEX should succeed");
    let _ = ingress_connection_bytes(
        &mut app,
        &mut connection,
        &resp_command(&[b"SETEX", b"k", b"0", b"v"]),
    )
    .expect("SETEX should parse");

    let entries = app
        .replication_guard_with_latest_journal()
        .journal_entries();
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

    let _ = ingress_connection_bytes(
        &mut app,
        &mut connection,
        &resp_command(&[b"PSETEX", b"k", b"1500", b"v"]),
    )
    .expect("PSETEX should succeed");
    let _ = ingress_connection_bytes(
        &mut app,
        &mut connection,
        &resp_command(&[b"PSETEX", b"k", b"0", b"v"]),
    )
    .expect("PSETEX should parse");

    let entries = app
        .replication_guard_with_latest_journal()
        .journal_entries();
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

    let _ = ingress_connection_bytes(
        &mut app,
        &mut connection,
        &resp_command(&[b"SET", b"k", b"v"]),
    )
    .expect("SET should succeed");
    let _ = ingress_connection_bytes(
        &mut app,
        &mut connection,
        &resp_command(&[b"PEXPIRE", b"k", b"1000"]),
    )
    .expect("PEXPIRE with positive timeout should succeed");
    let _ = ingress_connection_bytes(
        &mut app,
        &mut connection,
        &resp_command(&[b"SET", b"k", b"v"]),
    )
    .expect("SET should recreate key");
    let _ = ingress_connection_bytes(
        &mut app,
        &mut connection,
        &resp_command(&[b"PEXPIRE", b"k", b"0"]),
    )
    .expect("PEXPIRE with zero timeout should succeed");

    let entries = app
        .replication_guard_with_latest_journal()
        .journal_entries();
    assert_that!(entries.len(), eq(4_usize));
    assert_that!(entries[1].op, eq(JournalOp::Command));
    assert_that!(entries[3].op, eq(JournalOp::Expired));
}

#[rstest]
fn journal_records_expire_options_only_when_update_is_applied() {
    let mut app = ServerApp::new(RuntimeConfig::default());
    let mut connection = ServerApp::new_connection(ClientProtocol::Resp);

    let _ = ingress_connection_bytes(
        &mut app,
        &mut connection,
        &resp_command(&[b"SET", b"k", b"v"]),
    )
    .expect("SET should succeed");
    let _ = ingress_connection_bytes(
        &mut app,
        &mut connection,
        &resp_command(&[b"EXPIRE", b"k", b"10", b"GT"]),
    )
    .expect("EXPIRE GT should execute");
    let _ = ingress_connection_bytes(
        &mut app,
        &mut connection,
        &resp_command(&[b"EXPIRE", b"k", b"10", b"LT"]),
    )
    .expect("EXPIRE LT should execute");
    let _ = ingress_connection_bytes(
        &mut app,
        &mut connection,
        &resp_command(&[b"EXPIRE", b"k", b"20", b"NX"]),
    )
    .expect("EXPIRE NX should execute");
    let _ = ingress_connection_bytes(
        &mut app,
        &mut connection,
        &resp_command(&[b"EXPIRE", b"k", b"0", b"LT"]),
    )
    .expect("EXPIRE LT delete should execute");

    let entries = app
        .replication_guard_with_latest_journal()
        .journal_entries();
    assert_that!(entries.len(), eq(3_usize));
    assert_that!(entries[1].op, eq(JournalOp::Command));
    assert_that!(entries[2].op, eq(JournalOp::Expired));
}

#[rstest]
fn journal_records_pexpireat_as_command_or_expired_by_timestamp() {
    let mut app = ServerApp::new(RuntimeConfig::default());
    let mut connection = ServerApp::new_connection(ClientProtocol::Resp);

    let _ = ingress_connection_bytes(
        &mut app,
        &mut connection,
        &resp_command(&[b"SET", b"k", b"v"]),
    )
    .expect("SET should succeed");
    let future_timestamp = (SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system time should be after epoch")
        .as_millis()
        + 120_000)
        .to_string()
        .into_bytes();
    let _ = ingress_connection_bytes(
        &mut app,
        &mut connection,
        &resp_command(&[b"PEXPIREAT", b"k", &future_timestamp]),
    )
    .expect("PEXPIREAT future should succeed");
    let _ = ingress_connection_bytes(
        &mut app,
        &mut connection,
        &resp_command(&[b"SET", b"k", b"v"]),
    )
    .expect("SET should recreate key");
    let _ = ingress_connection_bytes(
        &mut app,
        &mut connection,
        &resp_command(&[b"PEXPIREAT", b"k", b"1"]),
    )
    .expect("PEXPIREAT past should succeed");

    let entries = app
        .replication_guard_with_latest_journal()
        .journal_entries();
    assert_that!(entries.len(), eq(4_usize));
    assert_that!(entries[1].op, eq(JournalOp::Command));
    assert_that!(entries[3].op, eq(JournalOp::Expired));
}

#[rstest]
fn journal_records_mset_as_single_write_command() {
    let mut app = ServerApp::new(RuntimeConfig::default());
    let mut connection = ServerApp::new_connection(ClientProtocol::Resp);

    let mset = resp_command(&[b"MSET", b"a", b"1", b"b", b"2"]);
    let _ =
        ingress_connection_bytes(&mut app, &mut connection, &mset).expect("MSET should succeed");

    let entries = app
        .replication_guard_with_latest_journal()
        .journal_entries();
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

    let _ = ingress_connection_bytes(
        &mut app,
        &mut connection,
        &resp_command(&[b"MSETNX", b"a", b"1", b"b", b"2"]),
    )
    .expect("first MSETNX should succeed");
    let _ = ingress_connection_bytes(
        &mut app,
        &mut connection,
        &resp_command(&[b"MSETNX", b"a", b"x", b"c", b"3"]),
    )
    .expect("second MSETNX should succeed");

    let entries = app
        .replication_guard_with_latest_journal()
        .journal_entries();
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

    let _ = ingress_connection_bytes(
        &mut app,
        &mut connection,
        &resp_command(&[b"SET", b"src", b"v"]),
    )
    .expect("SET should succeed");
    let _ = ingress_connection_bytes(
        &mut app,
        &mut connection,
        &resp_command(&[b"RENAMENX", b"src", b"dst"]),
    )
    .expect("RENAMENX success should execute");
    let _ = ingress_connection_bytes(
        &mut app,
        &mut connection,
        &resp_command(&[b"RENAMENX", b"dst", b"dst"]),
    )
    .expect("RENAMENX same key should execute");
    let _ = ingress_connection_bytes(
        &mut app,
        &mut connection,
        &resp_command(&[b"RENAME", b"dst", b"dst"]),
    )
    .expect("RENAME same key should execute");

    let entries = app
        .replication_guard_with_latest_journal()
        .journal_entries();
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

    let _ = ingress_connection_bytes(
        &mut app,
        &mut connection,
        &resp_command(&[b"SET", b"src", b"v1"]),
    )
    .expect("SET should succeed");
    let _ = ingress_connection_bytes(
        &mut app,
        &mut connection,
        &resp_command(&[b"COPY", b"missing", b"dst"]),
    )
    .expect("COPY missing should execute");
    let _ = ingress_connection_bytes(
        &mut app,
        &mut connection,
        &resp_command(&[b"COPY", b"src", b"dst"]),
    )
    .expect("COPY success should execute");
    let _ = ingress_connection_bytes(
        &mut app,
        &mut connection,
        &resp_command(&[b"COPY", b"src", b"dst"]),
    )
    .expect("COPY blocked should execute");
    let _ = ingress_connection_bytes(
        &mut app,
        &mut connection,
        &resp_command(&[b"COPY", b"src", b"dst", b"REPLACE"]),
    )
    .expect("COPY REPLACE should execute");

    let entries = app
        .replication_guard_with_latest_journal()
        .journal_entries();
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

    let _ = ingress_connection_bytes(
        &mut app,
        &mut connection,
        &resp_command(&[b"DEL", b"missing"]),
    )
    .expect("DEL missing should succeed");
    let _ = ingress_connection_bytes(
        &mut app,
        &mut connection,
        &resp_command(&[b"SET", b"k", b"v"]),
    )
    .expect("SET should succeed");
    let _ = ingress_connection_bytes(&mut app, &mut connection, &resp_command(&[b"DEL", b"k"]))
        .expect("DEL existing should succeed");

    let entries = app
        .replication_guard_with_latest_journal()
        .journal_entries();
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

    let _ = ingress_connection_bytes(
        &mut app,
        &mut connection,
        &resp_command(&[b"UNLINK", b"missing"]),
    )
    .expect("UNLINK missing should succeed");
    let _ = ingress_connection_bytes(
        &mut app,
        &mut connection,
        &resp_command(&[b"SET", b"k", b"v"]),
    )
    .expect("SET should succeed");
    let _ = ingress_connection_bytes(&mut app, &mut connection, &resp_command(&[b"UNLINK", b"k"]))
        .expect("UNLINK existing should succeed");

    let entries = app
        .replication_guard_with_latest_journal()
        .journal_entries();
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

    let _ = ingress_connection_bytes(
        &mut app,
        &mut connection,
        &resp_command(&[b"MOVE", b"missing", b"1"]),
    )
    .expect("MOVE missing should succeed");
    let _ = ingress_connection_bytes(
        &mut app,
        &mut connection,
        &resp_command(&[b"SET", b"k", b"v"]),
    )
    .expect("SET should succeed");
    let _ = ingress_connection_bytes(
        &mut app,
        &mut connection,
        &resp_command(&[b"MOVE", b"k", b"1"]),
    )
    .expect("MOVE existing should succeed");

    let entries = app
        .replication_guard_with_latest_journal()
        .journal_entries();
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

    let _ = ingress_connection_bytes(
        &mut app,
        &mut connection,
        &resp_command(&[b"SET", b"k", b"v"]),
    )
    .expect("SET should succeed");
    let _ = ingress_connection_bytes(
        &mut app,
        &mut connection,
        &resp_command(&[b"PERSIST", b"k"]),
    )
    .expect("PERSIST without expiry should succeed");
    let _ = ingress_connection_bytes(
        &mut app,
        &mut connection,
        &resp_command(&[b"EXPIRE", b"k", b"10"]),
    )
    .expect("EXPIRE should succeed");
    let _ = ingress_connection_bytes(
        &mut app,
        &mut connection,
        &resp_command(&[b"PERSIST", b"k"]),
    )
    .expect("PERSIST should clear expiry");

    let entries = app
        .replication_guard_with_latest_journal()
        .journal_entries();
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

    let _ = ingress_connection_bytes(
        &mut app,
        &mut connection,
        &resp_command(&[b"INCR", b"counter"]),
    )
    .expect("INCR should succeed");
    let _ = ingress_connection_bytes(
        &mut app,
        &mut connection,
        &resp_command(&[b"INCRBY", b"counter", b"2"]),
    )
    .expect("INCRBY should succeed");
    let _ = ingress_connection_bytes(
        &mut app,
        &mut connection,
        &resp_command(&[b"SET", b"bad", b"abc"]),
    )
    .expect("SET should succeed");
    let _ = ingress_connection_bytes(&mut app, &mut connection, &resp_command(&[b"INCR", b"bad"]))
        .expect("INCR bad should parse");

    let entries = app
        .replication_guard_with_latest_journal()
        .journal_entries();
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

    let _ = ingress_connection_bytes(
        &mut app,
        &mut connection,
        &resp_command(&[b"SETNX", b"k", b"v1"]),
    )
    .expect("first SETNX should succeed");
    let _ = ingress_connection_bytes(
        &mut app,
        &mut connection,
        &resp_command(&[b"SETNX", b"k", b"v2"]),
    )
    .expect("second SETNX should succeed");

    let entries = app
        .replication_guard_with_latest_journal()
        .journal_entries();
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

    let _ = ingress_connection_bytes(
        &mut app,
        &mut connection,
        &resp_command(&[b"GETSET", b"k", b"v1"]),
    )
    .expect("first GETSET should succeed");
    let _ = ingress_connection_bytes(
        &mut app,
        &mut connection,
        &resp_command(&[b"GETSET", b"k", b"v2"]),
    )
    .expect("second GETSET should succeed");
    let _ = ingress_connection_bytes(&mut app, &mut connection, &resp_command(&[b"GETDEL", b"k"]))
        .expect("GETDEL existing key should succeed");
    let _ = ingress_connection_bytes(&mut app, &mut connection, &resp_command(&[b"GETDEL", b"k"]))
        .expect("GETDEL missing key should succeed");

    let entries = app
        .replication_guard_with_latest_journal()
        .journal_entries();
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

    let _ = ingress_connection_bytes(
        &mut app,
        &mut connection,
        &resp_command(&[b"APPEND", b"k", b"he"]),
    )
    .expect("first APPEND should succeed");
    let _ = ingress_connection_bytes(
        &mut app,
        &mut connection,
        &resp_command(&[b"APPEND", b"k", b"llo"]),
    )
    .expect("second APPEND should succeed");

    let entries = app
        .replication_guard_with_latest_journal()
        .journal_entries();
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

    let _ = ingress_connection_bytes(
        &mut app,
        &mut connection,
        &resp_command(&[b"SETRANGE", b"k", b"0", b""]),
    )
    .expect("SETRANGE with empty payload should succeed");
    let _ = ingress_connection_bytes(
        &mut app,
        &mut connection,
        &resp_command(&[b"SETRANGE", b"k", b"0", b"xy"]),
    )
    .expect("SETRANGE with payload should succeed");

    let entries = app
        .replication_guard_with_latest_journal()
        .journal_entries();
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

    let _ = ingress_connection_bytes(
        &mut app,
        &mut connection,
        &resp_command(&[b"SET", b"k", b"v"]),
    )
    .expect("SET should succeed");
    let _ = ingress_connection_bytes(&mut app, &mut connection, &resp_command(&[b"FLUSHDB"]))
        .expect("FLUSHDB should succeed");
    let _ = ingress_connection_bytes(&mut app, &mut connection, &resp_command(&[b"FLUSHALL"]))
        .expect("FLUSHALL should succeed");

    let entries = app
        .replication_guard_with_latest_journal()
        .journal_entries();
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

    let _ = ingress_connection_bytes(&mut app, &mut connection, b"*1\r\n$5\r\nMULTI\r\n")
        .expect("MULTI should succeed");
    let _ = ingress_connection_bytes(
        &mut app,
        &mut connection,
        b"*3\r\n$3\r\nSET\r\n$3\r\nfoo\r\n$3\r\nbar\r\n",
    )
    .expect("SET should queue");
    let _ = ingress_connection_bytes(
        &mut app,
        &mut connection,
        b"*3\r\n$6\r\nEXPIRE\r\n$3\r\nfoo\r\n$1\r\n0\r\n",
    )
    .expect("EXPIRE should queue");
    let _ = ingress_connection_bytes(&mut app, &mut connection, b"*1\r\n$4\r\nEXEC\r\n")
        .expect("EXEC should succeed");

    let entries = app
        .replication_guard_with_latest_journal()
        .journal_entries();
    assert_that!(entries.len(), eq(2_usize));
    assert_that!(entries[0].txid, eq(entries[1].txid));
    assert_that!(entries[1].op, eq(JournalOp::Expired));
}

#[rstest]
fn journal_append_lane_preserves_txid_order() {
    let mut app = ServerApp::new(RuntimeConfig::default());
    let mut connection = ServerApp::new_connection(ClientProtocol::Resp);

    for index in 0..64_u16 {
        let key = format!("lane:key:{index}").into_bytes();
        let value = format!("v{index}").into_bytes();
        let command = resp_command(&[b"SET", &key, &value]);
        let _ = ingress_connection_bytes(&mut app, &mut connection, &command)
            .expect("SET should succeed");
    }

    let entries = app
        .replication_guard_with_latest_journal()
        .journal_entries();
    assert_that!(entries.len(), eq(64_usize));
    for pair in entries.windows(2) {
        assert_that!(pair[0].txid <= pair[1].txid, eq(true));
    }
}

