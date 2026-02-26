use super::*;

#[rstest]
fn resp_discard_drops_queued_commands() {
    let mut app = ServerApp::new(RuntimeConfig::default());
    let mut connection = ServerApp::new_connection(ClientProtocol::Resp);

    let _ = ingress_connection_bytes(&mut app, &mut connection, b"*1\r\n$5\r\nMULTI\r\n")
        .expect("MULTI should succeed");
    let _ = ingress_connection_bytes(
        &mut app,
        &mut connection,
        b"*3\r\n$3\r\nSET\r\n$3\r\nfoo\r\n$3\r\nbar\r\n",
    )
    .expect("SET should be queued");
    let discard = ingress_connection_bytes(&mut app, &mut connection, b"*1\r\n$7\r\nDISCARD\r\n")
        .expect("DISCARD should succeed");
    assert_that!(&discard, eq(&vec![b"+OK\r\n".to_vec()]));

    let get = ingress_connection_bytes(
        &mut app,
        &mut connection,
        b"*2\r\n$3\r\nGET\r\n$3\r\nfoo\r\n",
    )
    .expect("GET should run after discard");
    assert_that!(&get, eq(&vec![b"$-1\r\n".to_vec()]));
}

#[rstest]
fn resp_exec_without_multi_returns_error() {
    let mut app = ServerApp::new(RuntimeConfig::default());
    let mut connection = ServerApp::new_connection(ClientProtocol::Resp);

    let exec = ingress_connection_bytes(&mut app, &mut connection, b"*1\r\n$4\r\nEXEC\r\n")
        .expect("EXEC command should parse");
    assert_that!(&exec, eq(&vec![b"-ERR EXEC without MULTI\r\n".to_vec()]));
}

#[rstest]
fn resp_exec_without_multi_remains_error_even_when_watch_is_dirty() {
    let mut app = ServerApp::new(RuntimeConfig::default());
    let mut watch_conn = ServerApp::new_connection(ClientProtocol::Resp);
    let mut writer_conn = ServerApp::new_connection(ClientProtocol::Resp);

    let _ = ingress_connection_bytes(
        &mut app,
        &mut watch_conn,
        &resp_command(&[b"WATCH", b"key"]),
    )
    .expect("WATCH should succeed");
    let _ = ingress_connection_bytes(
        &mut app,
        &mut writer_conn,
        &resp_command(&[b"SET", b"key", b"other"]),
    )
    .expect("writer SET should succeed");

    let exec = ingress_connection_bytes(&mut app, &mut watch_conn, &resp_command(&[b"EXEC"]))
        .expect("EXEC should parse");
    assert_that!(&exec, eq(&vec![b"-ERR EXEC without MULTI\r\n".to_vec()]));
}

#[rstest]
fn resp_execaborts_after_unknown_command_during_multi_queueing() {
    let mut app = ServerApp::new(RuntimeConfig::default());
    let mut connection = ServerApp::new_connection(ClientProtocol::Resp);

    let _ = ingress_connection_bytes(&mut app, &mut connection, &resp_command(&[b"MULTI"]))
        .expect("MULTI should succeed");
    let queue_error =
        ingress_connection_bytes(&mut app, &mut connection, &resp_command(&[b"NOPE"]))
            .expect("unknown command should parse");
    assert_that!(
        &queue_error,
        eq(&vec![b"-ERR unknown command 'NOPE'\r\n".to_vec()])
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
fn resp_execaborts_after_core_arity_error_during_multi_queueing() {
    let mut app = ServerApp::new(RuntimeConfig::default());
    let mut connection = ServerApp::new_connection(ClientProtocol::Resp);

    let _ = ingress_connection_bytes(&mut app, &mut connection, &resp_command(&[b"MULTI"]))
        .expect("MULTI should succeed");
    let queue_error = ingress_connection_bytes(&mut app, &mut connection, &resp_command(&[b"GET"]))
        .expect("invalid GET arity should parse");
    assert_that!(
        &queue_error,
        eq(&vec![
            b"-ERR wrong number of arguments for 'GET' command\r\n".to_vec()
        ])
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
fn resp_execaborts_after_server_command_arity_error_during_multi_queueing() {
    let mut app = ServerApp::new(RuntimeConfig::default());
    let mut connection = ServerApp::new_connection(ClientProtocol::Resp);

    let _ = ingress_connection_bytes(&mut app, &mut connection, &resp_command(&[b"MULTI"]))
        .expect("MULTI should succeed");
    let queue_error =
        ingress_connection_bytes(&mut app, &mut connection, &resp_command(&[b"WAIT", b"1"]))
            .expect("invalid WAIT arity should parse");
    assert_that!(
        &queue_error,
        eq(&vec![
            b"-ERR wrong number of arguments for 'WAIT' command\r\n".to_vec()
        ])
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
fn resp_execaborts_after_replconf_syntax_error_during_multi_queueing() {
    let mut app = ServerApp::new(RuntimeConfig::default());
    let mut connection = ServerApp::new_connection(ClientProtocol::Resp);

    let _ = ingress_connection_bytes(&mut app, &mut connection, &resp_command(&[b"MULTI"]))
        .expect("MULTI should succeed");
    let queue_error = ingress_connection_bytes(
        &mut app,
        &mut connection,
        &resp_command(&[b"REPLCONF", b"ACK"]),
    )
    .expect("invalid REPLCONF shape should parse");
    assert_that!(&queue_error, eq(&vec![b"-ERR syntax error\r\n".to_vec()]));

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
fn resp_watch_aborts_exec_when_watched_key_changes() {
    let mut app = ServerApp::new(RuntimeConfig::default());
    let mut watch_conn = ServerApp::new_connection(ClientProtocol::Resp);
    let mut writer_conn = ServerApp::new_connection(ClientProtocol::Resp);

    let _ = ingress_connection_bytes(
        &mut app,
        &mut watch_conn,
        b"*3\r\n$3\r\nSET\r\n$3\r\nkey\r\n$3\r\nold\r\n",
    )
    .expect("seed SET should succeed");
    let _ = ingress_connection_bytes(
        &mut app,
        &mut watch_conn,
        b"*2\r\n$5\r\nWATCH\r\n$3\r\nkey\r\n",
    )
    .expect("WATCH should succeed");
    let _ = ingress_connection_bytes(&mut app, &mut watch_conn, b"*1\r\n$5\r\nMULTI\r\n")
        .expect("MULTI should succeed");
    let _ = ingress_connection_bytes(
        &mut app,
        &mut watch_conn,
        b"*3\r\n$3\r\nSET\r\n$3\r\nkey\r\n$4\r\nmine\r\n",
    )
    .expect("queued SET should succeed");
    let _ = ingress_connection_bytes(
        &mut app,
        &mut writer_conn,
        b"*3\r\n$3\r\nSET\r\n$3\r\nkey\r\n$5\r\nother\r\n",
    )
    .expect("concurrent SET should succeed");

    let exec = ingress_connection_bytes(&mut app, &mut watch_conn, b"*1\r\n$4\r\nEXEC\r\n")
        .expect("EXEC should parse");
    assert_that!(&exec, eq(&vec![b"*-1\r\n".to_vec()]));

    let check = ingress_connection_bytes(
        &mut app,
        &mut watch_conn,
        b"*2\r\n$3\r\nGET\r\n$3\r\nkey\r\n",
    )
    .expect("GET should succeed");
    assert_that!(&check, eq(&vec![b"$5\r\nother\r\n".to_vec()]));
}

#[rstest]
fn resp_watch_aborts_exec_when_flushdb_deletes_watched_key() {
    let mut app = ServerApp::new(RuntimeConfig::default());
    let mut watch_conn = ServerApp::new_connection(ClientProtocol::Resp);
    let mut writer_conn = ServerApp::new_connection(ClientProtocol::Resp);

    let _ = ingress_connection_bytes(
        &mut app,
        &mut watch_conn,
        &resp_command(&[b"SET", b"watch:key", b"v"]),
    )
    .expect("seed SET should succeed");
    let _ = ingress_connection_bytes(
        &mut app,
        &mut watch_conn,
        &resp_command(&[b"WATCH", b"watch:key"]),
    )
    .expect("WATCH should succeed");
    let _ = ingress_connection_bytes(&mut app, &mut writer_conn, &resp_command(&[b"FLUSHDB"]))
        .expect("FLUSHDB should succeed");

    let _ = ingress_connection_bytes(&mut app, &mut watch_conn, &resp_command(&[b"MULTI"]))
        .expect("MULTI should succeed");
    let _ = ingress_connection_bytes(
        &mut app,
        &mut watch_conn,
        &resp_command(&[b"SET", b"watch:key", b"mine"]),
    )
    .expect("SET should queue");

    let exec = ingress_connection_bytes(&mut app, &mut watch_conn, &resp_command(&[b"EXEC"]))
        .expect("EXEC should parse");
    assert_that!(&exec, eq(&vec![b"*-1\r\n".to_vec()]));
}

#[rstest]
fn resp_execaborts_after_watch_or_unwatch_error_inside_multi() {
    let mut app = ServerApp::new(RuntimeConfig::default());
    let mut watch_conn = ServerApp::new_connection(ClientProtocol::Resp);

    let _ = ingress_connection_bytes(&mut app, &mut watch_conn, &resp_command(&[b"MULTI"]))
        .expect("MULTI should succeed");
    let watch_error =
        ingress_connection_bytes(&mut app, &mut watch_conn, &resp_command(&[b"WATCH", b"k"]))
            .expect("WATCH should parse");
    assert_that!(
        &watch_error,
        eq(&vec![
            b"-ERR WATCH inside MULTI is not allowed\r\n".to_vec()
        ])
    );
    let exec_after_watch_error =
        ingress_connection_bytes(&mut app, &mut watch_conn, &resp_command(&[b"EXEC"]))
            .expect("EXEC should parse");
    assert_that!(
        &exec_after_watch_error,
        eq(&vec![
            b"-ERR EXECABORT Transaction discarded because of previous errors\r\n".to_vec()
        ])
    );

    let mut unwatch_conn = ServerApp::new_connection(ClientProtocol::Resp);
    let _ = ingress_connection_bytes(&mut app, &mut unwatch_conn, &resp_command(&[b"MULTI"]))
        .expect("MULTI should succeed");
    let unwatch_error =
        ingress_connection_bytes(&mut app, &mut unwatch_conn, &resp_command(&[b"UNWATCH"]))
            .expect("UNWATCH should parse");
    assert_that!(
        &unwatch_error,
        eq(&vec![
            b"-ERR UNWATCH inside MULTI is not allowed\r\n".to_vec()
        ])
    );
    let exec_after_unwatch_error =
        ingress_connection_bytes(&mut app, &mut unwatch_conn, &resp_command(&[b"EXEC"]))
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

    let _ = ingress_connection_bytes(
        &mut app,
        &mut connection,
        b"*2\r\n$5\r\nWATCH\r\n$3\r\nkey\r\n",
    )
    .expect("WATCH should succeed");
    let _ = ingress_connection_bytes(&mut app, &mut connection, b"*1\r\n$5\r\nMULTI\r\n")
        .expect("MULTI should succeed");
    let _ = ingress_connection_bytes(
        &mut app,
        &mut connection,
        b"*3\r\n$3\r\nSET\r\n$3\r\nkey\r\n$3\r\nval\r\n",
    )
    .expect("SET should queue");

    let exec = ingress_connection_bytes(&mut app, &mut connection, b"*1\r\n$4\r\nEXEC\r\n")
        .expect("EXEC should succeed");
    assert_that!(&exec, eq(&vec![b"*1\r\n+OK\r\n".to_vec()]));
}

#[rstest]
fn resp_unwatch_clears_watch_set() {
    let mut app = ServerApp::new(RuntimeConfig::default());
    let mut watch_conn = ServerApp::new_connection(ClientProtocol::Resp);
    let mut writer_conn = ServerApp::new_connection(ClientProtocol::Resp);

    let _ = ingress_connection_bytes(
        &mut app,
        &mut watch_conn,
        b"*2\r\n$5\r\nWATCH\r\n$3\r\nkey\r\n",
    )
    .expect("WATCH should succeed");
    let _ = ingress_connection_bytes(&mut app, &mut watch_conn, b"*1\r\n$7\r\nUNWATCH\r\n")
        .expect("UNWATCH should succeed");
    let _ = ingress_connection_bytes(
        &mut app,
        &mut writer_conn,
        b"*3\r\n$3\r\nSET\r\n$3\r\nkey\r\n$5\r\nother\r\n",
    )
    .expect("external SET should succeed");
    let _ = ingress_connection_bytes(&mut app, &mut watch_conn, b"*1\r\n$5\r\nMULTI\r\n")
        .expect("MULTI should succeed");
    let _ = ingress_connection_bytes(
        &mut app,
        &mut watch_conn,
        b"*3\r\n$3\r\nSET\r\n$3\r\nkey\r\n$4\r\nmine\r\n",
    )
    .expect("SET should queue");

    let exec = ingress_connection_bytes(&mut app, &mut watch_conn, b"*1\r\n$4\r\nEXEC\r\n")
        .expect("EXEC should run queued SET");
    assert_that!(&exec, eq(&vec![b"*1\r\n+OK\r\n".to_vec()]));
}

#[rstest]
fn resp_exec_rejects_watch_and_exec_across_databases() {
    let mut app = ServerApp::new(RuntimeConfig::default());
    let mut connection = ServerApp::new_connection(ClientProtocol::Resp);

    let _ = ingress_connection_bytes(
        &mut app,
        &mut connection,
        &resp_command(&[b"WATCH", b"cross:key"]),
    )
    .expect("WATCH should succeed in DB 0");
    let _ = ingress_connection_bytes(&mut app, &mut connection, &resp_command(&[b"SELECT", b"1"]))
        .expect("SELECT 1 should succeed");
    let _ = ingress_connection_bytes(&mut app, &mut connection, &resp_command(&[b"MULTI"]))
        .expect("MULTI should succeed");
    let queued = ingress_connection_bytes(
        &mut app,
        &mut connection,
        &resp_command(&[b"SET", b"cross:key", b"db1-value"]),
    )
    .expect("SET should queue");
    assert_that!(&queued, eq(&vec![b"+QUEUED\r\n".to_vec()]));

    let exec = ingress_connection_bytes(&mut app, &mut connection, &resp_command(&[b"EXEC"]))
        .expect("EXEC should parse");
    assert_that!(
        &exec,
        eq(&vec![
            b"-ERR Dragonfly does not allow WATCH and EXEC on different databases\r\n".to_vec()
        ])
    );

    let multi_again =
        ingress_connection_bytes(&mut app, &mut connection, &resp_command(&[b"MULTI"]))
            .expect("MULTI should succeed after EXEC failure cleanup");
    assert_that!(&multi_again, eq(&vec![b"+OK\r\n".to_vec()]));
}

