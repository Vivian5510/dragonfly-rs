use super::*;

#[rstest]
fn resp_role_reports_master_with_empty_replica_list() {
    let mut app = ServerApp::new(RuntimeConfig::default());
    let mut connection = ServerApp::new_connection(ClientProtocol::Resp);

    let reply = ingress_connection_bytes(&mut app, &mut connection, &resp_command(&[b"ROLE"]))
        .expect("ROLE should succeed");
    assert_that!(&reply, eq(&vec![b"*2\r\n$6\r\nmaster\r\n*0\r\n".to_vec()]));
}

#[rstest]
fn resp_info_replication_reports_master_offsets() {
    let mut app = ServerApp::new(RuntimeConfig::default());
    let mut connection = ServerApp::new_connection(ClientProtocol::Resp);

    let _ = ingress_connection_bytes(
        &mut app,
        &mut connection,
        &resp_command(&[b"SET", b"info:key", b"info:value"]),
    )
    .expect("SET should succeed");

    let reply = ingress_connection_bytes(
        &mut app,
        &mut connection,
        &resp_command(&[b"INFO", b"REPLICATION"]),
    )
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
fn resp_info_default_sections_include_replication_and_persistence() {
    let mut app = ServerApp::new(RuntimeConfig::default());
    let mut connection = ServerApp::new_connection(ClientProtocol::Resp);

    let reply = ingress_connection_bytes(&mut app, &mut connection, &resp_command(&[b"INFO"]))
        .expect("INFO should succeed");
    assert_that!(reply.len(), eq(1_usize));
    let body = decode_resp_bulk_payload(&reply[0]);
    assert_that!(body.contains("# Replication\r\n"), eq(true));
    assert_that!(body.contains("# Persistence\r\n"), eq(true));
}

#[rstest]
fn resp_info_supports_multiple_sections_and_includes_persistence() {
    let mut app = ServerApp::new(RuntimeConfig::default());
    let mut connection = ServerApp::new_connection(ClientProtocol::Resp);

    let _ = ingress_connection_bytes(
        &mut app,
        &mut connection,
        &resp_command(&[b"SET", b"info:multi:key", b"value"]),
    )
    .expect("SET should succeed");

    let reply = ingress_connection_bytes(
        &mut app,
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

    let reply = ingress_connection_bytes(
        &mut app,
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

    let _ = ingress_connection_bytes(
        &mut app,
        &mut connection,
        &resp_command(&[b"REPLCONF", b"LISTENING-PORT", b"7001"]),
    )
    .expect("REPLCONF LISTENING-PORT should succeed");
    let _ = ingress_connection_bytes(
        &mut app,
        &mut connection,
        &resp_command(&[b"SET", b"ack:key", b"ack:value"]),
    )
    .expect("SET should succeed");

    let ack_reply = ingress_connection_bytes(
        &mut app,
        &mut connection,
        &resp_command(&[b"REPLCONF", b"ACK", b"999"]),
    )
    .expect("REPLCONF ACK should parse");
    assert_that!(&ack_reply, eq(&Vec::<Vec<u8>>::new()));
    assert_that!(
        app.replication_guard_with_latest_journal().last_acked_lsn(),
        eq(1_u64)
    );

    let info = ingress_connection_bytes(
        &mut app,
        &mut connection,
        &resp_command(&[b"INFO", b"REPLICATION"]),
    )
    .expect("INFO REPLICATION should succeed");
    let body = decode_resp_bulk_payload(&info[0]);
    assert_that!(body.contains("last_ack_lsn:1\r\n"), eq(true));
}

#[rstest]
fn resp_replconf_ack_without_registered_endpoint_is_tracked_via_implicit_endpoint() {
    let mut app = ServerApp::new(RuntimeConfig::default());
    let mut connection = ServerApp::new_connection(ClientProtocol::Resp);

    let _ = ingress_connection_bytes(
        &mut app,
        &mut connection,
        &resp_command(&[b"SET", b"ack:key", b"ack:value"]),
    )
    .expect("SET should succeed");

    let ack_reply = ingress_connection_bytes(
        &mut app,
        &mut connection,
        &resp_command(&[b"REPLCONF", b"ACK", b"999"]),
    )
    .expect("REPLCONF ACK should parse");
    assert_that!(&ack_reply, eq(&Vec::<Vec<u8>>::new()));
    assert_that!(
        app.replication_guard_with_latest_journal().last_acked_lsn(),
        eq(1_u64)
    );

    let info = ingress_connection_bytes(
        &mut app,
        &mut connection,
        &resp_command(&[b"INFO", b"REPLICATION"]),
    )
    .expect("INFO REPLICATION should succeed");
    let body = decode_resp_bulk_payload(&info[0]);
    assert_that!(body.contains("connected_slaves:1\r\n"), eq(true));
    assert_that!(body.contains("last_ack_lsn:1\r\n"), eq(true));

    let wait = ingress_connection_bytes(
        &mut app,
        &mut connection,
        &resp_command(&[b"WAIT", b"1", b"0"]),
    )
    .expect("WAIT should execute");
    assert_that!(&wait, eq(&vec![b":1\r\n".to_vec()]));
}

#[rstest]
fn resp_replconf_ack_requires_standalone_pair() {
    let mut app = ServerApp::new(RuntimeConfig::default());
    let mut connection = ServerApp::new_connection(ClientProtocol::Resp);

    let reply = ingress_connection_bytes(
        &mut app,
        &mut connection,
        &resp_command(&[b"REPLCONF", b"ACK", b"1", b"LISTENING-PORT", b"7001"]),
    )
    .expect("invalid REPLCONF ACK shape should parse");
    assert_that!(&reply, eq(&vec![b"-ERR syntax error\r\n".to_vec()]));

    let info = ingress_connection_bytes(
        &mut app,
        &mut connection,
        &resp_command(&[b"INFO", b"REPLICATION"]),
    )
    .expect("INFO REPLICATION should succeed");
    let body = decode_resp_bulk_payload(&info[0]);
    assert_that!(body.contains("connected_slaves:0\r\n"), eq(true));
}

#[rstest]
fn resp_replconf_client_id_is_stored_and_requires_standalone_pair() {
    let mut app = ServerApp::new(RuntimeConfig::default());
    let mut connection = ServerApp::new_connection(ClientProtocol::Resp);

    let ok_reply = ingress_connection_bytes(
        &mut app,
        &mut connection,
        &resp_command(&[b"REPLCONF", b"CLIENT-ID", b"replica-1"]),
    )
    .expect("REPLCONF CLIENT-ID should succeed");
    assert_that!(&ok_reply, eq(&vec![b"+OK\r\n".to_vec()]));
    assert_that!(
        &connection.replica_client_id,
        eq(&Some("replica-1".to_owned()))
    );

    let error_reply = ingress_connection_bytes(
        &mut app,
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

    let bad_int_reply = ingress_connection_bytes(
        &mut app,
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

    let mixed_reply = ingress_connection_bytes(
        &mut app,
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

    let ok_reply = ingress_connection_bytes(
        &mut app,
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

    let wait = ingress_connection_bytes(
        &mut app,
        &mut connection,
        &resp_command(&[b"WAIT", b"1", b"0"]),
    )
    .expect("WAIT should execute");
    assert_that!(&wait, eq(&vec![b":0\r\n".to_vec()]));
}

#[rstest]
fn resp_wait_counts_replicas_after_ack_reaches_current_offset() {
    let mut app = ServerApp::new(RuntimeConfig::default());
    let mut connection = ServerApp::new_connection(ClientProtocol::Resp);

    let _ = ingress_connection_bytes(
        &mut app,
        &mut connection,
        &resp_command(&[b"REPLCONF", b"LISTENING-PORT", b"7001"]),
    )
    .expect("REPLCONF LISTENING-PORT should succeed");
    let _ = ingress_connection_bytes(
        &mut app,
        &mut connection,
        &resp_command(&[b"SET", b"wait:key", b"v1"]),
    )
    .expect("SET should succeed");

    let wait_before_ack = ingress_connection_bytes(
        &mut app,
        &mut connection,
        &resp_command(&[b"WAIT", b"1", b"0"]),
    )
    .expect("WAIT should execute");
    assert_that!(&wait_before_ack, eq(&vec![b":0\r\n".to_vec()]));

    let _ = ingress_connection_bytes(
        &mut app,
        &mut connection,
        &resp_command(&[b"REPLCONF", b"ACK", b"1"]),
    )
    .expect("REPLCONF ACK should parse");

    let wait_after_ack = ingress_connection_bytes(
        &mut app,
        &mut connection,
        &resp_command(&[b"WAIT", b"1", b"0"]),
    )
    .expect("WAIT should execute");
    assert_that!(&wait_after_ack, eq(&vec![b":1\r\n".to_vec()]));
}

#[rstest]
fn resp_wait_counts_only_replicas_that_acked_target_offset() {
    let mut app = ServerApp::new(RuntimeConfig::default());
    let mut replica1 = ServerApp::new_connection(ClientProtocol::Resp);
    let mut replica2 = ServerApp::new_connection(ClientProtocol::Resp);
    let mut client = ServerApp::new_connection(ClientProtocol::Resp);

    let _ = ingress_connection_bytes(
        &mut app,
        &mut replica1,
        &resp_command(&[b"REPLCONF", b"LISTENING-PORT", b"7001"]),
    )
    .expect("replica1 REPLCONF LISTENING-PORT should succeed");
    let _ = ingress_connection_bytes(
        &mut app,
        &mut replica2,
        &resp_command(&[b"REPLCONF", b"LISTENING-PORT", b"7002"]),
    )
    .expect("replica2 REPLCONF LISTENING-PORT should succeed");

    let _ = ingress_connection_bytes(
        &mut app,
        &mut client,
        &resp_command(&[b"SET", b"wait:key", b"value"]),
    )
    .expect("SET should succeed");

    let _ = ingress_connection_bytes(
        &mut app,
        &mut replica1,
        &resp_command(&[b"REPLCONF", b"ACK", b"1"]),
    )
    .expect("replica1 ACK should parse");

    let wait_one =
        ingress_connection_bytes(&mut app, &mut client, &resp_command(&[b"WAIT", b"2", b"0"]))
            .expect("WAIT should execute");
    assert_that!(&wait_one, eq(&vec![b":1\r\n".to_vec()]));

    let _ = ingress_connection_bytes(
        &mut app,
        &mut replica2,
        &resp_command(&[b"REPLCONF", b"ACK", b"1"]),
    )
    .expect("replica2 ACK should parse");

    let wait_two =
        ingress_connection_bytes(&mut app, &mut client, &resp_command(&[b"WAIT", b"2", b"0"]))
            .expect("WAIT should execute");
    assert_that!(&wait_two, eq(&vec![b":2\r\n".to_vec()]));
}

#[rstest]
fn resp_wait_honors_timeout_when_required_replica_count_is_unmet() {
    let mut app = ServerApp::new(RuntimeConfig::default());
    let mut replica1 = ServerApp::new_connection(ClientProtocol::Resp);
    let mut replica2 = ServerApp::new_connection(ClientProtocol::Resp);
    let mut client = ServerApp::new_connection(ClientProtocol::Resp);

    let _ = ingress_connection_bytes(
        &mut app,
        &mut replica1,
        &resp_command(&[b"REPLCONF", b"LISTENING-PORT", b"7001"]),
    )
    .expect("replica1 REPLCONF LISTENING-PORT should succeed");
    let _ = ingress_connection_bytes(
        &mut app,
        &mut replica2,
        &resp_command(&[b"REPLCONF", b"LISTENING-PORT", b"7002"]),
    )
    .expect("replica2 REPLCONF LISTENING-PORT should succeed");
    let _ = ingress_connection_bytes(
        &mut app,
        &mut client,
        &resp_command(&[b"SET", b"wait:key", b"value"]),
    )
    .expect("SET should succeed");
    let _ = ingress_connection_bytes(
        &mut app,
        &mut replica1,
        &resp_command(&[b"REPLCONF", b"ACK", b"1"]),
    )
    .expect("replica1 ACK should parse");

    let started = std::time::Instant::now();
    let wait = ingress_connection_bytes(
        &mut app,
        &mut client,
        &resp_command(&[b"WAIT", b"2", b"12"]),
    )
    .expect("WAIT should execute");
    let elapsed = started.elapsed();
    assert_that!(&wait, eq(&vec![b":1\r\n".to_vec()]));
    assert_that!(elapsed >= std::time::Duration::from_millis(6), eq(true));
}

#[rstest]
fn resp_wait_reports_actual_acked_replica_count_even_when_requested_is_lower() {
    let mut app = ServerApp::new(RuntimeConfig::default());
    let mut replica1 = ServerApp::new_connection(ClientProtocol::Resp);
    let mut replica2 = ServerApp::new_connection(ClientProtocol::Resp);
    let mut client = ServerApp::new_connection(ClientProtocol::Resp);

    let _ = ingress_connection_bytes(
        &mut app,
        &mut replica1,
        &resp_command(&[b"REPLCONF", b"LISTENING-PORT", b"7001"]),
    )
    .expect("replica1 REPLCONF LISTENING-PORT should succeed");
    let _ = ingress_connection_bytes(
        &mut app,
        &mut replica2,
        &resp_command(&[b"REPLCONF", b"LISTENING-PORT", b"7002"]),
    )
    .expect("replica2 REPLCONF LISTENING-PORT should succeed");
    let _ = ingress_connection_bytes(
        &mut app,
        &mut client,
        &resp_command(&[b"SET", b"wait:key", b"value"]),
    )
    .expect("SET should succeed");
    let _ = ingress_connection_bytes(
        &mut app,
        &mut replica1,
        &resp_command(&[b"REPLCONF", b"ACK", b"1"]),
    )
    .expect("replica1 ACK should parse");
    let _ = ingress_connection_bytes(
        &mut app,
        &mut replica2,
        &resp_command(&[b"REPLCONF", b"ACK", b"1"]),
    )
    .expect("replica2 ACK should parse");

    let wait =
        ingress_connection_bytes(&mut app, &mut client, &resp_command(&[b"WAIT", b"1", b"0"]))
            .expect("WAIT should execute");
    assert_that!(&wait, eq(&vec![b":2\r\n".to_vec()]));
}

#[rstest]
fn resp_wait_does_not_reuse_ack_after_replica_re_registers() {
    let mut app = ServerApp::new(RuntimeConfig::default());
    let mut replica = ServerApp::new_connection(ClientProtocol::Resp);
    let mut client = ServerApp::new_connection(ClientProtocol::Resp);

    let _ = ingress_connection_bytes(
        &mut app,
        &mut replica,
        &resp_command(&[b"REPLCONF", b"LISTENING-PORT", b"7001"]),
    )
    .expect("replica REPLCONF LISTENING-PORT should succeed");
    let _ = ingress_connection_bytes(
        &mut app,
        &mut client,
        &resp_command(&[b"SET", b"wait:key", b"value"]),
    )
    .expect("SET should succeed");
    let _ = ingress_connection_bytes(
        &mut app,
        &mut replica,
        &resp_command(&[b"REPLCONF", b"ACK", b"1"]),
    )
    .expect("replica ACK should parse");

    let wait_after_ack =
        ingress_connection_bytes(&mut app, &mut client, &resp_command(&[b"WAIT", b"1", b"0"]))
            .expect("WAIT should execute");
    assert_that!(&wait_after_ack, eq(&vec![b":1\r\n".to_vec()]));

    let _ = ingress_connection_bytes(
        &mut app,
        &mut replica,
        &resp_command(&[b"REPLCONF", b"LISTENING-PORT", b"7001"]),
    )
    .expect("replica REPLCONF LISTENING-PORT re-registration should succeed");

    let wait_after_reregister =
        ingress_connection_bytes(&mut app, &mut client, &resp_command(&[b"WAIT", b"1", b"0"]))
            .expect("WAIT should execute");
    assert_that!(&wait_after_reregister, eq(&vec![b":0\r\n".to_vec()]));
}

#[rstest]
fn resp_info_replication_recomputes_last_ack_after_replica_reregister() {
    let mut app = ServerApp::new(RuntimeConfig::default());
    let mut replica1 = ServerApp::new_connection(ClientProtocol::Resp);
    let mut replica2 = ServerApp::new_connection(ClientProtocol::Resp);
    let mut client = ServerApp::new_connection(ClientProtocol::Resp);

    let _ = ingress_connection_bytes(
        &mut app,
        &mut replica1,
        &resp_command(&[b"REPLCONF", b"LISTENING-PORT", b"7001"]),
    )
    .expect("replica1 REPLCONF LISTENING-PORT should succeed");
    let _ = ingress_connection_bytes(
        &mut app,
        &mut replica2,
        &resp_command(&[b"REPLCONF", b"LISTENING-PORT", b"7002"]),
    )
    .expect("replica2 REPLCONF LISTENING-PORT should succeed");
    let _ = ingress_connection_bytes(
        &mut app,
        &mut client,
        &resp_command(&[b"SET", b"info:ack:key1", b"value1"]),
    )
    .expect("first SET should succeed");
    let _ = ingress_connection_bytes(
        &mut app,
        &mut replica1,
        &resp_command(&[b"REPLCONF", b"ACK", b"1"]),
    )
    .expect("replica1 first ACK should parse");
    let _ = ingress_connection_bytes(
        &mut app,
        &mut replica2,
        &resp_command(&[b"REPLCONF", b"ACK", b"1"]),
    )
    .expect("replica2 first ACK should parse");
    let _ = ingress_connection_bytes(
        &mut app,
        &mut client,
        &resp_command(&[b"SET", b"info:ack:key2", b"value2"]),
    )
    .expect("second SET should succeed");
    let _ = ingress_connection_bytes(
        &mut app,
        &mut replica1,
        &resp_command(&[b"REPLCONF", b"ACK", b"2"]),
    )
    .expect("replica1 second ACK should parse");

    let before = ingress_connection_bytes(
        &mut app,
        &mut client,
        &resp_command(&[b"INFO", b"REPLICATION"]),
    )
    .expect("INFO REPLICATION should succeed");
    let before_body = decode_resp_bulk_payload(&before[0]);
    assert_that!(before_body.contains("last_ack_lsn:2\r\n"), eq(true));

    let _ = ingress_connection_bytes(
        &mut app,
        &mut replica1,
        &resp_command(&[b"REPLCONF", b"LISTENING-PORT", b"7001"]),
    )
    .expect("replica1 REPLCONF LISTENING-PORT re-registration should succeed");

    let after = ingress_connection_bytes(
        &mut app,
        &mut client,
        &resp_command(&[b"INFO", b"REPLICATION"]),
    )
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

    let _ = ingress_connection_bytes(
        &mut app,
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
    let _ = ingress_connection_bytes(
        &mut app,
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

    let _ = ingress_connection_bytes(
        &mut app,
        &mut client,
        &resp_command(&[b"SET", b"info:replica:key", b"value"]),
    )
    .expect("SET should succeed");
    let _ = ingress_connection_bytes(
        &mut app,
        &mut replica1,
        &resp_command(&[b"REPLCONF", b"ACK", b"1"]),
    )
    .expect("replica1 ACK should parse");

    let info = ingress_connection_bytes(
        &mut app,
        &mut client,
        &resp_command(&[b"INFO", b"REPLICATION"]),
    )
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

    let reply = ingress_connection_bytes(
        &mut app,
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

    let first = ingress_connection_bytes(
        &mut app,
        &mut connection,
        &resp_command(&[b"REPLCONF", b"CAPA", b"dragonfly"]),
    )
    .expect("first REPLCONF CAPA dragonfly should succeed");
    let second = ingress_connection_bytes(
        &mut app,
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

    let handshake = ingress_connection_bytes(
        &mut app,
        &mut connection,
        &resp_command(&[b"REPLCONF", b"CAPA", b"dragonfly"]),
    )
    .expect("REPLCONF CAPA dragonfly should succeed");
    let sync_id = extract_sync_id_from_capa_reply(&handshake[0]).into_bytes();
    let master_id = app
        .replication_guard_with_latest_journal()
        .master_replid()
        .as_bytes()
        .to_vec();

    let _ = ingress_connection_bytes(
        &mut app,
        &mut connection,
        &resp_command(&[b"SET", b"flow:key", b"flow:value"]),
    )
    .expect("SET should succeed");

    let flow_reply = ingress_connection_bytes(
        &mut app,
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
    app.replication_guard_with_latest_journal().journal = InMemoryJournal::with_backlog(1);
    let mut connection = ServerApp::new_connection(ClientProtocol::Resp);

    let handshake = ingress_connection_bytes(
        &mut app,
        &mut connection,
        &resp_command(&[b"REPLCONF", b"CAPA", b"dragonfly"]),
    )
    .expect("REPLCONF CAPA dragonfly should succeed");
    let sync_id = extract_sync_id_from_capa_reply(&handshake[0]).into_bytes();
    let master_id = app
        .replication_guard_with_latest_journal()
        .master_replid()
        .as_bytes()
        .to_vec();

    let _ = ingress_connection_bytes(
        &mut app,
        &mut connection,
        &resp_command(&[b"SET", b"flow:key", b"v1"]),
    )
    .expect("first SET should succeed");
    let _ = ingress_connection_bytes(
        &mut app,
        &mut connection,
        &resp_command(&[b"SET", b"flow:key", b"v2"]),
    )
    .expect("second SET should succeed");

    let flow_reply = ingress_connection_bytes(
        &mut app,
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
    app.replication_guard_with_latest_journal().journal = InMemoryJournal::with_backlog(1);
    let mut connection = ServerApp::new_connection(ClientProtocol::Resp);

    let handshake = ingress_connection_bytes(
        &mut app,
        &mut connection,
        &resp_command(&[b"REPLCONF", b"CAPA", b"dragonfly"]),
    )
    .expect("REPLCONF CAPA dragonfly should succeed");
    let sync_id_text = extract_sync_id_from_capa_reply(&handshake[0]);
    let sync_id = sync_id_text.as_bytes().to_vec();
    let master_id = app
        .replication_guard_with_latest_journal()
        .master_replid()
        .as_bytes()
        .to_vec();

    let _ = ingress_connection_bytes(
        &mut app,
        &mut connection,
        &resp_command(&[b"SET", b"flow:stale:key", b"v1"]),
    )
    .expect("first SET should succeed");
    let _ = ingress_connection_bytes(
        &mut app,
        &mut connection,
        &resp_command(&[b"SET", b"flow:stale:key", b"v2"]),
    )
    .expect("second SET should succeed");

    let flow_reply = ingress_connection_bytes(
        &mut app,
        &mut connection,
        &resp_command(&[b"DFLY", b"FLOW", &master_id, &sync_id, b"0", b"0"]),
    )
    .expect("DFLY FLOW should succeed");
    let (sync_type, _) = extract_dfly_flow_reply(&flow_reply[0]);
    assert_that!(sync_type.as_str(), eq("FULL"));

    let flow_start_offset_is_none = {
        let replication = app.replication_guard_with_latest_journal();
        replication
            .state
            .sync_flow(&sync_id_text, 0)
            .expect("flow must be registered")
            .start_offset
            .is_none()
    };
    assert_that!(flow_start_offset_is_none, eq(true));
}

#[rstest]
fn resp_dfly_sync_and_startstable_update_replica_role_state() {
    let mut app = ServerApp::new(RuntimeConfig::default());
    let mut connection = ServerApp::new_connection(ClientProtocol::Resp);

    let _ = ingress_connection_bytes(
        &mut app,
        &mut connection,
        &resp_command(&[b"REPLCONF", b"LISTENING-PORT", b"7001"]),
    )
    .expect("REPLCONF LISTENING-PORT should succeed");

    let handshake = ingress_connection_bytes(
        &mut app,
        &mut connection,
        &resp_command(&[b"REPLCONF", b"CAPA", b"dragonfly"]),
    )
    .expect("REPLCONF CAPA dragonfly should succeed");
    let sync_id = extract_sync_id_from_capa_reply(&handshake[0]).into_bytes();
    let master_id = app
        .replication_guard_with_latest_journal()
        .master_replid()
        .as_bytes()
        .to_vec();
    for flow_id in 0..usize::from(app.config.shard_count.get()) {
        let flow_id_text = flow_id.to_string().into_bytes();
        let flow_reply = ingress_connection_bytes(
            &mut app,
            &mut connection,
            &resp_command(&[b"DFLY", b"FLOW", &master_id, &sync_id, &flow_id_text]),
        )
        .expect("DFLY FLOW should succeed");
        let (sync_type, eof_token) = extract_dfly_flow_reply(&flow_reply[0]);
        assert_that!(sync_type.as_str(), eq("FULL"));
        assert_that!(eof_token.len(), eq(40_usize));
    }

    let sync_reply = ingress_connection_bytes(
        &mut app,
        &mut connection,
        &resp_command(&[b"DFLY", b"SYNC", &sync_id]),
    )
    .expect("DFLY SYNC should succeed");
    assert_that!(&sync_reply, eq(&vec![b"+OK\r\n".to_vec()]));

    let role_after_sync =
        ingress_connection_bytes(&mut app, &mut connection, &resp_command(&[b"ROLE"]))
            .expect("ROLE should succeed");
    let role_payload = std::str::from_utf8(&role_after_sync[0]).expect("ROLE is UTF-8");
    assert_that!(role_payload.contains("full_sync"), eq(true));

    let stable_reply = ingress_connection_bytes(
        &mut app,
        &mut connection,
        &resp_command(&[b"DFLY", b"STARTSTABLE", &sync_id]),
    )
    .expect("DFLY STARTSTABLE should succeed");
    assert_that!(&stable_reply, eq(&vec![b"+OK\r\n".to_vec()]));

    let role_after_stable =
        ingress_connection_bytes(&mut app, &mut connection, &resp_command(&[b"ROLE"]))
            .expect("ROLE should succeed");
    let role_payload = std::str::from_utf8(&role_after_stable[0]).expect("ROLE is UTF-8");
    assert_that!(role_payload.contains("stable_sync"), eq(true));
}

#[rstest]
fn resp_dfly_sync_requires_all_flows_registered() {
    let mut app = ServerApp::new(RuntimeConfig::default());
    let mut connection = ServerApp::new_connection(ClientProtocol::Resp);

    let handshake = ingress_connection_bytes(
        &mut app,
        &mut connection,
        &resp_command(&[b"REPLCONF", b"CAPA", b"dragonfly"]),
    )
    .expect("REPLCONF CAPA dragonfly should succeed");
    let sync_id = extract_sync_id_from_capa_reply(&handshake[0]).into_bytes();
    let master_id = app
        .replication_guard_with_latest_journal()
        .master_replid()
        .as_bytes()
        .to_vec();

    let flow_reply = ingress_connection_bytes(
        &mut app,
        &mut connection,
        &resp_command(&[b"DFLY", b"FLOW", &master_id, &sync_id, b"0"]),
    )
    .expect("first DFLY FLOW should succeed");
    let (sync_type, _) = extract_dfly_flow_reply(&flow_reply[0]);
    assert_that!(sync_type.as_str(), eq("FULL"));

    let sync_reply = ingress_connection_bytes(
        &mut app,
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

    let _ = ingress_connection_bytes(
        &mut app,
        &mut connection,
        &resp_command(&[b"SET", b"ro:key", b"v1"]),
    )
    .expect("first SET should succeed");
    let _ = ingress_connection_bytes(
        &mut app,
        &mut connection,
        &resp_command(&[b"SET", b"ro:key", b"v2"]),
    )
    .expect("second SET should succeed");

    let reply = ingress_connection_bytes(
        &mut app,
        &mut connection,
        &resp_command(&[b"DFLY", b"REPLICAOFFSET"]),
    )
    .expect("DFLY REPLICAOFFSET should execute");
    let expected_entry = format!(
        ":{}\r\n",
        app.replication_guard_with_latest_journal()
            .replication_offset()
    );
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

    let handshake = ingress_connection_bytes(
        &mut app,
        &mut connection,
        &resp_command(&[b"REPLCONF", b"CAPA", b"dragonfly"]),
    )
    .expect("REPLCONF CAPA dragonfly should succeed");
    let sync_id = extract_sync_id_from_capa_reply(&handshake[0]).into_bytes();
    let master_id = app
        .replication_guard_with_latest_journal()
        .master_replid()
        .as_bytes()
        .to_vec();

    for flow_id in 0..usize::from(app.config.shard_count.get()) {
        let flow_id_text = flow_id.to_string().into_bytes();
        let _ = ingress_connection_bytes(
            &mut app,
            &mut connection,
            &resp_command(&[b"DFLY", b"FLOW", &master_id, &sync_id, &flow_id_text]),
        )
        .expect("DFLY FLOW should succeed");
    }
    let _ = ingress_connection_bytes(
        &mut app,
        &mut connection,
        &resp_command(&[b"DFLY", b"SYNC", &sync_id]),
    )
    .expect("DFLY SYNC should succeed");

    let replay_flow = ingress_connection_bytes(
        &mut app,
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

    let handshake = ingress_connection_bytes(
        &mut app,
        &mut connection,
        &resp_command(&[b"REPLCONF", b"CAPA", b"dragonfly"]),
    )
    .expect("REPLCONF CAPA dragonfly should succeed");
    let sync_id = extract_sync_id_from_capa_reply(&handshake[0]).into_bytes();
    let master_id = app
        .replication_guard_with_latest_journal()
        .master_replid()
        .as_bytes()
        .to_vec();

    let _ = ingress_connection_bytes(
        &mut app,
        &mut connection,
        &resp_command(&[b"SET", b"vector:key", b"vector:value"]),
    )
    .expect("SET should succeed");

    let lsn_vec = vec!["0"; usize::from(app.config.shard_count.get())]
        .join("-")
        .into_bytes();
    let flow_reply = ingress_connection_bytes(
        &mut app,
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

    let handshake = ingress_connection_bytes(
        &mut app,
        &mut connection,
        &resp_command(&[b"REPLCONF", b"CAPA", b"dragonfly"]),
    )
    .expect("REPLCONF CAPA dragonfly should succeed");
    let sync_id = extract_sync_id_from_capa_reply(&handshake[0]).into_bytes();
    let master_id = app
        .replication_guard_with_latest_journal()
        .master_replid()
        .as_bytes()
        .to_vec();
    let lsn_vec = vec!["0"; usize::from(app.config.shard_count.get())]
        .join("-")
        .into_bytes();

    let flow_reply = ingress_connection_bytes(
        &mut app,
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

    let handshake = ingress_connection_bytes(
        &mut app,
        &mut connection,
        &resp_command(&[b"REPLCONF", b"CAPA", b"dragonfly"]),
    )
    .expect("REPLCONF CAPA dragonfly should succeed");
    let sync_id = extract_sync_id_from_capa_reply(&handshake[0]).into_bytes();
    let master_id = app
        .replication_guard_with_latest_journal()
        .master_replid()
        .as_bytes()
        .to_vec();
    let first = ingress_connection_bytes(
        &mut app,
        &mut connection,
        &resp_command(&[b"DFLY", b"FLOW", &master_id, &sync_id, b"0"]),
    )
    .expect("first DFLY FLOW should succeed");
    let (sync_type, _) = extract_dfly_flow_reply(&first[0]);
    assert_that!(sync_type.as_str(), eq("FULL"));

    let second = ingress_connection_bytes(
        &mut app,
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

    let register = ingress_connection_bytes(
        &mut app,
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

    let role_after_register =
        ingress_connection_bytes(&mut app, &mut connection, &resp_command(&[b"ROLE"]))
            .expect("ROLE should succeed");
    let role_payload = std::str::from_utf8(&role_after_register[0]).expect("ROLE is UTF-8");
    assert_that!(role_payload.contains("10.0.0.2"), eq(true));
    assert_that!(role_payload.contains("7001"), eq(true));
    assert_that!(role_payload.contains("preparation"), eq(true));

    let _ = ingress_connection_bytes(
        &mut app,
        &mut connection,
        &resp_command(&[b"PSYNC", b"?", b"-1"]),
    )
    .expect("PSYNC should succeed");

    let role_after_fullsync =
        ingress_connection_bytes(&mut app, &mut connection, &resp_command(&[b"ROLE"]))
            .expect("ROLE should succeed after PSYNC");
    let role_payload = std::str::from_utf8(&role_after_fullsync[0]).expect("ROLE is UTF-8");
    assert_that!(role_payload.contains("full_sync"), eq(true));

    let ack_reply = ingress_connection_bytes(
        &mut app,
        &mut connection,
        &resp_command(&[b"REPLCONF", b"ACK", b"0"]),
    )
    .expect("REPLCONF ACK should parse");
    assert_that!(&ack_reply, eq(&Vec::<Vec<u8>>::new()));

    let role_after_ack =
        ingress_connection_bytes(&mut app, &mut connection, &resp_command(&[b"ROLE"]))
            .expect("ROLE should succeed after ACK");
    let role_payload = std::str::from_utf8(&role_after_ack[0]).expect("ROLE is UTF-8");
    assert_that!(role_payload.contains("stable_sync"), eq(true));

    let info = ingress_connection_bytes(
        &mut app,
        &mut connection,
        &resp_command(&[b"INFO", b"REPLICATION"]),
    )
    .expect("INFO REPLICATION should succeed");
    let body = decode_resp_bulk_payload(&info[0]);
    assert_that!(body.contains("connected_slaves:1\r\n"), eq(true));
}

#[rstest]
fn resp_replconf_endpoint_identity_update_replaces_stale_endpoint_row() {
    let mut app = ServerApp::new(RuntimeConfig::default());
    let mut replica = ServerApp::new_connection(ClientProtocol::Resp);
    let mut client = ServerApp::new_connection(ClientProtocol::Resp);

    let _ = ingress_connection_bytes(
        &mut app,
        &mut replica,
        &resp_command(&[b"REPLCONF", b"LISTENING-PORT", b"7001"]),
    )
    .expect("replica REPLCONF LISTENING-PORT should succeed");
    let _ = ingress_connection_bytes(
        &mut app,
        &mut client,
        &resp_command(&[b"SET", b"replconf:identity:key", b"value"]),
    )
    .expect("SET should succeed");
    let _ = ingress_connection_bytes(
        &mut app,
        &mut replica,
        &resp_command(&[b"REPLCONF", b"ACK", b"1"]),
    )
    .expect("replica ACK should parse");
    assert_that!(
        app.replication_guard_with_latest_journal().last_acked_lsn(),
        eq(1_u64)
    );

    let _ = ingress_connection_bytes(
        &mut app,
        &mut replica,
        &resp_command(&[b"REPLCONF", b"IP-ADDRESS", b"10.0.0.8"]),
    )
    .expect("replica REPLCONF IP-ADDRESS should succeed");

    let info = ingress_connection_bytes(
        &mut app,
        &mut client,
        &resp_command(&[b"INFO", b"REPLICATION"]),
    )
    .expect("INFO REPLICATION should succeed");
    let body = decode_resp_bulk_payload(&info[0]);
    assert_that!(body.contains("connected_slaves:1\r\n"), eq(true));
    assert_that!(
        body.contains("slave0:ip=10.0.0.8,port=7001,state=preparation,lag=0\r\n"),
        eq(true)
    );
    assert_that!(body.contains("127.0.0.1"), eq(false));

    let wait =
        ingress_connection_bytes(&mut app, &mut client, &resp_command(&[b"WAIT", b"1", b"0"]))
            .expect("WAIT should execute");
    assert_that!(&wait, eq(&vec![b":0\r\n".to_vec()]));
}

#[rstest]
fn disconnect_connection_unregisters_replica_endpoint_and_ack_progress() {
    let mut app = ServerApp::new(RuntimeConfig::default());
    let mut replica = ServerApp::new_connection(ClientProtocol::Resp);
    let mut client = ServerApp::new_connection(ClientProtocol::Resp);

    let _ = ingress_connection_bytes(
        &mut app,
        &mut replica,
        &resp_command(&[b"REPLCONF", b"LISTENING-PORT", b"7001"]),
    )
    .expect("replica REPLCONF LISTENING-PORT should succeed");
    let _ = ingress_connection_bytes(
        &mut app,
        &mut client,
        &resp_command(&[b"SET", b"disconnect:replica:key", b"value"]),
    )
    .expect("SET should succeed");
    let _ = ingress_connection_bytes(
        &mut app,
        &mut replica,
        &resp_command(&[b"REPLCONF", b"ACK", b"1"]),
    )
    .expect("replica ACK should parse");
    assert_that!(
        app.replication_guard_with_latest_journal().last_acked_lsn(),
        eq(1_u64)
    );

    app.disconnect_connection(&mut replica);

    let info = ingress_connection_bytes(
        &mut app,
        &mut client,
        &resp_command(&[b"INFO", b"REPLICATION"]),
    )
    .expect("INFO REPLICATION should succeed");
    let body = decode_resp_bulk_payload(&info[0]);
    assert_that!(body.contains("connected_slaves:0\r\n"), eq(true));
    assert_that!(body.contains("last_ack_lsn:0\r\n"), eq(true));

    let wait =
        ingress_connection_bytes(&mut app, &mut client, &resp_command(&[b"WAIT", b"1", b"0"]))
            .expect("WAIT should execute");
    assert_that!(&wait, eq(&vec![b":0\r\n".to_vec()]));
}

#[rstest]
fn resp_psync_with_unknown_replid_returns_full_resync_header() {
    let mut app = ServerApp::new(RuntimeConfig::default());
    let mut connection = ServerApp::new_connection(ClientProtocol::Resp);

    let reply = ingress_connection_bytes(
        &mut app,
        &mut connection,
        &resp_command(&[b"PSYNC", b"?", b"-1"]),
    )
    .expect("PSYNC should succeed");
    let expected = format!(
        "+FULLRESYNC {} 0\r\n",
        app.replication_guard_with_latest_journal().master_replid()
    )
    .into_bytes();
    assert_that!(&reply, eq(&vec![expected]));
}

#[rstest]
fn resp_psync_returns_continue_when_offset_is_available() {
    let mut app = ServerApp::new(RuntimeConfig::default());
    let mut connection = ServerApp::new_connection(ClientProtocol::Resp);

    let _ = ingress_connection_bytes(
        &mut app,
        &mut connection,
        &resp_command(&[b"SET", b"psync:key", b"psync:value"]),
    )
    .expect("SET should succeed");

    let replid = app
        .replication_guard_with_latest_journal()
        .master_replid()
        .as_bytes()
        .to_vec();
    let reply = ingress_connection_bytes(
        &mut app,
        &mut connection,
        &resp_command(&[b"PSYNC", &replid, b"0"]),
    )
    .expect("PSYNC should succeed");
    assert_that!(&reply, eq(&vec![b"+CONTINUE\r\n".to_vec()]));
}

#[rstest]
fn resp_psync_falls_back_to_full_resync_when_backlog_is_stale() {
    let mut app = ServerApp::new(RuntimeConfig::default());
    app.replication_guard_with_latest_journal().journal = InMemoryJournal::with_backlog(1);
    let mut connection = ServerApp::new_connection(ClientProtocol::Resp);

    let _ = ingress_connection_bytes(
        &mut app,
        &mut connection,
        &resp_command(&[b"SET", b"stale:key", b"v1"]),
    )
    .expect("first SET should succeed");
    let _ = ingress_connection_bytes(
        &mut app,
        &mut connection,
        &resp_command(&[b"SET", b"stale:key", b"v2"]),
    )
    .expect("second SET should succeed");

    let replid = app
        .replication_guard_with_latest_journal()
        .master_replid()
        .as_bytes()
        .to_vec();
    let reply = ingress_connection_bytes(
        &mut app,
        &mut connection,
        &resp_command(&[b"PSYNC", &replid, b"0"]),
    )
    .expect("PSYNC should succeed");
    let expected = format!(
        "+FULLRESYNC {} 2\r\n",
        app.replication_guard_with_latest_journal().master_replid()
    )
    .into_bytes();
    assert_that!(&reply, eq(&vec![expected]));
}

