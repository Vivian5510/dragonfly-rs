use super::*;

#[rstest]
fn server_snapshot_roundtrip_restores_data_across_databases() {
    let path = unique_test_snapshot_path("snapshot-roundtrip-cross-db");
    let path_bytes = path.to_string_lossy().as_bytes().to_vec();

    let mut source = ServerApp::new(RuntimeConfig::default());
    let mut source_connection = ServerApp::new_connection(ClientProtocol::Resp);

    let _ = ingress_connection_bytes(
        &mut source,
        &mut source_connection,
        b"*3\r\n$3\r\nSET\r\n$6\r\nshared\r\n$3\r\ndb0\r\n",
    )
    .expect("db0 SET should succeed");
    let _ = ingress_connection_bytes(
        &mut source,
        &mut source_connection,
        b"*2\r\n$6\r\nSELECT\r\n$1\r\n2\r\n",
    )
    .expect("SELECT 2 should succeed");
    let _ = ingress_connection_bytes(
        &mut source,
        &mut source_connection,
        b"*3\r\n$3\r\nSET\r\n$6\r\nshared\r\n$3\r\ndb2\r\n",
    )
    .expect("db2 SET should succeed");

    let save_reply = ingress_connection_bytes(
        &mut source,
        &mut source_connection,
        &resp_command(&[b"DFLY", b"SAVE", &path_bytes]),
    )
    .expect("DFLY SAVE should succeed");
    assert_that!(&save_reply, eq(&vec![b"+OK\r\n".to_vec()]));

    let mut restored = ServerApp::new(RuntimeConfig::default());
    let mut restored_connection = ServerApp::new_connection(ClientProtocol::Resp);
    let load_reply = ingress_connection_bytes(
        &mut restored,
        &mut restored_connection,
        &resp_command(&[b"DFLY", b"LOAD", &path_bytes]),
    )
    .expect("DFLY LOAD should succeed");
    assert_that!(&load_reply, eq(&vec![b"+OK\r\n".to_vec()]));

    let db0_get = ingress_connection_bytes(
        &mut restored,
        &mut restored_connection,
        b"*2\r\n$3\r\nGET\r\n$6\r\nshared\r\n",
    )
    .expect("db0 GET should succeed");
    assert_that!(&db0_get, eq(&vec![b"$3\r\ndb0\r\n".to_vec()]));

    let _ = ingress_connection_bytes(
        &mut restored,
        &mut restored_connection,
        b"*2\r\n$6\r\nSELECT\r\n$1\r\n2\r\n",
    )
    .expect("SELECT 2 should succeed after restore");
    let db2_get = ingress_connection_bytes(
        &mut restored,
        &mut restored_connection,
        b"*2\r\n$3\r\nGET\r\n$6\r\nshared\r\n",
    )
    .expect("db2 GET should succeed");
    assert_that!(&db2_get, eq(&vec![b"$3\r\ndb2\r\n".to_vec()]));

    let _ = std::fs::remove_file(path);
}

#[rstest]
fn server_snapshot_load_rejects_malformed_payload() {
    let path = unique_test_snapshot_path("snapshot-malformed");
    std::fs::write(&path, b"not-a-snapshot").expect("write malformed payload should succeed");
    let path_bytes = path.to_string_lossy().as_bytes().to_vec();
    let mut app = ServerApp::new(RuntimeConfig::default());
    let mut connection = ServerApp::new_connection(ClientProtocol::Resp);

    let reply = ingress_connection_bytes(
        &mut app,
        &mut connection,
        &resp_command(&[b"DFLY", b"LOAD", &path_bytes]),
    )
    .expect("DFLY LOAD should parse");
    assert_that!(reply.len(), eq(1_usize));
    let payload = std::str::from_utf8(&reply[0]).expect("reply should be valid UTF-8");
    assert_that!(payload.contains("snapshot payload error"), eq(true));

    let _ = std::fs::remove_file(path);
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
    let reply = ingress_connection_bytes(&mut app, &mut connection, &command)
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

    let reply = ingress_connection_bytes(
        &mut app,
        &mut connection,
        &resp_command(&[b"CLUSTER", b"MYID"]),
    )
    .expect("CLUSTER MYID should execute");
    assert_that!(reply.len(), eq(1_usize));
    let body = decode_resp_bulk_payload(&reply[0]);
    assert_that!(body.as_str(), eq(app.cluster_read_guard().node_id.as_str()));
}

#[rstest]
fn resp_cluster_commands_are_rejected_when_cluster_mode_is_disabled() {
    let mut app = ServerApp::new(RuntimeConfig::default());
    let mut connection = ServerApp::new_connection(ClientProtocol::Resp);

    let reply = ingress_connection_bytes(
        &mut app,
        &mut connection,
        &resp_command(&[b"CLUSTER", b"INFO"]),
    )
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

    let reply = ingress_connection_bytes(&mut app, &mut connection, &resp_command(&[b"READONLY"]))
        .expect("READONLY should execute");
    assert_that!(&reply, eq(&vec![b"+OK\r\n".to_vec()]));
}

#[rstest]
fn resp_readwrite_requires_cluster_emulated_mode() {
    let mut disabled = ServerApp::new(RuntimeConfig::default());
    let mut disabled_connection = ServerApp::new_connection(ClientProtocol::Resp);
    let disabled_reply = ingress_connection_bytes(
        &mut disabled,
        &mut disabled_connection,
        &resp_command(&[b"READWRITE"]),
    )
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
    let emulated_reply = ingress_connection_bytes(
        &mut emulated,
        &mut emulated_connection,
        &resp_command(&[b"READWRITE"]),
    )
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

    let reply = ingress_connection_bytes(
        &mut app,
        &mut connection,
        &resp_command(&[b"CLUSTER", b"HELP"]),
    )
    .expect("CLUSTER HELP should execute");
    assert_that!(reply.len(), eq(1_usize));
    let payload = std::str::from_utf8(&reply[0]).expect("payload must be UTF-8");
    assert_that!(payload.starts_with("*15\r\n"), eq(true));
    assert_that!(payload.contains("$5\r\nSLOTS\r\n"), eq(true));
    assert_that!(payload.contains("COUNTKEYSINSLOT <slot>\r\n"), eq(true));
    assert_that!(
        payload.contains("GETKEYSINSLOT <slot> <count>\r\n"),
        eq(true)
    );
    assert_that!(payload.contains("$5\r\nNODES\r\n"), eq(true));
    assert_that!(payload.contains("$4\r\nINFO\r\n"), eq(true));
    assert_that!(payload.contains("$4\r\nHELP\r\n"), eq(true));
}

#[rstest]
fn resp_cluster_getkeysinslot_returns_slot_keys_using_layered_index() {
    let config = RuntimeConfig {
        cluster_mode: ClusterMode::Emulated,
        ..RuntimeConfig::default()
    };
    let mut app = ServerApp::new(config);
    let mut connection = ServerApp::new_connection(ClientProtocol::Resp);

    let first_key = b"{42}:a".to_vec();
    let second_key = b"{42}:b".to_vec();
    let expired_key = b"{42}:z".to_vec();
    let other_slot_key = b"{43}:x".to_vec();
    let slot = key_slot(&first_key);

    let _ = ingress_connection_bytes(
        &mut app,
        &mut connection,
        &resp_command(&[b"SET", &first_key, b"v1"]),
    )
    .expect("SET should succeed");
    let _ = ingress_connection_bytes(
        &mut app,
        &mut connection,
        &resp_command(&[b"SET", &second_key, b"v2"]),
    )
    .expect("SET should succeed");
    let _ = ingress_connection_bytes(
        &mut app,
        &mut connection,
        &resp_command(&[b"SET", &other_slot_key, b"x"]),
    )
    .expect("SET should succeed");
    let _ = ingress_connection_bytes(
        &mut app,
        &mut connection,
        &resp_command(&[b"SET", &expired_key, b"gone"]),
    )
    .expect("SET should succeed");
    let _ = ingress_connection_bytes(
        &mut app,
        &mut connection,
        &resp_command(&[b"EXPIRE", &expired_key, b"0"]),
    )
    .expect("EXPIRE should succeed");

    let slot_text = slot.to_string().into_bytes();
    let limited = ingress_connection_bytes(
        &mut app,
        &mut connection,
        &resp_command(&[b"CLUSTER", b"GETKEYSINSLOT", &slot_text, b"1"]),
    )
    .expect("CLUSTER GETKEYSINSLOT should execute");
    assert_that!(
        &limited,
        eq(&vec![
            format!(
                "*1\r\n${}\r\n{}\r\n",
                first_key.len(),
                String::from_utf8_lossy(&first_key)
            )
            .into_bytes()
        ])
    );

    let full = ingress_connection_bytes(
        &mut app,
        &mut connection,
        &resp_command(&[b"CLUSTER", b"GETKEYSINSLOT", &slot_text, b"10"]),
    )
    .expect("CLUSTER GETKEYSINSLOT should execute");
    assert_that!(
        &full,
        eq(&vec![
            format!(
                "*2\r\n${}\r\n{}\r\n${}\r\n{}\r\n",
                first_key.len(),
                String::from_utf8_lossy(&first_key),
                second_key.len(),
                String::from_utf8_lossy(&second_key)
            )
            .into_bytes()
        ])
    );
}

#[rstest]
fn resp_cluster_getkeysinslot_validates_arguments() {
    let config = RuntimeConfig {
        cluster_mode: ClusterMode::Emulated,
        ..RuntimeConfig::default()
    };
    let mut app = ServerApp::new(config);
    let mut connection = ServerApp::new_connection(ClientProtocol::Resp);

    let arity = ingress_connection_bytes(
        &mut app,
        &mut connection,
        &resp_command(&[b"CLUSTER", b"GETKEYSINSLOT", b"42"]),
    )
    .expect("CLUSTER GETKEYSINSLOT should parse");
    assert_that!(
        &arity,
        eq(&vec![
            b"-ERR wrong number of arguments for 'CLUSTER GETKEYSINSLOT' command\r\n".to_vec()
        ])
    );

    let invalid_slot = ingress_connection_bytes(
        &mut app,
        &mut connection,
        &resp_command(&[b"CLUSTER", b"GETKEYSINSLOT", b"abc", b"10"]),
    )
    .expect("CLUSTER GETKEYSINSLOT should parse");
    assert_that!(
        &invalid_slot,
        eq(&vec![
            b"-ERR value is not an integer or out of range\r\n".to_vec()
        ])
    );

    let out_of_range = ingress_connection_bytes(
        &mut app,
        &mut connection,
        &resp_command(&[b"CLUSTER", b"GETKEYSINSLOT", b"20000", b"10"]),
    )
    .expect("CLUSTER GETKEYSINSLOT should parse");
    assert_that!(
        &out_of_range,
        eq(&vec![b"-ERR slot is out of range\r\n".to_vec()])
    );

    let invalid_count = ingress_connection_bytes(
        &mut app,
        &mut connection,
        &resp_command(&[b"CLUSTER", b"GETKEYSINSLOT", b"42", b"-1"]),
    )
    .expect("CLUSTER GETKEYSINSLOT should parse");
    assert_that!(
        &invalid_count,
        eq(&vec![
            b"-ERR value is not an integer or out of range\r\n".to_vec()
        ])
    );
}

#[rstest]
fn resp_cluster_countkeysinslot_returns_live_cardinality() {
    let config = RuntimeConfig {
        cluster_mode: ClusterMode::Emulated,
        ..RuntimeConfig::default()
    };
    let mut app = ServerApp::new(config);
    let mut connection = ServerApp::new_connection(ClientProtocol::Resp);

    let first_key = b"{7}:a".to_vec();
    let second_key = b"{7}:b".to_vec();
    let expired_key = b"{7}:z".to_vec();
    let other_slot_key = b"{8}:x".to_vec();
    let slot = key_slot(&first_key);

    let _ = ingress_connection_bytes(
        &mut app,
        &mut connection,
        &resp_command(&[b"SET", &first_key, b"v1"]),
    )
    .expect("SET should succeed");
    let _ = ingress_connection_bytes(
        &mut app,
        &mut connection,
        &resp_command(&[b"SET", &second_key, b"v2"]),
    )
    .expect("SET should succeed");
    let _ = ingress_connection_bytes(
        &mut app,
        &mut connection,
        &resp_command(&[b"SET", &other_slot_key, b"x"]),
    )
    .expect("SET should succeed");
    let _ = ingress_connection_bytes(
        &mut app,
        &mut connection,
        &resp_command(&[b"SET", &expired_key, b"gone"]),
    )
    .expect("SET should succeed");
    let _ = ingress_connection_bytes(
        &mut app,
        &mut connection,
        &resp_command(&[b"EXPIRE", &expired_key, b"0"]),
    )
    .expect("EXPIRE should succeed");

    let slot_text = slot.to_string().into_bytes();
    let reply = ingress_connection_bytes(
        &mut app,
        &mut connection,
        &resp_command(&[b"CLUSTER", b"COUNTKEYSINSLOT", &slot_text]),
    )
    .expect("CLUSTER COUNTKEYSINSLOT should execute");
    assert_that!(&reply, eq(&vec![b":2\r\n".to_vec()]));
}

#[rstest]
fn resp_cluster_countkeysinslot_validates_arguments() {
    let config = RuntimeConfig {
        cluster_mode: ClusterMode::Emulated,
        ..RuntimeConfig::default()
    };
    let mut app = ServerApp::new(config);
    let mut connection = ServerApp::new_connection(ClientProtocol::Resp);

    let arity = ingress_connection_bytes(
        &mut app,
        &mut connection,
        &resp_command(&[b"CLUSTER", b"COUNTKEYSINSLOT"]),
    )
    .expect("CLUSTER COUNTKEYSINSLOT should parse");
    assert_that!(
        &arity,
        eq(&vec![
            b"-ERR wrong number of arguments for 'CLUSTER COUNTKEYSINSLOT' command\r\n".to_vec()
        ])
    );

    let invalid_slot = ingress_connection_bytes(
        &mut app,
        &mut connection,
        &resp_command(&[b"CLUSTER", b"COUNTKEYSINSLOT", b"abc"]),
    )
    .expect("CLUSTER COUNTKEYSINSLOT should parse");
    assert_that!(
        &invalid_slot,
        eq(&vec![
            b"-ERR value is not an integer or out of range\r\n".to_vec()
        ])
    );

    let out_of_range = ingress_connection_bytes(
        &mut app,
        &mut connection,
        &resp_command(&[b"CLUSTER", b"COUNTKEYSINSLOT", b"20000"]),
    )
    .expect("CLUSTER COUNTKEYSINSLOT should parse");
    assert_that!(
        &out_of_range,
        eq(&vec![b"-ERR slot is out of range\r\n".to_vec()])
    );
}

#[rstest]
fn resp_cluster_shards_reports_single_master_descriptor() {
    let config = RuntimeConfig {
        cluster_mode: ClusterMode::Emulated,
        ..RuntimeConfig::default()
    };
    let mut app = ServerApp::new(config);
    app.cluster
        .write()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
        .set_owned_ranges(vec![
            SlotRange { start: 0, end: 99 },
            SlotRange {
                start: 200,
                end: 300,
            },
        ]);
    let mut connection = ServerApp::new_connection(ClientProtocol::Resp);

    let reply = ingress_connection_bytes(
        &mut app,
        &mut connection,
        &resp_command(&[b"CLUSTER", b"SHARDS"]),
    )
    .expect("CLUSTER SHARDS should execute");
    assert_that!(reply.len(), eq(1_usize));

    let payload = std::str::from_utf8(&reply[0]).expect("payload must be UTF-8");
    assert_that!(payload.starts_with("*1\r\n*4\r\n$5\r\nslots\r\n"), eq(true));
    assert_that!(payload.contains(":0\r\n:99\r\n:200\r\n:300\r\n"), eq(true));
    assert_that!(payload.contains("$5\r\nnodes\r\n"), eq(true));
    assert_that!(
        payload.contains(&app.cluster_read_guard().node_id),
        eq(true)
    );
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
    app.cluster
        .write()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
        .set_owned_ranges(vec![
            SlotRange { start: 0, end: 99 },
            SlotRange {
                start: 200,
                end: 300,
            },
        ]);
    let mut connection = ServerApp::new_connection(ClientProtocol::Resp);

    let command = resp_command(&[b"CLUSTER", b"SLOTS"]);
    let reply = ingress_connection_bytes(&mut app, &mut connection, &command)
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
    app.cluster
        .write()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
        .set_owned_ranges(vec![
            SlotRange { start: 0, end: 99 },
            SlotRange {
                start: 200,
                end: 300,
            },
        ]);
    let mut connection = ServerApp::new_connection(ClientProtocol::Resp);

    let reply = ingress_connection_bytes(
        &mut app,
        &mut connection,
        &resp_command(&[b"CLUSTER", b"INFO"]),
    )
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
    app.cluster
        .write()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
        .set_owned_ranges(vec![
            SlotRange { start: 0, end: 99 },
            SlotRange {
                start: 200,
                end: 300,
            },
        ]);
    let mut connection = ServerApp::new_connection(ClientProtocol::Resp);

    let reply = ingress_connection_bytes(
        &mut app,
        &mut connection,
        &resp_command(&[b"CLUSTER", b"NODES"]),
    )
    .expect("CLUSTER NODES should execute");
    let body = decode_resp_bulk_payload(&reply[0]);
    assert_that!(body.contains("myself,master"), eq(true));
    assert_that!(body.contains(&app.cluster_read_guard().node_id), eq(true));
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
    let reply =
        ingress_connection_bytes(&mut app, &mut connection, &request).expect("MSET should parse");
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
    app.cluster
        .write()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
        .set_owned_ranges(vec![owned]);

    let command = resp_command(&[b"GET", key]);
    let reply =
        ingress_connection_bytes(&mut app, &mut connection, &command).expect("GET should parse");
    let expected = vec![format!("-MOVED {slot} 127.0.0.1:7000\r\n").into_bytes()];
    assert_that!(&reply, eq(&expected));
}

#[rstest]
fn dfly_save_then_load_restores_snapshot_file() {
    let path = unique_test_snapshot_path("dfly-save-load");
    let path_bytes = path.to_string_lossy().as_bytes().to_vec();

    let mut source = ServerApp::new(RuntimeConfig::default());
    let mut source_conn = ServerApp::new_connection(ClientProtocol::Resp);
    let _ = ingress_connection_bytes(
        &mut source,
        &mut source_conn,
        b"*3\r\n$3\r\nSET\r\n$3\r\nfoo\r\n$3\r\nbar\r\n",
    )
    .expect("seed SET should succeed");

    let save = resp_command(&[b"DFLY", b"SAVE", &path_bytes]);
    let save_reply = ingress_connection_bytes(&mut source, &mut source_conn, &save)
        .expect("DFLY SAVE should execute");
    assert_that!(&save_reply, eq(&vec![b"+OK\r\n".to_vec()]));

    let mut restored = ServerApp::new(RuntimeConfig::default());
    let mut restored_conn = ServerApp::new_connection(ClientProtocol::Resp);
    let load = resp_command(&[b"DFLY", b"LOAD", &path_bytes]);
    let load_reply = ingress_connection_bytes(&mut restored, &mut restored_conn, &load)
        .expect("DFLY LOAD should execute");
    assert_that!(&load_reply, eq(&vec![b"+OK\r\n".to_vec()]));

    let get_reply = ingress_connection_bytes(
        &mut restored,
        &mut restored_conn,
        b"*2\r\n$3\r\nGET\r\n$3\r\nfoo\r\n",
    )
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
    let _ = ingress_connection_bytes(
        &mut source,
        &mut source_conn,
        &resp_command(&[b"SET", b"load:watch:key", b"seed"]),
    )
    .expect("seed key should be created for snapshot");
    let _ = ingress_connection_bytes(
        &mut source,
        &mut source_conn,
        &resp_command(&[b"DFLY", b"SAVE", &path_bytes]),
    )
    .expect("DFLY SAVE should succeed");

    let mut app = ServerApp::new(RuntimeConfig::default());
    let mut watch_conn = ServerApp::new_connection(ClientProtocol::Resp);
    let mut admin_conn = ServerApp::new_connection(ClientProtocol::Resp);

    let _ = ingress_connection_bytes(
        &mut app,
        &mut watch_conn,
        &resp_command(&[b"WATCH", b"load:watch:key"]),
    )
    .expect("WATCH should succeed");
    let load_reply = ingress_connection_bytes(
        &mut app,
        &mut admin_conn,
        &resp_command(&[b"DFLY", b"LOAD", &path_bytes]),
    )
    .expect("DFLY LOAD should execute");
    assert_that!(&load_reply, eq(&vec![b"+OK\r\n".to_vec()]));

    let _ = ingress_connection_bytes(&mut app, &mut watch_conn, &resp_command(&[b"MULTI"]))
        .expect("MULTI should succeed");
    let _ = ingress_connection_bytes(
        &mut app,
        &mut watch_conn,
        &resp_command(&[b"SET", b"load:watch:key", b"mine"]),
    )
    .expect("SET should queue");

    let exec_reply = ingress_connection_bytes(&mut app, &mut watch_conn, &resp_command(&[b"EXEC"]))
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
    let reply =
        ingress_connection_bytes(&mut app, &mut conn, &load).expect("DFLY LOAD should parse");
    assert_that!(
        reply[0].starts_with(b"-ERR DFLY LOAD failed: io error:"),
        eq(true)
    );
}

#[rstest]
fn resp_dfly_load_resets_replication_state() {
    let mut app = ServerApp::new(RuntimeConfig::default());
    let mut connection = ServerApp::new_connection(ClientProtocol::Resp);

    let _ = ingress_connection_bytes(
        &mut app,
        &mut connection,
        &resp_command(&[b"SET", b"load:reset:key", b"initial"]),
    )
    .expect("initial SET should succeed");
    assert_that!(
        app.replication_guard_with_latest_journal()
            .replication_offset(),
        gt(0_u64)
    );

    let path = unique_test_snapshot_path("dfly-load-reset");
    let path_bytes = path.to_string_lossy().as_bytes().to_vec();

    let save_reply = ingress_connection_bytes(
        &mut app,
        &mut connection,
        &resp_command(&[b"DFLY", b"SAVE", &path_bytes]),
    )
    .expect("DFLY SAVE should execute");
    assert_that!(&save_reply, eq(&vec![b"+OK\r\n".to_vec()]));

    let _ = ingress_connection_bytes(
        &mut app,
        &mut connection,
        &resp_command(&[b"SET", b"load:reset:key", b"new"]),
    )
    .expect("second SET should succeed");
    assert_that!(
        app.replication_guard_with_latest_journal()
            .replication_offset(),
        gt(1_u64)
    );

    let load_reply = ingress_connection_bytes(
        &mut app,
        &mut connection,
        &resp_command(&[b"DFLY", b"LOAD", &path_bytes]),
    )
    .expect("DFLY LOAD should execute");
    assert_that!(&load_reply, eq(&vec![b"+OK\r\n".to_vec()]));

    assert_that!(
        app.replication_guard_with_latest_journal()
            .replication_offset(),
        eq(0_u64)
    );
    assert_that!(
        app.replication_guard_with_latest_journal()
            .journal_entries()
            .is_empty(),
        eq(true)
    );
    assert_that!(
        app.replication_guard_with_latest_journal().journal_lsn(),
        eq(1_u64)
    );

    let _ = std::fs::remove_file(path);
}

