use super::*;

#[rstest]
fn resp_connection_executes_set_then_get() {
    let mut app = ServerApp::new(RuntimeConfig::default());
    let mut connection = ServerApp::new_connection(ClientProtocol::Resp);

    let set_responses = ingress_connection_bytes(
        &mut app,
        &mut connection,
        b"*3\r\n$3\r\nSET\r\n$3\r\nfoo\r\n$3\r\nbar\r\n",
    )
    .expect("SET command should execute");
    let expected_set = vec![b"+OK\r\n".to_vec()];
    assert_that!(&set_responses, eq(&expected_set));

    let get_responses = ingress_connection_bytes(
        &mut app,
        &mut connection,
        b"*2\r\n$3\r\nGET\r\n$3\r\nfoo\r\n",
    )
    .expect("GET command should execute");
    let expected_get = vec![b"$3\r\nbar\r\n".to_vec()];
    assert_that!(&get_responses, eq(&expected_get));
}

#[rstest]
fn resp_set_supports_conditional_and_get_options() {
    let mut app = ServerApp::new(RuntimeConfig::default());
    let mut connection = ServerApp::new_connection(ClientProtocol::Resp);

    let first = ingress_connection_bytes(
        &mut app,
        &mut connection,
        &resp_command(&[b"SET", b"k", b"v1", b"NX"]),
    )
    .expect("SET NX should execute");
    assert_that!(&first, eq(&vec![b"+OK\r\n".to_vec()]));

    let skipped = ingress_connection_bytes(
        &mut app,
        &mut connection,
        &resp_command(&[b"SET", b"k", b"v2", b"NX"]),
    )
    .expect("SET NX on existing key should execute");
    assert_that!(&skipped, eq(&vec![b"$-1\r\n".to_vec()]));

    let skipped_with_get = ingress_connection_bytes(
        &mut app,
        &mut connection,
        &resp_command(&[b"SET", b"k", b"ignored", b"NX", b"GET"]),
    )
    .expect("SET NX GET should execute");
    assert_that!(&skipped_with_get, eq(&vec![b"$2\r\nv1\r\n".to_vec()]));

    let missing_with_xx_get = ingress_connection_bytes(
        &mut app,
        &mut connection,
        &resp_command(&[b"SET", b"missing", b"value", b"XX", b"GET"]),
    )
    .expect("SET XX GET on missing key should execute");
    assert_that!(&missing_with_xx_get, eq(&vec![b"$-1\r\n".to_vec()]));

    let existing_with_xx_get = ingress_connection_bytes(
        &mut app,
        &mut connection,
        &resp_command(&[b"SET", b"k", b"v2", b"XX", b"GET"]),
    )
    .expect("SET XX GET on existing key should execute");
    assert_that!(&existing_with_xx_get, eq(&vec![b"$2\r\nv1\r\n".to_vec()]));

    let current =
        ingress_connection_bytes(&mut app, &mut connection, &resp_command(&[b"GET", b"k"]))
            .expect("GET should execute");
    assert_that!(&current, eq(&vec![b"$2\r\nv2\r\n".to_vec()]));
}

#[rstest]
fn resp_set_applies_expire_and_keepttl_options() {
    let mut app = ServerApp::new(RuntimeConfig::default());
    let mut connection = ServerApp::new_connection(ClientProtocol::Resp);

    let with_ex = ingress_connection_bytes(
        &mut app,
        &mut connection,
        &resp_command(&[b"SET", b"ttl:key", b"value", b"EX", b"30"]),
    )
    .expect("SET EX should execute");
    assert_that!(&with_ex, eq(&vec![b"+OK\r\n".to_vec()]));

    let ttl_before = ingress_connection_bytes(
        &mut app,
        &mut connection,
        &resp_command(&[b"TTL", b"ttl:key"]),
    )
    .expect("TTL should execute");
    assert_that!(parse_resp_integer(&ttl_before[0]) > 0, eq(true));

    let keep_ttl = ingress_connection_bytes(
        &mut app,
        &mut connection,
        &resp_command(&[b"SET", b"ttl:key", b"next", b"KEEPTTL"]),
    )
    .expect("SET KEEPTTL should execute");
    assert_that!(&keep_ttl, eq(&vec![b"+OK\r\n".to_vec()]));

    let ttl_after_keep = ingress_connection_bytes(
        &mut app,
        &mut connection,
        &resp_command(&[b"TTL", b"ttl:key"]),
    )
    .expect("TTL should execute");
    assert_that!(parse_resp_integer(&ttl_after_keep[0]) > 0, eq(true));

    let clear_ttl = ingress_connection_bytes(
        &mut app,
        &mut connection,
        &resp_command(&[b"SET", b"ttl:key", b"final"]),
    )
    .expect("plain SET should execute");
    assert_that!(&clear_ttl, eq(&vec![b"+OK\r\n".to_vec()]));

    let ttl_after_plain = ingress_connection_bytes(
        &mut app,
        &mut connection,
        &resp_command(&[b"TTL", b"ttl:key"]),
    )
    .expect("TTL should execute");
    assert_that!(&ttl_after_plain, eq(&vec![b":-1\r\n".to_vec()]));

    let with_px = ingress_connection_bytes(
        &mut app,
        &mut connection,
        &resp_command(&[b"SET", b"px:key", b"value", b"PX", b"1200"]),
    )
    .expect("SET PX should execute");
    assert_that!(&with_px, eq(&vec![b"+OK\r\n".to_vec()]));

    let pttl = ingress_connection_bytes(
        &mut app,
        &mut connection,
        &resp_command(&[b"PTTL", b"px:key"]),
    )
    .expect("PTTL should execute");
    assert_that!(parse_resp_integer(&pttl[0]) > 0, eq(true));
}

#[rstest]
fn resp_set_rejects_invalid_option_combinations() {
    let mut app = ServerApp::new(RuntimeConfig::default());
    let mut connection = ServerApp::new_connection(ClientProtocol::Resp);

    let nx_xx = ingress_connection_bytes(
        &mut app,
        &mut connection,
        &resp_command(&[b"SET", b"key", b"value", b"NX", b"XX"]),
    )
    .expect("SET NX XX should parse");
    assert_that!(&nx_xx, eq(&vec![b"-ERR syntax error\r\n".to_vec()]));

    let duplicate_expire = ingress_connection_bytes(
        &mut app,
        &mut connection,
        &resp_command(&[b"SET", b"key", b"value", b"EX", b"1", b"PX", b"1000"]),
    )
    .expect("SET EX PX should parse");
    assert_that!(
        &duplicate_expire,
        eq(&vec![b"-ERR syntax error\r\n".to_vec()])
    );

    let keep_with_expire = ingress_connection_bytes(
        &mut app,
        &mut connection,
        &resp_command(&[b"SET", b"key", b"value", b"EX", b"1", b"KEEPTTL"]),
    )
    .expect("SET EX KEEPTTL should parse");
    assert_that!(
        &keep_with_expire,
        eq(&vec![b"-ERR syntax error\r\n".to_vec()])
    );

    let invalid_integer = ingress_connection_bytes(
        &mut app,
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

    let invalid_expire = ingress_connection_bytes(
        &mut app,
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

    let missing = ingress_connection_bytes(
        &mut app,
        &mut connection,
        &resp_command(&[b"TYPE", b"missing"]),
    )
    .expect("TYPE should execute");
    assert_that!(&missing, eq(&vec![b"+none\r\n".to_vec()]));

    let _ = ingress_connection_bytes(
        &mut app,
        &mut connection,
        &resp_command(&[b"SET", b"k", b"v"]),
    )
    .expect("SET should execute");
    let existing =
        ingress_connection_bytes(&mut app, &mut connection, &resp_command(&[b"TYPE", b"k"]))
            .expect("TYPE should execute");
    assert_that!(&existing, eq(&vec![b"+string\r\n".to_vec()]));
}

#[rstest]
fn resp_dbsize_counts_keys_in_selected_database() {
    let mut app = ServerApp::new(RuntimeConfig::default());
    let mut connection = ServerApp::new_connection(ClientProtocol::Resp);

    let _ = ingress_connection_bytes(
        &mut app,
        &mut connection,
        &resp_command(&[b"SET", b"k0", b"v0"]),
    )
    .expect("SET db0 should execute");
    let _ = ingress_connection_bytes(&mut app, &mut connection, &resp_command(&[b"SELECT", b"1"]))
        .expect("SELECT 1 should execute");
    let _ = ingress_connection_bytes(
        &mut app,
        &mut connection,
        &resp_command(&[b"SET", b"k1", b"v1"]),
    )
    .expect("SET db1 should execute");

    let db1_size = ingress_connection_bytes(&mut app, &mut connection, &resp_command(&[b"DBSIZE"]))
        .expect("DBSIZE db1 should execute");
    assert_that!(&db1_size, eq(&vec![b":1\r\n".to_vec()]));

    let _ = ingress_connection_bytes(&mut app, &mut connection, &resp_command(&[b"SELECT", b"0"]))
        .expect("SELECT 0 should execute");
    let db0_size = ingress_connection_bytes(&mut app, &mut connection, &resp_command(&[b"DBSIZE"]))
        .expect("DBSIZE db0 should execute");
    assert_that!(&db0_size, eq(&vec![b":1\r\n".to_vec()]));
}

#[rstest]
fn resp_dbsize_rejects_extra_arguments() {
    let mut app = ServerApp::new(RuntimeConfig::default());
    let mut connection = ServerApp::new_connection(ClientProtocol::Resp);

    let reply = ingress_connection_bytes(
        &mut app,
        &mut connection,
        &resp_command(&[b"DBSIZE", b"extra"]),
    )
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

    let reply = ingress_connection_bytes(&mut app, &mut connection, &resp_command(&[b"RANDOMKEY"]))
        .expect("RANDOMKEY should execute");
    assert_that!(&reply, eq(&vec![b"$-1\r\n".to_vec()]));
}

#[rstest]
fn resp_randomkey_returns_existing_key_from_selected_database() {
    let mut app = ServerApp::new(RuntimeConfig::default());
    let mut connection = ServerApp::new_connection(ClientProtocol::Resp);

    let _ = ingress_connection_bytes(
        &mut app,
        &mut connection,
        &resp_command(&[b"SET", b"random:one", b"v1"]),
    )
    .expect("SET random:one should execute");
    let _ = ingress_connection_bytes(
        &mut app,
        &mut connection,
        &resp_command(&[b"SET", b"random:two", b"v2"]),
    )
    .expect("SET random:two should execute");
    let _ = ingress_connection_bytes(&mut app, &mut connection, &resp_command(&[b"SELECT", b"1"]))
        .expect("SELECT 1 should execute");
    let _ = ingress_connection_bytes(
        &mut app,
        &mut connection,
        &resp_command(&[b"SET", b"random:db1", b"x"]),
    )
    .expect("SET random:db1 should execute");
    let _ = ingress_connection_bytes(&mut app, &mut connection, &resp_command(&[b"SELECT", b"0"]))
        .expect("SELECT 0 should execute");

    let reply = ingress_connection_bytes(&mut app, &mut connection, &resp_command(&[b"RANDOMKEY"]))
        .expect("RANDOMKEY should execute");
    assert_that!(reply.len(), eq(1_usize));
    let key = decode_resp_bulk_payload(&reply[0]);
    assert_that!(key == "random:one" || key == "random:two", eq(true));
}

#[rstest]
fn resp_randomkey_rejects_arguments() {
    let mut app = ServerApp::new(RuntimeConfig::default());
    let mut connection = ServerApp::new_connection(ClientProtocol::Resp);

    let reply = ingress_connection_bytes(
        &mut app,
        &mut connection,
        &resp_command(&[b"RANDOMKEY", b"extra"]),
    )
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

    let _ = ingress_connection_bytes(
        &mut app,
        &mut connection,
        &resp_command(&[b"SET", b"user:1", b"v1"]),
    )
    .expect("SET user:1 should execute");
    let _ = ingress_connection_bytes(
        &mut app,
        &mut connection,
        &resp_command(&[b"SET", b"user:2", b"v2"]),
    )
    .expect("SET user:2 should execute");
    let _ = ingress_connection_bytes(
        &mut app,
        &mut connection,
        &resp_command(&[b"SET", b"admin:1", b"v3"]),
    )
    .expect("SET admin:1 should execute");
    let _ = ingress_connection_bytes(&mut app, &mut connection, &resp_command(&[b"SELECT", b"1"]))
        .expect("SELECT 1 should execute");
    let _ = ingress_connection_bytes(
        &mut app,
        &mut connection,
        &resp_command(&[b"SET", b"user:db1", b"x"]),
    )
    .expect("SET db1 key should execute");
    let _ = ingress_connection_bytes(&mut app, &mut connection, &resp_command(&[b"SELECT", b"0"]))
        .expect("SELECT 0 should execute");

    let user_keys = ingress_connection_bytes(
        &mut app,
        &mut connection,
        &resp_command(&[b"KEYS", b"user:*"]),
    )
    .expect("KEYS user:* should execute");
    assert_that!(
        &user_keys,
        eq(&vec![b"*2\r\n$6\r\nuser:1\r\n$6\r\nuser:2\r\n".to_vec()])
    );

    let no_match = ingress_connection_bytes(
        &mut app,
        &mut connection,
        &resp_command(&[b"KEYS", b"missing:*"]),
    )
    .expect("KEYS missing:* should execute");
    assert_that!(&no_match, eq(&vec![b"*0\r\n".to_vec()]));
}

#[rstest]
fn resp_keys_rejects_wrong_arity() {
    let mut app = ServerApp::new(RuntimeConfig::default());
    let mut connection = ServerApp::new_connection(ClientProtocol::Resp);

    let too_few = ingress_connection_bytes(&mut app, &mut connection, &resp_command(&[b"KEYS"]))
        .expect("KEYS should parse");
    assert_that!(
        &too_few,
        eq(&vec![
            b"-ERR wrong number of arguments for 'KEYS' command\r\n".to_vec()
        ])
    );

    let too_many = ingress_connection_bytes(
        &mut app,
        &mut connection,
        &resp_command(&[b"KEYS", b"a*", b"extra"]),
    )
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

    let reply = ingress_connection_bytes(&mut app, &mut connection, &resp_command(&[b"TIME"]))
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

    let reply = ingress_connection_bytes(
        &mut app,
        &mut connection,
        &resp_command(&[b"TIME", b"extra"]),
    )
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
    let mset_reply =
        ingress_connection_bytes(&mut app, &mut connection, &mset).expect("MSET should execute");
    assert_that!(&mset_reply, eq(&vec![b"+OK\r\n".to_vec()]));

    let mget = resp_command(&[b"MGET", b"foo", b"baz"]);
    let mget_reply =
        ingress_connection_bytes(&mut app, &mut connection, &mget).expect("MGET should execute");
    let expected = vec![b"*2\r\n$3\r\nbar\r\n$3\r\nqux\r\n".to_vec()];
    assert_that!(&mget_reply, eq(&expected));
}

#[rstest]
fn resp_mset_rejects_odd_arity() {
    let mut app = ServerApp::new(RuntimeConfig::default());
    let mut connection = ServerApp::new_connection(ClientProtocol::Resp);

    let invalid = resp_command(&[b"MSET", b"foo", b"bar", b"baz"]);
    let reply =
        ingress_connection_bytes(&mut app, &mut connection, &invalid).expect("MSET should parse");
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

    let first = ingress_connection_bytes(
        &mut app,
        &mut connection,
        &resp_command(&[b"MSETNX", b"a", b"1", b"b", b"2"]),
    )
    .expect("first MSETNX should execute");
    assert_that!(&first, eq(&vec![b":1\r\n".to_vec()]));

    let second = ingress_connection_bytes(
        &mut app,
        &mut connection,
        &resp_command(&[b"MSETNX", b"a", b"x", b"c", b"3"]),
    )
    .expect("second MSETNX should execute");
    assert_that!(&second, eq(&vec![b":0\r\n".to_vec()]));

    let values = ingress_connection_bytes(
        &mut app,
        &mut connection,
        &resp_command(&[b"MGET", b"a", b"b", b"c"]),
    )
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
    let reply =
        ingress_connection_bytes(&mut app, &mut connection, &invalid).expect("MSETNX should parse");
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

    let _ = ingress_connection_bytes(
        &mut app,
        &mut connection,
        &resp_command(&[b"SET", b"k1", b"v1"]),
    )
    .expect("SET k1 should succeed");
    let _ = ingress_connection_bytes(
        &mut app,
        &mut connection,
        &resp_command(&[b"SET", b"k2", b"v2"]),
    )
    .expect("SET k2 should succeed");

    let exists = ingress_connection_bytes(
        &mut app,
        &mut connection,
        &resp_command(&[b"EXISTS", b"k1", b"k2", b"missing", b"k1"]),
    )
    .expect("EXISTS should execute");
    assert_that!(&exists, eq(&vec![b":3\r\n".to_vec()]));

    let deleted = ingress_connection_bytes(
        &mut app,
        &mut connection,
        &resp_command(&[b"DEL", b"k1", b"k2", b"missing", b"k1"]),
    )
    .expect("DEL should execute");
    assert_that!(&deleted, eq(&vec![b":2\r\n".to_vec()]));

    let exists_after = ingress_connection_bytes(
        &mut app,
        &mut connection,
        &resp_command(&[b"EXISTS", b"k1", b"k2"]),
    )
    .expect("EXISTS should execute");
    assert_that!(&exists_after, eq(&vec![b":0\r\n".to_vec()]));
}

#[rstest]
fn resp_unlink_follows_multi_key_count_semantics() {
    let mut app = ServerApp::new(RuntimeConfig::default());
    let mut connection = ServerApp::new_connection(ClientProtocol::Resp);

    let _ = ingress_connection_bytes(
        &mut app,
        &mut connection,
        &resp_command(&[b"SET", b"k1", b"v1"]),
    )
    .expect("SET k1 should succeed");
    let _ = ingress_connection_bytes(
        &mut app,
        &mut connection,
        &resp_command(&[b"SET", b"k2", b"v2"]),
    )
    .expect("SET k2 should succeed");

    let unlinked = ingress_connection_bytes(
        &mut app,
        &mut connection,
        &resp_command(&[b"UNLINK", b"k1", b"k2", b"missing", b"k1"]),
    )
    .expect("UNLINK should execute");
    assert_that!(&unlinked, eq(&vec![b":2\r\n".to_vec()]));

    let exists_after = ingress_connection_bytes(
        &mut app,
        &mut connection,
        &resp_command(&[b"EXISTS", b"k1", b"k2"]),
    )
    .expect("EXISTS should execute");
    assert_that!(&exists_after, eq(&vec![b":0\r\n".to_vec()]));
}

#[rstest]
fn resp_touch_counts_existing_keys() {
    let mut app = ServerApp::new(RuntimeConfig::default());
    let mut connection = ServerApp::new_connection(ClientProtocol::Resp);

    let _ = ingress_connection_bytes(
        &mut app,
        &mut connection,
        &resp_command(&[b"SET", b"k1", b"v1"]),
    )
    .expect("SET k1 should succeed");
    let _ = ingress_connection_bytes(
        &mut app,
        &mut connection,
        &resp_command(&[b"SET", b"k2", b"v2"]),
    )
    .expect("SET k2 should succeed");

    let touched = ingress_connection_bytes(
        &mut app,
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

    let _ = ingress_connection_bytes(
        &mut app,
        &mut connection,
        &resp_command(&[b"SET", b"move:key", b"v"]),
    )
    .expect("SET should execute");
    let moved = ingress_connection_bytes(
        &mut app,
        &mut connection,
        &resp_command(&[b"MOVE", b"move:key", b"1"]),
    )
    .expect("MOVE should execute");
    assert_that!(&moved, eq(&vec![b":1\r\n".to_vec()]));

    let source = ingress_connection_bytes(
        &mut app,
        &mut connection,
        &resp_command(&[b"GET", b"move:key"]),
    )
    .expect("GET source should execute");
    assert_that!(&source, eq(&vec![b"$-1\r\n".to_vec()]));

    let _ = ingress_connection_bytes(&mut app, &mut connection, &resp_command(&[b"SELECT", b"1"]))
        .expect("SELECT 1 should execute");
    let target = ingress_connection_bytes(
        &mut app,
        &mut connection,
        &resp_command(&[b"GET", b"move:key"]),
    )
    .expect("GET target should execute");
    assert_that!(&target, eq(&vec![b"$1\r\nv\r\n".to_vec()]));
}

#[rstest]
fn resp_move_returns_zero_when_target_contains_key() {
    let mut app = ServerApp::new(RuntimeConfig::default());
    let mut connection = ServerApp::new_connection(ClientProtocol::Resp);

    let _ = ingress_connection_bytes(
        &mut app,
        &mut connection,
        &resp_command(&[b"SET", b"dup", b"db0"]),
    )
    .expect("SET db0 should execute");
    let _ = ingress_connection_bytes(&mut app, &mut connection, &resp_command(&[b"SELECT", b"1"]))
        .expect("SELECT 1 should execute");
    let _ = ingress_connection_bytes(
        &mut app,
        &mut connection,
        &resp_command(&[b"SET", b"dup", b"db1"]),
    )
    .expect("SET db1 should execute");
    let _ = ingress_connection_bytes(&mut app, &mut connection, &resp_command(&[b"SELECT", b"0"]))
        .expect("SELECT 0 should execute");

    let moved = ingress_connection_bytes(
        &mut app,
        &mut connection,
        &resp_command(&[b"MOVE", b"dup", b"1"]),
    )
    .expect("MOVE should execute");
    assert_that!(&moved, eq(&vec![b":0\r\n".to_vec()]));

    let source =
        ingress_connection_bytes(&mut app, &mut connection, &resp_command(&[b"GET", b"dup"]))
            .expect("GET source should execute");
    assert_that!(&source, eq(&vec![b"$3\r\ndb0\r\n".to_vec()]));
}

#[rstest]
fn resp_copy_follows_dragonfly_replace_and_error_semantics() {
    let mut app = ServerApp::new(RuntimeConfig::default());
    let mut connection = ServerApp::new_connection(ClientProtocol::Resp);

    let missing = ingress_connection_bytes(
        &mut app,
        &mut connection,
        &resp_command(&[b"COPY", b"missing", b"dst"]),
    )
    .expect("COPY missing key should parse");
    assert_that!(&missing, eq(&vec![b":0\r\n".to_vec()]));

    let _ = ingress_connection_bytes(
        &mut app,
        &mut connection,
        &resp_command(&[b"SETEX", b"src", b"60", b"v1"]),
    )
    .expect("SETEX src should execute");
    let _ = ingress_connection_bytes(
        &mut app,
        &mut connection,
        &resp_command(&[b"SET", b"dst", b"v2"]),
    )
    .expect("SET dst should execute");

    let blocked = ingress_connection_bytes(
        &mut app,
        &mut connection,
        &resp_command(&[b"COPY", b"src", b"dst"]),
    )
    .expect("COPY without REPLACE should execute");
    assert_that!(&blocked, eq(&vec![b":0\r\n".to_vec()]));

    let replaced = ingress_connection_bytes(
        &mut app,
        &mut connection,
        &resp_command(&[b"COPY", b"src", b"dst", b"REPLACE"]),
    )
    .expect("COPY REPLACE should execute");
    assert_that!(&replaced, eq(&vec![b":1\r\n".to_vec()]));

    let src = ingress_connection_bytes(&mut app, &mut connection, &resp_command(&[b"GET", b"src"]))
        .expect("GET src should execute");
    let dst = ingress_connection_bytes(&mut app, &mut connection, &resp_command(&[b"GET", b"dst"]))
        .expect("GET dst should execute");
    assert_that!(&src, eq(&vec![b"$2\r\nv1\r\n".to_vec()]));
    assert_that!(&dst, eq(&vec![b"$2\r\nv1\r\n".to_vec()]));

    let same = ingress_connection_bytes(
        &mut app,
        &mut connection,
        &resp_command(&[b"COPY", b"dst", b"dst"]),
    )
    .expect("COPY same key should parse");
    assert_that!(
        &same,
        eq(&vec![
            b"-ERR source and destination objects are the same\r\n".to_vec()
        ])
    );

    let unsupported_db = ingress_connection_bytes(
        &mut app,
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

    let _ = ingress_connection_bytes(
        &mut app,
        &mut connection,
        &resp_command(&[b"SET", &source_key, b"value"]),
    )
    .expect("SET source should execute");
    let reply = ingress_connection_bytes(
        &mut app,
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

    let missing = ingress_connection_bytes(
        &mut app,
        &mut connection,
        &resp_command(&[b"RENAME", b"missing", b"dst"]),
    )
    .expect("RENAME missing should parse");
    assert_that!(&missing, eq(&vec![b"-ERR no such key\r\n".to_vec()]));

    let _ = ingress_connection_bytes(
        &mut app,
        &mut connection,
        &resp_command(&[b"SET", b"src", b"v1"]),
    )
    .expect("SET src should execute");
    let _ = ingress_connection_bytes(
        &mut app,
        &mut connection,
        &resp_command(&[b"SET", b"dst", b"v2"]),
    )
    .expect("SET dst should execute");

    let blocked = ingress_connection_bytes(
        &mut app,
        &mut connection,
        &resp_command(&[b"RENAMENX", b"src", b"dst"]),
    )
    .expect("RENAMENX should execute");
    assert_that!(&blocked, eq(&vec![b":0\r\n".to_vec()]));

    let renamed = ingress_connection_bytes(
        &mut app,
        &mut connection,
        &resp_command(&[b"RENAME", b"src", b"dst"]),
    )
    .expect("RENAME should execute");
    assert_that!(&renamed, eq(&vec![b"+OK\r\n".to_vec()]));

    let src = ingress_connection_bytes(&mut app, &mut connection, &resp_command(&[b"GET", b"src"]))
        .expect("GET src should execute");
    assert_that!(&src, eq(&vec![b"$-1\r\n".to_vec()]));
    let dst = ingress_connection_bytes(&mut app, &mut connection, &resp_command(&[b"GET", b"dst"]))
        .expect("GET dst should execute");
    assert_that!(&dst, eq(&vec![b"$2\r\nv1\r\n".to_vec()]));

    let same = ingress_connection_bytes(
        &mut app,
        &mut connection,
        &resp_command(&[b"RENAME", b"dst", b"dst"]),
    )
    .expect("RENAME same key should execute");
    assert_that!(&same, eq(&vec![b"+OK\r\n".to_vec()]));

    let same_nx = ingress_connection_bytes(
        &mut app,
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

    let _ = ingress_connection_bytes(
        &mut app,
        &mut connection,
        &resp_command(&[b"SET", &source_key, b"v"]),
    )
    .expect("SET source should execute");
    let reply = ingress_connection_bytes(
        &mut app,
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

    let _ = ingress_connection_bytes(
        &mut app,
        &mut connection,
        &resp_command(&[b"SET", b"ttl:key", b"v"]),
    )
    .expect("SET should succeed");
    let _ = ingress_connection_bytes(
        &mut app,
        &mut connection,
        &resp_command(&[b"EXPIRE", b"ttl:key", b"10"]),
    )
    .expect("EXPIRE should succeed");

    let persist = ingress_connection_bytes(
        &mut app,
        &mut connection,
        &resp_command(&[b"PERSIST", b"ttl:key"]),
    )
    .expect("PERSIST should execute");
    assert_that!(&persist, eq(&vec![b":1\r\n".to_vec()]));

    let ttl = ingress_connection_bytes(
        &mut app,
        &mut connection,
        &resp_command(&[b"TTL", b"ttl:key"]),
    )
    .expect("TTL should execute");
    assert_that!(&ttl, eq(&vec![b":-1\r\n".to_vec()]));

    let value = ingress_connection_bytes(
        &mut app,
        &mut connection,
        &resp_command(&[b"GET", b"ttl:key"]),
    )
    .expect("GET should execute");
    assert_that!(&value, eq(&vec![b"$1\r\nv\r\n".to_vec()]));
}

#[rstest]
fn resp_setex_sets_value_with_ttl_and_rejects_non_positive_ttl() {
    let mut app = ServerApp::new(RuntimeConfig::default());
    let mut connection = ServerApp::new_connection(ClientProtocol::Resp);

    let ok = ingress_connection_bytes(
        &mut app,
        &mut connection,
        &resp_command(&[b"SETEX", b"session", b"60", b"alive"]),
    )
    .expect("SETEX should execute");
    assert_that!(&ok, eq(&vec![b"+OK\r\n".to_vec()]));

    let value = ingress_connection_bytes(
        &mut app,
        &mut connection,
        &resp_command(&[b"GET", b"session"]),
    )
    .expect("GET should execute");
    assert_that!(&value, eq(&vec![b"$5\r\nalive\r\n".to_vec()]));

    let ttl = ingress_connection_bytes(
        &mut app,
        &mut connection,
        &resp_command(&[b"TTL", b"session"]),
    )
    .expect("TTL should execute");
    let ttl_value = parse_resp_integer(&ttl[0]);
    assert_that!(ttl_value > 0, eq(true));

    let bad = ingress_connection_bytes(
        &mut app,
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

    let ok = ingress_connection_bytes(
        &mut app,
        &mut connection,
        &resp_command(&[b"PSETEX", b"session", b"1500", b"alive"]),
    )
    .expect("PSETEX should execute");
    assert_that!(&ok, eq(&vec![b"+OK\r\n".to_vec()]));

    let value = ingress_connection_bytes(
        &mut app,
        &mut connection,
        &resp_command(&[b"GET", b"session"]),
    )
    .expect("GET should execute");
    assert_that!(&value, eq(&vec![b"$5\r\nalive\r\n".to_vec()]));

    let pttl = ingress_connection_bytes(
        &mut app,
        &mut connection,
        &resp_command(&[b"PTTL", b"session"]),
    )
    .expect("PTTL should execute");
    let pttl_value = parse_resp_integer(&pttl[0]);
    assert_that!(pttl_value > 0, eq(true));

    let bad = ingress_connection_bytes(
        &mut app,
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

    let _ = ingress_connection_bytes(
        &mut app,
        &mut connection,
        &resp_command(&[b"SET", b"temp", b"value"]),
    )
    .expect("SET should execute");
    let pttl_without_expire = ingress_connection_bytes(
        &mut app,
        &mut connection,
        &resp_command(&[b"PTTL", b"temp"]),
    )
    .expect("PTTL should execute");
    assert_that!(&pttl_without_expire, eq(&vec![b":-1\r\n".to_vec()]));

    let pexpire = ingress_connection_bytes(
        &mut app,
        &mut connection,
        &resp_command(&[b"PEXPIRE", b"temp", b"1500"]),
    )
    .expect("PEXPIRE should execute");
    assert_that!(&pexpire, eq(&vec![b":1\r\n".to_vec()]));

    let pttl = ingress_connection_bytes(
        &mut app,
        &mut connection,
        &resp_command(&[b"PTTL", b"temp"]),
    )
    .expect("PTTL should execute");
    let pttl_value = parse_resp_integer(&pttl[0]);
    assert_that!(pttl_value > 0, eq(true));

    let pexpire_now = ingress_connection_bytes(
        &mut app,
        &mut connection,
        &resp_command(&[b"PEXPIRE", b"temp", b"0"]),
    )
    .expect("PEXPIRE should execute");
    assert_that!(&pexpire_now, eq(&vec![b":1\r\n".to_vec()]));

    let pttl_after = ingress_connection_bytes(
        &mut app,
        &mut connection,
        &resp_command(&[b"PTTL", b"temp"]),
    )
    .expect("PTTL should execute");
    assert_that!(&pttl_after, eq(&vec![b":-2\r\n".to_vec()]));
}

#[rstest]
fn resp_expire_family_supports_nx_xx_gt_lt_options() {
    let mut app = ServerApp::new(RuntimeConfig::default());
    let mut connection = ServerApp::new_connection(ClientProtocol::Resp);

    let _ = ingress_connection_bytes(
        &mut app,
        &mut connection,
        &resp_command(&[b"SET", b"opt:key", b"v"]),
    )
    .expect("SET should execute");

    let gt_on_persistent = ingress_connection_bytes(
        &mut app,
        &mut connection,
        &resp_command(&[b"EXPIRE", b"opt:key", b"10", b"GT"]),
    )
    .expect("EXPIRE GT should parse");
    assert_that!(&gt_on_persistent, eq(&vec![b":0\r\n".to_vec()]));

    let lt_on_persistent = ingress_connection_bytes(
        &mut app,
        &mut connection,
        &resp_command(&[b"EXPIRE", b"opt:key", b"10", b"LT"]),
    )
    .expect("EXPIRE LT should parse");
    assert_that!(&lt_on_persistent, eq(&vec![b":1\r\n".to_vec()]));

    let first_expiretime = ingress_connection_bytes(
        &mut app,
        &mut connection,
        &resp_command(&[b"EXPIRETIME", b"opt:key"]),
    )
    .expect("EXPIRETIME should execute");
    let first_expiretime = parse_resp_integer(&first_expiretime[0]);

    let nx_with_existing_expire = ingress_connection_bytes(
        &mut app,
        &mut connection,
        &resp_command(&[b"EXPIRE", b"opt:key", b"20", b"NX"]),
    )
    .expect("EXPIRE NX should parse");
    assert_that!(&nx_with_existing_expire, eq(&vec![b":0\r\n".to_vec()]));

    let xx_with_existing_expire = ingress_connection_bytes(
        &mut app,
        &mut connection,
        &resp_command(&[b"EXPIRE", b"opt:key", b"20", b"XX"]),
    )
    .expect("EXPIRE XX should parse");
    assert_that!(&xx_with_existing_expire, eq(&vec![b":1\r\n".to_vec()]));

    let second_expiretime = ingress_connection_bytes(
        &mut app,
        &mut connection,
        &resp_command(&[b"EXPIRETIME", b"opt:key"]),
    )
    .expect("EXPIRETIME should execute");
    let second_expiretime = parse_resp_integer(&second_expiretime[0]);
    assert_that!(second_expiretime > first_expiretime, eq(true));

    let gt_with_smaller_target = ingress_connection_bytes(
        &mut app,
        &mut connection,
        &resp_command(&[b"EXPIRE", b"opt:key", b"5", b"GT"]),
    )
    .expect("EXPIRE GT should parse");
    assert_that!(&gt_with_smaller_target, eq(&vec![b":0\r\n".to_vec()]));

    let lt_with_smaller_target = ingress_connection_bytes(
        &mut app,
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

    let nx_xx = ingress_connection_bytes(
        &mut app,
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

    let gt_lt = ingress_connection_bytes(
        &mut app,
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

    let unknown = ingress_connection_bytes(
        &mut app,
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

    let missing = ingress_connection_bytes(
        &mut app,
        &mut connection,
        &resp_command(&[b"EXPIRETIME", b"missing"]),
    )
    .expect("EXPIRETIME should execute");
    assert_that!(&missing, eq(&vec![b":-2\r\n".to_vec()]));

    let _ = ingress_connection_bytes(
        &mut app,
        &mut connection,
        &resp_command(&[b"SET", b"persist", b"v"]),
    )
    .expect("SET should execute");
    let persistent = ingress_connection_bytes(
        &mut app,
        &mut connection,
        &resp_command(&[b"EXPIRETIME", b"persist"]),
    )
    .expect("EXPIRETIME should execute");
    assert_that!(&persistent, eq(&vec![b":-1\r\n".to_vec()]));

    let _ = ingress_connection_bytes(
        &mut app,
        &mut connection,
        &resp_command(&[b"SETEX", b"temp", b"60", b"v"]),
    )
    .expect("SETEX should execute");
    let expiring = ingress_connection_bytes(
        &mut app,
        &mut connection,
        &resp_command(&[b"EXPIRETIME", b"temp"]),
    )
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

    let missing = ingress_connection_bytes(
        &mut app,
        &mut connection,
        &resp_command(&[b"PEXPIRETIME", b"missing"]),
    )
    .expect("PEXPIRETIME should execute");
    assert_that!(&missing, eq(&vec![b":-2\r\n".to_vec()]));

    let _ = ingress_connection_bytes(
        &mut app,
        &mut connection,
        &resp_command(&[b"SET", b"persist", b"v"]),
    )
    .expect("SET should execute");
    let persistent = ingress_connection_bytes(
        &mut app,
        &mut connection,
        &resp_command(&[b"PEXPIRETIME", b"persist"]),
    )
    .expect("PEXPIRETIME should execute");
    assert_that!(&persistent, eq(&vec![b":-1\r\n".to_vec()]));

    let _ = ingress_connection_bytes(
        &mut app,
        &mut connection,
        &resp_command(&[b"SETEX", b"temp", b"60", b"v"]),
    )
    .expect("SETEX should execute");
    let expiring = ingress_connection_bytes(
        &mut app,
        &mut connection,
        &resp_command(&[b"PEXPIRETIME", b"temp"]),
    )
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

    let incr = ingress_connection_bytes(
        &mut app,
        &mut connection,
        &resp_command(&[b"INCR", b"counter"]),
    )
    .expect("INCR should execute");
    assert_that!(&incr, eq(&vec![b":1\r\n".to_vec()]));

    let incrby = ingress_connection_bytes(
        &mut app,
        &mut connection,
        &resp_command(&[b"INCRBY", b"counter", b"9"]),
    )
    .expect("INCRBY should execute");
    assert_that!(&incrby, eq(&vec![b":10\r\n".to_vec()]));

    let decr = ingress_connection_bytes(
        &mut app,
        &mut connection,
        &resp_command(&[b"DECR", b"counter"]),
    )
    .expect("DECR should execute");
    assert_that!(&decr, eq(&vec![b":9\r\n".to_vec()]));

    let decrby = ingress_connection_bytes(
        &mut app,
        &mut connection,
        &resp_command(&[b"DECRBY", b"counter", b"4"]),
    )
    .expect("DECRBY should execute");
    assert_that!(&decrby, eq(&vec![b":5\r\n".to_vec()]));

    let value = ingress_connection_bytes(
        &mut app,
        &mut connection,
        &resp_command(&[b"GET", b"counter"]),
    )
    .expect("GET should execute");
    assert_that!(&value, eq(&vec![b"$1\r\n5\r\n".to_vec()]));
}

#[rstest]
fn resp_incr_rejects_non_integer_values() {
    let mut app = ServerApp::new(RuntimeConfig::default());
    let mut connection = ServerApp::new_connection(ClientProtocol::Resp);

    let _ = ingress_connection_bytes(
        &mut app,
        &mut connection,
        &resp_command(&[b"SET", b"counter", b"abc"]),
    )
    .expect("SET should execute");
    let incr = ingress_connection_bytes(
        &mut app,
        &mut connection,
        &resp_command(&[b"INCR", b"counter"]),
    )
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

    let first = ingress_connection_bytes(
        &mut app,
        &mut connection,
        &resp_command(&[b"SETNX", b"only", b"v1"]),
    )
    .expect("first SETNX should execute");
    assert_that!(&first, eq(&vec![b":1\r\n".to_vec()]));

    let second = ingress_connection_bytes(
        &mut app,
        &mut connection,
        &resp_command(&[b"SETNX", b"only", b"v2"]),
    )
    .expect("second SETNX should execute");
    assert_that!(&second, eq(&vec![b":0\r\n".to_vec()]));

    let value =
        ingress_connection_bytes(&mut app, &mut connection, &resp_command(&[b"GET", b"only"]))
            .expect("GET should execute");
    assert_that!(&value, eq(&vec![b"$2\r\nv1\r\n".to_vec()]));
}

#[rstest]
fn resp_getset_returns_previous_value_and_replaces_key() {
    let mut app = ServerApp::new(RuntimeConfig::default());
    let mut connection = ServerApp::new_connection(ClientProtocol::Resp);

    let first = ingress_connection_bytes(
        &mut app,
        &mut connection,
        &resp_command(&[b"GETSET", b"k", b"v1"]),
    )
    .expect("first GETSET should execute");
    assert_that!(&first, eq(&vec![b"$-1\r\n".to_vec()]));

    let second = ingress_connection_bytes(
        &mut app,
        &mut connection,
        &resp_command(&[b"GETSET", b"k", b"v2"]),
    )
    .expect("second GETSET should execute");
    assert_that!(&second, eq(&vec![b"$2\r\nv1\r\n".to_vec()]));

    let value = ingress_connection_bytes(&mut app, &mut connection, &resp_command(&[b"GET", b"k"]))
        .expect("GET should execute");
    assert_that!(&value, eq(&vec![b"$2\r\nv2\r\n".to_vec()]));
}

#[rstest]
fn resp_getdel_returns_value_and_deletes_key() {
    let mut app = ServerApp::new(RuntimeConfig::default());
    let mut connection = ServerApp::new_connection(ClientProtocol::Resp);

    let _ = ingress_connection_bytes(
        &mut app,
        &mut connection,
        &resp_command(&[b"SET", b"k", b"v"]),
    )
    .expect("SET should execute");
    let removed =
        ingress_connection_bytes(&mut app, &mut connection, &resp_command(&[b"GETDEL", b"k"]))
            .expect("GETDEL should execute");
    assert_that!(&removed, eq(&vec![b"$1\r\nv\r\n".to_vec()]));

    let missing =
        ingress_connection_bytes(&mut app, &mut connection, &resp_command(&[b"GETDEL", b"k"]))
            .expect("GETDEL should execute");
    assert_that!(&missing, eq(&vec![b"$-1\r\n".to_vec()]));
}

#[rstest]
fn resp_append_and_strlen_track_string_size() {
    let mut app = ServerApp::new(RuntimeConfig::default());
    let mut connection = ServerApp::new_connection(ClientProtocol::Resp);

    let first = ingress_connection_bytes(
        &mut app,
        &mut connection,
        &resp_command(&[b"APPEND", b"k", b"he"]),
    )
    .expect("first APPEND should execute");
    assert_that!(&first, eq(&vec![b":2\r\n".to_vec()]));

    let second = ingress_connection_bytes(
        &mut app,
        &mut connection,
        &resp_command(&[b"APPEND", b"k", b"llo"]),
    )
    .expect("second APPEND should execute");
    assert_that!(&second, eq(&vec![b":5\r\n".to_vec()]));

    let strlen =
        ingress_connection_bytes(&mut app, &mut connection, &resp_command(&[b"STRLEN", b"k"]))
            .expect("STRLEN should execute");
    assert_that!(&strlen, eq(&vec![b":5\r\n".to_vec()]));

    let value = ingress_connection_bytes(&mut app, &mut connection, &resp_command(&[b"GET", b"k"]))
        .expect("GET should execute");
    assert_that!(&value, eq(&vec![b"$5\r\nhello\r\n".to_vec()]));
}

#[rstest]
fn resp_getrange_and_setrange_follow_redis_offset_rules() {
    let mut app = ServerApp::new(RuntimeConfig::default());
    let mut connection = ServerApp::new_connection(ClientProtocol::Resp);

    let _ = ingress_connection_bytes(
        &mut app,
        &mut connection,
        &resp_command(&[b"SET", b"k", b"hello"]),
    )
    .expect("SET should execute");
    let range = ingress_connection_bytes(
        &mut app,
        &mut connection,
        &resp_command(&[b"GETRANGE", b"k", b"1", b"3"]),
    )
    .expect("GETRANGE should execute");
    assert_that!(&range, eq(&vec![b"$3\r\nell\r\n".to_vec()]));

    let setrange = ingress_connection_bytes(
        &mut app,
        &mut connection,
        &resp_command(&[b"SETRANGE", b"k", b"1", b"i"]),
    )
    .expect("SETRANGE should execute");
    assert_that!(&setrange, eq(&vec![b":5\r\n".to_vec()]));

    let value = ingress_connection_bytes(&mut app, &mut connection, &resp_command(&[b"GET", b"k"]))
        .expect("GET should execute");
    assert_that!(&value, eq(&vec![b"$5\r\nhillo\r\n".to_vec()]));
}

#[rstest]
fn resp_expireat_sets_future_expire_and_deletes_past_keys() {
    let mut app = ServerApp::new(RuntimeConfig::default());
    let mut connection = ServerApp::new_connection(ClientProtocol::Resp);

    let _ = ingress_connection_bytes(
        &mut app,
        &mut connection,
        &resp_command(&[b"SET", b"future", b"v"]),
    )
    .expect("SET future should execute");
    let future_timestamp = (SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system time should be after epoch")
        .as_secs()
        + 120)
        .to_string()
        .into_bytes();
    let expireat_future = ingress_connection_bytes(
        &mut app,
        &mut connection,
        &resp_command(&[b"EXPIREAT", b"future", &future_timestamp]),
    )
    .expect("EXPIREAT future should execute");
    assert_that!(&expireat_future, eq(&vec![b":1\r\n".to_vec()]));

    let ttl_future = ingress_connection_bytes(
        &mut app,
        &mut connection,
        &resp_command(&[b"TTL", b"future"]),
    )
    .expect("TTL should execute");
    let ttl_value = parse_resp_integer(&ttl_future[0]);
    assert_that!(ttl_value > 0, eq(true));

    let _ = ingress_connection_bytes(
        &mut app,
        &mut connection,
        &resp_command(&[b"SET", b"past", b"v"]),
    )
    .expect("SET past should execute");
    let expireat_past = ingress_connection_bytes(
        &mut app,
        &mut connection,
        &resp_command(&[b"EXPIREAT", b"past", b"1"]),
    )
    .expect("EXPIREAT past should execute");
    assert_that!(&expireat_past, eq(&vec![b":1\r\n".to_vec()]));

    let removed =
        ingress_connection_bytes(&mut app, &mut connection, &resp_command(&[b"GET", b"past"]))
            .expect("GET should execute");
    assert_that!(&removed, eq(&vec![b"$-1\r\n".to_vec()]));
}

#[rstest]
fn resp_pexpireat_sets_future_expire_and_deletes_past_keys() {
    let mut app = ServerApp::new(RuntimeConfig::default());
    let mut connection = ServerApp::new_connection(ClientProtocol::Resp);

    let _ = ingress_connection_bytes(
        &mut app,
        &mut connection,
        &resp_command(&[b"SET", b"future", b"v"]),
    )
    .expect("SET future should execute");
    let future_timestamp = (SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system time should be after epoch")
        .as_millis()
        + 120_000)
        .to_string()
        .into_bytes();
    let pexpireat_future = ingress_connection_bytes(
        &mut app,
        &mut connection,
        &resp_command(&[b"PEXPIREAT", b"future", &future_timestamp]),
    )
    .expect("PEXPIREAT future should execute");
    assert_that!(&pexpireat_future, eq(&vec![b":1\r\n".to_vec()]));

    let pttl_future = ingress_connection_bytes(
        &mut app,
        &mut connection,
        &resp_command(&[b"PTTL", b"future"]),
    )
    .expect("PTTL should execute");
    let pttl_value = parse_resp_integer(&pttl_future[0]);
    assert_that!(pttl_value > 0, eq(true));

    let _ = ingress_connection_bytes(
        &mut app,
        &mut connection,
        &resp_command(&[b"SET", b"past", b"v"]),
    )
    .expect("SET past should execute");
    let pexpireat_past = ingress_connection_bytes(
        &mut app,
        &mut connection,
        &resp_command(&[b"PEXPIREAT", b"past", b"1"]),
    )
    .expect("PEXPIREAT past should execute");
    assert_that!(&pexpireat_past, eq(&vec![b":1\r\n".to_vec()]));

    let removed =
        ingress_connection_bytes(&mut app, &mut connection, &resp_command(&[b"GET", b"past"]))
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
    let _ =
        ingress_connection_bytes(&mut app, &mut connection, &mset).expect("MSET should execute");

    let mget = resp_command(&[b"MGET", &second_key, &first_key, b"missing"]);
    let reply =
        ingress_connection_bytes(&mut app, &mut connection, &mget).expect("MGET should execute");
    let expected = vec![b"*3\r\n$4\r\nbeta\r\n$5\r\nalpha\r\n$-1\r\n".to_vec()];
    assert_that!(&reply, eq(&expected));
}

#[rstest]
fn resp_connection_handles_partial_input() {
    let mut app = ServerApp::new(RuntimeConfig::default());
    let mut connection = ServerApp::new_connection(ClientProtocol::Resp);

    let partial_responses =
        ingress_connection_bytes(&mut app, &mut connection, b"*2\r\n$4\r\nECHO\r\n$5\r\nhe")
            .expect("partial input should not fail");
    assert_that!(partial_responses.is_empty(), eq(true));

    let final_responses = ingress_connection_bytes(&mut app, &mut connection, b"llo\r\n")
        .expect("completion bytes should execute pending command");
    let expected = vec![b"$5\r\nhello\r\n".to_vec()];
    assert_that!(&final_responses, eq(&expected));
}

#[rstest]
fn resp_connection_applies_parsed_prefix_before_protocol_error_in_same_chunk() {
    let mut app = ServerApp::new(RuntimeConfig::default());
    let mut broken_connection = ServerApp::new_connection(ClientProtocol::Resp);

    let error = ingress_connection_bytes(
        &mut app,
        &mut broken_connection,
        b"*3\r\n$3\r\nSET\r\n$3\r\nfoo\r\n$3\r\nbar\r\n*1\r\n$A\r\nPING\r\n",
    )
    .expect_err("malformed second command should fail");
    let DflyError::Protocol(_) = error else {
        panic!("expected protocol error");
    };

    // Keep readback on a clean connection because malformed bytes remain buffered on the
    // failing connection, while keyspace mutations from parsed prefix command must persist.
    let mut reader = ServerApp::new_connection(ClientProtocol::Resp);
    let get = ingress_connection_bytes(&mut app, &mut reader, &resp_command(&[b"GET", b"foo"]))
        .expect("GET should execute after malformed write chunk");
    assert_that!(&get, eq(&vec![b"$3\r\nbar\r\n".to_vec()]));
}

