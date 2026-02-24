use super::ServerApp;
use dfly_cluster::slot::{SlotRange, key_slot};
use dfly_common::config::{ClusterMode, RuntimeConfig};
use dfly_common::error::DflyError;
use dfly_core::command::{CommandFrame, CommandReply};
use dfly_facade::protocol::ClientProtocol;
use dfly_replication::journal::{InMemoryJournal, JournalEntry, JournalOp};
use dfly_transaction::plan::{TransactionHop, TransactionMode, TransactionPlan};
use dfly_transaction::scheduler::TransactionScheduler;
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
    assert_that!(plan.touched_shards.len(), eq(2_usize));
    assert_that!(plan.touched_shards.contains(&first_shard), eq(true));
    assert_that!(plan.touched_shards.contains(&second_shard), eq(true));

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
    assert_that!(plan.touched_shards.len(), eq(2_usize));
    assert_that!(plan.touched_shards.contains(&first_shard), eq(true));
    assert_that!(plan.touched_shards.contains(&second_shard), eq(true));

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
    let expected_touched = app.all_runtime_shards();

    let plan = app.build_exec_plan(&queued);
    assert_that!(plan.mode, eq(TransactionMode::Global));
    assert_that!(plan.hops.len(), eq(3_usize));
    assert_that!(
        plan.hops.iter().all(|hop| hop.per_shard.len() == 1),
        eq(true)
    );
    assert_that!(&plan.touched_shards, eq(&expected_touched));

    let flattened = plan
        .hops
        .iter()
        .flat_map(|hop| hop.per_shard.iter().map(|(_, command)| command.clone()))
        .collect::<Vec<_>>();
    assert_that!(&flattened, eq(&queued));
}

#[rstest]
fn exec_plan_global_mode_layers_by_runtime_shard_dependencies() {
    let mut app = ServerApp::new(RuntimeConfig::default());

    let first_key = b"plan:global:mset:1".to_vec();
    let first_shard = app.core.resolve_shard_for_key(&first_key);

    let mut second_key = b"plan:global:mset:2".to_vec();
    let mut second_shard = app.core.resolve_shard_for_key(&second_key);
    let mut suffix = 0_u32;
    while second_shard == first_shard {
        suffix = suffix.saturating_add(1);
        second_key = format!("plan:global:mset:2:{suffix}").into_bytes();
        second_shard = app.core.resolve_shard_for_key(&second_key);
    }

    let mut third_key = b"plan:global:set:3".to_vec();
    let mut third_shard = app.core.resolve_shard_for_key(&third_key);
    while third_shard == first_shard || third_shard == second_shard {
        suffix = suffix.saturating_add(1);
        third_key = format!("plan:global:set:3:{suffix}").into_bytes();
        third_shard = app.core.resolve_shard_for_key(&third_key);
    }

    let queued = vec![
        CommandFrame::new(
            "MSET",
            vec![
                first_key.clone(),
                b"a".to_vec(),
                second_key.clone(),
                b"b".to_vec(),
            ],
        ),
        CommandFrame::new("SET", vec![third_key.clone(), b"c".to_vec()]),
        CommandFrame::new("GET", vec![first_key.clone()]),
    ];

    let plan = app.build_exec_plan(&queued);
    assert_that!(plan.mode, eq(TransactionMode::Global));
    assert_that!(plan.hops.len(), eq(2_usize));
    assert_that!(plan.hops[0].per_shard.len(), eq(2_usize));
    assert_that!(plan.hops[1].per_shard.len(), eq(1_usize));
    assert_that!(plan.touched_shards.contains(&first_shard), eq(true));
    assert_that!(plan.touched_shards.contains(&second_shard), eq(true));
    assert_that!(plan.touched_shards.contains(&third_shard), eq(true));

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
    assert_that!(first_runtime[0].execute_on_worker, eq(true));
    assert_that!(second_runtime[0].execute_on_worker, eq(true));
}

#[rstest]
fn exec_plan_lockahead_single_key_runs_once_in_worker() {
    let mut app = ServerApp::new(RuntimeConfig::default());
    let key = b"plan:rt:worker:single:incr".to_vec();
    let shard = app.core.resolve_shard_for_key(&key);
    let plan = TransactionPlan {
        txid: 77,
        mode: TransactionMode::LockAhead,
        hops: vec![TransactionHop {
            per_shard: vec![(shard, CommandFrame::new("INCR", vec![key.clone()]))],
        }],
        touched_shards: vec![shard],
    };

    let replies = app.execute_transaction_plan(0, &plan);
    assert_that!(&replies, eq(&vec![CommandReply::Integer(1)]));

    let value = app
        .core
        .execute_in_db(0, &CommandFrame::new("GET", vec![key.clone()]));
    assert_that!(&value, eq(&CommandReply::BulkString(b"1".to_vec())));

    let runtime = app
        .runtime
        .drain_processed_for_shard(shard)
        .expect("drain should succeed");
    assert_that!(runtime.len(), eq(1_usize));
    assert_that!(runtime[0].execute_on_worker, eq(true));
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
        touched_shards: vec![shard],
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
    assert_that!(&plan.touched_shards, eq(&vec![first_shard, second_shard]));

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
fn exec_plan_global_same_shard_copy_executes_on_worker_fiber() {
    let mut app = ServerApp::new(RuntimeConfig::default());
    let source_key = b"plan:rt:global:copy:source".to_vec();
    let shard = app.core.resolve_shard_for_key(&source_key);
    let mut destination_key = b"plan:rt:global:copy:destination".to_vec();
    let mut destination_shard = app.core.resolve_shard_for_key(&destination_key);
    let mut suffix = 0_u32;
    while destination_shard != shard {
        suffix = suffix.saturating_add(1);
        destination_key = format!("plan:rt:global:copy:destination:{suffix}").into_bytes();
        destination_shard = app.core.resolve_shard_for_key(&destination_key);
    }

    let set = app.execute_user_command(
        0,
        &CommandFrame::new("SET", vec![source_key.clone(), b"value".to_vec()]),
        None,
    );
    assert_that!(&set, eq(&CommandReply::SimpleString("OK".to_owned())));
    for shard_id in 0_u16..app.config.shard_count.get() {
        let _ = app
            .runtime
            .drain_processed_for_shard(shard_id)
            .expect("drain should succeed");
    }

    let queued = vec![CommandFrame::new(
        "COPY",
        vec![source_key.clone(), destination_key.clone()],
    )];
    let plan = app.build_exec_plan(&queued);
    assert_that!(plan.mode, eq(TransactionMode::Global));

    let replies = app.execute_transaction_plan(0, &plan);
    assert_that!(&replies, eq(&vec![CommandReply::Integer(1)]));
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
    assert_that!(&runtime[0].command, eq(&queued[0]));
    assert_that!(runtime[0].execute_on_worker, eq(true));

    let destination_value = app
        .core
        .execute_in_db(0, &CommandFrame::new("GET", vec![destination_key]));
    assert_that!(
        &destination_value,
        eq(&CommandReply::BulkString(b"value".to_vec()))
    );
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
fn direct_command_respects_scheduler_barrier() {
    let app = ServerApp::new(RuntimeConfig::default());
    let key = b"direct:rt:block".to_vec();
    let shard = app.core.resolve_shard_for_key(&key);
    let plan = TransactionPlan {
        txid: 1000,
        mode: TransactionMode::LockAhead,
        hops: vec![TransactionHop {
            per_shard: vec![(
                shard,
                CommandFrame::new("SET", vec![key.clone(), b"value".to_vec()]),
            )],
        }],
        touched_shards: vec![shard],
    };

    assert_that!(app.transaction.scheduler.schedule(&plan).is_ok(), eq(true));

    let blocked_command = CommandFrame::new("SET", vec![key.clone(), b"blocked".to_vec()]);
    let error = app
        .dispatch_direct_command_runtime(0, &blocked_command)
        .expect_err("scheduler should block conflicting direct command");
    assert_eq!(
        error,
        DflyError::InvalidState("transaction shard queue is busy")
    );

    assert_that!(
        app.transaction.scheduler.conclude(plan.txid).is_ok(),
        eq(true)
    );
}

#[rstest]
fn direct_mget_dispatches_runtime_to_each_touched_shard() {
    let mut app = ServerApp::new(RuntimeConfig::default());
    let first_key = b"direct:rt:mget:1".to_vec();
    let first_key_for_assert = first_key.clone();
    let first_shard = app.core.resolve_shard_for_key(&first_key);
    let mut second_key = b"direct:rt:mget:2".to_vec();
    let mut second_shard = app.core.resolve_shard_for_key(&second_key);
    let mut suffix = 0_u32;
    while second_shard == first_shard {
        suffix = suffix.saturating_add(1);
        second_key = format!("direct:rt:mget:2:{suffix}").into_bytes();
        second_shard = app.core.resolve_shard_for_key(&second_key);
    }
    let second_key_for_assert = second_key.clone();

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
    assert_that!(&first_runtime[0].command.name, eq(&"GET".to_owned()));
    assert_that!(&second_runtime[0].command.name, eq(&"GET".to_owned()));
    assert_that!(
        &first_runtime[0].command.args,
        eq(&vec![first_key_for_assert])
    );
    assert_that!(
        &second_runtime[0].command.args,
        eq(&vec![second_key_for_assert])
    );
    assert_that!(first_runtime[0].execute_on_worker, eq(true));
    assert_that!(second_runtime[0].execute_on_worker, eq(true));
}

#[rstest]
fn direct_mget_dispatches_runtime_once_when_keys_share_same_shard() {
    let mut app = ServerApp::new(RuntimeConfig::default());
    let first_key = b"direct:rt:same:1".to_vec();
    let first_key_for_assert = first_key.clone();
    let shard = app.core.resolve_shard_for_key(&first_key);
    let mut second_key = b"direct:rt:same:2".to_vec();
    let mut second_shard = app.core.resolve_shard_for_key(&second_key);
    let mut suffix = 0_u32;
    while second_shard != shard {
        suffix = suffix.saturating_add(1);
        second_key = format!("direct:rt:same:2:{suffix}").into_bytes();
        second_shard = app.core.resolve_shard_for_key(&second_key);
    }
    let second_key_for_assert = second_key.clone();

    let frame = CommandFrame::new("MGET", vec![first_key, second_key]);
    let _ = app.execute_user_command(0, &frame, None);
    let runtime = app
        .runtime
        .drain_processed_for_shard(shard)
        .expect("drain should succeed");
    assert_that!(runtime.len(), eq(2_usize));
    assert_that!(&runtime[0].command.name, eq(&"GET".to_owned()));
    assert_that!(&runtime[1].command.name, eq(&"GET".to_owned()));
    assert_that!(&runtime[0].command.args, eq(&vec![first_key_for_assert]));
    assert_that!(&runtime[1].command.args, eq(&vec![second_key_for_assert]));
    assert_that!(runtime[0].execute_on_worker, eq(true));
    assert_that!(runtime[1].execute_on_worker, eq(true));
}

#[rstest]
fn direct_mset_executes_each_key_on_worker_fiber() {
    let mut app = ServerApp::new(RuntimeConfig::default());
    let first_key = b"direct:rt:mset:1".to_vec();
    let first_shard = app.core.resolve_shard_for_key(&first_key);
    let mut second_key = b"direct:rt:mset:2".to_vec();
    let mut second_shard = app.core.resolve_shard_for_key(&second_key);
    let mut suffix = 0_u32;
    while second_shard == first_shard {
        suffix = suffix.saturating_add(1);
        second_key = format!("direct:rt:mset:2:{suffix}").into_bytes();
        second_shard = app.core.resolve_shard_for_key(&second_key);
    }

    let frame = CommandFrame::new(
        "MSET",
        vec![
            first_key.clone(),
            b"a".to_vec(),
            second_key.clone(),
            b"b".to_vec(),
        ],
    );
    let reply = app.execute_user_command(0, &frame, None);
    assert_that!(&reply, eq(&CommandReply::SimpleString("OK".to_owned())));
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
    assert_that!(&first_runtime[0].command.name, eq(&"SET".to_owned()));
    assert_that!(&second_runtime[0].command.name, eq(&"SET".to_owned()));
    assert_that!(
        &first_runtime[0].command.args,
        eq(&vec![first_key.clone(), b"a".to_vec()])
    );
    assert_that!(
        &second_runtime[0].command.args,
        eq(&vec![second_key.clone(), b"b".to_vec()])
    );
    assert_that!(first_runtime[0].execute_on_worker, eq(true));
    assert_that!(second_runtime[0].execute_on_worker, eq(true));
}

#[rstest]
fn direct_copy_same_shard_executes_on_worker_fiber() {
    let mut app = ServerApp::new(RuntimeConfig::default());
    let source_key = b"direct:rt:copy:source".to_vec();
    let shard = app.core.resolve_shard_for_key(&source_key);
    let mut destination_key = b"direct:rt:copy:destination".to_vec();
    let mut destination_shard = app.core.resolve_shard_for_key(&destination_key);
    let mut suffix = 0_u32;
    while destination_shard != shard {
        suffix = suffix.saturating_add(1);
        destination_key = format!("direct:rt:copy:destination:{suffix}").into_bytes();
        destination_shard = app.core.resolve_shard_for_key(&destination_key);
    }

    let set = app.execute_user_command(
        0,
        &CommandFrame::new("SET", vec![source_key.clone(), b"value".to_vec()]),
        None,
    );
    assert_that!(&set, eq(&CommandReply::SimpleString("OK".to_owned())));
    for shard_id in 0_u16..app.config.shard_count.get() {
        let _ = app
            .runtime
            .drain_processed_for_shard(shard_id)
            .expect("drain should succeed");
    }

    let frame = CommandFrame::new("COPY", vec![source_key.clone(), destination_key.clone()]);
    let reply = app.execute_user_command(0, &frame, None);
    assert_that!(&reply, eq(&CommandReply::Integer(1)));
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
    assert_that!(runtime[0].execute_on_worker, eq(true));

    let destination_value = app
        .core
        .execute_in_db(0, &CommandFrame::new("GET", vec![destination_key]));
    assert_that!(
        &destination_value,
        eq(&CommandReply::BulkString(b"value".to_vec()))
    );
}

#[rstest]
fn direct_del_multikey_executes_each_key_on_worker_fiber() {
    let mut app = ServerApp::new(RuntimeConfig::default());
    let first_key = b"direct:rt:del:1".to_vec();
    let first_shard = app.core.resolve_shard_for_key(&first_key);
    let mut second_key = b"direct:rt:del:2".to_vec();
    let mut second_shard = app.core.resolve_shard_for_key(&second_key);
    let mut suffix = 0_u32;
    while second_shard == first_shard {
        suffix = suffix.saturating_add(1);
        second_key = format!("direct:rt:del:2:{suffix}").into_bytes();
        second_shard = app.core.resolve_shard_for_key(&second_key);
    }

    let set_first = app.execute_user_command(
        0,
        &CommandFrame::new("SET", vec![first_key.clone(), b"a".to_vec()]),
        None,
    );
    let set_second = app.execute_user_command(
        0,
        &CommandFrame::new("SET", vec![second_key.clone(), b"b".to_vec()]),
        None,
    );
    assert_that!(&set_first, eq(&CommandReply::SimpleString("OK".to_owned())));
    assert_that!(
        &set_second,
        eq(&CommandReply::SimpleString("OK".to_owned()))
    );

    for shard in 0_u16..app.config.shard_count.get() {
        let _ = app
            .runtime
            .drain_processed_for_shard(shard)
            .expect("drain should succeed");
    }

    let frame = CommandFrame::new("DEL", vec![first_key.clone(), second_key.clone()]);
    let reply = app.execute_user_command(0, &frame, None);
    assert_that!(&reply, eq(&CommandReply::Integer(2)));

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
    assert_that!(&first_runtime[0].command.name, eq(&"DEL".to_owned()));
    assert_that!(&second_runtime[0].command.name, eq(&"DEL".to_owned()));
    assert_that!(&first_runtime[0].command.args, eq(&vec![first_key]));
    assert_that!(&second_runtime[0].command.args, eq(&vec![second_key]));
    assert_that!(first_runtime[0].execute_on_worker, eq(true));
    assert_that!(second_runtime[0].execute_on_worker, eq(true));
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
        touched_shards: vec![u16::MAX],
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
        touched_shards: vec![shard, u16::MAX],
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
        touched_shards: vec![shard, u16::MAX],
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
        touched_shards: vec![0, key_shard],
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
        touched_shards: vec![0],
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
fn resp_info_default_sections_include_replication_and_persistence() {
    let mut app = ServerApp::new(RuntimeConfig::default());
    let mut connection = ServerApp::new_connection(ClientProtocol::Resp);

    let reply = app
        .feed_connection_bytes(&mut connection, &resp_command(&[b"INFO"]))
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
fn resp_replconf_ack_without_registered_endpoint_is_tracked_via_implicit_endpoint() {
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
    assert_that!(app.replication.last_acked_lsn(), eq(1_u64));

    let info = app
        .feed_connection_bytes(&mut connection, &resp_command(&[b"INFO", b"REPLICATION"]))
        .expect("INFO REPLICATION should succeed");
    let body = decode_resp_bulk_payload(&info[0]);
    assert_that!(body.contains("connected_slaves:1\r\n"), eq(true));
    assert_that!(body.contains("last_ack_lsn:1\r\n"), eq(true));

    let wait = app
        .feed_connection_bytes(&mut connection, &resp_command(&[b"WAIT", b"1", b"0"]))
        .expect("WAIT should execute");
    assert_that!(&wait, eq(&vec![b":1\r\n".to_vec()]));
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
fn resp_wait_honors_timeout_when_required_replica_count_is_unmet() {
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

    let started = std::time::Instant::now();
    let wait = app
        .feed_connection_bytes(&mut client, &resp_command(&[b"WAIT", b"2", b"12"]))
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
    let expected = format!("+FULLRESYNC {} 0\r\n", app.replication.master_replid()).into_bytes();
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
    let expected = format!("+FULLRESYNC {} 2\r\n", app.replication.master_replid()).into_bytes();
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

    let _ = app
        .feed_connection_bytes(&mut connection, &resp_command(&[b"SET", &first_key, b"v1"]))
        .expect("SET should succeed");
    let _ = app
        .feed_connection_bytes(
            &mut connection,
            &resp_command(&[b"SET", &second_key, b"v2"]),
        )
        .expect("SET should succeed");
    let _ = app
        .feed_connection_bytes(
            &mut connection,
            &resp_command(&[b"SET", &other_slot_key, b"x"]),
        )
        .expect("SET should succeed");
    let _ = app
        .feed_connection_bytes(
            &mut connection,
            &resp_command(&[b"SET", &expired_key, b"gone"]),
        )
        .expect("SET should succeed");
    let _ = app
        .feed_connection_bytes(
            &mut connection,
            &resp_command(&[b"EXPIRE", &expired_key, b"0"]),
        )
        .expect("EXPIRE should succeed");

    let slot_text = slot.to_string().into_bytes();
    let limited = app
        .feed_connection_bytes(
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

    let full = app
        .feed_connection_bytes(
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

    let arity = app
        .feed_connection_bytes(
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

    let invalid_slot = app
        .feed_connection_bytes(
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

    let out_of_range = app
        .feed_connection_bytes(
            &mut connection,
            &resp_command(&[b"CLUSTER", b"GETKEYSINSLOT", b"20000", b"10"]),
        )
        .expect("CLUSTER GETKEYSINSLOT should parse");
    assert_that!(
        &out_of_range,
        eq(&vec![b"-ERR slot is out of range\r\n".to_vec()])
    );

    let invalid_count = app
        .feed_connection_bytes(
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

    let _ = app
        .feed_connection_bytes(&mut connection, &resp_command(&[b"SET", &first_key, b"v1"]))
        .expect("SET should succeed");
    let _ = app
        .feed_connection_bytes(
            &mut connection,
            &resp_command(&[b"SET", &second_key, b"v2"]),
        )
        .expect("SET should succeed");
    let _ = app
        .feed_connection_bytes(
            &mut connection,
            &resp_command(&[b"SET", &other_slot_key, b"x"]),
        )
        .expect("SET should succeed");
    let _ = app
        .feed_connection_bytes(
            &mut connection,
            &resp_command(&[b"SET", &expired_key, b"gone"]),
        )
        .expect("SET should succeed");
    let _ = app
        .feed_connection_bytes(
            &mut connection,
            &resp_command(&[b"EXPIRE", &expired_key, b"0"]),
        )
        .expect("EXPIRE should succeed");

    let slot_text = slot.to_string().into_bytes();
    let reply = app
        .feed_connection_bytes(
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

    let arity = app
        .feed_connection_bytes(
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

    let invalid_slot = app
        .feed_connection_bytes(
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

    let out_of_range = app
        .feed_connection_bytes(
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
fn resp_dfly_load_resets_replication_state() {
    let mut app = ServerApp::new(RuntimeConfig::default());
    let mut connection = ServerApp::new_connection(ClientProtocol::Resp);

    let _ = app
        .feed_connection_bytes(
            &mut connection,
            &resp_command(&[b"SET", b"load:reset:key", b"initial"]),
        )
        .expect("initial SET should succeed");
    assert_that!(app.replication.replication_offset(), gt(0_u64));

    let path = unique_test_snapshot_path("dfly-load-reset");
    let path_bytes = path.to_string_lossy().as_bytes().to_vec();

    let save_reply = app
        .feed_connection_bytes(
            &mut connection,
            &resp_command(&[b"DFLY", b"SAVE", &path_bytes]),
        )
        .expect("DFLY SAVE should execute");
    assert_that!(&save_reply, eq(&vec![b"+OK\r\n".to_vec()]));

    let _ = app
        .feed_connection_bytes(
            &mut connection,
            &resp_command(&[b"SET", b"load:reset:key", b"new"]),
        )
        .expect("second SET should succeed");
    assert_that!(app.replication.replication_offset(), gt(1_u64));

    let load_reply = app
        .feed_connection_bytes(
            &mut connection,
            &resp_command(&[b"DFLY", b"LOAD", &path_bytes]),
        )
        .expect("DFLY LOAD should execute");
    assert_that!(&load_reply, eq(&vec![b"+OK\r\n".to_vec()]));

    assert_that!(app.replication.replication_offset(), eq(0_u64));
    assert_that!(app.replication.journal_entries().is_empty(), eq(true));
    assert_that!(app.replication.journal_lsn(), eq(1_u64));

    let _ = std::fs::remove_file(path);
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
fn journal_replay_bypasses_scheduler_barrier_for_recovery() {
    let mut app = ServerApp::new(RuntimeConfig::default());
    let key = b"journal:recovery:barrier".to_vec();
    let shard = app.core.resolve_shard_for_key(&key);
    let blocking_plan = TransactionPlan {
        txid: 900,
        mode: TransactionMode::LockAhead,
        hops: vec![TransactionHop {
            per_shard: vec![(
                shard,
                CommandFrame::new("SET", vec![key.clone(), b"pending".to_vec()]),
            )],
        }],
        touched_shards: vec![shard],
    };
    assert_that!(
        app.transaction.scheduler.schedule(&blocking_plan).is_ok(),
        eq(true)
    );

    app.replication.append_journal(JournalEntry {
        txid: 1,
        db: 0,
        op: JournalOp::Command,
        payload: resp_command(&[b"SET", &key, b"replayed"]),
    });

    let applied = app
        .recover_from_replication_journal()
        .expect("recovery replay should bypass scheduler barrier");
    assert_that!(applied, eq(1_usize));
    let value = app
        .core
        .execute_in_db(0, &CommandFrame::new("GET", vec![key.clone()]));
    assert_that!(&value, eq(&CommandReply::BulkString(b"replayed".to_vec())));

    assert_that!(
        app.transaction
            .scheduler
            .conclude(blocking_plan.txid)
            .is_ok(),
        eq(true)
    );
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
