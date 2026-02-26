    use super::{CommandRegistry, DispatchState};
    use crate::command::{CommandFrame, CommandReply};
    use dfly_cluster::slot::key_slot;
    use googletest::prelude::*;
    use hashbrown::HashSet;
    use rstest::rstest;

    #[rstest]
    fn dispatch_rebuilds_expire_index_for_layered_db_table() {
        let registry = CommandRegistry::with_builtin_commands();
        let mut state = DispatchState::default();
        let key = b"layered:expire:key".to_vec();

        let set = registry.dispatch(
            0,
            &CommandFrame::new(
                "SET",
                vec![
                    key.clone(),
                    b"value".to_vec(),
                    b"EX".to_vec(),
                    b"30".to_vec(),
                ],
            ),
            &mut state,
        );
        assert_that!(&set, eq(&CommandReply::SimpleString("OK".to_owned())));

        let Some(table) = state.db_tables.get(&0) else {
            panic!("db table must exist after SET");
        };
        assert_that!(table.expire.contains_key(&key), eq(true));
        let expire_at = table
            .expire
            .get(&key)
            .copied()
            .expect("set with EX should keep expiry index");
        assert_that!(
            table
                .expire_order
                .get(&expire_at)
                .is_some_and(|keys| keys.contains(&key)),
            eq(true)
        );

        let _ = registry.dispatch(0, &CommandFrame::new("DEL", vec![key.clone()]), &mut state);
        let Some(table) = state.db_tables.get(&0) else {
            panic!("db table should be kept after key deletion");
        };
        assert_that!(table.expire.contains_key(&key), eq(false));
        assert_that!(
            table.expire_order.values().any(|keys| keys.contains(&key)),
            eq(false)
        );
    }

    #[rstest]
    fn dispatch_rebuilds_ordered_expire_index_after_ttl_updates() {
        let registry = CommandRegistry::with_builtin_commands();
        let mut state = DispatchState::default();
        let key = b"layered:expire:update:key".to_vec();

        let set = registry.dispatch(
            0,
            &CommandFrame::new(
                "SET",
                vec![
                    key.clone(),
                    b"value".to_vec(),
                    b"EX".to_vec(),
                    b"30".to_vec(),
                ],
            ),
            &mut state,
        );
        assert_that!(&set, eq(&CommandReply::SimpleString("OK".to_owned())));
        let original_deadline = state
            .db_tables
            .get(&0)
            .and_then(|table| table.expire.get(&key))
            .copied()
            .expect("initial ttl should be indexed");

        let expire = registry.dispatch(
            0,
            &CommandFrame::new("EXPIRE", vec![key.clone(), b"90".to_vec()]),
            &mut state,
        );
        assert_that!(&expire, eq(&CommandReply::Integer(1)));
        let Some(table) = state.db_tables.get(&0) else {
            panic!("db table should exist after EXPIRE");
        };
        let updated_deadline = table
            .expire
            .get(&key)
            .copied()
            .expect("updated ttl should be indexed");
        assert_that!(updated_deadline > original_deadline, eq(true));
        assert_that!(
            table
                .expire_order
                .get(&original_deadline)
                .is_none_or(|keys| !keys.contains(&key)),
            eq(true)
        );
        assert_that!(
            table
                .expire_order
                .get(&updated_deadline)
                .is_some_and(|keys| keys.contains(&key)),
            eq(true)
        );

        let persist = registry.dispatch(
            0,
            &CommandFrame::new("PERSIST", vec![key.clone()]),
            &mut state,
        );
        assert_that!(&persist, eq(&CommandReply::Integer(1)));
        let Some(table) = state.db_tables.get(&0) else {
            panic!("db table should remain allocated after PERSIST");
        };
        assert_that!(table.expire.is_empty(), eq(true));
        assert_that!(
            table
                .expire_order
                .values()
                .flat_map(|keys| keys.iter())
                .any(|indexed_key| indexed_key == &key),
            eq(false)
        );
    }

    #[rstest]
    fn dispatch_rebuilds_slot_stats_for_layered_db_table() {
        let registry = CommandRegistry::with_builtin_commands();
        let mut state = DispatchState::default();
        let key = b"layered:slot:key".to_vec();
        let slot = key_slot(&key);

        let _ = registry.dispatch(
            0,
            &CommandFrame::new("SET", vec![key.clone(), b"value".to_vec()]),
            &mut state,
        );
        let Some(table) = state.db_tables.get(&0) else {
            panic!("db table must exist after SET");
        };
        assert_that!(table.slot_stats.get(slot), eq(1_usize));
        assert_that!(table.slot_stats.total_writes(slot), eq(1_u64));
        assert_that!(
            table.slot_stats.memory_bytes(slot),
            eq(key.len().saturating_add(b"value".len()))
        );
        assert_that!(
            table.slot_keys.get(&slot).map(HashSet::len),
            eq(Some(1_usize))
        );
        assert_that!(&state.slot_keys(0, slot), eq(&vec![key.clone()]));

        let _ = registry.dispatch(0, &CommandFrame::new("DEL", vec![key.clone()]), &mut state);
        let Some(table) = state.db_tables.get(&0) else {
            panic!("db table should be kept after key deletion");
        };
        assert_that!(table.slot_stats.get(slot), eq(0_usize));
        assert_that!(table.slot_stats.total_writes(slot), eq(2_u64));
        assert_that!(table.slot_stats.memory_bytes(slot), eq(0_usize));
        assert_that!(table.slot_keys.get(&slot).map(HashSet::len), eq(None));
        assert_that!(&state.slot_keys(0, slot), eq(&Vec::<Vec<u8>>::new()));
    }

    #[rstest]
    fn dispatch_slot_live_queries_reconcile_drifted_slot_index() {
        let registry = CommandRegistry::with_builtin_commands();
        let mut state = DispatchState::default();
        let key = b"layered:slot:drift".to_vec();
        let slot = key_slot(&key);

        let _ = registry.dispatch(
            0,
            &CommandFrame::new("SET", vec![key.clone(), b"value".to_vec()]),
            &mut state,
        );
        if let Some(table) = state.db_tables.get_mut(&0) {
            let _ = table.slot_keys.insert(slot, HashSet::new());
            table.slot_stats.set_count(slot, 0_usize);
            table.slot_stats.set_memory_bytes(slot, 0_usize);
        }

        assert_that!(state.count_live_keys_in_slot(0, slot), eq(1_usize));
        let Some(table) = state.db_tables.get(&0) else {
            panic!("db table should remain after slot reconciliation");
        };
        assert_that!(table.slot_stats.get(slot), eq(1_usize));
        assert_that!(
            table.slot_keys.get(&slot).map(HashSet::len),
            eq(Some(1_usize))
        );
        assert_that!(
            table.slot_stats.memory_bytes(slot),
            eq(key.len().saturating_add(b"value".len()))
        );
        assert_that!(&state.slot_keys(0, slot), eq(&vec![key.clone()]));
    }

    #[rstest]
    fn dispatch_slot_keys_fallback_recovers_missing_slot_index_entry() {
        let registry = CommandRegistry::with_builtin_commands();
        let mut state = DispatchState::default();
        let key = b"layered:slot:index:fallback".to_vec();
        let slot = key_slot(&key);

        let _ = registry.dispatch(
            0,
            &CommandFrame::new("SET", vec![key.clone(), b"value".to_vec()]),
            &mut state,
        );
        if let Some(table) = state.db_tables.get_mut(&0) {
            let _ = table.slot_keys.remove(&slot);
            table.slot_stats.set_count(slot, 1_usize);
        }

        assert_that!(&state.slot_keys(0, slot), eq(&vec![key]));
    }

    #[rstest]
    fn dispatch_tracks_slot_read_and_write_counters() {
        let registry = CommandRegistry::with_builtin_commands();
        let mut state = DispatchState::default();
        let key = b"layered:slot:reads".to_vec();
        let slot = key_slot(&key);

        let _ = registry.dispatch(
            0,
            &CommandFrame::new("SET", vec![key.clone(), b"value".to_vec()]),
            &mut state,
        );
        let _ = registry.dispatch(0, &CommandFrame::new("GET", vec![key.clone()]), &mut state);
        let _ = registry.dispatch(
            0,
            &CommandFrame::new("MGET", vec![key.clone(), b"missing".to_vec()]),
            &mut state,
        );
        let _ = registry.dispatch(
            0,
            &CommandFrame::new("EXISTS", vec![key.clone(), b"missing".to_vec()]),
            &mut state,
        );
        let _ = registry.dispatch(
            0,
            &CommandFrame::new("STRLEN", vec![key.clone()]),
            &mut state,
        );
        let _ = registry.dispatch(
            0,
            &CommandFrame::new("GETRANGE", vec![key.clone(), b"0".to_vec(), b"0".to_vec()]),
            &mut state,
        );
        let _ = registry.dispatch(0, &CommandFrame::new("TYPE", vec![key.clone()]), &mut state);

        let Some(table) = state.db_tables.get(&0) else {
            panic!("db table must exist after read/write sequence");
        };
        assert_that!(table.slot_stats.get(slot), eq(1_usize));
        assert_that!(table.slot_stats.total_writes(slot), eq(1_u64));
        assert_that!(table.slot_stats.total_reads(slot), eq(6_u64));
    }

    #[rstest]
    fn dispatch_slot_memory_counter_tracks_overwrite_delta() {
        let registry = CommandRegistry::with_builtin_commands();
        let mut state = DispatchState::default();
        let key = b"layered:slot:memory".to_vec();
        let slot = key_slot(&key);

        let _ = registry.dispatch(
            0,
            &CommandFrame::new("SET", vec![key.clone(), b"first".to_vec()]),
            &mut state,
        );
        let _ = registry.dispatch(
            0,
            &CommandFrame::new("SET", vec![key.clone(), b"x".to_vec()]),
            &mut state,
        );

        let Some(table) = state.db_tables.get(&0) else {
            panic!("db table must exist after overwrite sequence");
        };
        assert_that!(table.slot_stats.get(slot), eq(1_usize));
        assert_that!(table.slot_stats.total_writes(slot), eq(2_u64));
        assert_that!(
            table.slot_stats.memory_bytes(slot),
            eq(key.len().saturating_add(b"x".len()))
        );
    }

    #[rstest]
    fn dispatch_ping_and_echo() {
        let registry = CommandRegistry::with_builtin_commands();
        let mut state = DispatchState::default();

        let ping = registry.dispatch(0, &CommandFrame::new("PING", Vec::new()), &mut state);
        assert_that!(&ping, eq(&CommandReply::SimpleString("PONG".to_owned())));

        let echo = registry.dispatch(
            0,
            &CommandFrame::new("ECHO", vec![b"hello".to_vec()]),
            &mut state,
        );
        assert_that!(&echo, eq(&CommandReply::BulkString(b"hello".to_vec())));
    }

    #[rstest]
    fn dispatch_set_then_get_roundtrip() {
        let registry = CommandRegistry::with_builtin_commands();
        let mut state = DispatchState::default();

        let set_reply = registry.dispatch(
            0,
            &CommandFrame::new("SET", vec![b"user:1".to_vec(), b"alice".to_vec()]),
            &mut state,
        );
        assert_that!(&set_reply, eq(&CommandReply::SimpleString("OK".to_owned())));

        let get_reply = registry.dispatch(
            0,
            &CommandFrame::new("GET", vec![b"user:1".to_vec()]),
            &mut state,
        );
        assert_that!(&get_reply, eq(&CommandReply::BulkString(b"alice".to_vec())));
    }

    #[rstest]
    fn dispatch_set_supports_conditional_and_get_options() {
        let registry = CommandRegistry::with_builtin_commands();
        let mut state = DispatchState::default();

        let first = registry.dispatch(
            0,
            &CommandFrame::new("SET", vec![b"k".to_vec(), b"v1".to_vec(), b"NX".to_vec()]),
            &mut state,
        );
        assert_that!(&first, eq(&CommandReply::SimpleString("OK".to_owned())));

        let skipped = registry.dispatch(
            0,
            &CommandFrame::new(
                "SET",
                vec![b"k".to_vec(), b"ignored".to_vec(), b"nx".to_vec()],
            ),
            &mut state,
        );
        assert_that!(&skipped, eq(&CommandReply::Null));

        let skipped_with_get = registry.dispatch(
            0,
            &CommandFrame::new(
                "SET",
                vec![
                    b"k".to_vec(),
                    b"ignored".to_vec(),
                    b"NX".to_vec(),
                    b"GET".to_vec(),
                ],
            ),
            &mut state,
        );
        assert_that!(
            &skipped_with_get,
            eq(&CommandReply::BulkString(b"v1".to_vec()))
        );

        let xx_missing = registry.dispatch(
            0,
            &CommandFrame::new(
                "SET",
                vec![
                    b"missing".to_vec(),
                    b"value".to_vec(),
                    b"XX".to_vec(),
                    b"GET".to_vec(),
                ],
            ),
            &mut state,
        );
        assert_that!(&xx_missing, eq(&CommandReply::Null));

        let xx_existing = registry.dispatch(
            0,
            &CommandFrame::new(
                "SET",
                vec![
                    b"k".to_vec(),
                    b"v2".to_vec(),
                    b"xx".to_vec(),
                    b"get".to_vec(),
                ],
            ),
            &mut state,
        );
        assert_that!(&xx_existing, eq(&CommandReply::BulkString(b"v1".to_vec())));

        let current = registry.dispatch(
            0,
            &CommandFrame::new("GET", vec![b"k".to_vec()]),
            &mut state,
        );
        assert_that!(&current, eq(&CommandReply::BulkString(b"v2".to_vec())));
    }

    #[rstest]
    fn dispatch_set_applies_expire_and_keepttl_options() {
        let registry = CommandRegistry::with_builtin_commands();
        let mut state = DispatchState::default();

        let with_ex = registry.dispatch(
            0,
            &CommandFrame::new(
                "SET",
                vec![
                    b"ttl:key".to_vec(),
                    b"v1".to_vec(),
                    b"EX".to_vec(),
                    b"60".to_vec(),
                ],
            ),
            &mut state,
        );
        assert_that!(&with_ex, eq(&CommandReply::SimpleString("OK".to_owned())));

        let first_ttl = registry.dispatch(
            0,
            &CommandFrame::new("TTL", vec![b"ttl:key".to_vec()]),
            &mut state,
        );
        let CommandReply::Integer(first_ttl) = first_ttl else {
            panic!("TTL must return integer");
        };
        assert_that!(first_ttl > 0, eq(true));

        let keep_ttl = registry.dispatch(
            0,
            &CommandFrame::new(
                "SET",
                vec![b"ttl:key".to_vec(), b"v2".to_vec(), b"KEEPTTL".to_vec()],
            ),
            &mut state,
        );
        assert_that!(&keep_ttl, eq(&CommandReply::SimpleString("OK".to_owned())));

        let ttl_after_keep = registry.dispatch(
            0,
            &CommandFrame::new("TTL", vec![b"ttl:key".to_vec()]),
            &mut state,
        );
        let CommandReply::Integer(ttl_after_keep) = ttl_after_keep else {
            panic!("TTL must return integer");
        };
        assert_that!(ttl_after_keep > 0, eq(true));

        let clear_ttl = registry.dispatch(
            0,
            &CommandFrame::new("SET", vec![b"ttl:key".to_vec(), b"v3".to_vec()]),
            &mut state,
        );
        assert_that!(&clear_ttl, eq(&CommandReply::SimpleString("OK".to_owned())));

        let ttl_after_plain = registry.dispatch(
            0,
            &CommandFrame::new("TTL", vec![b"ttl:key".to_vec()]),
            &mut state,
        );
        assert_that!(&ttl_after_plain, eq(&CommandReply::Integer(-1)));

        let with_px = registry.dispatch(
            0,
            &CommandFrame::new(
                "SET",
                vec![
                    b"px:key".to_vec(),
                    b"value".to_vec(),
                    b"PX".to_vec(),
                    b"1200".to_vec(),
                ],
            ),
            &mut state,
        );
        assert_that!(&with_px, eq(&CommandReply::SimpleString("OK".to_owned())));

        let pttl = registry.dispatch(
            0,
            &CommandFrame::new("PTTL", vec![b"px:key".to_vec()]),
            &mut state,
        );
        let CommandReply::Integer(pttl) = pttl else {
            panic!("PTTL must return integer");
        };
        assert_that!(pttl > 0, eq(true));
    }

    #[rstest]
    fn dispatch_set_rejects_invalid_option_combinations() {
        let registry = CommandRegistry::with_builtin_commands();
        let mut state = DispatchState::default();

        let nx_xx = registry.dispatch(
            0,
            &CommandFrame::new(
                "SET",
                vec![
                    b"key".to_vec(),
                    b"value".to_vec(),
                    b"NX".to_vec(),
                    b"XX".to_vec(),
                ],
            ),
            &mut state,
        );
        assert_that!(&nx_xx, eq(&CommandReply::Error("syntax error".to_owned())));

        let duplicate_expire = registry.dispatch(
            0,
            &CommandFrame::new(
                "SET",
                vec![
                    b"key".to_vec(),
                    b"value".to_vec(),
                    b"EX".to_vec(),
                    b"1".to_vec(),
                    b"PX".to_vec(),
                    b"1000".to_vec(),
                ],
            ),
            &mut state,
        );
        assert_that!(
            &duplicate_expire,
            eq(&CommandReply::Error("syntax error".to_owned()))
        );

        let keepttl_with_expire = registry.dispatch(
            0,
            &CommandFrame::new(
                "SET",
                vec![
                    b"key".to_vec(),
                    b"value".to_vec(),
                    b"EX".to_vec(),
                    b"5".to_vec(),
                    b"KEEPTTL".to_vec(),
                ],
            ),
            &mut state,
        );
        assert_that!(
            &keepttl_with_expire,
            eq(&CommandReply::Error("syntax error".to_owned()))
        );

        let missing_expire_value = registry.dispatch(
            0,
            &CommandFrame::new(
                "SET",
                vec![b"key".to_vec(), b"value".to_vec(), b"EX".to_vec()],
            ),
            &mut state,
        );
        assert_that!(
            &missing_expire_value,
            eq(&CommandReply::Error("syntax error".to_owned()))
        );

        let invalid_expire_integer = registry.dispatch(
            0,
            &CommandFrame::new(
                "SET",
                vec![
                    b"key".to_vec(),
                    b"value".to_vec(),
                    b"EX".to_vec(),
                    b"abc".to_vec(),
                ],
            ),
            &mut state,
        );
        assert_that!(
            &invalid_expire_integer,
            eq(&CommandReply::Error(
                "value is not an integer or out of range".to_owned()
            ))
        );

        let invalid_expire_time = registry.dispatch(
            0,
            &CommandFrame::new(
                "SET",
                vec![
                    b"key".to_vec(),
                    b"value".to_vec(),
                    b"PX".to_vec(),
                    b"0".to_vec(),
                ],
            ),
            &mut state,
        );
        assert_that!(
            &invalid_expire_time,
            eq(&CommandReply::Error(
                "invalid expire time in 'SET' command".to_owned()
            ))
        );

        let unknown_option = registry.dispatch(
            0,
            &CommandFrame::new(
                "SET",
                vec![b"key".to_vec(), b"value".to_vec(), b"SOMETHING".to_vec()],
            ),
            &mut state,
        );
        assert_that!(
            &unknown_option,
            eq(&CommandReply::Error("syntax error".to_owned()))
        );
    }

    #[rstest]
    fn dispatch_type_reports_none_or_string() {
        let registry = CommandRegistry::with_builtin_commands();
        let mut state = DispatchState::default();

        let missing = registry.dispatch(
            0,
            &CommandFrame::new("TYPE", vec![b"nope".to_vec()]),
            &mut state,
        );
        assert_that!(&missing, eq(&CommandReply::SimpleString("none".to_owned())));

        let _ = registry.dispatch(
            0,
            &CommandFrame::new("SET", vec![b"k".to_vec(), b"v".to_vec()]),
            &mut state,
        );
        let existing = registry.dispatch(
            0,
            &CommandFrame::new("TYPE", vec![b"k".to_vec()]),
            &mut state,
        );
        assert_that!(
            &existing,
            eq(&CommandReply::SimpleString("string".to_owned()))
        );
    }

    #[rstest]
    fn dispatch_setnx_sets_only_when_key_is_missing() {
        let registry = CommandRegistry::with_builtin_commands();
        let mut state = DispatchState::default();

        let first = registry.dispatch(
            0,
            &CommandFrame::new("SETNX", vec![b"k".to_vec(), b"v1".to_vec()]),
            &mut state,
        );
        assert_that!(&first, eq(&CommandReply::Integer(1)));

        let second = registry.dispatch(
            0,
            &CommandFrame::new("SETNX", vec![b"k".to_vec(), b"v2".to_vec()]),
            &mut state,
        );
        assert_that!(&second, eq(&CommandReply::Integer(0)));

        let value = registry.dispatch(
            0,
            &CommandFrame::new("GET", vec![b"k".to_vec()]),
            &mut state,
        );
        assert_that!(&value, eq(&CommandReply::BulkString(b"v1".to_vec())));
    }

    #[rstest]
    fn dispatch_mget_preserves_order_and_null_entries() {
        let registry = CommandRegistry::with_builtin_commands();
        let mut state = DispatchState::default();

        let _ = registry.dispatch(
            0,
            &CommandFrame::new("SET", vec![b"k1".to_vec(), b"v1".to_vec()]),
            &mut state,
        );
        let _ = registry.dispatch(
            0,
            &CommandFrame::new("SET", vec![b"k2".to_vec(), b"v2".to_vec()]),
            &mut state,
        );

        let mget = registry.dispatch(
            0,
            &CommandFrame::new(
                "MGET",
                vec![b"k2".to_vec(), b"missing".to_vec(), b"k1".to_vec()],
            ),
            &mut state,
        );
        assert_that!(
            &mget,
            eq(&CommandReply::Array(vec![
                CommandReply::BulkString(b"v2".to_vec()),
                CommandReply::Null,
                CommandReply::BulkString(b"v1".to_vec()),
            ]))
        );
    }

    #[rstest]
    fn dispatch_mset_updates_pairs_and_rejects_odd_arity() {
        let registry = CommandRegistry::with_builtin_commands();
        let mut state = DispatchState::default();

        let ok = registry.dispatch(
            0,
            &CommandFrame::new(
                "MSET",
                vec![
                    b"k1".to_vec(),
                    b"v1".to_vec(),
                    b"k2".to_vec(),
                    b"v2".to_vec(),
                ],
            ),
            &mut state,
        );
        assert_that!(&ok, eq(&CommandReply::SimpleString("OK".to_owned())));

        let k1 = registry.dispatch(
            0,
            &CommandFrame::new("GET", vec![b"k1".to_vec()]),
            &mut state,
        );
        let k2 = registry.dispatch(
            0,
            &CommandFrame::new("GET", vec![b"k2".to_vec()]),
            &mut state,
        );
        assert_that!(&k1, eq(&CommandReply::BulkString(b"v1".to_vec())));
        assert_that!(&k2, eq(&CommandReply::BulkString(b"v2".to_vec())));

        let invalid = registry.dispatch(
            0,
            &CommandFrame::new(
                "MSET",
                vec![b"only".to_vec(), b"one".to_vec(), b"orphan".to_vec()],
            ),
            &mut state,
        );
        assert_that!(
            &invalid,
            eq(&CommandReply::Error(
                "wrong number of arguments for 'MSET' command".to_owned()
            ))
        );
    }

    #[rstest]
    fn dispatch_msetnx_sets_all_or_none_and_rejects_odd_arity() {
        let registry = CommandRegistry::with_builtin_commands();
        let mut state = DispatchState::default();

        let first = registry.dispatch(
            0,
            &CommandFrame::new(
                "MSETNX",
                vec![
                    b"k1".to_vec(),
                    b"v1".to_vec(),
                    b"k2".to_vec(),
                    b"v2".to_vec(),
                ],
            ),
            &mut state,
        );
        assert_that!(&first, eq(&CommandReply::Integer(1)));

        let second = registry.dispatch(
            0,
            &CommandFrame::new(
                "MSETNX",
                vec![
                    b"k1".to_vec(),
                    b"next".to_vec(),
                    b"k3".to_vec(),
                    b"v3".to_vec(),
                ],
            ),
            &mut state,
        );
        assert_that!(&second, eq(&CommandReply::Integer(0)));

        let k1 = registry.dispatch(
            0,
            &CommandFrame::new("GET", vec![b"k1".to_vec()]),
            &mut state,
        );
        let k3 = registry.dispatch(
            0,
            &CommandFrame::new("GET", vec![b"k3".to_vec()]),
            &mut state,
        );
        assert_that!(&k1, eq(&CommandReply::BulkString(b"v1".to_vec())));
        assert_that!(&k3, eq(&CommandReply::Null));

        let invalid = registry.dispatch(
            0,
            &CommandFrame::new(
                "MSETNX",
                vec![b"odd".to_vec(), b"pair".to_vec(), b"tail".to_vec()],
            ),
            &mut state,
        );
        assert_that!(
            &invalid,
            eq(&CommandReply::Error(
                "wrong number of arguments for 'MSETNX' command".to_owned()
            ))
        );
    }

    #[rstest]
    fn dispatch_getset_returns_previous_value_and_overwrites_key() {
        let registry = CommandRegistry::with_builtin_commands();
        let mut state = DispatchState::default();

        let first = registry.dispatch(
            0,
            &CommandFrame::new("GETSET", vec![b"k".to_vec(), b"v1".to_vec()]),
            &mut state,
        );
        assert_that!(&first, eq(&CommandReply::Null));

        let second = registry.dispatch(
            0,
            &CommandFrame::new("GETSET", vec![b"k".to_vec(), b"v2".to_vec()]),
            &mut state,
        );
        assert_that!(&second, eq(&CommandReply::BulkString(b"v1".to_vec())));

        let value = registry.dispatch(
            0,
            &CommandFrame::new("GET", vec![b"k".to_vec()]),
            &mut state,
        );
        assert_that!(&value, eq(&CommandReply::BulkString(b"v2".to_vec())));
    }

    #[rstest]
    fn dispatch_getdel_returns_value_and_removes_key() {
        let registry = CommandRegistry::with_builtin_commands();
        let mut state = DispatchState::default();

        let _ = registry.dispatch(
            0,
            &CommandFrame::new("SET", vec![b"k".to_vec(), b"v".to_vec()]),
            &mut state,
        );
        let removed = registry.dispatch(
            0,
            &CommandFrame::new("GETDEL", vec![b"k".to_vec()]),
            &mut state,
        );
        assert_that!(&removed, eq(&CommandReply::BulkString(b"v".to_vec())));

        let missing = registry.dispatch(
            0,
            &CommandFrame::new("GETDEL", vec![b"k".to_vec()]),
            &mut state,
        );
        assert_that!(&missing, eq(&CommandReply::Null));
    }

    #[rstest]
    fn dispatch_append_and_strlen_manage_string_length() {
        let registry = CommandRegistry::with_builtin_commands();
        let mut state = DispatchState::default();

        let first = registry.dispatch(
            0,
            &CommandFrame::new("APPEND", vec![b"k".to_vec(), b"he".to_vec()]),
            &mut state,
        );
        assert_that!(&first, eq(&CommandReply::Integer(2)));

        let second = registry.dispatch(
            0,
            &CommandFrame::new("APPEND", vec![b"k".to_vec(), b"llo".to_vec()]),
            &mut state,
        );
        assert_that!(&second, eq(&CommandReply::Integer(5)));

        let strlen = registry.dispatch(
            0,
            &CommandFrame::new("STRLEN", vec![b"k".to_vec()]),
            &mut state,
        );
        assert_that!(&strlen, eq(&CommandReply::Integer(5)));

        let value = registry.dispatch(
            0,
            &CommandFrame::new("GET", vec![b"k".to_vec()]),
            &mut state,
        );
        assert_that!(&value, eq(&CommandReply::BulkString(b"hello".to_vec())));
    }

    #[rstest]
    fn dispatch_getrange_returns_slice_with_redis_index_rules() {
        let registry = CommandRegistry::with_builtin_commands();
        let mut state = DispatchState::default();

        let _ = registry.dispatch(
            0,
            &CommandFrame::new("SET", vec![b"k".to_vec(), b"hello".to_vec()]),
            &mut state,
        );

        let middle = registry.dispatch(
            0,
            &CommandFrame::new(
                "GETRANGE",
                vec![b"k".to_vec(), b"1".to_vec(), b"3".to_vec()],
            ),
            &mut state,
        );
        assert_that!(&middle, eq(&CommandReply::BulkString(b"ell".to_vec())));

        let tail = registry.dispatch(
            0,
            &CommandFrame::new(
                "GETRANGE",
                vec![b"k".to_vec(), b"-2".to_vec(), b"-1".to_vec()],
            ),
            &mut state,
        );
        assert_that!(&tail, eq(&CommandReply::BulkString(b"lo".to_vec())));
    }

    #[rstest]
    fn dispatch_setrange_updates_offset_and_zero_pads_missing_bytes() {
        let registry = CommandRegistry::with_builtin_commands();
        let mut state = DispatchState::default();

        let _ = registry.dispatch(
            0,
            &CommandFrame::new("SET", vec![b"k".to_vec(), b"hello".to_vec()]),
            &mut state,
        );
        let updated = registry.dispatch(
            0,
            &CommandFrame::new(
                "SETRANGE",
                vec![b"k".to_vec(), b"1".to_vec(), b"i".to_vec()],
            ),
            &mut state,
        );
        assert_that!(&updated, eq(&CommandReply::Integer(5)));

        let _ = registry.dispatch(
            0,
            &CommandFrame::new(
                "SETRANGE",
                vec![b"k2".to_vec(), b"3".to_vec(), b"ab".to_vec()],
            ),
            &mut state,
        );

        let key1 = registry.dispatch(
            0,
            &CommandFrame::new("GET", vec![b"k".to_vec()]),
            &mut state,
        );
        assert_that!(&key1, eq(&CommandReply::BulkString(b"hillo".to_vec())));

        let key2 = registry.dispatch(
            0,
            &CommandFrame::new("GET", vec![b"k2".to_vec()]),
            &mut state,
        );
        assert_that!(
            &key2,
            eq(&CommandReply::BulkString(vec![0, 0, 0, b'a', b'b']))
        );
    }

    #[rstest]
    fn dispatch_del_and_exists_follow_redis_counting_semantics() {
        let registry = CommandRegistry::with_builtin_commands();
        let mut state = DispatchState::default();

        let _ = registry.dispatch(
            0,
            &CommandFrame::new("SET", vec![b"k1".to_vec(), b"v1".to_vec()]),
            &mut state,
        );
        let _ = registry.dispatch(
            0,
            &CommandFrame::new("SET", vec![b"k2".to_vec(), b"v2".to_vec()]),
            &mut state,
        );

        let exists = registry.dispatch(
            0,
            &CommandFrame::new(
                "EXISTS",
                vec![
                    b"k1".to_vec(),
                    b"k2".to_vec(),
                    b"missing".to_vec(),
                    b"k1".to_vec(),
                ],
            ),
            &mut state,
        );
        assert_that!(&exists, eq(&CommandReply::Integer(3)));

        let deleted = registry.dispatch(
            0,
            &CommandFrame::new(
                "DEL",
                vec![
                    b"k1".to_vec(),
                    b"k2".to_vec(),
                    b"missing".to_vec(),
                    b"k1".to_vec(),
                ],
            ),
            &mut state,
        );
        assert_that!(&deleted, eq(&CommandReply::Integer(2)));

        let exists_after_delete = registry.dispatch(
            0,
            &CommandFrame::new("EXISTS", vec![b"k1".to_vec(), b"k2".to_vec()]),
            &mut state,
        );
        assert_that!(&exists_after_delete, eq(&CommandReply::Integer(0)));
    }

    #[rstest]
    fn dispatch_unlink_removes_keys_with_del_counting_semantics() {
        let registry = CommandRegistry::with_builtin_commands();
        let mut state = DispatchState::default();

        let _ = registry.dispatch(
            0,
            &CommandFrame::new("SET", vec![b"k1".to_vec(), b"v1".to_vec()]),
            &mut state,
        );
        let _ = registry.dispatch(
            0,
            &CommandFrame::new("SET", vec![b"k2".to_vec(), b"v2".to_vec()]),
            &mut state,
        );

        let unlinked = registry.dispatch(
            0,
            &CommandFrame::new(
                "UNLINK",
                vec![
                    b"k1".to_vec(),
                    b"k2".to_vec(),
                    b"missing".to_vec(),
                    b"k1".to_vec(),
                ],
            ),
            &mut state,
        );
        assert_that!(&unlinked, eq(&CommandReply::Integer(2)));

        let exists_after = registry.dispatch(
            0,
            &CommandFrame::new("EXISTS", vec![b"k1".to_vec(), b"k2".to_vec()]),
            &mut state,
        );
        assert_that!(&exists_after, eq(&CommandReply::Integer(0)));
    }

    #[rstest]
    fn dispatch_touch_counts_existing_keys_like_exists() {
        let registry = CommandRegistry::with_builtin_commands();
        let mut state = DispatchState::default();

        let _ = registry.dispatch(
            0,
            &CommandFrame::new("SET", vec![b"k1".to_vec(), b"v1".to_vec()]),
            &mut state,
        );
        let _ = registry.dispatch(
            0,
            &CommandFrame::new("SET", vec![b"k2".to_vec(), b"v2".to_vec()]),
            &mut state,
        );

        let touched = registry.dispatch(
            0,
            &CommandFrame::new(
                "TOUCH",
                vec![
                    b"k1".to_vec(),
                    b"k2".to_vec(),
                    b"missing".to_vec(),
                    b"k1".to_vec(),
                ],
            ),
            &mut state,
        );
        assert_that!(&touched, eq(&CommandReply::Integer(3)));
    }

    #[rstest]
    fn dispatch_move_transfers_key_between_databases() {
        let registry = CommandRegistry::with_builtin_commands();
        let mut state = DispatchState::default();

        let _ = registry.dispatch(
            0,
            &CommandFrame::new("SET", vec![b"move:key".to_vec(), b"value".to_vec()]),
            &mut state,
        );

        let moved = registry.dispatch(
            0,
            &CommandFrame::new("MOVE", vec![b"move:key".to_vec(), b"1".to_vec()]),
            &mut state,
        );
        assert_that!(&moved, eq(&CommandReply::Integer(1)));

        let source = registry.dispatch(
            0,
            &CommandFrame::new("GET", vec![b"move:key".to_vec()]),
            &mut state,
        );
        assert_that!(&source, eq(&CommandReply::Null));
        let target = registry.dispatch(
            1,
            &CommandFrame::new("GET", vec![b"move:key".to_vec()]),
            &mut state,
        );
        assert_that!(&target, eq(&CommandReply::BulkString(b"value".to_vec())));
    }

    #[rstest]
    fn dispatch_move_returns_zero_when_source_missing_or_target_exists() {
        let registry = CommandRegistry::with_builtin_commands();
        let mut state = DispatchState::default();

        let missing = registry.dispatch(
            0,
            &CommandFrame::new("MOVE", vec![b"missing".to_vec(), b"1".to_vec()]),
            &mut state,
        );
        assert_that!(&missing, eq(&CommandReply::Integer(0)));

        let _ = registry.dispatch(
            0,
            &CommandFrame::new("SET", vec![b"dup".to_vec(), b"db0".to_vec()]),
            &mut state,
        );
        let _ = registry.dispatch(
            1,
            &CommandFrame::new("SET", vec![b"dup".to_vec(), b"db1".to_vec()]),
            &mut state,
        );
        let blocked = registry.dispatch(
            0,
            &CommandFrame::new("MOVE", vec![b"dup".to_vec(), b"1".to_vec()]),
            &mut state,
        );
        assert_that!(&blocked, eq(&CommandReply::Integer(0)));

        let source = registry.dispatch(
            0,
            &CommandFrame::new("GET", vec![b"dup".to_vec()]),
            &mut state,
        );
        let target = registry.dispatch(
            1,
            &CommandFrame::new("GET", vec![b"dup".to_vec()]),
            &mut state,
        );
        assert_that!(&source, eq(&CommandReply::BulkString(b"db0".to_vec())));
        assert_that!(&target, eq(&CommandReply::BulkString(b"db1".to_vec())));
    }

    #[rstest]
    fn dispatch_copy_duplicates_source_without_removing_it() {
        let registry = CommandRegistry::with_builtin_commands();
        let mut state = DispatchState::default();

        let _ = registry.dispatch(
            0,
            &CommandFrame::new(
                "SETEX",
                vec![b"src".to_vec(), b"60".to_vec(), b"value".to_vec()],
            ),
            &mut state,
        );

        let copied = registry.dispatch(
            0,
            &CommandFrame::new("COPY", vec![b"src".to_vec(), b"dst".to_vec()]),
            &mut state,
        );
        assert_that!(&copied, eq(&CommandReply::Integer(1)));

        let source = registry.dispatch(
            0,
            &CommandFrame::new("GET", vec![b"src".to_vec()]),
            &mut state,
        );
        let destination = registry.dispatch(
            0,
            &CommandFrame::new("GET", vec![b"dst".to_vec()]),
            &mut state,
        );
        assert_that!(&source, eq(&CommandReply::BulkString(b"value".to_vec())));
        assert_that!(
            &destination,
            eq(&CommandReply::BulkString(b"value".to_vec()))
        );

        let source_expire = state
            .db_map(0)
            .and_then(|map| map.get(b"src".as_slice()))
            .and_then(|entry| entry.expire_at_unix_secs);
        let destination_expire = state
            .db_map(0)
            .and_then(|map| map.get(b"dst".as_slice()))
            .and_then(|entry| entry.expire_at_unix_secs);
        assert_that!(source_expire, eq(destination_expire));
    }

    #[rstest]
    fn dispatch_copy_requires_replace_to_overwrite_existing_destination() {
        let registry = CommandRegistry::with_builtin_commands();
        let mut state = DispatchState::default();

        let _ = registry.dispatch(
            0,
            &CommandFrame::new("SET", vec![b"src".to_vec(), b"next".to_vec()]),
            &mut state,
        );
        let _ = registry.dispatch(
            0,
            &CommandFrame::new("SET", vec![b"dst".to_vec(), b"old".to_vec()]),
            &mut state,
        );

        let blocked = registry.dispatch(
            0,
            &CommandFrame::new("COPY", vec![b"src".to_vec(), b"dst".to_vec()]),
            &mut state,
        );
        assert_that!(&blocked, eq(&CommandReply::Integer(0)));

        let replaced = registry.dispatch(
            0,
            &CommandFrame::new(
                "COPY",
                vec![b"src".to_vec(), b"dst".to_vec(), b"REPLACE".to_vec()],
            ),
            &mut state,
        );
        assert_that!(&replaced, eq(&CommandReply::Integer(1)));

        let destination = registry.dispatch(
            0,
            &CommandFrame::new("GET", vec![b"dst".to_vec()]),
            &mut state,
        );
        assert_that!(
            &destination,
            eq(&CommandReply::BulkString(b"next".to_vec()))
        );
    }

    #[rstest]
    fn dispatch_copy_validates_same_key_and_options() {
        let registry = CommandRegistry::with_builtin_commands();
        let mut state = DispatchState::default();

        let same = registry.dispatch(
            0,
            &CommandFrame::new("COPY", vec![b"k".to_vec(), b"k".to_vec()]),
            &mut state,
        );
        assert_that!(
            &same,
            eq(&CommandReply::Error(
                "source and destination objects are the same".to_owned()
            ))
        );

        let invalid_option = registry.dispatch(
            0,
            &CommandFrame::new(
                "COPY",
                vec![
                    b"src".to_vec(),
                    b"dst".to_vec(),
                    b"DB".to_vec(),
                    b"1".to_vec(),
                ],
            ),
            &mut state,
        );
        assert_that!(
            &invalid_option,
            eq(&CommandReply::Error("syntax error".to_owned()))
        );
    }

    #[rstest]
    fn dispatch_rename_moves_value_to_destination_key() {
        let registry = CommandRegistry::with_builtin_commands();
        let mut state = DispatchState::default();

        let _ = registry.dispatch(
            0,
            &CommandFrame::new("SET", vec![b"src".to_vec(), b"v1".to_vec()]),
            &mut state,
        );
        let _ = registry.dispatch(
            0,
            &CommandFrame::new("SET", vec![b"dst".to_vec(), b"v2".to_vec()]),
            &mut state,
        );

        let renamed = registry.dispatch(
            0,
            &CommandFrame::new("RENAME", vec![b"src".to_vec(), b"dst".to_vec()]),
            &mut state,
        );
        assert_that!(&renamed, eq(&CommandReply::SimpleString("OK".to_owned())));

        let src_value = registry.dispatch(
            0,
            &CommandFrame::new("GET", vec![b"src".to_vec()]),
            &mut state,
        );
        assert_that!(&src_value, eq(&CommandReply::Null));

        let dst_value = registry.dispatch(
            0,
            &CommandFrame::new("GET", vec![b"dst".to_vec()]),
            &mut state,
        );
        assert_that!(&dst_value, eq(&CommandReply::BulkString(b"v1".to_vec())));
    }

    #[rstest]
    fn dispatch_renamenx_only_moves_when_destination_is_missing() {
        let registry = CommandRegistry::with_builtin_commands();
        let mut state = DispatchState::default();

        let _ = registry.dispatch(
            0,
            &CommandFrame::new("SET", vec![b"src".to_vec(), b"v1".to_vec()]),
            &mut state,
        );
        let _ = registry.dispatch(
            0,
            &CommandFrame::new("SET", vec![b"dst".to_vec(), b"v2".to_vec()]),
            &mut state,
        );

        let blocked = registry.dispatch(
            0,
            &CommandFrame::new("RENAMENX", vec![b"src".to_vec(), b"dst".to_vec()]),
            &mut state,
        );
        assert_that!(&blocked, eq(&CommandReply::Integer(0)));

        let moved = registry.dispatch(
            0,
            &CommandFrame::new("RENAMENX", vec![b"src".to_vec(), b"new".to_vec()]),
            &mut state,
        );
        assert_that!(&moved, eq(&CommandReply::Integer(1)));

        let src_value = registry.dispatch(
            0,
            &CommandFrame::new("GET", vec![b"src".to_vec()]),
            &mut state,
        );
        assert_that!(&src_value, eq(&CommandReply::Null));

        let new_value = registry.dispatch(
            0,
            &CommandFrame::new("GET", vec![b"new".to_vec()]),
            &mut state,
        );
        assert_that!(&new_value, eq(&CommandReply::BulkString(b"v1".to_vec())));
    }

    #[rstest]
    fn dispatch_rename_same_name_requires_existing_key() {
        let registry = CommandRegistry::with_builtin_commands();
        let mut state = DispatchState::default();

        let missing = registry.dispatch(
            0,
            &CommandFrame::new("RENAME", vec![b"same".to_vec(), b"same".to_vec()]),
            &mut state,
        );
        assert_that!(&missing, eq(&CommandReply::Error("no such key".to_owned())));

        let _ = registry.dispatch(
            0,
            &CommandFrame::new("SET", vec![b"same".to_vec(), b"value".to_vec()]),
            &mut state,
        );
        let rename_ok = registry.dispatch(
            0,
            &CommandFrame::new("RENAME", vec![b"same".to_vec(), b"same".to_vec()]),
            &mut state,
        );
        assert_that!(&rename_ok, eq(&CommandReply::SimpleString("OK".to_owned())));

        let renamenx_same = registry.dispatch(
            0,
            &CommandFrame::new("RENAMENX", vec![b"same".to_vec(), b"same".to_vec()]),
            &mut state,
        );
        assert_that!(&renamenx_same, eq(&CommandReply::Integer(0)));
    }

    #[rstest]
    fn dispatch_unknown_command_returns_error() {
        let registry = CommandRegistry::with_builtin_commands();
        let mut state = DispatchState::default();

        let reply = registry.dispatch(0, &CommandFrame::new("NOPE", Vec::new()), &mut state);
        let CommandReply::Error(message) = reply else {
            panic!("expected error reply");
        };
        assert_that!(message.contains("unknown command"), eq(true));
    }

    #[rstest]
    fn dispatch_ttl_and_expire_lifecycle() {
        let registry = CommandRegistry::with_builtin_commands();
        let mut state = DispatchState::default();

        let _ = registry.dispatch(
            0,
            &CommandFrame::new("SET", vec![b"temp".to_vec(), b"value".to_vec()]),
            &mut state,
        );

        let ttl_without_expire = registry.dispatch(
            0,
            &CommandFrame::new("TTL", vec![b"temp".to_vec()]),
            &mut state,
        );
        assert_that!(&ttl_without_expire, eq(&CommandReply::Integer(-1)));

        let expire_now = registry.dispatch(
            0,
            &CommandFrame::new("EXPIRE", vec![b"temp".to_vec(), b"0".to_vec()]),
            &mut state,
        );
        assert_that!(&expire_now, eq(&CommandReply::Integer(1)));

        let get_after_expire = registry.dispatch(
            0,
            &CommandFrame::new("GET", vec![b"temp".to_vec()]),
            &mut state,
        );
        assert_that!(&get_after_expire, eq(&CommandReply::Null));

        let ttl_after_expire = registry.dispatch(
            0,
            &CommandFrame::new("TTL", vec![b"temp".to_vec()]),
            &mut state,
        );
        assert_that!(&ttl_after_expire, eq(&CommandReply::Integer(-2)));
    }

    #[rstest]
    fn dispatch_pttl_and_pexpire_lifecycle() {
        let registry = CommandRegistry::with_builtin_commands();
        let mut state = DispatchState::default();

        let _ = registry.dispatch(
            0,
            &CommandFrame::new("SET", vec![b"temp".to_vec(), b"value".to_vec()]),
            &mut state,
        );

        let pttl_without_expire = registry.dispatch(
            0,
            &CommandFrame::new("PTTL", vec![b"temp".to_vec()]),
            &mut state,
        );
        assert_that!(&pttl_without_expire, eq(&CommandReply::Integer(-1)));

        let pexpire = registry.dispatch(
            0,
            &CommandFrame::new("PEXPIRE", vec![b"temp".to_vec(), b"1500".to_vec()]),
            &mut state,
        );
        assert_that!(&pexpire, eq(&CommandReply::Integer(1)));

        let pttl = registry.dispatch(
            0,
            &CommandFrame::new("PTTL", vec![b"temp".to_vec()]),
            &mut state,
        );
        let CommandReply::Integer(pttl_ms) = pttl else {
            panic!("PTTL must return integer");
        };
        assert_that!(pttl_ms > 0, eq(true));

        let pexpire_now = registry.dispatch(
            0,
            &CommandFrame::new("PEXPIRE", vec![b"temp".to_vec(), b"0".to_vec()]),
            &mut state,
        );
        assert_that!(&pexpire_now, eq(&CommandReply::Integer(1)));

        let pttl_after = registry.dispatch(
            0,
            &CommandFrame::new("PTTL", vec![b"temp".to_vec()]),
            &mut state,
        );
        assert_that!(&pttl_after, eq(&CommandReply::Integer(-2)));
    }

    #[rstest]
    fn dispatch_expiretime_reports_missing_persistent_and_expiring_keys() {
        let registry = CommandRegistry::with_builtin_commands();
        let mut state = DispatchState::default();

        let missing = registry.dispatch(
            0,
            &CommandFrame::new("EXPIRETIME", vec![b"missing".to_vec()]),
            &mut state,
        );
        assert_that!(&missing, eq(&CommandReply::Integer(-2)));

        let _ = registry.dispatch(
            0,
            &CommandFrame::new("SET", vec![b"persist".to_vec(), b"v".to_vec()]),
            &mut state,
        );
        let persistent = registry.dispatch(
            0,
            &CommandFrame::new("EXPIRETIME", vec![b"persist".to_vec()]),
            &mut state,
        );
        assert_that!(&persistent, eq(&CommandReply::Integer(-1)));

        let _ = registry.dispatch(
            0,
            &CommandFrame::new(
                "SETEX",
                vec![b"temp".to_vec(), b"60".to_vec(), b"v".to_vec()],
            ),
            &mut state,
        );
        let expiring = registry.dispatch(
            0,
            &CommandFrame::new("EXPIRETIME", vec![b"temp".to_vec()]),
            &mut state,
        );
        let CommandReply::Integer(expire_at) = expiring else {
            panic!("EXPIRETIME must return integer");
        };
        let now = i64::try_from(DispatchState::now_unix_seconds()).unwrap_or(i64::MAX);
        assert_that!(expire_at >= now, eq(true));
    }

    #[rstest]
    fn dispatch_pexpiretime_reports_missing_persistent_and_expiring_keys() {
        let registry = CommandRegistry::with_builtin_commands();
        let mut state = DispatchState::default();

        let missing = registry.dispatch(
            0,
            &CommandFrame::new("PEXPIRETIME", vec![b"missing".to_vec()]),
            &mut state,
        );
        assert_that!(&missing, eq(&CommandReply::Integer(-2)));

        let _ = registry.dispatch(
            0,
            &CommandFrame::new("SET", vec![b"persist".to_vec(), b"v".to_vec()]),
            &mut state,
        );
        let persistent = registry.dispatch(
            0,
            &CommandFrame::new("PEXPIRETIME", vec![b"persist".to_vec()]),
            &mut state,
        );
        assert_that!(&persistent, eq(&CommandReply::Integer(-1)));

        let _ = registry.dispatch(
            0,
            &CommandFrame::new(
                "SETEX",
                vec![b"temp".to_vec(), b"60".to_vec(), b"v".to_vec()],
            ),
            &mut state,
        );
        let expiring = registry.dispatch(
            0,
            &CommandFrame::new("PEXPIRETIME", vec![b"temp".to_vec()]),
            &mut state,
        );
        let CommandReply::Integer(expire_at) = expiring else {
            panic!("PEXPIRETIME must return integer");
        };
        let now = i64::try_from(DispatchState::now_unix_millis()).unwrap_or(i64::MAX);
        assert_that!(expire_at >= now, eq(true));
    }

    #[rstest]
    fn dispatch_persist_removes_expiry_without_deleting_key() {
        let registry = CommandRegistry::with_builtin_commands();
        let mut state = DispatchState::default();

        let _ = registry.dispatch(
            0,
            &CommandFrame::new("SET", vec![b"temp".to_vec(), b"value".to_vec()]),
            &mut state,
        );
        let _ = registry.dispatch(
            0,
            &CommandFrame::new("EXPIRE", vec![b"temp".to_vec(), b"10".to_vec()]),
            &mut state,
        );

        let persist = registry.dispatch(
            0,
            &CommandFrame::new("PERSIST", vec![b"temp".to_vec()]),
            &mut state,
        );
        assert_that!(&persist, eq(&CommandReply::Integer(1)));

        let ttl = registry.dispatch(
            0,
            &CommandFrame::new("TTL", vec![b"temp".to_vec()]),
            &mut state,
        );
        assert_that!(&ttl, eq(&CommandReply::Integer(-1)));

        let value = registry.dispatch(
            0,
            &CommandFrame::new("GET", vec![b"temp".to_vec()]),
            &mut state,
        );
        assert_that!(&value, eq(&CommandReply::BulkString(b"value".to_vec())));
    }

    #[rstest]
    fn dispatch_incr_family_updates_counter_with_redis_semantics() {
        let registry = CommandRegistry::with_builtin_commands();
        let mut state = DispatchState::default();

        let incr = registry.dispatch(
            0,
            &CommandFrame::new("INCR", vec![b"counter".to_vec()]),
            &mut state,
        );
        assert_that!(&incr, eq(&CommandReply::Integer(1)));

        let incrby = registry.dispatch(
            0,
            &CommandFrame::new("INCRBY", vec![b"counter".to_vec(), b"9".to_vec()]),
            &mut state,
        );
        assert_that!(&incrby, eq(&CommandReply::Integer(10)));

        let decr = registry.dispatch(
            0,
            &CommandFrame::new("DECR", vec![b"counter".to_vec()]),
            &mut state,
        );
        assert_that!(&decr, eq(&CommandReply::Integer(9)));

        let decrby = registry.dispatch(
            0,
            &CommandFrame::new("DECRBY", vec![b"counter".to_vec(), b"4".to_vec()]),
            &mut state,
        );
        assert_that!(&decrby, eq(&CommandReply::Integer(5)));
    }

    #[rstest]
    fn dispatch_incr_family_rejects_non_integer_or_overflow_values() {
        let registry = CommandRegistry::with_builtin_commands();
        let mut state = DispatchState::default();

        let _ = registry.dispatch(
            0,
            &CommandFrame::new("SET", vec![b"counter".to_vec(), b"abc".to_vec()]),
            &mut state,
        );
        let non_integer = registry.dispatch(
            0,
            &CommandFrame::new("INCR", vec![b"counter".to_vec()]),
            &mut state,
        );
        assert_that!(
            &non_integer,
            eq(&CommandReply::Error(
                "value is not an integer or out of range".to_owned()
            ))
        );

        let _ = registry.dispatch(
            0,
            &CommandFrame::new(
                "SET",
                vec![b"counter".to_vec(), i64::MAX.to_string().into_bytes()],
            ),
            &mut state,
        );
        let overflow = registry.dispatch(
            0,
            &CommandFrame::new("INCR", vec![b"counter".to_vec()]),
            &mut state,
        );
        assert_that!(
            &overflow,
            eq(&CommandReply::Error(
                "value is not an integer or out of range".to_owned()
            ))
        );
    }

    #[rstest]
    fn dispatch_expire_missing_key_returns_zero() {
        let registry = CommandRegistry::with_builtin_commands();
        let mut state = DispatchState::default();

        let reply = registry.dispatch(
            0,
            &CommandFrame::new("EXPIRE", vec![b"nope".to_vec(), b"10".to_vec()]),
            &mut state,
        );
        assert_that!(&reply, eq(&CommandReply::Integer(0)));
    }

    #[rstest]
    fn dispatch_expire_options_control_update_conditions() {
        let registry = CommandRegistry::with_builtin_commands();
        let mut state = DispatchState::default();

        let _ = registry.dispatch(
            0,
            &CommandFrame::new("SET", vec![b"key".to_vec(), b"value".to_vec()]),
            &mut state,
        );

        let gt_on_persistent = registry.dispatch(
            0,
            &CommandFrame::new(
                "EXPIRE",
                vec![b"key".to_vec(), b"10".to_vec(), b"GT".to_vec()],
            ),
            &mut state,
        );
        assert_that!(&gt_on_persistent, eq(&CommandReply::Integer(0)));

        let lt_on_persistent = registry.dispatch(
            0,
            &CommandFrame::new(
                "EXPIRE",
                vec![b"key".to_vec(), b"10".to_vec(), b"LT".to_vec()],
            ),
            &mut state,
        );
        assert_that!(&lt_on_persistent, eq(&CommandReply::Integer(1)));

        let first_expiretime = registry.dispatch(
            0,
            &CommandFrame::new("EXPIRETIME", vec![b"key".to_vec()]),
            &mut state,
        );
        let CommandReply::Integer(first_expiretime) = first_expiretime else {
            panic!("EXPIRETIME must return integer");
        };

        let nx_with_existing_ttl = registry.dispatch(
            0,
            &CommandFrame::new(
                "EXPIRE",
                vec![b"key".to_vec(), b"20".to_vec(), b"NX".to_vec()],
            ),
            &mut state,
        );
        assert_that!(&nx_with_existing_ttl, eq(&CommandReply::Integer(0)));

        let xx_with_existing_ttl = registry.dispatch(
            0,
            &CommandFrame::new(
                "EXPIRE",
                vec![b"key".to_vec(), b"20".to_vec(), b"XX".to_vec()],
            ),
            &mut state,
        );
        assert_that!(&xx_with_existing_ttl, eq(&CommandReply::Integer(1)));

        let second_expiretime = registry.dispatch(
            0,
            &CommandFrame::new("EXPIRETIME", vec![b"key".to_vec()]),
            &mut state,
        );
        let CommandReply::Integer(second_expiretime) = second_expiretime else {
            panic!("EXPIRETIME must return integer");
        };
        assert_that!(second_expiretime > first_expiretime, eq(true));

        let gt_with_smaller_target = registry.dispatch(
            0,
            &CommandFrame::new(
                "EXPIRE",
                vec![b"key".to_vec(), b"5".to_vec(), b"GT".to_vec()],
            ),
            &mut state,
        );
        assert_that!(&gt_with_smaller_target, eq(&CommandReply::Integer(0)));

        let unchanged_expiretime = registry.dispatch(
            0,
            &CommandFrame::new("EXPIRETIME", vec![b"key".to_vec()]),
            &mut state,
        );
        assert_that!(
            &unchanged_expiretime,
            eq(&CommandReply::Integer(second_expiretime))
        );

        let lt_with_smaller_target = registry.dispatch(
            0,
            &CommandFrame::new(
                "EXPIRE",
                vec![b"key".to_vec(), b"5".to_vec(), b"LT".to_vec()],
            ),
            &mut state,
        );
        assert_that!(&lt_with_smaller_target, eq(&CommandReply::Integer(1)));

        let third_expiretime = registry.dispatch(
            0,
            &CommandFrame::new("EXPIRETIME", vec![b"key".to_vec()]),
            &mut state,
        );
        let CommandReply::Integer(third_expiretime) = third_expiretime else {
            panic!("EXPIRETIME must return integer");
        };
        assert_that!(third_expiretime < second_expiretime, eq(true));
    }

    #[rstest]
    fn dispatch_pexpire_options_control_millisecond_predicates() {
        let registry = CommandRegistry::with_builtin_commands();
        let mut state = DispatchState::default();

        let _ = registry.dispatch(
            0,
            &CommandFrame::new("SET", vec![b"key".to_vec(), b"value".to_vec()]),
            &mut state,
        );
        let first = registry.dispatch(
            0,
            &CommandFrame::new(
                "PEXPIRE",
                vec![b"key".to_vec(), b"1000".to_vec(), b"LT".to_vec()],
            ),
            &mut state,
        );
        assert_that!(&first, eq(&CommandReply::Integer(1)));

        let first_expiretime = registry.dispatch(
            0,
            &CommandFrame::new("PEXPIRETIME", vec![b"key".to_vec()]),
            &mut state,
        );
        let CommandReply::Integer(first_expiretime) = first_expiretime else {
            panic!("PEXPIRETIME must return integer");
        };

        let gt_smaller = registry.dispatch(
            0,
            &CommandFrame::new(
                "PEXPIRE",
                vec![b"key".to_vec(), b"500".to_vec(), b"GT".to_vec()],
            ),
            &mut state,
        );
        assert_that!(&gt_smaller, eq(&CommandReply::Integer(0)));

        let gt_bigger = registry.dispatch(
            0,
            &CommandFrame::new(
                "PEXPIRE",
                vec![b"key".to_vec(), b"2000".to_vec(), b"GT".to_vec()],
            ),
            &mut state,
        );
        assert_that!(&gt_bigger, eq(&CommandReply::Integer(1)));

        let second_expiretime = registry.dispatch(
            0,
            &CommandFrame::new("PEXPIRETIME", vec![b"key".to_vec()]),
            &mut state,
        );
        let CommandReply::Integer(second_expiretime) = second_expiretime else {
            panic!("PEXPIRETIME must return integer");
        };
        assert_that!(second_expiretime > first_expiretime, eq(true));
    }

    #[rstest]
    fn dispatch_expire_options_reject_invalid_combinations_or_unknown_tokens() {
        let registry = CommandRegistry::with_builtin_commands();
        let mut state = DispatchState::default();

        let nx_xx = registry.dispatch(
            0,
            &CommandFrame::new(
                "EXPIRE",
                vec![
                    b"key".to_vec(),
                    b"10".to_vec(),
                    b"NX".to_vec(),
                    b"XX".to_vec(),
                ],
            ),
            &mut state,
        );
        assert_that!(
            &nx_xx,
            eq(&CommandReply::Error(
                "NX and XX options at the same time are not compatible".to_owned()
            ))
        );

        let gt_lt = registry.dispatch(
            0,
            &CommandFrame::new(
                "PEXPIRE",
                vec![
                    b"key".to_vec(),
                    b"10".to_vec(),
                    b"GT".to_vec(),
                    b"LT".to_vec(),
                ],
            ),
            &mut state,
        );
        assert_that!(
            &gt_lt,
            eq(&CommandReply::Error(
                "GT and LT options at the same time are not compatible".to_owned()
            ))
        );

        let unknown = registry.dispatch(
            0,
            &CommandFrame::new(
                "EXPIREAT",
                vec![b"key".to_vec(), b"1".to_vec(), b"SOMETHING".to_vec()],
            ),
            &mut state,
        );
        assert_that!(
            &unknown,
            eq(&CommandReply::Error(
                "Unsupported option: SOMETHING".to_owned()
            ))
        );
    }

    #[rstest]
    fn dispatch_setex_sets_value_and_ttl() {
        let registry = CommandRegistry::with_builtin_commands();
        let mut state = DispatchState::default();

        let setex = registry.dispatch(
            0,
            &CommandFrame::new(
                "SETEX",
                vec![b"session".to_vec(), b"60".to_vec(), b"alive".to_vec()],
            ),
            &mut state,
        );
        assert_that!(&setex, eq(&CommandReply::SimpleString("OK".to_owned())));

        let value = registry.dispatch(
            0,
            &CommandFrame::new("GET", vec![b"session".to_vec()]),
            &mut state,
        );
        assert_that!(&value, eq(&CommandReply::BulkString(b"alive".to_vec())));

        let ttl = registry.dispatch(
            0,
            &CommandFrame::new("TTL", vec![b"session".to_vec()]),
            &mut state,
        );
        let CommandReply::Integer(ttl_secs) = ttl else {
            panic!("TTL must return integer");
        };
        assert_that!(ttl_secs > 0, eq(true));
    }

    #[rstest]
    fn dispatch_psetex_sets_value_and_pttl() {
        let registry = CommandRegistry::with_builtin_commands();
        let mut state = DispatchState::default();

        let psetex = registry.dispatch(
            0,
            &CommandFrame::new(
                "PSETEX",
                vec![b"session".to_vec(), b"1500".to_vec(), b"alive".to_vec()],
            ),
            &mut state,
        );
        assert_that!(&psetex, eq(&CommandReply::SimpleString("OK".to_owned())));

        let value = registry.dispatch(
            0,
            &CommandFrame::new("GET", vec![b"session".to_vec()]),
            &mut state,
        );
        assert_that!(&value, eq(&CommandReply::BulkString(b"alive".to_vec())));

        let pttl = registry.dispatch(
            0,
            &CommandFrame::new("PTTL", vec![b"session".to_vec()]),
            &mut state,
        );
        let CommandReply::Integer(pttl_ms) = pttl else {
            panic!("PTTL must return integer");
        };
        assert_that!(pttl_ms > 0, eq(true));
    }

    #[rstest]
    fn dispatch_setex_rejects_non_positive_expire_values() {
        let registry = CommandRegistry::with_builtin_commands();
        let mut state = DispatchState::default();

        let reply = registry.dispatch(
            0,
            &CommandFrame::new(
                "SETEX",
                vec![b"session".to_vec(), b"0".to_vec(), b"alive".to_vec()],
            ),
            &mut state,
        );
        assert_that!(
            &reply,
            eq(&CommandReply::Error(
                "invalid expire time in 'SETEX' command".to_owned()
            ))
        );
    }

    #[rstest]
    fn dispatch_psetex_rejects_non_positive_expire_values() {
        let registry = CommandRegistry::with_builtin_commands();
        let mut state = DispatchState::default();

        let reply = registry.dispatch(
            0,
            &CommandFrame::new(
                "PSETEX",
                vec![b"session".to_vec(), b"0".to_vec(), b"alive".to_vec()],
            ),
            &mut state,
        );
        assert_that!(
            &reply,
            eq(&CommandReply::Error(
                "invalid expire time in 'PSETEX' command".to_owned()
            ))
        );
    }

    #[rstest]
    fn dispatch_expireat_sets_future_or_removes_past_keys() {
        let registry = CommandRegistry::with_builtin_commands();
        let mut state = DispatchState::default();

        let _ = registry.dispatch(
            0,
            &CommandFrame::new("SET", vec![b"future".to_vec(), b"v".to_vec()]),
            &mut state,
        );
        let future_timestamp = DispatchState::now_unix_seconds()
            .saturating_add(120)
            .to_string()
            .into_bytes();
        let future = registry.dispatch(
            0,
            &CommandFrame::new("EXPIREAT", vec![b"future".to_vec(), future_timestamp]),
            &mut state,
        );
        assert_that!(&future, eq(&CommandReply::Integer(1)));

        let _ = registry.dispatch(
            0,
            &CommandFrame::new("SET", vec![b"past".to_vec(), b"v".to_vec()]),
            &mut state,
        );
        let past = registry.dispatch(
            0,
            &CommandFrame::new("EXPIREAT", vec![b"past".to_vec(), b"1".to_vec()]),
            &mut state,
        );
        assert_that!(&past, eq(&CommandReply::Integer(1)));

        let removed = registry.dispatch(
            0,
            &CommandFrame::new("GET", vec![b"past".to_vec()]),
            &mut state,
        );
        assert_that!(&removed, eq(&CommandReply::Null));
    }

    #[rstest]
    fn dispatch_pexpireat_sets_future_or_removes_past_keys() {
        let registry = CommandRegistry::with_builtin_commands();
        let mut state = DispatchState::default();

        let _ = registry.dispatch(
            0,
            &CommandFrame::new("SET", vec![b"future".to_vec(), b"v".to_vec()]),
            &mut state,
        );
        let future_timestamp = DispatchState::now_unix_millis()
            .saturating_add(120_000)
            .to_string()
            .into_bytes();
        let future = registry.dispatch(
            0,
            &CommandFrame::new("PEXPIREAT", vec![b"future".to_vec(), future_timestamp]),
            &mut state,
        );
        assert_that!(&future, eq(&CommandReply::Integer(1)));

        let _ = registry.dispatch(
            0,
            &CommandFrame::new("SET", vec![b"past".to_vec(), b"v".to_vec()]),
            &mut state,
        );
        let past = registry.dispatch(
            0,
            &CommandFrame::new("PEXPIREAT", vec![b"past".to_vec(), b"1".to_vec()]),
            &mut state,
        );
        assert_that!(&past, eq(&CommandReply::Integer(1)));

        let removed = registry.dispatch(
            0,
            &CommandFrame::new("GET", vec![b"past".to_vec()]),
            &mut state,
        );
        assert_that!(&removed, eq(&CommandReply::Null));
    }

    #[rstest]
    fn dispatch_keeps_values_isolated_between_databases() {
        let registry = CommandRegistry::with_builtin_commands();
        let mut state = DispatchState::default();

        let _ = registry.dispatch(
            0,
            &CommandFrame::new("SET", vec![b"shared".to_vec(), b"db0".to_vec()]),
            &mut state,
        );
        let _ = registry.dispatch(
            1,
            &CommandFrame::new("SET", vec![b"shared".to_vec(), b"db1".to_vec()]),
            &mut state,
        );

        let db0 = registry.dispatch(
            0,
            &CommandFrame::new("GET", vec![b"shared".to_vec()]),
            &mut state,
        );
        let db1 = registry.dispatch(
            1,
            &CommandFrame::new("GET", vec![b"shared".to_vec()]),
            &mut state,
        );
        assert_that!(&db0, eq(&CommandReply::BulkString(b"db0".to_vec())));
        assert_that!(&db1, eq(&CommandReply::BulkString(b"db1".to_vec())));
    }

    #[rstest]
    fn dispatch_bumps_key_version_on_mutations() {
        let registry = CommandRegistry::with_builtin_commands();
        let mut state = DispatchState::default();
        let key = b"watched:key".to_vec();

        assert_that!(state.key_version(0, &key), eq(0_u64));

        let _ = registry.dispatch(
            0,
            &CommandFrame::new("SET", vec![key.clone(), b"v1".to_vec()]),
            &mut state,
        );
        assert_that!(state.key_version(0, &key), eq(1_u64));

        let _ = registry.dispatch(
            0,
            &CommandFrame::new("EXPIRE", vec![key.clone(), b"10".to_vec()]),
            &mut state,
        );
        assert_that!(state.key_version(0, &key), eq(2_u64));

        let _ = registry.dispatch(
            0,
            &CommandFrame::new("EXPIRE", vec![key.clone(), b"0".to_vec()]),
            &mut state,
        );
        assert_that!(state.key_version(0, &key), eq(3_u64));
    }
