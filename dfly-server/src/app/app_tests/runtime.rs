use super::*;

#[rstest]
fn exec_plan_groups_commands_without_duplicate_shards_per_hop() {
    let app = ServerApp::new(RuntimeConfig::default());

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
    let app = ServerApp::new(RuntimeConfig::default());

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
    let app = ServerApp::new(RuntimeConfig::default());
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
    let app = ServerApp::new(RuntimeConfig::default());

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
    let app = ServerApp::new(RuntimeConfig::default());

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
    let app = ServerApp::new(RuntimeConfig::default());
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
    let app = ServerApp::new(RuntimeConfig::default());
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
    let app = ServerApp::new(RuntimeConfig::default());
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
        vec![
            first_key.clone(),
            b"a".to_vec(),
            second_key.clone(),
            b"b".to_vec(),
        ],
    )];
    let plan = app.build_exec_plan(&queued);
    assert_that!(plan.mode, eq(TransactionMode::Global));
    assert_that!(plan.touched_shards.len(), eq(2_usize));
    assert_that!(plan.touched_shards.contains(&first_shard), eq(true));
    assert_that!(plan.touched_shards.contains(&second_shard), eq(true));

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
    assert_that!(&first_runtime[0].command.name, eq(&"MSET".to_owned()));
    assert_that!(&second_runtime[0].command.name, eq(&"MSET".to_owned()));
    assert_that!(
        &first_runtime[0].command.args,
        eq(&vec![first_key, b"a".to_vec()])
    );
    assert_that!(
        &second_runtime[0].command.args,
        eq(&vec![second_key, b"b".to_vec()])
    );
    assert_that!(first_runtime[0].execute_on_worker, eq(true));
    assert_that!(second_runtime[0].execute_on_worker, eq(true));
}

#[rstest]
fn exec_plan_global_same_shard_copy_executes_on_worker_fiber() {
    let app = ServerApp::new(RuntimeConfig::default());
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
fn exec_plan_global_cross_shard_copy_executes_on_owner_worker_fiber() {
    let app = ServerApp::new(RuntimeConfig::default());
    let source_key = b"plan:rt:global:copy:cross:source".to_vec();
    let source_shard = app.core.resolve_shard_for_key(&source_key);
    let mut destination_key = b"plan:rt:global:copy:cross:destination".to_vec();
    let mut destination_shard = app.core.resolve_shard_for_key(&destination_key);
    let mut suffix = 0_u32;
    while destination_shard == source_shard {
        suffix = suffix.saturating_add(1);
        destination_key = format!("plan:rt:global:copy:cross:destination:{suffix}").into_bytes();
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
    assert_that!(plan.touched_shards.contains(&source_shard), eq(true));
    assert_that!(plan.touched_shards.contains(&destination_shard), eq(true));

    let replies = app.execute_transaction_plan(0, &plan);
    assert_that!(&replies, eq(&vec![CommandReply::Integer(1)]));
    assert_that!(
        app.runtime
            .wait_for_processed_count(source_shard, 1, Duration::from_millis(200))
            .expect("wait should succeed"),
        eq(true)
    );
    assert_that!(
        app.runtime
            .wait_for_processed_count(destination_shard, 1, Duration::from_millis(200))
            .expect("wait should succeed"),
        eq(true)
    );

    let source_runtime = app
        .runtime
        .drain_processed_for_shard(source_shard)
        .expect("drain should succeed");
    let destination_runtime = app
        .runtime
        .drain_processed_for_shard(destination_shard)
        .expect("drain should succeed");
    assert_that!(source_runtime.len(), eq(1_usize));
    assert_that!(destination_runtime.len(), eq(1_usize));
    assert_that!(&source_runtime[0].command, eq(&queued[0]));
    assert_that!(&destination_runtime[0].command, eq(&queued[0]));
    assert_that!(source_runtime[0].execute_on_worker, eq(true));
    assert_that!(destination_runtime[0].execute_on_worker, eq(false));

    let destination_value = app
        .core
        .execute_in_db(0, &CommandFrame::new("GET", vec![destination_key]));
    assert_that!(
        &destination_value,
        eq(&CommandReply::BulkString(b"value".to_vec()))
    );
}

#[rstest]
fn exec_plan_global_cross_shard_rename_executes_on_owner_worker_fiber() {
    let app = ServerApp::new(RuntimeConfig::default());
    let source_key = b"plan:rt:global:rename:cross:source".to_vec();
    let source_shard = app.core.resolve_shard_for_key(&source_key);
    let mut destination_key = b"plan:rt:global:rename:cross:destination".to_vec();
    let mut destination_shard = app.core.resolve_shard_for_key(&destination_key);
    let mut suffix = 0_u32;
    while destination_shard == source_shard {
        suffix = suffix.saturating_add(1);
        destination_key = format!("plan:rt:global:rename:cross:destination:{suffix}").into_bytes();
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
        "RENAME",
        vec![source_key.clone(), destination_key.clone()],
    )];
    let plan = app.build_exec_plan(&queued);
    assert_that!(plan.mode, eq(TransactionMode::Global));
    assert_that!(plan.touched_shards.contains(&source_shard), eq(true));
    assert_that!(plan.touched_shards.contains(&destination_shard), eq(true));

    let replies = app.execute_transaction_plan(0, &plan);
    assert_that!(
        &replies,
        eq(&vec![CommandReply::SimpleString("OK".to_owned())])
    );
    assert_that!(
        app.runtime
            .wait_for_processed_count(source_shard, 1, Duration::from_millis(200))
            .expect("wait should succeed"),
        eq(true)
    );
    assert_that!(
        app.runtime
            .wait_for_processed_count(destination_shard, 1, Duration::from_millis(200))
            .expect("wait should succeed"),
        eq(true)
    );

    let source_runtime = app
        .runtime
        .drain_processed_for_shard(source_shard)
        .expect("drain should succeed");
    let destination_runtime = app
        .runtime
        .drain_processed_for_shard(destination_shard)
        .expect("drain should succeed");
    assert_that!(source_runtime.len(), eq(1_usize));
    assert_that!(destination_runtime.len(), eq(1_usize));
    assert_that!(&source_runtime[0].command, eq(&queued[0]));
    assert_that!(&destination_runtime[0].command, eq(&queued[0]));
    assert_that!(source_runtime[0].execute_on_worker, eq(true));
    assert_that!(destination_runtime[0].execute_on_worker, eq(false));

    let source_value = app
        .core
        .execute_in_db(0, &CommandFrame::new("GET", vec![source_key]));
    let destination_value = app
        .core
        .execute_in_db(0, &CommandFrame::new("GET", vec![destination_key]));
    assert_that!(&source_value, eq(&CommandReply::Null));
    assert_that!(
        &destination_value,
        eq(&CommandReply::BulkString(b"value".to_vec()))
    );
}

#[rstest]
fn exec_plan_global_same_shard_multikey_count_executes_on_worker_fiber() {
    let app = ServerApp::new(RuntimeConfig::default());
    let first_key = b"plan:rt:global:del:1".to_vec();
    let shard = app.core.resolve_shard_for_key(&first_key);
    let mut second_key = b"plan:rt:global:del:2".to_vec();
    let mut second_shard = app.core.resolve_shard_for_key(&second_key);
    let mut suffix = 0_u32;
    while second_shard != shard {
        suffix = suffix.saturating_add(1);
        second_key = format!("plan:rt:global:del:2:{suffix}").into_bytes();
        second_shard = app.core.resolve_shard_for_key(&second_key);
    }

    let _ = app.execute_user_command(
        0,
        &CommandFrame::new("SET", vec![first_key.clone(), b"a".to_vec()]),
        None,
    );
    let _ = app.execute_user_command(
        0,
        &CommandFrame::new("SET", vec![second_key.clone(), b"b".to_vec()]),
        None,
    );
    for shard_id in 0_u16..app.config.shard_count.get() {
        let _ = app
            .runtime
            .drain_processed_for_shard(shard_id)
            .expect("drain should succeed");
    }

    let queued = vec![CommandFrame::new(
        "DEL",
        vec![first_key.clone(), second_key.clone()],
    )];
    let plan = app.build_exec_plan(&queued);
    assert_that!(plan.mode, eq(TransactionMode::Global));
    assert_that!(&plan.touched_shards, eq(&vec![shard]));

    let replies = app.execute_transaction_plan(0, &plan);
    assert_that!(&replies, eq(&vec![CommandReply::Integer(2)]));
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

    let verify = app.core.execute_in_db(
        0,
        &CommandFrame::new("EXISTS", vec![first_key.clone(), second_key.clone()]),
    );
    assert_that!(&verify, eq(&CommandReply::Integer(0)));
}

#[rstest]
fn exec_plan_global_same_shard_multikey_string_executes_on_worker_fiber() {
    let app = ServerApp::new(RuntimeConfig::default());
    let first_key = b"plan:rt:global:mset:same:1".to_vec();
    let shard = app.core.resolve_shard_for_key(&first_key);
    let mut second_key = b"plan:rt:global:mset:same:2".to_vec();
    let mut second_shard = app.core.resolve_shard_for_key(&second_key);
    let mut suffix = 0_u32;
    while second_shard != shard {
        suffix = suffix.saturating_add(1);
        second_key = format!("plan:rt:global:mset:same:2:{suffix}").into_bytes();
        second_shard = app.core.resolve_shard_for_key(&second_key);
    }

    let queued_set = vec![CommandFrame::new(
        "MSET",
        vec![
            first_key.clone(),
            b"va".to_vec(),
            second_key.clone(),
            b"vb".to_vec(),
        ],
    )];
    let set_plan = app.build_exec_plan(&queued_set);
    assert_that!(set_plan.mode, eq(TransactionMode::Global));
    assert_that!(&set_plan.touched_shards, eq(&vec![shard]));

    let set_replies = app.execute_transaction_plan(0, &set_plan);
    assert_that!(
        &set_replies,
        eq(&vec![CommandReply::SimpleString("OK".to_owned())])
    );
    assert_that!(
        app.runtime
            .wait_for_processed_count(shard, 1, Duration::from_millis(200))
            .expect("wait should succeed"),
        eq(true)
    );

    let set_runtime = app
        .runtime
        .drain_processed_for_shard(shard)
        .expect("drain should succeed");
    assert_that!(set_runtime.len(), eq(1_usize));
    assert_that!(&set_runtime[0].command, eq(&queued_set[0]));
    assert_that!(set_runtime[0].execute_on_worker, eq(true));

    let queued_get = vec![CommandFrame::new(
        "MGET",
        vec![second_key.clone(), first_key.clone()],
    )];
    let get_plan = app.build_exec_plan(&queued_get);
    assert_that!(get_plan.mode, eq(TransactionMode::Global));
    assert_that!(&get_plan.touched_shards, eq(&vec![shard]));

    let get_replies = app.execute_transaction_plan(0, &get_plan);
    assert_that!(
        &get_replies,
        eq(&vec![CommandReply::Array(vec![
            CommandReply::BulkString(b"vb".to_vec()),
            CommandReply::BulkString(b"va".to_vec()),
        ])])
    );
    assert_that!(
        app.runtime
            .wait_for_processed_count(shard, 1, Duration::from_millis(200))
            .expect("wait should succeed"),
        eq(true)
    );

    let get_runtime = app
        .runtime
        .drain_processed_for_shard(shard)
        .expect("drain should succeed");
    assert_that!(get_runtime.len(), eq(1_usize));
    assert_that!(&get_runtime[0].command, eq(&queued_get[0]));
    assert_that!(get_runtime[0].execute_on_worker, eq(true));
}

#[rstest]
fn exec_plan_global_non_key_command_uses_planner_shard_hint() {
    let app = ServerApp::new(RuntimeConfig::default());
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
fn runtime_post_barrier_recovers_single_key_via_worker_execution() {
    let app = ServerApp::new(RuntimeConfig::default());
    let key = b"plan:rt:fallback:single:key".to_vec();
    let frame = CommandFrame::new("SET", vec![key.clone(), b"value".to_vec()]);
    let shard = app.core.resolve_shard_for_key(&key);

    let reply = app.execute_command_after_runtime_barrier(0, &frame);
    assert_that!(&reply, eq(&CommandReply::SimpleString("OK".to_owned())));

    let value = app
        .core
        .execute_in_db(0, &CommandFrame::new("GET", vec![key]));
    assert_that!(&value, eq(&CommandReply::BulkString(b"value".to_vec())));

    let runtime = app
        .runtime
        .drain_processed_for_shard(shard)
        .expect("drain should succeed");
    assert_that!(runtime.len(), eq(1_usize));
    assert_that!(&runtime[0].command, eq(&frame));
    assert_that!(runtime[0].execute_on_worker, eq(true));
}

#[rstest]
fn runtime_post_barrier_dispatches_global_command_when_not_pre_dispatched() {
    let app = ServerApp::new(RuntimeConfig::default());
    let frame = CommandFrame::new("FLUSHDB", Vec::new());

    let reply = app.execute_command_after_runtime_barrier(0, &frame);
    assert_that!(&reply, eq(&CommandReply::SimpleString("OK".to_owned())));

    for shard in 0_u16..app.config.shard_count.get() {
        let runtime = app
            .runtime
            .drain_processed_for_shard(shard)
            .expect("drain should succeed");
        assert_that!(runtime.len(), eq(1_usize));
        assert_that!(&runtime[0].command, eq(&frame));
        assert_that!(runtime[0].execute_on_worker, eq(false));
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
fn direct_dispatch_runtime_returns_worker_reply_for_key_fallback() {
    let app = ServerApp::new(RuntimeConfig::default());
    let key = b"direct:rt:fallback:key".to_vec();
    let shard = app.core.resolve_shard_for_key(&key);
    let frame = CommandFrame::new("SET", vec![key.clone(), b"value".to_vec()]);

    let reply = app
        .dispatch_direct_command_runtime(0, &frame)
        .expect("fallback runtime dispatch should succeed");
    assert_that!(
        &reply,
        eq(&Some(CommandReply::SimpleString("OK".to_owned())))
    );

    let runtime = app
        .runtime
        .drain_processed_for_shard(shard)
        .expect("drain should succeed");
    assert_that!(runtime.len(), eq(1_usize));
    assert_that!(&runtime[0].command, eq(&frame));
    assert_that!(runtime[0].execute_on_worker, eq(true));

    let value = app
        .core
        .execute_in_db(0, &CommandFrame::new("GET", vec![key]));
    assert_that!(&value, eq(&CommandReply::BulkString(b"value".to_vec())));
}

#[rstest]
fn direct_single_key_execution_drains_worker_reply_buffer() {
    let app = ServerApp::new(RuntimeConfig::default());
    let key = b"direct:rt:reply:drain".to_vec();
    let shard = app.core.resolve_shard_for_key(&key);
    let frame = CommandFrame::new("SET", vec![key.clone(), b"value".to_vec()]);

    let reply = app.execute_user_command(0, &frame, None);
    assert_that!(&reply, eq(&CommandReply::SimpleString("OK".to_owned())));
    assert_that!(
        app.runtime
            .pending_reply_count(shard)
            .expect("pending reply count should succeed"),
        eq(0_usize)
    );
}

#[rstest]
fn direct_mget_dispatches_runtime_to_each_touched_shard() {
    let app = ServerApp::new(RuntimeConfig::default());
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
    assert_that!(&first_runtime[0].command.name, eq(&"MGET".to_owned()));
    assert_that!(&second_runtime[0].command.name, eq(&"MGET".to_owned()));
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
fn direct_mget_groups_keys_by_shard_worker_command() {
    let app = ServerApp::new(RuntimeConfig::default());
    let first_key = b"direct:rt:mget:group:1".to_vec();
    let first_shard = app.core.resolve_shard_for_key(&first_key);
    let mut second_key = b"direct:rt:mget:group:2".to_vec();
    let mut second_shard = app.core.resolve_shard_for_key(&second_key);
    let mut suffix = 0_u32;
    while second_shard != first_shard {
        suffix = suffix.saturating_add(1);
        second_key = format!("direct:rt:mget:group:2:{suffix}").into_bytes();
        second_shard = app.core.resolve_shard_for_key(&second_key);
    }
    let mut third_key = b"direct:rt:mget:group:3".to_vec();
    let mut third_shard = app.core.resolve_shard_for_key(&third_key);
    while third_shard == first_shard {
        suffix = suffix.saturating_add(1);
        third_key = format!("direct:rt:mget:group:3:{suffix}").into_bytes();
        third_shard = app.core.resolve_shard_for_key(&third_key);
    }

    let _ = app.execute_user_command(
        0,
        &CommandFrame::new("SET", vec![first_key.clone(), b"a".to_vec()]),
        None,
    );
    let _ = app.execute_user_command(
        0,
        &CommandFrame::new("SET", vec![second_key.clone(), b"b".to_vec()]),
        None,
    );
    let _ = app.execute_user_command(
        0,
        &CommandFrame::new("SET", vec![third_key.clone(), b"c".to_vec()]),
        None,
    );
    for shard in 0_u16..app.config.shard_count.get() {
        let _ = app
            .runtime
            .drain_processed_for_shard(shard)
            .expect("drain should succeed");
    }

    let frame = CommandFrame::new(
        "MGET",
        vec![first_key.clone(), second_key.clone(), third_key.clone()],
    );
    let reply = app.execute_user_command(0, &frame, None);
    assert_that!(
        &reply,
        eq(&CommandReply::Array(vec![
            CommandReply::BulkString(b"a".to_vec()),
            CommandReply::BulkString(b"b".to_vec()),
            CommandReply::BulkString(b"c".to_vec()),
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
            .wait_for_processed_count(third_shard, 1, Duration::from_millis(200))
            .expect("wait should succeed"),
        eq(true)
    );

    let first_runtime = app
        .runtime
        .drain_processed_for_shard(first_shard)
        .expect("drain should succeed");
    let third_runtime = app
        .runtime
        .drain_processed_for_shard(third_shard)
        .expect("drain should succeed");
    assert_that!(first_runtime.len(), eq(1_usize));
    assert_that!(third_runtime.len(), eq(1_usize));
    assert_that!(&first_runtime[0].command.name, eq(&"MGET".to_owned()));
    assert_that!(&third_runtime[0].command.name, eq(&"MGET".to_owned()));
    assert_that!(
        &first_runtime[0].command.args,
        eq(&vec![first_key.clone(), second_key.clone()])
    );
    assert_that!(&third_runtime[0].command.args, eq(&vec![third_key.clone()]));
    assert_that!(first_runtime[0].execute_on_worker, eq(true));
    assert_that!(third_runtime[0].execute_on_worker, eq(true));
}

#[rstest]
fn direct_mget_dispatches_runtime_once_when_keys_share_same_shard() {
    let app = ServerApp::new(RuntimeConfig::default());
    let first_key = b"direct:rt:same:1".to_vec();
    let shard = app.core.resolve_shard_for_key(&first_key);
    let mut second_key = b"direct:rt:same:2".to_vec();
    let mut second_shard = app.core.resolve_shard_for_key(&second_key);
    let mut suffix = 0_u32;
    while second_shard != shard {
        suffix = suffix.saturating_add(1);
        second_key = format!("direct:rt:same:2:{suffix}").into_bytes();
        second_shard = app.core.resolve_shard_for_key(&second_key);
    }

    let frame = CommandFrame::new("MGET", vec![first_key.clone(), second_key.clone()]);
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
}

#[rstest]
fn direct_mset_dispatches_runtime_to_each_touched_shard() {
    let app = ServerApp::new(RuntimeConfig::default());
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
    assert_that!(&first_runtime[0].command.name, eq(&"MSET".to_owned()));
    assert_that!(&second_runtime[0].command.name, eq(&"MSET".to_owned()));
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
fn direct_mset_groups_pairs_by_shard_worker_command() {
    let app = ServerApp::new(RuntimeConfig::default());
    let first_key = b"direct:rt:mset:group:1".to_vec();
    let first_shard = app.core.resolve_shard_for_key(&first_key);
    let mut second_key = b"direct:rt:mset:group:2".to_vec();
    let mut second_shard = app.core.resolve_shard_for_key(&second_key);
    let mut suffix = 0_u32;
    while second_shard != first_shard {
        suffix = suffix.saturating_add(1);
        second_key = format!("direct:rt:mset:group:2:{suffix}").into_bytes();
        second_shard = app.core.resolve_shard_for_key(&second_key);
    }
    let mut third_key = b"direct:rt:mset:group:3".to_vec();
    let mut third_shard = app.core.resolve_shard_for_key(&third_key);
    while third_shard == first_shard {
        suffix = suffix.saturating_add(1);
        third_key = format!("direct:rt:mset:group:3:{suffix}").into_bytes();
        third_shard = app.core.resolve_shard_for_key(&third_key);
    }

    let frame = CommandFrame::new(
        "MSET",
        vec![
            first_key.clone(),
            b"a".to_vec(),
            second_key.clone(),
            b"b".to_vec(),
            third_key.clone(),
            b"c".to_vec(),
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
            .wait_for_processed_count(third_shard, 1, Duration::from_millis(200))
            .expect("wait should succeed"),
        eq(true)
    );

    let first_runtime = app
        .runtime
        .drain_processed_for_shard(first_shard)
        .expect("drain should succeed");
    let third_runtime = app
        .runtime
        .drain_processed_for_shard(third_shard)
        .expect("drain should succeed");
    assert_that!(first_runtime.len(), eq(1_usize));
    assert_that!(third_runtime.len(), eq(1_usize));
    assert_that!(&first_runtime[0].command.name, eq(&"MSET".to_owned()));
    assert_that!(&third_runtime[0].command.name, eq(&"MSET".to_owned()));
    assert_that!(
        &first_runtime[0].command.args,
        eq(&vec![
            first_key.clone(),
            b"a".to_vec(),
            second_key.clone(),
            b"b".to_vec(),
        ])
    );
    assert_that!(
        &third_runtime[0].command.args,
        eq(&vec![third_key.clone(), b"c".to_vec()])
    );
    assert_that!(first_runtime[0].execute_on_worker, eq(true));
    assert_that!(third_runtime[0].execute_on_worker, eq(true));

    let first_value = app
        .core
        .execute_in_db(0, &CommandFrame::new("GET", vec![first_key]));
    let second_value = app
        .core
        .execute_in_db(0, &CommandFrame::new("GET", vec![second_key]));
    let third_value = app
        .core
        .execute_in_db(0, &CommandFrame::new("GET", vec![third_key]));
    assert_that!(&first_value, eq(&CommandReply::BulkString(b"a".to_vec())));
    assert_that!(&second_value, eq(&CommandReply::BulkString(b"b".to_vec())));
    assert_that!(&third_value, eq(&CommandReply::BulkString(b"c".to_vec())));
}

#[rstest]
fn direct_mset_same_shard_executes_single_worker_command() {
    let app = ServerApp::new(RuntimeConfig::default());
    let first_key = b"direct:rt:mset:same:1".to_vec();
    let shard = app.core.resolve_shard_for_key(&first_key);
    let mut second_key = b"direct:rt:mset:same:2".to_vec();
    let mut second_shard = app.core.resolve_shard_for_key(&second_key);
    let mut suffix = 0_u32;
    while second_shard != shard {
        suffix = suffix.saturating_add(1);
        second_key = format!("direct:rt:mset:same:2:{suffix}").into_bytes();
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

    let first_value = app
        .core
        .execute_in_db(0, &CommandFrame::new("GET", vec![first_key]));
    let second_value = app
        .core
        .execute_in_db(0, &CommandFrame::new("GET", vec![second_key]));
    assert_that!(&first_value, eq(&CommandReply::BulkString(b"a".to_vec())));
    assert_that!(&second_value, eq(&CommandReply::BulkString(b"b".to_vec())));
}

#[rstest]
fn direct_copy_same_shard_executes_on_worker_fiber() {
    let app = ServerApp::new(RuntimeConfig::default());
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
fn direct_copy_cross_shard_executes_on_source_worker_with_destination_barrier() {
    let app = ServerApp::new(RuntimeConfig::default());
    let source_key = b"direct:rt:copy:cross:source".to_vec();
    let source_shard = app.core.resolve_shard_for_key(&source_key);
    let mut destination_key = b"direct:rt:copy:cross:destination".to_vec();
    let mut destination_shard = app.core.resolve_shard_for_key(&destination_key);
    let mut suffix = 0_u32;
    while destination_shard == source_shard {
        suffix = suffix.saturating_add(1);
        destination_key = format!("direct:rt:copy:cross:destination:{suffix}").into_bytes();
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
            .wait_for_processed_count(source_shard, 1, Duration::from_millis(200))
            .expect("wait should succeed"),
        eq(true)
    );
    assert_that!(
        app.runtime
            .wait_for_processed_count(destination_shard, 1, Duration::from_millis(200))
            .expect("wait should succeed"),
        eq(true)
    );

    let source_runtime = app
        .runtime
        .drain_processed_for_shard(source_shard)
        .expect("drain should succeed");
    let destination_runtime = app
        .runtime
        .drain_processed_for_shard(destination_shard)
        .expect("drain should succeed");
    assert_that!(source_runtime.len(), eq(1_usize));
    assert_that!(destination_runtime.len(), eq(1_usize));
    assert_that!(&source_runtime[0].command, eq(&frame));
    assert_that!(&destination_runtime[0].command, eq(&frame));
    assert_that!(source_runtime[0].execute_on_worker, eq(true));
    assert_that!(destination_runtime[0].execute_on_worker, eq(false));

    let destination_value = app
        .core
        .execute_in_db(0, &CommandFrame::new("GET", vec![destination_key]));
    assert_that!(
        &destination_value,
        eq(&CommandReply::BulkString(b"value".to_vec()))
    );
}

#[rstest]
fn direct_rename_cross_shard_executes_on_source_worker_with_destination_barrier() {
    let app = ServerApp::new(RuntimeConfig::default());
    let source_key = b"direct:rt:rename:cross:source".to_vec();
    let source_shard = app.core.resolve_shard_for_key(&source_key);
    let mut destination_key = b"direct:rt:rename:cross:destination".to_vec();
    let mut destination_shard = app.core.resolve_shard_for_key(&destination_key);
    let mut suffix = 0_u32;
    while destination_shard == source_shard {
        suffix = suffix.saturating_add(1);
        destination_key = format!("direct:rt:rename:cross:destination:{suffix}").into_bytes();
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

    let frame = CommandFrame::new("RENAME", vec![source_key.clone(), destination_key.clone()]);
    let reply = app.execute_user_command(0, &frame, None);
    assert_that!(&reply, eq(&CommandReply::SimpleString("OK".to_owned())));

    assert_that!(
        app.runtime
            .wait_for_processed_count(source_shard, 1, Duration::from_millis(200))
            .expect("wait should succeed"),
        eq(true)
    );
    assert_that!(
        app.runtime
            .wait_for_processed_count(destination_shard, 1, Duration::from_millis(200))
            .expect("wait should succeed"),
        eq(true)
    );

    let source_runtime = app
        .runtime
        .drain_processed_for_shard(source_shard)
        .expect("drain should succeed");
    let destination_runtime = app
        .runtime
        .drain_processed_for_shard(destination_shard)
        .expect("drain should succeed");
    assert_that!(source_runtime.len(), eq(1_usize));
    assert_that!(destination_runtime.len(), eq(1_usize));
    assert_that!(&source_runtime[0].command, eq(&frame));
    assert_that!(&destination_runtime[0].command, eq(&frame));
    assert_that!(source_runtime[0].execute_on_worker, eq(true));
    assert_that!(destination_runtime[0].execute_on_worker, eq(false));

    let source_value = app
        .core
        .execute_in_db(0, &CommandFrame::new("GET", vec![source_key]));
    let destination_value = app
        .core
        .execute_in_db(0, &CommandFrame::new("GET", vec![destination_key]));
    assert_that!(&source_value, eq(&CommandReply::Null));
    assert_that!(
        &destination_value,
        eq(&CommandReply::BulkString(b"value".to_vec()))
    );
}

#[rstest]
fn direct_del_multikey_executes_each_key_on_worker_fiber() {
    let app = ServerApp::new(RuntimeConfig::default());
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
fn direct_del_multikey_groups_keys_by_shard_worker_command() {
    let app = ServerApp::new(RuntimeConfig::default());
    let first_key = b"direct:rt:del:group:1".to_vec();
    let first_shard = app.core.resolve_shard_for_key(&first_key);

    let mut second_key = b"direct:rt:del:group:2".to_vec();
    let mut second_shard = app.core.resolve_shard_for_key(&second_key);
    let mut suffix = 0_u32;
    while second_shard != first_shard {
        suffix = suffix.saturating_add(1);
        second_key = format!("direct:rt:del:group:2:{suffix}").into_bytes();
        second_shard = app.core.resolve_shard_for_key(&second_key);
    }

    let mut third_key = b"direct:rt:del:group:3".to_vec();
    let mut third_shard = app.core.resolve_shard_for_key(&third_key);
    while third_shard == first_shard {
        suffix = suffix.saturating_add(1);
        third_key = format!("direct:rt:del:group:3:{suffix}").into_bytes();
        third_shard = app.core.resolve_shard_for_key(&third_key);
    }

    let _ = app.execute_user_command(
        0,
        &CommandFrame::new("SET", vec![first_key.clone(), b"a".to_vec()]),
        None,
    );
    let _ = app.execute_user_command(
        0,
        &CommandFrame::new("SET", vec![second_key.clone(), b"b".to_vec()]),
        None,
    );
    let _ = app.execute_user_command(
        0,
        &CommandFrame::new("SET", vec![third_key.clone(), b"c".to_vec()]),
        None,
    );
    for shard in 0_u16..app.config.shard_count.get() {
        let _ = app
            .runtime
            .drain_processed_for_shard(shard)
            .expect("drain should succeed");
    }

    let frame = CommandFrame::new(
        "DEL",
        vec![first_key.clone(), second_key.clone(), third_key.clone()],
    );
    let reply = app.execute_user_command(0, &frame, None);
    assert_that!(&reply, eq(&CommandReply::Integer(3)));

    assert_that!(
        app.runtime
            .wait_for_processed_count(first_shard, 1, Duration::from_millis(200))
            .expect("wait should succeed"),
        eq(true)
    );
    assert_that!(
        app.runtime
            .wait_for_processed_count(third_shard, 1, Duration::from_millis(200))
            .expect("wait should succeed"),
        eq(true)
    );

    let first_runtime = app
        .runtime
        .drain_processed_for_shard(first_shard)
        .expect("drain should succeed");
    let third_runtime = app
        .runtime
        .drain_processed_for_shard(third_shard)
        .expect("drain should succeed");
    assert_that!(first_runtime.len(), eq(1_usize));
    assert_that!(third_runtime.len(), eq(1_usize));
    assert_that!(&first_runtime[0].command.name, eq(&"DEL".to_owned()));
    assert_that!(&third_runtime[0].command.name, eq(&"DEL".to_owned()));
    assert_that!(
        &first_runtime[0].command.args,
        eq(&vec![first_key.clone(), second_key.clone()])
    );
    assert_that!(&third_runtime[0].command.args, eq(&vec![third_key.clone()]));
    assert_that!(first_runtime[0].execute_on_worker, eq(true));
    assert_that!(third_runtime[0].execute_on_worker, eq(true));
}

#[rstest]
fn direct_del_same_shard_executes_single_worker_command() {
    let app = ServerApp::new(RuntimeConfig::default());
    let first_key = b"direct:rt:del:same:1".to_vec();
    let shard = app.core.resolve_shard_for_key(&first_key);
    let mut second_key = b"direct:rt:del:same:2".to_vec();
    let mut second_shard = app.core.resolve_shard_for_key(&second_key);
    let mut suffix = 0_u32;
    while second_shard != shard {
        suffix = suffix.saturating_add(1);
        second_key = format!("direct:rt:del:same:2:{suffix}").into_bytes();
        second_shard = app.core.resolve_shard_for_key(&second_key);
    }

    let _ = app.execute_user_command(
        0,
        &CommandFrame::new("SET", vec![first_key.clone(), b"a".to_vec()]),
        None,
    );
    let _ = app.execute_user_command(
        0,
        &CommandFrame::new("SET", vec![second_key.clone(), b"b".to_vec()]),
        None,
    );
    for shard_id in 0_u16..app.config.shard_count.get() {
        let _ = app
            .runtime
            .drain_processed_for_shard(shard_id)
            .expect("drain should succeed");
    }

    let frame = CommandFrame::new("DEL", vec![first_key.clone(), second_key.clone()]);
    let reply = app.execute_user_command(0, &frame, None);
    assert_that!(&reply, eq(&CommandReply::Integer(2)));
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
}

#[rstest]
fn direct_mset_with_odd_arity_does_not_dispatch_runtime() {
    let app = ServerApp::new(RuntimeConfig::default());
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
    let app = ServerApp::new(RuntimeConfig::default());
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
    let app = ServerApp::new(RuntimeConfig::default());
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
    let app = ServerApp::new(RuntimeConfig::default());
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
    let app = ServerApp::new(RuntimeConfig::default());
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
    let app = ServerApp::new(RuntimeConfig::default());
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
    let app = ServerApp::new(RuntimeConfig::default());
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
    let app = ServerApp::new(RuntimeConfig::default());
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

    let _ = ingress_connection_bytes(
        &mut app,
        &mut connection,
        &resp_command(&[b"SET", b"k", b"v"]),
    )
    .expect("seed SET should succeed");
    assert_that!(
        app.replication_guard_with_latest_journal()
            .journal_entries()
            .len(),
        eq(1_usize)
    );

    let _ = ingress_connection_bytes(&mut app, &mut connection, &resp_command(&[b"MULTI"]))
        .expect("MULTI should succeed");
    let _ = ingress_connection_bytes(&mut app, &mut connection, &resp_command(&[b"GET", b"k"]))
        .expect("GET should queue");
    let _ = ingress_connection_bytes(&mut app, &mut connection, &resp_command(&[b"TYPE", b"k"]))
        .expect("TYPE should queue");

    let exec = ingress_connection_bytes(&mut app, &mut connection, &resp_command(&[b"EXEC"]))
        .expect("EXEC should succeed");
    let expected = vec![b"*2\r\n$1\r\nv\r\n+string\r\n".to_vec()];
    assert_that!(&exec, eq(&expected));

    // Read-only transaction must not create new journal records.
    assert_that!(
        app.replication_guard_with_latest_journal()
            .journal_entries()
            .len(),
        eq(1_usize)
    );
}

