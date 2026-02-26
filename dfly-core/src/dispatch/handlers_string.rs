use super::{DispatchState, ValueEntry, normalize_redis_range, parse_redis_i64, parse_redis_u64};
use crate::command::{CommandFrame, CommandReply};
use dfly_common::ids::DbIndex;

use super::parse_opts::{
    parse_set_options, resolve_set_expire_at_unix_secs, set_condition_satisfied,
};

pub(super) fn handle_ping(
    _db: DbIndex,
    frame: &CommandFrame,
    _state: &mut DispatchState,
) -> CommandReply {
    if frame.args.is_empty() {
        return CommandReply::SimpleString("PONG".to_owned());
    }
    if frame.args.len() == 1 {
        return CommandReply::BulkString(frame.args[0].clone());
    }
    CommandReply::Error("wrong number of arguments for 'PING' command".to_owned())
}

pub(super) fn handle_echo(
    _db: DbIndex,
    frame: &CommandFrame,
    _state: &mut DispatchState,
) -> CommandReply {
    CommandReply::BulkString(frame.args[0].clone())
}

pub(super) fn handle_set(db: DbIndex, frame: &CommandFrame, state: &mut DispatchState) -> CommandReply {
    let key = frame.args[0].clone();
    let value = frame.args[1].clone();
    let options = match parse_set_options(&frame.args[2..]) {
        Ok(options) => options,
        Err(error) => return CommandReply::Error(error),
    };
    state.purge_expired_key(db, &key);

    let existing = state.db_map(db).and_then(|map| map.get(&key)).cloned();
    let key_exists = existing.is_some();
    let previous_value = existing.as_ref().map(ValueEntry::clone_string_bytes);
    let previous_expire_at = existing.and_then(|entry| entry.expire_at_unix_secs);

    if !set_condition_satisfied(options.condition, key_exists) {
        if options.return_previous {
            return previous_value.map_or(CommandReply::Null, CommandReply::BulkString);
        }
        return CommandReply::Null;
    }

    let expire_at_unix_secs = if let Some(expire) = options.expire {
        Some(resolve_set_expire_at_unix_secs(expire))
    } else if options.keep_ttl {
        previous_expire_at
    } else {
        None
    };

    let _ = state.upsert_key(db, &key, ValueEntry::new_string(value, expire_at_unix_secs));
    state.bump_key_version(db, &key);

    if options.return_previous {
        previous_value.map_or(CommandReply::Null, CommandReply::BulkString)
    } else {
        CommandReply::SimpleString("OK".to_owned())
    }
}

pub(super) fn handle_setnx(db: DbIndex, frame: &CommandFrame, state: &mut DispatchState) -> CommandReply {
    let key = frame.args[0].clone();
    let value = frame.args[1].clone();
    state.purge_expired_key(db, &key);

    if state.db_map(db).is_some_and(|map| map.contains_key(&key)) {
        return CommandReply::Integer(0);
    }

    let _ = state.upsert_key(db, &key, ValueEntry::new_string(value, None));
    state.bump_key_version(db, &key);
    CommandReply::Integer(1)
}

pub(super) fn handle_mget(db: DbIndex, frame: &CommandFrame, state: &mut DispatchState) -> CommandReply {
    let mut values = Vec::with_capacity(frame.args.len());
    for key in &frame.args {
        state.purge_expired_key(db, key);
        let value = if let Some(value) = state
            .db_map(db)
            .and_then(|map| map.get(key))
            .map(ValueEntry::clone_string_bytes)
        {
            if let Some(table) = state.db_tables.get_mut(&db) {
                DispatchState::increment_slot_read_stat(table, key);
            }
            CommandReply::BulkString(value)
        } else {
            CommandReply::Null
        };
        values.push(value);
    }
    CommandReply::Array(values)
}

pub(super) fn handle_mset(db: DbIndex, frame: &CommandFrame, state: &mut DispatchState) -> CommandReply {
    if frame.args.is_empty() || !frame.args.len().is_multiple_of(2) {
        return CommandReply::Error("wrong number of arguments for 'MSET' command".to_owned());
    }

    for pair in frame.args.chunks_exact(2) {
        let key = pair[0].clone();
        let value = pair[1].clone();
        state.purge_expired_key(db, &key);
        let _ = state.upsert_key(db, &key, ValueEntry::new_string(value, None));
        state.bump_key_version(db, &key);
    }
    CommandReply::SimpleString("OK".to_owned())
}

pub(super) fn handle_msetnx(
    db: DbIndex,
    frame: &CommandFrame,
    state: &mut DispatchState,
) -> CommandReply {
    if frame.args.is_empty() || !frame.args.len().is_multiple_of(2) {
        return CommandReply::Error("wrong number of arguments for 'MSETNX' command".to_owned());
    }

    for pair in frame.args.chunks_exact(2) {
        let key = &pair[0];
        state.purge_expired_key(db, key);
        if state.db_map(db).is_some_and(|map| map.contains_key(key)) {
            return CommandReply::Integer(0);
        }
    }

    for pair in frame.args.chunks_exact(2) {
        let key = pair[0].clone();
        let value = pair[1].clone();
        let _ = state.upsert_key(db, &key, ValueEntry::new_string(value, None));
        state.bump_key_version(db, &key);
    }
    CommandReply::Integer(1)
}

pub(super) fn handle_get(db: DbIndex, frame: &CommandFrame, state: &mut DispatchState) -> CommandReply {
    let key = &frame.args[0];
    state.purge_expired_key(db, key);
    match state
        .db_map(db)
        .and_then(|map| map.get(key))
        .map(ValueEntry::clone_string_bytes)
    {
        Some(value) => {
            if let Some(table) = state.db_tables.get_mut(&db) {
                DispatchState::increment_slot_read_stat(table, key);
            }
            CommandReply::BulkString(value)
        }
        None => CommandReply::Null,
    }
}

pub(super) fn handle_type(db: DbIndex, frame: &CommandFrame, state: &mut DispatchState) -> CommandReply {
    let key = &frame.args[0];
    state.purge_expired_key(db, key);
    if state.db_map(db).is_some_and(|map| map.contains_key(key)) {
        if let Some(table) = state.db_tables.get_mut(&db) {
            DispatchState::increment_slot_read_stat(table, key);
        }
        return CommandReply::SimpleString("string".to_owned());
    }
    CommandReply::SimpleString("none".to_owned())
}

pub(super) fn handle_getset(db: DbIndex, frame: &CommandFrame, state: &mut DispatchState) -> CommandReply {
    let key = frame.args[0].clone();
    let next_value = frame.args[1].clone();
    state.purge_expired_key(db, &key);

    let previous = state
        .db_map(db)
        .and_then(|map| map.get(&key))
        .map(ValueEntry::clone_string_bytes);

    let _ = state.upsert_key(db, &key, ValueEntry::new_string(next_value, None));
    state.bump_key_version(db, &key);

    previous.map_or(CommandReply::Null, CommandReply::BulkString)
}

pub(super) fn handle_getdel(db: DbIndex, frame: &CommandFrame, state: &mut DispatchState) -> CommandReply {
    let key = &frame.args[0];
    state.purge_expired_key(db, key);

    let removed = state.remove_key(db, key);
    if let Some(entry) = removed {
        state.bump_key_version(db, key);
        return CommandReply::BulkString(entry.into_string_bytes());
    }
    CommandReply::Null
}

pub(super) fn handle_append(db: DbIndex, frame: &CommandFrame, state: &mut DispatchState) -> CommandReply {
    let key = frame.args[0].clone();
    let suffix = &frame.args[1];
    state.purge_expired_key(db, &key);

    let (mut value, expire_at_unix_secs) =
        if let Some(existing) = state.db_map(db).and_then(|map| map.get(&key)) {
            (existing.clone_string_bytes(), existing.expire_at_unix_secs)
        } else {
            (Vec::new(), None)
        };
    value.extend_from_slice(suffix);

    let _ = state.upsert_key(
        db,
        &key,
        ValueEntry::new_string(value.clone(), expire_at_unix_secs),
    );
    state.bump_key_version(db, &key);
    CommandReply::Integer(i64::try_from(value.len()).unwrap_or(i64::MAX))
}

pub(super) fn handle_strlen(db: DbIndex, frame: &CommandFrame, state: &mut DispatchState) -> CommandReply {
    let key = &frame.args[0];
    state.purge_expired_key(db, key);
    let length = if let Some(length) = state
        .db_map(db)
        .and_then(|map| map.get(key))
        .map(ValueEntry::string_len)
    {
        if let Some(table) = state.db_tables.get_mut(&db) {
            DispatchState::increment_slot_read_stat(table, key);
        }
        length
    } else {
        0_usize
    };
    CommandReply::Integer(i64::try_from(length).unwrap_or(i64::MAX))
}

pub(super) fn handle_getrange(db: DbIndex, frame: &CommandFrame, state: &mut DispatchState) -> CommandReply {
    let key = &frame.args[0];
    state.purge_expired_key(db, key);

    let Ok(start) = parse_redis_i64(&frame.args[1]) else {
        return CommandReply::Error("value is not an integer or out of range".to_owned());
    };
    let Ok(end) = parse_redis_i64(&frame.args[2]) else {
        return CommandReply::Error("value is not an integer or out of range".to_owned());
    };

    let Some(value) = state
        .db_map(db)
        .and_then(|map| map.get(key))
        .map(ValueEntry::clone_string_bytes)
    else {
        return CommandReply::BulkString(Vec::new());
    };

    let Some((start_index, end_index)) = normalize_redis_range(start, end, value.len()) else {
        return CommandReply::BulkString(Vec::new());
    };
    if let Some(table) = state.db_tables.get_mut(&db) {
        DispatchState::increment_slot_read_stat(table, key);
    }
    CommandReply::BulkString(value[start_index..=end_index].to_vec())
}

pub(super) fn handle_setrange(db: DbIndex, frame: &CommandFrame, state: &mut DispatchState) -> CommandReply {
    let key = frame.args[0].clone();
    let Ok(offset_u64) = parse_redis_u64(&frame.args[1]) else {
        return CommandReply::Error("value is not an integer or out of range".to_owned());
    };
    let Ok(offset) = usize::try_from(offset_u64) else {
        return CommandReply::Error("value is not an integer or out of range".to_owned());
    };
    let payload = &frame.args[2];
    state.purge_expired_key(db, &key);

    let (mut value, expire_at_unix_secs) =
        if let Some(existing) = state.db_map(db).and_then(|map| map.get(&key)) {
            (existing.clone_string_bytes(), existing.expire_at_unix_secs)
        } else {
            (Vec::new(), None)
        };

    if payload.is_empty() {
        return CommandReply::Integer(i64::try_from(value.len()).unwrap_or(i64::MAX));
    }

    if offset > value.len() {
        value.resize(offset, 0_u8);
    }
    let needed_len = offset.saturating_add(payload.len());
    if needed_len > value.len() {
        value.resize(needed_len, 0_u8);
    }
    value[offset..offset + payload.len()].copy_from_slice(payload);

    let _ = state.upsert_key(
        db,
        &key,
        ValueEntry::new_string(value.clone(), expire_at_unix_secs),
    );
    state.bump_key_version(db, &key);
    CommandReply::Integer(i64::try_from(value.len()).unwrap_or(i64::MAX))
}
