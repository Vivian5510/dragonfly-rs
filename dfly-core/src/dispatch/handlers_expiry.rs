use super::{DispatchState, ValueEntry};
use super::parse_numbers::parse_redis_i64;
use crate::command::{CommandFrame, CommandReply};
use dfly_common::ids::DbIndex;

use super::parse_opts::{expire_options_satisfied, parse_expire_options};

pub(super) fn handle_setex(db: DbIndex, frame: &CommandFrame, state: &mut DispatchState) -> CommandReply {
    let key = frame.args[0].clone();
    let Ok(seconds) = parse_redis_i64(&frame.args[1]) else {
        return CommandReply::Error("value is not an integer or out of range".to_owned());
    };
    if seconds <= 0 {
        return CommandReply::Error("invalid expire time in 'SETEX' command".to_owned());
    }
    let Ok(expire_delta) = u64::try_from(seconds) else {
        return CommandReply::Error("value is not an integer or out of range".to_owned());
    };
    let expire_at = DispatchState::now_unix_seconds().saturating_add(expire_delta);

    let _ = state.upsert_key(
        db,
        &key,
        ValueEntry::new_string(frame.args[2].clone(), Some(expire_at)),
    );
    state.bump_key_version(db, &key);
    CommandReply::SimpleString("OK".to_owned())
}

pub(super) fn handle_psetex(db: DbIndex, frame: &CommandFrame, state: &mut DispatchState) -> CommandReply {
    let key = frame.args[0].clone();
    let Ok(milliseconds) = parse_redis_i64(&frame.args[1]) else {
        return CommandReply::Error("value is not an integer or out of range".to_owned());
    };
    if milliseconds <= 0 {
        return CommandReply::Error("invalid expire time in 'PSETEX' command".to_owned());
    }
    let Ok(expire_delta_millis) = u64::try_from(milliseconds) else {
        return CommandReply::Error("value is not an integer or out of range".to_owned());
    };
    let now_millis = DispatchState::now_unix_millis();
    let expire_at_millis = now_millis.saturating_add(expire_delta_millis);
    let expire_at_secs = expire_at_millis.saturating_add(999) / 1000;

    let _ = state.upsert_key(
        db,
        &key,
        ValueEntry::new_string(frame.args[2].clone(), Some(expire_at_secs)),
    );
    state.bump_key_version(db, &key);
    CommandReply::SimpleString("OK".to_owned())
}

pub(super) fn handle_expire(db: DbIndex, frame: &CommandFrame, state: &mut DispatchState) -> CommandReply {
    let key = &frame.args[0];
    state.purge_expired_key(db, key);

    let options = match parse_expire_options(&frame.args[2..]) {
        Ok(options) => options,
        Err(error) => return CommandReply::Error(error),
    };
    let seconds = match parse_i64_arg(&frame.args[1], "EXPIRE seconds") {
        Ok(seconds) => seconds,
        Err(error) => return CommandReply::Error(error),
    };
    let current_expire_at = state
        .db_map(db)
        .and_then(|map| map.get(key))
        .map(|entry| entry.expire_at_unix_secs);
    let Some(current_expire_at) = current_expire_at else {
        return CommandReply::Integer(0);
    };

    let now_secs = DispatchState::now_unix_seconds();
    let target_expire_at = if seconds <= 0 {
        now_secs.saturating_sub(1)
    } else {
        let Ok(expire_delta) = u64::try_from(seconds) else {
            return CommandReply::Error("EXPIRE seconds is out of range".to_owned());
        };
        now_secs.saturating_add(expire_delta)
    };
    if !expire_options_satisfied(options, current_expire_at, target_expire_at) {
        return CommandReply::Integer(0);
    }

    if seconds <= 0 {
        if state.remove_key(db, key).is_some() {
            state.bump_key_version(db, key);
            return CommandReply::Integer(1);
        }
        return CommandReply::Integer(0);
    }

    if state.set_key_expire(db, key, Some(target_expire_at)) {
        state.bump_key_version(db, key);
        return CommandReply::Integer(1);
    }
    CommandReply::Integer(0)
}

pub(super) fn handle_pexpire(db: DbIndex, frame: &CommandFrame, state: &mut DispatchState) -> CommandReply {
    let key = &frame.args[0];
    state.purge_expired_key(db, key);

    let options = match parse_expire_options(&frame.args[2..]) {
        Ok(options) => options,
        Err(error) => return CommandReply::Error(error),
    };
    let milliseconds = match parse_i64_arg(&frame.args[1], "PEXPIRE milliseconds") {
        Ok(milliseconds) => milliseconds,
        Err(error) => return CommandReply::Error(error),
    };
    let current_expire_at_secs = state
        .db_map(db)
        .and_then(|map| map.get(key))
        .map(|entry| entry.expire_at_unix_secs);
    let Some(current_expire_at_secs) = current_expire_at_secs else {
        return CommandReply::Integer(0);
    };

    let now_millis = DispatchState::now_unix_millis();
    let target_expire_at_millis = if milliseconds <= 0 {
        now_millis.saturating_sub(1)
    } else {
        let Ok(expire_delta_millis) = u64::try_from(milliseconds) else {
            return CommandReply::Error("PEXPIRE milliseconds is out of range".to_owned());
        };
        now_millis.saturating_add(expire_delta_millis)
    };
    let current_expire_at_millis = current_expire_at_secs
        .map(|expire_at_secs| expire_at_secs.saturating_mul(1000).saturating_sub(1));
    if !expire_options_satisfied(options, current_expire_at_millis, target_expire_at_millis) {
        return CommandReply::Integer(0);
    }

    if milliseconds <= 0 {
        if state.remove_key(db, key).is_some() {
            state.bump_key_version(db, key);
            return CommandReply::Integer(1);
        }
        return CommandReply::Integer(0);
    }

    let expire_at_secs = target_expire_at_millis.saturating_add(999) / 1000;

    if state.set_key_expire(db, key, Some(expire_at_secs)) {
        state.bump_key_version(db, key);
        return CommandReply::Integer(1);
    }
    CommandReply::Integer(0)
}

pub(super) fn handle_pexpireat(
    db: DbIndex,
    frame: &CommandFrame,
    state: &mut DispatchState,
) -> CommandReply {
    let key = &frame.args[0];
    state.purge_expired_key(db, key);

    let options = match parse_expire_options(&frame.args[2..]) {
        Ok(options) => options,
        Err(error) => return CommandReply::Error(error),
    };
    let timestamp_millis = match parse_i64_arg(&frame.args[1], "PEXPIREAT timestamp") {
        Ok(timestamp) => timestamp,
        Err(error) => return CommandReply::Error(error),
    };
    let current_expire_at_secs = state
        .db_map(db)
        .and_then(|map| map.get(key))
        .map(|entry| entry.expire_at_unix_secs);
    let Some(current_expire_at_secs) = current_expire_at_secs else {
        return CommandReply::Integer(0);
    };

    let timestamp_millis = timestamp_millis.max(0);
    let now_millis = i64::try_from(DispatchState::now_unix_millis()).unwrap_or(i64::MAX);
    let Ok(target_expire_at_millis) = u64::try_from(timestamp_millis) else {
        return CommandReply::Error("PEXPIREAT timestamp is out of range".to_owned());
    };
    let current_expire_at_millis = current_expire_at_secs
        .map(|expire_at_secs| expire_at_secs.saturating_mul(1000).saturating_sub(1));
    if !expire_options_satisfied(options, current_expire_at_millis, target_expire_at_millis) {
        return CommandReply::Integer(0);
    }

    if i64::try_from(target_expire_at_millis).unwrap_or(i64::MAX) <= now_millis {
        if state.remove_key(db, key).is_some() {
            state.bump_key_version(db, key);
            return CommandReply::Integer(1);
        }
        return CommandReply::Integer(0);
    }

    let expire_at_secs = target_expire_at_millis.saturating_add(999) / 1000;
    if state.set_key_expire(db, key, Some(expire_at_secs)) {
        state.bump_key_version(db, key);
        return CommandReply::Integer(1);
    }
    CommandReply::Integer(0)
}

pub(super) fn handle_expireat(db: DbIndex, frame: &CommandFrame, state: &mut DispatchState) -> CommandReply {
    let key = &frame.args[0];
    state.purge_expired_key(db, key);

    let options = match parse_expire_options(&frame.args[2..]) {
        Ok(options) => options,
        Err(error) => return CommandReply::Error(error),
    };
    let timestamp = match parse_i64_arg(&frame.args[1], "EXPIREAT timestamp") {
        Ok(timestamp) => timestamp,
        Err(error) => return CommandReply::Error(error),
    };
    let current_expire_at = state
        .db_map(db)
        .and_then(|map| map.get(key))
        .map(|entry| entry.expire_at_unix_secs);
    let Some(current_expire_at) = current_expire_at else {
        return CommandReply::Integer(0);
    };

    let timestamp = timestamp.max(0);
    let now = i64::try_from(DispatchState::now_unix_seconds()).unwrap_or(i64::MAX);
    let Ok(target_expire_at) = u64::try_from(timestamp) else {
        return CommandReply::Error("EXPIREAT timestamp is out of range".to_owned());
    };
    if !expire_options_satisfied(options, current_expire_at, target_expire_at) {
        return CommandReply::Integer(0);
    }

    if i64::try_from(target_expire_at).unwrap_or(i64::MAX) <= now {
        if state.remove_key(db, key).is_some() {
            state.bump_key_version(db, key);
            return CommandReply::Integer(1);
        }
        return CommandReply::Integer(0);
    }

    if state.set_key_expire(db, key, Some(target_expire_at)) {
        state.bump_key_version(db, key);
        return CommandReply::Integer(1);
    }
    CommandReply::Integer(0)
}

pub(super) fn handle_ttl(db: DbIndex, frame: &CommandFrame, state: &mut DispatchState) -> CommandReply {
    let key = &frame.args[0];
    state.purge_expired_key(db, key);

    let Some(expire_at) = state
        .db_map(db)
        .and_then(|map| map.get(key))
        .and_then(|entry| entry.expire_at_unix_secs)
    else {
        if state.db_map(db).is_some_and(|map| map.contains_key(key)) {
            return CommandReply::Integer(-1);
        }
        return CommandReply::Integer(-2);
    };

    let now = DispatchState::now_unix_seconds();
    if expire_at <= now {
        let _ = state.remove_key(db, key);
        return CommandReply::Integer(-2);
    }
    let remaining = expire_at.saturating_sub(now);
    match i64::try_from(remaining) {
        Ok(remaining) => CommandReply::Integer(remaining),
        Err(_) => CommandReply::Integer(i64::MAX),
    }
}

pub(super) fn handle_pttl(db: DbIndex, frame: &CommandFrame, state: &mut DispatchState) -> CommandReply {
    let key = &frame.args[0];
    state.purge_expired_key(db, key);

    let Some(expire_at_secs) = state
        .db_map(db)
        .and_then(|map| map.get(key))
        .and_then(|entry| entry.expire_at_unix_secs)
    else {
        if state.db_map(db).is_some_and(|map| map.contains_key(key)) {
            return CommandReply::Integer(-1);
        }
        return CommandReply::Integer(-2);
    };

    let now_millis = DispatchState::now_unix_millis();
    let expire_at_millis = expire_at_secs.saturating_mul(1000);
    if expire_at_millis <= now_millis {
        let _ = state.remove_key(db, key);
        return CommandReply::Integer(-2);
    }
    let remaining = expire_at_millis.saturating_sub(now_millis);
    match i64::try_from(remaining) {
        Ok(remaining) => CommandReply::Integer(remaining),
        Err(_) => CommandReply::Integer(i64::MAX),
    }
}

pub(super) fn handle_expiretime(
    db: DbIndex,
    frame: &CommandFrame,
    state: &mut DispatchState,
) -> CommandReply {
    let key = &frame.args[0];
    state.purge_expired_key(db, key);

    let Some(expire_at) = state
        .db_map(db)
        .and_then(|map| map.get(key))
        .and_then(|entry| entry.expire_at_unix_secs)
    else {
        if state.db_map(db).is_some_and(|map| map.contains_key(key)) {
            return CommandReply::Integer(-1);
        }
        return CommandReply::Integer(-2);
    };

    match i64::try_from(expire_at) {
        Ok(expire_at) => CommandReply::Integer(expire_at),
        Err(_) => CommandReply::Integer(i64::MAX),
    }
}

pub(super) fn handle_pexpiretime(
    db: DbIndex,
    frame: &CommandFrame,
    state: &mut DispatchState,
) -> CommandReply {
    let key = &frame.args[0];
    state.purge_expired_key(db, key);

    let Some(expire_at_secs) = state
        .db_map(db)
        .and_then(|map| map.get(key))
        .and_then(|entry| entry.expire_at_unix_secs)
    else {
        if state.db_map(db).is_some_and(|map| map.contains_key(key)) {
            return CommandReply::Integer(-1);
        }
        return CommandReply::Integer(-2);
    };

    let expire_at_millis = expire_at_secs.saturating_mul(1000);
    match i64::try_from(expire_at_millis) {
        Ok(expire_at) => CommandReply::Integer(expire_at),
        Err(_) => CommandReply::Integer(i64::MAX),
    }
}

pub(super) fn handle_persist(db: DbIndex, frame: &CommandFrame, state: &mut DispatchState) -> CommandReply {
    let key = &frame.args[0];
    state.purge_expired_key(db, key);

    if state
        .db_map(db)
        .and_then(|map| map.get(key))
        .is_some_and(|entry| entry.expire_at_unix_secs.is_some())
        && state.set_key_expire(db, key, None)
    {
        state.bump_key_version(db, key);
        return CommandReply::Integer(1);
    }
    CommandReply::Integer(0)
}

fn parse_i64_arg(arg: &[u8], field_name: &str) -> Result<i64, String> {
    let text = std::str::from_utf8(arg).map_err(|_| format!("{field_name} must be valid UTF-8"))?;
    text.parse::<i64>()
        .map_err(|_| format!("{field_name} must be an integer"))
}
