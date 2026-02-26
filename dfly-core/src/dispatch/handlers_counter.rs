use super::{DispatchState, ValueEntry};
use super::parse_numbers::parse_redis_i64;
use crate::command::{CommandFrame, CommandReply};
use dfly_common::ids::DbIndex;

pub(super) fn handle_incr(db: DbIndex, frame: &CommandFrame, state: &mut DispatchState) -> CommandReply {
    mutate_counter_by(db, &frame.args[0], 1, state)
}

pub(super) fn handle_decr(db: DbIndex, frame: &CommandFrame, state: &mut DispatchState) -> CommandReply {
    mutate_counter_by(db, &frame.args[0], -1, state)
}

pub(super) fn handle_incrby(
    db: DbIndex,
    frame: &CommandFrame,
    state: &mut DispatchState,
) -> CommandReply {
    let Ok(delta) = parse_redis_i64(&frame.args[1]) else {
        return CommandReply::Error("value is not an integer or out of range".to_owned());
    };
    mutate_counter_by(db, &frame.args[0], delta, state)
}

pub(super) fn handle_decrby(
    db: DbIndex,
    frame: &CommandFrame,
    state: &mut DispatchState,
) -> CommandReply {
    let Ok(amount) = parse_redis_i64(&frame.args[1]) else {
        return CommandReply::Error("value is not an integer or out of range".to_owned());
    };
    let Some(delta) = amount.checked_neg() else {
        return CommandReply::Error("value is not an integer or out of range".to_owned());
    };
    mutate_counter_by(db, &frame.args[0], delta, state)
}

/// Applies one signed integer delta to a key using Redis-compatible counter semantics.
///
/// Missing keys are treated as zero and created. Existing TTL metadata is preserved.
fn mutate_counter_by(
    db: DbIndex,
    key: &[u8],
    delta: i64,
    state: &mut DispatchState,
) -> CommandReply {
    state.purge_expired_key(db, key);

    let (current, expire_at_unix_secs) =
        if let Some(existing) = state.db_map(db).and_then(|map| map.get(key)) {
            let Ok(current) = parse_redis_i64(existing.string_bytes()) else {
                return CommandReply::Error("value is not an integer or out of range".to_owned());
            };
            (current, existing.expire_at_unix_secs)
        } else {
            (0_i64, None)
        };

    let Some(next) = current.checked_add(delta) else {
        return CommandReply::Error("value is not an integer or out of range".to_owned());
    };

    let _ = state.upsert_key(
        db,
        key,
        ValueEntry::new_string(next.to_string().into_bytes(), expire_at_unix_secs),
    );
    state.bump_key_version(db, key);
    CommandReply::Integer(next)
}
