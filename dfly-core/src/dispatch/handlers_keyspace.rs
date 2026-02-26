use super::{DispatchState, parse_redis_i64};
use crate::command::{CommandFrame, CommandReply};
use dfly_common::ids::DbIndex;

pub(super) fn handle_del(db: DbIndex, frame: &CommandFrame, state: &mut DispatchState) -> CommandReply {
    let mut deleted = 0_i64;

    for key in &frame.args {
        state.purge_expired_key(db, key);
        if state.remove_key(db, key).is_some() {
            state.bump_key_version(db, key);
            deleted = deleted.saturating_add(1);
        }
    }

    CommandReply::Integer(deleted)
}

pub(super) fn handle_exists(
    db: DbIndex,
    frame: &CommandFrame,
    state: &mut DispatchState,
) -> CommandReply {
    let mut existing = 0_i64;

    for key in &frame.args {
        state.purge_expired_key(db, key);
        if state.db_map(db).is_some_and(|map| map.contains_key(key)) {
            if let Some(table) = state.db_tables.get_mut(&db) {
                DispatchState::increment_slot_read_stat(table, key);
            }
            existing = existing.saturating_add(1);
        }
    }

    CommandReply::Integer(existing)
}

pub(super) fn handle_move(db: DbIndex, frame: &CommandFrame, state: &mut DispatchState) -> CommandReply {
    let key = frame.args[0].clone();
    state.purge_expired_key(db, &key);

    let Ok(target_db_i64) = parse_redis_i64(&frame.args[1]) else {
        return CommandReply::Error("value is not an integer or out of range".to_owned());
    };
    let Ok(target_db) = u16::try_from(target_db_i64) else {
        return CommandReply::Error("value is not an integer or out of range".to_owned());
    };

    state.purge_expired_key(target_db, &key);
    if !state.db_map(db).is_some_and(|map| map.contains_key(&key)) {
        return CommandReply::Integer(0);
    }
    if state
        .db_map(target_db)
        .is_some_and(|map| map.contains_key(&key))
    {
        return CommandReply::Integer(0);
    }

    let Some(entry) = state.remove_key(db, &key) else {
        return CommandReply::Integer(0);
    };
    let _ = state.upsert_key(target_db, &key, entry);
    state.bump_key_version(db, &key);
    state.bump_key_version(target_db, &key);
    CommandReply::Integer(1)
}

pub(super) fn handle_copy(db: DbIndex, frame: &CommandFrame, state: &mut DispatchState) -> CommandReply {
    copy_single_state(db, &frame.args, state)
}

pub(super) fn handle_rename(db: DbIndex, frame: &CommandFrame, state: &mut DispatchState) -> CommandReply {
    rename_single_state(db, &frame.args[0], &frame.args[1], false, state)
}

pub(super) fn handle_renamenx(
    db: DbIndex,
    frame: &CommandFrame,
    state: &mut DispatchState,
) -> CommandReply {
    rename_single_state(db, &frame.args[0], &frame.args[1], true, state)
}

/// Moves one key between two different shard states.
///
/// This helper exists for cross-shard `RENAME/RENAMENX` in `CoreModule`, where source and
/// destination keys hash to different owners and therefore cannot be handled by one shard-local
/// command dispatch.
#[must_use]
pub fn rename_between_states(
    db: DbIndex,
    source_key: &[u8],
    destination_key: &[u8],
    destination_should_not_exist: bool,
    source_state: &mut DispatchState,
    destination_state: &mut DispatchState,
) -> CommandReply {
    source_state.purge_expired_key(db, source_key);
    destination_state.purge_expired_key(db, destination_key);

    if !source_state
        .db_map(db)
        .is_some_and(|map| map.contains_key(source_key))
    {
        return CommandReply::Error("no such key".to_owned());
    }
    if destination_should_not_exist
        && destination_state
            .db_map(db)
            .is_some_and(|map| map.contains_key(destination_key))
    {
        return CommandReply::Integer(0);
    }

    let Some(source_entry) = source_state.remove_key(db, source_key) else {
        return CommandReply::Error("no such key".to_owned());
    };

    if !destination_should_not_exist {
        let _ = destination_state.remove_key(db, destination_key);
    }
    let _ = destination_state.upsert_key(db, destination_key, source_entry);

    source_state.bump_key_version(db, source_key);
    destination_state.bump_key_version(db, destination_key);

    if destination_should_not_exist {
        CommandReply::Integer(1)
    } else {
        CommandReply::SimpleString("OK".to_owned())
    }
}

/// Copies one key between two different shard states.
///
/// This helper exists for cross-shard `COPY` in `CoreModule`, where source and destination keys
/// hash to different owners and therefore cannot be handled by one shard-local command dispatch.
#[must_use]
pub fn copy_between_states(
    db: DbIndex,
    args: &[Vec<u8>],
    source_state: &mut DispatchState,
    destination_state: &mut DispatchState,
) -> CommandReply {
    let copy = match parse_copy_command(args) {
        Ok(copy) => copy,
        Err(reply) => return reply,
    };
    if copy.source_key == copy.destination_key {
        return CommandReply::Error("source and destination objects are the same".to_owned());
    }

    source_state.purge_expired_key(db, copy.source_key);
    destination_state.purge_expired_key(db, copy.destination_key);

    let Some(source_entry) = source_state
        .db_map(db)
        .and_then(|map| map.get(copy.source_key))
        .cloned()
    else {
        return CommandReply::Integer(0);
    };

    if !copy.replace
        && destination_state
            .db_map(db)
            .is_some_and(|map| map.contains_key(copy.destination_key))
    {
        return CommandReply::Integer(0);
    }

    let _ = destination_state.upsert_key(db, copy.destination_key, source_entry);
    destination_state.bump_key_version(db, copy.destination_key);
    CommandReply::Integer(1)
}

fn copy_single_state(db: DbIndex, args: &[Vec<u8>], state: &mut DispatchState) -> CommandReply {
    let copy = match parse_copy_command(args) {
        Ok(copy) => copy,
        Err(reply) => return reply,
    };
    if copy.source_key == copy.destination_key {
        return CommandReply::Error("source and destination objects are the same".to_owned());
    }

    state.purge_expired_key(db, copy.source_key);
    state.purge_expired_key(db, copy.destination_key);

    let Some(source_entry) = state
        .db_map(db)
        .and_then(|map| map.get(copy.source_key))
        .cloned()
    else {
        return CommandReply::Integer(0);
    };

    if !copy.replace
        && state
            .db_map(db)
            .is_some_and(|map| map.contains_key(copy.destination_key))
    {
        return CommandReply::Integer(0);
    }

    let _ = state.upsert_key(db, copy.destination_key, source_entry);
    state.bump_key_version(db, copy.destination_key);
    CommandReply::Integer(1)
}

fn rename_single_state(
    db: DbIndex,
    source_key: &[u8],
    destination_key: &[u8],
    destination_should_not_exist: bool,
    state: &mut DispatchState,
) -> CommandReply {
    state.purge_expired_key(db, source_key);
    state.purge_expired_key(db, destination_key);

    if !state
        .db_map(db)
        .is_some_and(|map| map.contains_key(source_key))
    {
        return CommandReply::Error("no such key".to_owned());
    }
    if source_key == destination_key {
        if destination_should_not_exist {
            return CommandReply::Integer(0);
        }
        return CommandReply::SimpleString("OK".to_owned());
    }
    if destination_should_not_exist
        && state
            .db_map(db)
            .is_some_and(|map| map.contains_key(destination_key))
    {
        return CommandReply::Integer(0);
    }

    let Some(source_entry) = state.remove_key(db, source_key) else {
        return CommandReply::Error("no such key".to_owned());
    };

    if !destination_should_not_exist {
        let _ = state.remove_key(db, destination_key);
    }
    let _ = state.upsert_key(db, destination_key, source_entry);
    state.bump_key_version(db, source_key);
    state.bump_key_version(db, destination_key);

    if destination_should_not_exist {
        CommandReply::Integer(1)
    } else {
        CommandReply::SimpleString("OK".to_owned())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct CopyCommand<'a> {
    source_key: &'a [u8],
    destination_key: &'a [u8],
    replace: bool,
}

fn parse_copy_command(args: &[Vec<u8>]) -> Result<CopyCommand<'_>, CommandReply> {
    let Some(source_key) = args.first().map(Vec::as_slice) else {
        return Err(CommandReply::Error(
            "wrong number of arguments for 'COPY' command".to_owned(),
        ));
    };
    let Some(destination_key) = args.get(1).map(Vec::as_slice) else {
        return Err(CommandReply::Error(
            "wrong number of arguments for 'COPY' command".to_owned(),
        ));
    };

    let replace = match args.get(2) {
        None => false,
        Some(option) if option.eq_ignore_ascii_case(b"REPLACE") => true,
        Some(_) => return Err(CommandReply::Error("syntax error".to_owned())),
    };
    if args.len() > 3 {
        return Err(CommandReply::Error("syntax error".to_owned()));
    }

    Ok(CopyCommand {
        source_key,
        destination_key,
        replace,
    })
}
