use super::DispatchState;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum SetCondition {
    Always,
    IfMissing,
    IfExists,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum SetExpire {
    Seconds(u64),
    Milliseconds(u64),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) struct SetOptions {
    pub(super) condition: SetCondition,
    pub(super) return_previous: bool,
    pub(super) keep_ttl: bool,
    pub(super) expire: Option<SetExpire>,
}

impl Default for SetOptions {
    fn default() -> Self {
        Self {
            condition: SetCondition::Always,
            return_previous: false,
            keep_ttl: false,
            expire: None,
        }
    }
}

pub(super) fn parse_set_options(args: &[Vec<u8>]) -> Result<SetOptions, String> {
    let mut options = SetOptions::default();
    let mut index = 0_usize;

    while let Some(arg) = args.get(index) {
        if arg.eq_ignore_ascii_case(b"NX") {
            if options.condition == SetCondition::IfExists {
                return Err("syntax error".to_owned());
            }
            options.condition = SetCondition::IfMissing;
            index = index.saturating_add(1);
            continue;
        }
        if arg.eq_ignore_ascii_case(b"XX") {
            if options.condition == SetCondition::IfMissing {
                return Err("syntax error".to_owned());
            }
            options.condition = SetCondition::IfExists;
            index = index.saturating_add(1);
            continue;
        }
        if arg.eq_ignore_ascii_case(b"GET") {
            options.return_previous = true;
            index = index.saturating_add(1);
            continue;
        }
        if arg.eq_ignore_ascii_case(b"KEEPTTL") {
            options.keep_ttl = true;
            index = index.saturating_add(1);
            continue;
        }
        if arg.eq_ignore_ascii_case(b"EX") || arg.eq_ignore_ascii_case(b"PX") {
            if options.expire.is_some() {
                return Err("syntax error".to_owned());
            }
            let Some(raw_expire) = args.get(index.saturating_add(1)) else {
                return Err("syntax error".to_owned());
            };
            let Ok(expire) = super::parse_redis_i64(raw_expire) else {
                return Err("value is not an integer or out of range".to_owned());
            };
            if expire <= 0 {
                return Err("invalid expire time in 'SET' command".to_owned());
            }
            let Ok(expire) = u64::try_from(expire) else {
                return Err("value is not an integer or out of range".to_owned());
            };

            options.expire = if arg.eq_ignore_ascii_case(b"EX") {
                Some(SetExpire::Seconds(expire))
            } else {
                Some(SetExpire::Milliseconds(expire))
            };
            index = index.saturating_add(2);
            continue;
        }

        return Err("syntax error".to_owned());
    }

    if options.keep_ttl && options.expire.is_some() {
        return Err("syntax error".to_owned());
    }
    Ok(options)
}

pub(super) fn set_condition_satisfied(condition: SetCondition, key_exists: bool) -> bool {
    match condition {
        SetCondition::Always => true,
        SetCondition::IfMissing => !key_exists,
        SetCondition::IfExists => key_exists,
    }
}

pub(super) fn resolve_set_expire_at_unix_secs(expire: SetExpire) -> u64 {
    match expire {
        SetExpire::Seconds(seconds) => DispatchState::now_unix_seconds().saturating_add(seconds),
        SetExpire::Milliseconds(milliseconds) => {
            DispatchState::now_unix_millis()
                .saturating_add(milliseconds)
                .saturating_add(999)
                / 1000
        }
    }
}

pub(super) type ExpireOptions = u8;

const EXPIRE_ALWAYS: ExpireOptions = 0;
const EXPIRE_NX: ExpireOptions = 1 << 0;
const EXPIRE_XX: ExpireOptions = 1 << 1;
const EXPIRE_GT: ExpireOptions = 1 << 2;
const EXPIRE_LT: ExpireOptions = 1 << 3;

pub(super) fn parse_expire_options(args: &[Vec<u8>]) -> Result<ExpireOptions, String> {
    let mut options = EXPIRE_ALWAYS;

    for arg in args {
        if arg.eq_ignore_ascii_case(b"NX") {
            options |= EXPIRE_NX;
        } else if arg.eq_ignore_ascii_case(b"XX") {
            options |= EXPIRE_XX;
        } else if arg.eq_ignore_ascii_case(b"GT") {
            options |= EXPIRE_GT;
        } else if arg.eq_ignore_ascii_case(b"LT") {
            options |= EXPIRE_LT;
        } else {
            let option = String::from_utf8_lossy(arg).to_ascii_uppercase();
            return Err(format!("Unsupported option: {option}"));
        }
    }

    if (options & EXPIRE_NX != 0) && (options & EXPIRE_XX != 0) {
        return Err("NX and XX options at the same time are not compatible".to_owned());
    }
    if (options & EXPIRE_GT != 0) && (options & EXPIRE_LT != 0) {
        return Err("GT and LT options at the same time are not compatible".to_owned());
    }

    Ok(options)
}

/// Evaluates EXPIRE-option predicates using Dragonfly's update semantics.
///
/// When no current expiry exists, comparisons treat it as an infinite timestamp.
pub(super) fn expire_options_satisfied(
    options: ExpireOptions,
    current_expire_at: Option<u64>,
    next_expire_at: u64,
) -> bool {
    if options == EXPIRE_ALWAYS {
        return true;
    }

    let mut satisfied = false;
    let current_cmp = current_expire_at.unwrap_or(u64::MAX);
    if current_expire_at.is_some() {
        satisfied |= options & EXPIRE_XX != 0;
    } else {
        satisfied |= options & EXPIRE_NX != 0;
    }
    satisfied |= options & EXPIRE_LT != 0 && next_expire_at < current_cmp;
    satisfied |= options & EXPIRE_GT != 0 && next_expire_at > current_cmp;
    satisfied
}
