use std::str;

pub(super) fn parse_redis_i64(payload: &[u8]) -> Result<i64, ()> {
    let Ok(text) = str::from_utf8(payload) else {
        return Err(());
    };
    text.parse::<i64>().map_err(|_| ())
}

pub(super) fn parse_redis_u64(payload: &[u8]) -> Result<u64, ()> {
    let Ok(text) = str::from_utf8(payload) else {
        return Err(());
    };
    text.parse::<u64>().map_err(|_| ())
}

pub(super) fn normalize_redis_range(start: i64, end: i64, len: usize) -> Option<(usize, usize)> {
    if len == 0 {
        return None;
    }

    let len_i64 = i64::try_from(len).unwrap_or(i64::MAX);
    let mut start = if start < 0 {
        len_i64.saturating_add(start)
    } else {
        start
    };
    let mut end = if end < 0 {
        len_i64.saturating_add(end)
    } else {
        end
    };

    if start < 0 {
        start = 0;
    }
    if end < 0 {
        return None;
    }
    if start >= len_i64 {
        return None;
    }
    if end >= len_i64 {
        end = len_i64.saturating_sub(1);
    }
    if start > end {
        return None;
    }

    let Ok(start_index) = usize::try_from(start) else {
        return None;
    };
    let Ok(end_index) = usize::try_from(end) else {
        return None;
    };
    Some((start_index, end_index))
}
