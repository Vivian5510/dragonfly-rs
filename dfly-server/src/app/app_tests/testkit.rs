use std::path::PathBuf;
use std::time::{SystemTime, UNIX_EPOCH};

pub(super) fn resp_command(parts: &[&[u8]]) -> Vec<u8> {
    let mut payload = format!("*{}\r\n", parts.len()).into_bytes();
    for part in parts {
        payload.extend_from_slice(format!("${}\r\n", part.len()).as_bytes());
        payload.extend_from_slice(part);
        payload.extend_from_slice(b"\r\n");
    }
    payload
}

pub(super) fn decode_resp_bulk_payload(frame: &[u8]) -> String {
    assert_eq!(frame.first(), Some(&b'$'));

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

pub(super) fn parse_resp_integer(frame: &[u8]) -> i64 {
    assert_eq!(frame.first(), Some(&b':'));
    assert!(frame.ends_with(b"\r\n"));

    let number = std::str::from_utf8(&frame[1..frame.len() - 2])
        .expect("RESP integer payload must be UTF-8");
    number
        .parse::<i64>()
        .expect("RESP integer payload must parse")
}

pub(super) fn extract_sync_id_from_capa_reply(frame: &[u8]) -> String {
    let text = std::str::from_utf8(frame).expect("payload must be UTF-8");
    let lines = text.split("\r\n").collect::<Vec<_>>();
    let Some(sync_id) = lines.get(4) else {
        panic!("REPLCONF CAPA reply must contain sync id as second bulk string");
    };
    (*sync_id).to_owned()
}

pub(super) fn extract_dfly_flow_reply(frame: &[u8]) -> (String, String) {
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

pub(super) fn unique_test_snapshot_path(tag: &str) -> PathBuf {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0_u128, |duration| duration.as_nanos());
    std::env::temp_dir().join(format!("dragonfly-rs-{tag}-{nanos}.snapshot"))
}
