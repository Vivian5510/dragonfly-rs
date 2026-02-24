//! Binary snapshot codec used by storage pathways.

use dfly_common::error::{DflyError, DflyResult};
use dfly_core::{CoreSnapshot, SnapshotEntry};

/// Fixed magic marker at the beginning of every snapshot payload.
const SNAPSHOT_MAGIC: &[u8; 8] = b"DFLYSNAP";
/// Current storage snapshot format version.
const SNAPSHOT_VERSION: u16 = 1;

/// Encodes a core snapshot into storage bytes.
///
/// # Errors
///
/// Returns `DflyError::Protocol` when entry list or byte fields exceed format limits.
pub fn encode_core_snapshot(snapshot: &CoreSnapshot) -> DflyResult<Vec<u8>> {
    let mut output = Vec::new();
    output.extend_from_slice(SNAPSHOT_MAGIC);
    write_u16(&mut output, SNAPSHOT_VERSION);
    write_u32(
        &mut output,
        to_u32_len(snapshot.entries.len(), "snapshot entry count")?,
    );

    for entry in &snapshot.entries {
        write_u16(&mut output, entry.shard);
        write_u16(&mut output, entry.db);
        match entry.expire_at_unix_secs {
            Some(expire_at) => {
                write_u8(&mut output, 1);
                write_u64(&mut output, expire_at);
            }
            None => write_u8(&mut output, 0),
        }
        write_len_prefixed_bytes(&mut output, &entry.key)?;
        write_len_prefixed_bytes(&mut output, &entry.value)?;
    }

    Ok(output)
}

/// Decodes storage bytes into a core snapshot.
///
/// # Errors
///
/// Returns `DflyError::Protocol` when payload is truncated or semantically invalid.
pub fn decode_core_snapshot(payload: &[u8]) -> DflyResult<CoreSnapshot> {
    let mut cursor = SnapshotCursor::new(payload);

    let magic = cursor.read_slice(SNAPSHOT_MAGIC.len())?;
    if magic != SNAPSHOT_MAGIC {
        return Err(snapshot_error("invalid snapshot magic"));
    }

    let version = cursor.read_u16()?;
    if version != SNAPSHOT_VERSION {
        return Err(snapshot_error(format!(
            "unsupported snapshot version {version}"
        )));
    }

    let entry_count = usize::try_from(cursor.read_u32()?)
        .map_err(|_| snapshot_error("entry count exceeds platform limits"))?;
    let mut entries = Vec::with_capacity(entry_count);

    for entry_index in 0..entry_count {
        let shard = cursor.read_u16()?;
        let db = cursor.read_u16()?;
        let expire_flag = cursor.read_u8()?;
        let expire_at_unix_secs = match expire_flag {
            0 => None,
            1 => Some(cursor.read_u64()?),
            _ => {
                return Err(snapshot_error(format!(
                    "invalid expire flag {expire_flag} for entry index {entry_index}"
                )));
            }
        };

        let key_len = usize::try_from(cursor.read_u32()?)
            .map_err(|_| snapshot_error("key length exceeds platform limits"))?;
        let key = cursor.read_vec(key_len)?;

        let value_len = usize::try_from(cursor.read_u32()?)
            .map_err(|_| snapshot_error("value length exceeds platform limits"))?;
        let value = cursor.read_vec(value_len)?;

        entries.push(SnapshotEntry {
            shard,
            db,
            key,
            value,
            expire_at_unix_secs,
        });
    }

    cursor.ensure_fully_consumed()?;
    Ok(CoreSnapshot { entries })
}

fn write_len_prefixed_bytes(output: &mut Vec<u8>, payload: &[u8]) -> DflyResult<()> {
    write_u32(output, to_u32_len(payload.len(), "field length")?);
    output.extend_from_slice(payload);
    Ok(())
}

fn to_u32_len(value: usize, field_name: &str) -> DflyResult<u32> {
    u32::try_from(value).map_err(|_| snapshot_error(format!("{field_name} exceeds u32::MAX")))
}

fn write_u8(output: &mut Vec<u8>, value: u8) {
    output.push(value);
}

fn write_u16(output: &mut Vec<u8>, value: u16) {
    output.extend_from_slice(&value.to_le_bytes());
}

fn write_u32(output: &mut Vec<u8>, value: u32) {
    output.extend_from_slice(&value.to_le_bytes());
}

fn write_u64(output: &mut Vec<u8>, value: u64) {
    output.extend_from_slice(&value.to_le_bytes());
}

fn snapshot_error(message: impl Into<String>) -> DflyError {
    DflyError::Protocol(format!("snapshot payload error: {}", message.into()))
}

/// Stateful byte reader that tracks decoding position for robust error messages.
#[derive(Debug)]
struct SnapshotCursor<'a> {
    payload: &'a [u8],
    offset: usize,
}

impl<'a> SnapshotCursor<'a> {
    fn new(payload: &'a [u8]) -> Self {
        Self { payload, offset: 0 }
    }

    fn read_u8(&mut self) -> DflyResult<u8> {
        let bytes = self.read_slice(1)?;
        Ok(bytes[0])
    }

    fn read_u16(&mut self) -> DflyResult<u16> {
        let bytes = self.read_slice(2)?;
        let mut array = [0_u8; 2];
        array.copy_from_slice(bytes);
        Ok(u16::from_le_bytes(array))
    }

    fn read_u32(&mut self) -> DflyResult<u32> {
        let bytes = self.read_slice(4)?;
        let mut array = [0_u8; 4];
        array.copy_from_slice(bytes);
        Ok(u32::from_le_bytes(array))
    }

    fn read_u64(&mut self) -> DflyResult<u64> {
        let bytes = self.read_slice(8)?;
        let mut array = [0_u8; 8];
        array.copy_from_slice(bytes);
        Ok(u64::from_le_bytes(array))
    }

    fn read_vec(&mut self, len: usize) -> DflyResult<Vec<u8>> {
        Ok(self.read_slice(len)?.to_vec())
    }

    fn read_slice(&mut self, len: usize) -> DflyResult<&'a [u8]> {
        let end = self
            .offset
            .checked_add(len)
            .ok_or_else(|| snapshot_error("offset overflow while decoding"))?;
        if end > self.payload.len() {
            return Err(snapshot_error(format!(
                "unexpected end of payload at byte offset {} while reading {len} bytes",
                self.offset
            )));
        }

        let bytes = &self.payload[self.offset..end];
        self.offset = end;
        Ok(bytes)
    }

    fn ensure_fully_consumed(&self) -> DflyResult<()> {
        if self.offset == self.payload.len() {
            return Ok(());
        }
        Err(snapshot_error(format!(
            "trailing bytes after snapshot body: consumed {}, total {}",
            self.offset,
            self.payload.len()
        )))
    }
}

#[cfg(test)]
mod tests {
    use super::{SNAPSHOT_MAGIC, decode_core_snapshot, encode_core_snapshot};
    use dfly_common::error::DflyError;
    use dfly_core::{CoreSnapshot, SnapshotEntry};
    use googletest::prelude::*;
    use rstest::rstest;

    #[rstest]
    fn snapshot_codec_roundtrip_preserves_all_fields() {
        let source = CoreSnapshot {
            entries: vec![
                SnapshotEntry {
                    shard: 0,
                    db: 1,
                    key: b"user:1".to_vec(),
                    value: b"alice".to_vec(),
                    expire_at_unix_secs: None,
                },
                SnapshotEntry {
                    shard: 3,
                    db: 2,
                    key: b"cache:item".to_vec(),
                    value: b"payload".to_vec(),
                    expire_at_unix_secs: Some(1_700_000_123),
                },
            ],
        };

        let encoded = encode_core_snapshot(&source).expect("encoding should succeed");
        let decoded = decode_core_snapshot(&encoded).expect("decoding should succeed");
        assert_that!(&decoded, eq(&source));
    }

    #[rstest]
    fn snapshot_codec_rejects_truncated_payload() {
        let source = CoreSnapshot {
            entries: vec![SnapshotEntry {
                shard: 1,
                db: 0,
                key: b"k".to_vec(),
                value: b"v".to_vec(),
                expire_at_unix_secs: None,
            }],
        };
        let mut encoded = encode_core_snapshot(&source).expect("encoding should succeed");
        let _ = encoded.pop();

        let error = decode_core_snapshot(&encoded).expect_err("truncated payload must fail");
        let DflyError::Protocol(message) = error else {
            panic!("expected protocol error");
        };
        assert_that!(message.contains("unexpected end of payload"), eq(true));
    }

    #[rstest]
    fn snapshot_codec_rejects_unknown_version() {
        let source = CoreSnapshot::default();
        let mut encoded = encode_core_snapshot(&source).expect("encoding should succeed");
        encoded[SNAPSHOT_MAGIC.len()] = 9;

        let error = decode_core_snapshot(&encoded).expect_err("unknown version must fail");
        let DflyError::Protocol(message) = error else {
            panic!("expected protocol error");
        };
        assert_that!(message.contains("unsupported snapshot version"), eq(true));
    }

    #[rstest]
    fn snapshot_codec_rejects_invalid_expire_flag() {
        let source = CoreSnapshot {
            entries: vec![SnapshotEntry {
                shard: 0,
                db: 0,
                key: b"k".to_vec(),
                value: b"v".to_vec(),
                expire_at_unix_secs: None,
            }],
        };
        let mut encoded = encode_core_snapshot(&source).expect("encoding should succeed");
        let expire_flag_offset = SNAPSHOT_MAGIC.len() + 2 + 4 + 2 + 2;
        encoded[expire_flag_offset] = 7;

        let error = decode_core_snapshot(&encoded).expect_err("invalid flag must fail");
        let DflyError::Protocol(message) = error else {
            panic!("expected protocol error");
        };
        assert_that!(message.contains("invalid expire flag"), eq(true));
    }
}
