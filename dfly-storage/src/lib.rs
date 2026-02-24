//! Storage-facing interfaces and bootstrap model.

pub mod db;
pub mod namespace;
pub mod snapshot;

use std::fs;
use std::path::Path;

use dfly_common::error::DflyError;
use dfly_common::error::DflyResult;
use dfly_core::CoreSnapshot;

/// Storage subsystem bootstrap module.
#[derive(Debug, Default)]
pub struct StorageModule;

impl StorageModule {
    /// Creates the storage module.
    #[must_use]
    pub fn new() -> Self {
        Self
    }

    /// Encodes an in-memory core snapshot into storage payload bytes.
    ///
    /// # Errors
    ///
    /// Returns `DflyError::Protocol` when snapshot field sizes exceed format limits.
    pub fn serialize_snapshot(&self, snapshot: &CoreSnapshot) -> DflyResult<Vec<u8>> {
        snapshot::encode_core_snapshot(snapshot)
    }

    /// Decodes storage payload bytes into core snapshot representation.
    ///
    /// # Errors
    ///
    /// Returns `DflyError::Protocol` when payload is malformed.
    pub fn deserialize_snapshot(&self, payload: &[u8]) -> DflyResult<CoreSnapshot> {
        snapshot::decode_core_snapshot(payload)
    }

    /// Persists one core snapshot to a file path.
    ///
    /// # Errors
    ///
    /// Returns `DflyError::Protocol` for encoding failures and `DflyError::Io` for filesystem
    /// failures.
    pub fn write_snapshot_file<P: AsRef<Path>>(
        &self,
        path: P,
        snapshot: &CoreSnapshot,
    ) -> DflyResult<()> {
        let payload = self.serialize_snapshot(snapshot)?;
        fs::write(path, payload).map_err(|error| DflyError::Io(error.to_string()))
    }

    /// Loads one core snapshot from file path.
    ///
    /// # Errors
    ///
    /// Returns `DflyError::Io` for filesystem read failures and `DflyError::Protocol` for decode
    /// failures.
    pub fn read_snapshot_file<P: AsRef<Path>>(&self, path: P) -> DflyResult<CoreSnapshot> {
        let payload = fs::read(path).map_err(|error| DflyError::Io(error.to_string()))?;
        self.deserialize_snapshot(&payload)
    }
}
