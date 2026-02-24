//! Storage-facing interfaces and bootstrap model.

pub mod db;
pub mod namespace;

/// Storage subsystem bootstrap module.
#[derive(Debug, Default)]
pub struct StorageModule;

impl StorageModule {
    /// Creates the storage module.
    #[must_use]
    pub fn new() -> Self {
        Self
    }
}
