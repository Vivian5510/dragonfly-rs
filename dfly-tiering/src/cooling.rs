//! Cooling layer entry placeholders.

/// Descriptor for one cooled value entry.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CoolingEntry {
    /// Key identity.
    pub key: String,
    /// Serialized size of the cooled payload.
    pub serialized_size: usize,
}
