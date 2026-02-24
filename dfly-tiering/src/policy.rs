//! Tiering policy placeholders.

/// Policy thresholds for in-memory to disk offload.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct TieringPolicy {
    /// Start offloading when free-memory ratio drops below this value.
    pub offload_threshold: f32,
    /// Stop uploads while below this free-memory ratio.
    pub upload_threshold: f32,
}
