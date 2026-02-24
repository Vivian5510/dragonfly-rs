//! Slot migration interface placeholders.

use crate::slot::SlotRange;

/// Migration plan for moving one or more slot ranges.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MigrationPlan {
    /// Source node id.
    pub from_node: String,
    /// Destination node id.
    pub to_node: String,
    /// Ranges to migrate.
    pub ranges: Vec<SlotRange>,
}
