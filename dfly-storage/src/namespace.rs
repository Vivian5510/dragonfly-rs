//! Namespace catalog placeholders.

/// Namespace descriptor.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Namespace {
    /// Human-readable namespace name.
    pub name: String,
}

/// In-memory namespace registry.
#[derive(Debug, Default)]
pub struct NamespaceCatalog {
    namespaces: Vec<Namespace>,
}

impl NamespaceCatalog {
    /// Inserts a namespace descriptor.
    pub fn add(&mut self, namespace: Namespace) {
        self.namespaces.push(namespace);
    }

    /// Number of registered namespaces.
    #[must_use]
    pub fn len(&self) -> usize {
        self.namespaces.len()
    }

    /// Returns true if no namespaces are registered.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.namespaces.is_empty()
    }
}
