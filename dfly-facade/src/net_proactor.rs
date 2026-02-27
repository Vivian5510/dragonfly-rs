//! Network proactor/acceptor configuration and backend selection.
//!
//! This module models Dragonfly's top-level listener/proactor wiring choices:
//! pick one multiplex API, then run one acceptor over a fixed I/O worker range.

use dfly_common::config::RuntimeConfig;
#[cfg(target_os = "linux")]
use io_uring::IoUring;

/// Multiplex backend selected for network I/O.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MultiplexApi {
    /// Linux `io_uring` completion backend.
    IoUring,
    /// Portable readiness backend (`mio` with `epoll` on Linux).
    Mio,
}

/// Resolved network multiplex decision used by startup logs and engine factory.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MultiplexSelection {
    /// Selected backend.
    pub api: MultiplexApi,
    /// Optional reason explaining why a preferred backend was not selected.
    pub fallback_reason: Option<String>,
}

impl MultiplexSelection {
    /// Returns the startup log label for the selected multiplex backend.
    #[must_use]
    pub fn api_label(&self) -> &'static str {
        match self.api {
            MultiplexApi::IoUring => "io_uring",
            MultiplexApi::Mio => "mio",
        }
    }
}

/// Listener/proactor pool dispatch configuration derived from runtime flags.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct NetworkProactorConfig {
    /// Total number of worker threads allocated for connection I/O.
    pub io_threads: usize,
    /// First worker index used by connection assignment.
    pub io_thread_start: usize,
    /// Enables peer-hash affinity lookup before round-robin fallback.
    pub use_peer_hash_affinity: bool,
    /// Enables controlled connection migration support.
    pub migrate_connections: bool,
}

impl NetworkProactorConfig {
    /// Builds listener/proactor dispatch config from runtime flags.
    #[must_use]
    pub fn from_runtime_config(config: &RuntimeConfig) -> Self {
        let io_threads = if config.conn_io_threads == 0 {
            usize::from(config.shard_count.get().max(1))
        } else {
            usize::from(config.conn_io_threads.max(1))
        };
        Self {
            io_threads,
            io_thread_start: usize::from(config.conn_io_thread_start),
            use_peer_hash_affinity: config.conn_use_peer_hash_affinity,
            migrate_connections: config.migrate_connections,
        }
    }
}

/// Selects multiplex backend using Dragonfly-style Linux-first policy.
#[must_use]
pub fn select_multiplex_backend(config: &RuntimeConfig) -> MultiplexSelection {
    if config.force_epoll {
        return MultiplexSelection {
            api: MultiplexApi::Mio,
            fallback_reason: Some("forced by config force_epoll=true (mio backend)".to_owned()),
        };
    }
    if !cfg!(target_os = "linux") {
        return MultiplexSelection {
            api: MultiplexApi::Mio,
            fallback_reason: Some(
                "io_uring is Linux-only; current platform is development-only".to_owned(),
            ),
        };
    }

    match probe_io_uring_support() {
        Ok(()) => MultiplexSelection {
            api: MultiplexApi::IoUring,
            fallback_reason: None,
        },
        Err(reason) => MultiplexSelection {
            api: MultiplexApi::Mio,
            fallback_reason: Some(reason),
        },
    }
}

#[cfg(target_os = "linux")]
fn probe_io_uring_support() -> Result<(), String> {
    let disabled_path = "/proc/sys/kernel/io_uring_disabled";
    match std::fs::read_to_string(disabled_path) {
        Ok(raw) => {
            let value = raw.trim();
            if value == "0" {
                IoUring::new(2).map(|_| ()).map_err(|error| {
                    format!("io_uring probe failed: io_uring_queue_init_params returned {error}")
                })
            } else {
                Err(format!("io_uring probe failed: {disabled_path}={value}"))
            }
        }
        Err(error) => Err(format!(
            "io_uring probe failed: cannot read {disabled_path}: {error}"
        )),
    }
}

#[cfg(not(target_os = "linux"))]
fn probe_io_uring_support() -> Result<(), String> {
    Err("io_uring is Linux-only".to_owned())
}

#[cfg(test)]
mod tests {
    use super::MultiplexApi;
    use crate::net_proactor::select_multiplex_backend;
    use dfly_common::config::RuntimeConfig;
    use googletest::prelude::*;
    use rstest::rstest;

    #[rstest]
    fn backend_fallback_selection_honors_force_epoll() {
        let selection = select_multiplex_backend(&RuntimeConfig {
            force_epoll: true,
            ..RuntimeConfig::default()
        });
        assert_that!(selection.api, eq(MultiplexApi::Mio));
        assert_eq!(
            selection.fallback_reason,
            Some("forced by config force_epoll=true (mio backend)".to_owned())
        );
    }

    #[rstest]
    fn backend_selection_default_returns_supported_backend() {
        let selection = select_multiplex_backend(&RuntimeConfig::default());
        if cfg!(target_os = "linux") {
            assert_that!(
                selection.api == MultiplexApi::IoUring || selection.api == MultiplexApi::Mio,
                eq(true)
            );
            if selection.api == MultiplexApi::IoUring {
                assert_that!(selection.fallback_reason.is_none(), eq(true));
            }
        } else {
            assert_that!(selection.api, eq(MultiplexApi::Mio));
            assert_that!(selection.fallback_reason.is_some(), eq(true));
        }
    }
}
