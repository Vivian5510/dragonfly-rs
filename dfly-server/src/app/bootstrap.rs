use super::{AppExecutor, ServerApp};
use crate::network::{NetworkProactorPool, ServerReactorConfig};
use dfly_common::config::RuntimeConfig;
use dfly_common::error::DflyResult;
use dfly_facade::net_proactor::{NetworkProactorConfig, select_multiplex_backend};

pub(super) fn run_server() -> DflyResult<()> {
    let config = RuntimeConfig::default();
    let backend_selection = select_multiplex_backend(&config);
    let net_config = NetworkProactorConfig::from_runtime_config(&config);
    let redis_bind_addr = std::net::SocketAddr::from(([0, 0, 0, 0], config.redis_port));
    let memcache_bind_addr = config
        .memcached_port
        .map(|port| std::net::SocketAddr::from(([0, 0, 0, 0], port)));
    let app = AppExecutor::new(ServerApp::new(config.clone()));

    let worker_count = net_config
        .io_threads
        .saturating_add(net_config.io_thread_start)
        .max(1);
    let reactor_config = ServerReactorConfig {
        io_worker_count: worker_count,
        io_worker_dispatch_start: net_config.io_thread_start,
        io_worker_dispatch_count: net_config.io_threads,
        use_peer_hash_dispatch: net_config.use_peer_hash_affinity,
        enable_connection_migration: net_config.migrate_connections,
        ..ServerReactorConfig::default()
    };
    let mut pool = NetworkProactorPool::bind_with_memcache(
        redis_bind_addr,
        memcache_bind_addr,
        reactor_config,
        backend_selection,
        &app,
    )?;
    println!("{}", app.startup_summary());
    if !cfg!(target_os = "linux") {
        eprintln!(
            "linux-first runtime: non-Linux host is development-only; use Linux/WSL2/container for production parity"
        );
    }
    let selection = pool.selection();
    if let Some(reason) = selection.fallback_reason.as_deref() {
        println!(
            "network backend: multiplex_api={}, fallback_reason={reason}",
            selection.api_label()
        );
    } else {
        println!("network backend: multiplex_api={}", selection.api_label());
    }
    pool.run()
}
