//! Process composition root for `dfly-server`.

use dfly_cluster::ClusterModule;
use dfly_common::config::RuntimeConfig;
use dfly_common::error::DflyResult;
use dfly_core::CoreModule;
use dfly_facade::FacadeModule;
use dfly_replication::ReplicationModule;
use dfly_search::SearchModule;
use dfly_storage::StorageModule;
use dfly_tiering::TieringModule;
use dfly_transaction::TransactionModule;

/// Unit 0 composition container.
///
/// The fields intentionally mirror Dragonfly's major subsystem boundaries so later units can
/// replace placeholders with real implementations without changing the overall process topology.
#[derive(Debug)]
pub struct ServerApp {
    /// Runtime configuration.
    pub config: RuntimeConfig,
    /// Connection/protocol entry layer.
    pub facade: FacadeModule,
    /// Core routing and command model layer.
    pub core: CoreModule,
    /// Transaction planner/scheduler layer.
    pub transaction: TransactionModule,
    /// Storage layer.
    pub storage: StorageModule,
    /// Replication layer.
    pub replication: ReplicationModule,
    /// Cluster orchestration layer.
    pub cluster: ClusterModule,
    /// Search subsystem.
    pub search: SearchModule,
    /// Tiered-storage subsystem.
    pub tiering: TieringModule,
}

impl ServerApp {
    /// Creates a process composition from runtime config.
    #[must_use]
    pub fn new(config: RuntimeConfig) -> Self {
        let facade = FacadeModule::from_config(&config);
        let core = CoreModule::new(config.shard_count);
        let transaction = TransactionModule::new();
        let storage = StorageModule::new();
        let replication = ReplicationModule::new(false);
        let cluster = ClusterModule::new(config.cluster_mode);
        let search = SearchModule::new(true);
        let tiering = TieringModule::new(true);

        Self {
            config,
            facade,
            core,
            transaction,
            storage,
            replication,
            cluster,
            search,
            tiering,
        }
    }

    /// Human-readable startup summary.
    #[must_use]
    pub fn startup_summary(&self) -> String {
        format!(
            "dfly-server bootstrap: shard_count={}, redis_port={}, memcached_port={:?}, \
core_mod={:?}, tx_mod={:?}, storage_mod={:?}, repl_enabled={}, cluster_mode={:?}, search_enabled={}, tiering_enabled={}",
            self.config.shard_count.get(),
            self.facade.redis_port,
            self.facade.memcached_port,
            self.core,
            self.transaction,
            self.storage,
            self.replication.enabled,
            self.cluster.mode,
            self.search.enabled,
            self.tiering.enabled
        )
    }
}

/// Starts `dfly-server` process bootstrap.
pub fn run() -> DflyResult<()> {
    let app = ServerApp::new(RuntimeConfig::default());
    println!("{}", app.startup_summary());
    Ok(())
}
