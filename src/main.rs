use std::sync::Arc;
use std::time::Duration;

use anyhow::Context;
use tokio::sync::watch;
use tracing::{error, info};

use archiver_api::cluster::ClusterClient;
use archiver_api::services::impls::{ChannelArchiverControl, ClusterClientRouter, RegistryRepository};
use archiver_api::services::traits::ClusterRouter;
use archiver_api::AppState;
use archiver_bluesky::consumer::BlueskyConsumer;
use archiver_core::config::ArchiverConfig;
use archiver_core::etl::executor::EtlExecutor;
use archiver_core::registry::PvRegistry;
use archiver_core::storage::tiered::TieredStorage;
use archiver_core::storage::traits::StoragePlugin;
use archiver_engine::channel_manager::{self, ChannelManager};
use archiver_engine::policy::PolicyConfig;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "info,archiver=debug".into()),
        )
        .init();

    // Load configuration.
    let config_path = std::env::args()
        .nth(1)
        .unwrap_or_else(|| "archiver.toml".to_string());
    let config_str = tokio::fs::read_to_string(&config_path)
        .await
        .context(format!("Failed to read config file: {config_path}"))?;
    let config = ArchiverConfig::from_toml(&config_str)?;

    info!(
        listen = format!("{}:{}", config.listen_addr, config.listen_port),
        "Starting Rust EPICS Archiver"
    );

    // Open SQLite PV registry.
    let db_path = config
        .storage
        .sts
        .root_folder
        .parent()
        .unwrap_or(config.storage.sts.root_folder.as_ref())
        .join("archiver.db");
    if let Some(parent) = db_path.parent() {
        std::fs::create_dir_all(parent)?;
    }
    let registry = Arc::new(PvRegistry::open(&db_path)?);
    info!(?db_path, "PV registry opened");

    let pv_count = registry.count(None)?;
    info!(pv_count, "PVs in registry");

    // Initialize storage.
    let tiered = Arc::new(TieredStorage::from_config(&config.storage));
    let storage: Arc<dyn StoragePlugin> = tiered.clone();

    // Shutdown signal.
    let (shutdown_tx, shutdown_rx) = watch::channel(false);

    // Load policy config if specified.
    let policy = config
        .engine
        .policy_file
        .as_ref()
        .and_then(|path| match PolicyConfig::load(path) {
            Ok(p) => {
                info!(?path, policies = p.policies.len(), "Loaded PV policy config");
                Some(p)
            }
            Err(e) => {
                error!(?path, "Failed to load policy file: {e}");
                None
            }
        });

    // Initialize channel manager + write loop.
    let (channel_mgr, sample_rx) =
        ChannelManager::new(storage.clone(), registry.clone(), policy).await?;
    let channel_mgr = Arc::new(channel_mgr);

    // Restore PVs from registry.
    let restored = channel_mgr.restore_from_registry().await?;
    info!(restored, "PVs restored from registry");

    // Use configured write period for timestamp flush interval.
    let flush_period = Duration::from_secs(config.engine.write_period_secs);
    let write_storage = storage.clone();
    let write_registry = registry.clone();
    let write_shutdown = shutdown_rx.clone();
    tokio::spawn(async move {
        channel_manager::write_loop(
            write_storage,
            write_registry,
            sample_rx,
            write_shutdown,
            flush_period,
        )
        .await;
    });

    // Compute ETL periods from tier config.
    // STS→MTS: run at interval = hold * partition_granularity_seconds
    let sts_period_secs = config.storage.sts.hold as u64
        * config.storage.sts.partition_granularity.approx_seconds();
    let mts_period_secs = config.storage.mts.hold as u64
        * config.storage.mts.partition_granularity.approx_seconds();

    let etl_sts_mts = EtlExecutor::new(
        tiered.sts.clone(),
        tiered.mts.clone(),
        sts_period_secs,
        config.storage.sts.hold,
        config.storage.sts.gather,
    );
    let etl_mts_lts = EtlExecutor::new(
        tiered.mts.clone(),
        tiered.lts.clone(),
        mts_period_secs,
        config.storage.mts.hold,
        config.storage.mts.gather,
    );

    let etl1_shutdown = shutdown_rx.clone();
    tokio::spawn(async move { etl_sts_mts.run(etl1_shutdown).await });
    let etl2_shutdown = shutdown_rx.clone();
    tokio::spawn(async move { etl_mts_lts.run(etl2_shutdown).await });

    // Start Bluesky Kafka consumer if configured.
    if let Some(ref bluesky_config) = config.bluesky {
        let consumer =
            BlueskyConsumer::new(bluesky_config.clone(), storage.clone(), registry.clone());
        let bs_shutdown = shutdown_rx.clone();
        tokio::spawn(async move {
            if let Err(e) = consumer.run(bs_shutdown).await {
                error!("Bluesky consumer error: {e}");
            }
        });
        info!("Bluesky Kafka consumer enabled");
    }

    // Initialize cluster client if configured.
    let cluster: Option<Arc<dyn ClusterRouter>> = config.cluster.as_ref().map(|cc| {
        info!(
            identity = cc.identity.name,
            peers = cc.peers.len(),
            "Cluster mode enabled"
        );
        let client = Arc::new(ClusterClient::new(cc));
        Arc::new(ClusterClientRouter::new(client)) as Arc<dyn ClusterRouter>
    });

    // Install Prometheus metrics recorder.
    let metrics_handle = {
        let builder = metrics_exporter_prometheus::PrometheusBuilder::new();
        builder.install_recorder().ok()
    };

    // Build REST API.
    let app_state = AppState {
        storage: storage.clone(),
        pv_repo: Arc::new(RegistryRepository::new(registry.clone())),
        archiver: Arc::new(ChannelArchiverControl::new(channel_mgr.clone())),
        cluster,
        api_keys: config.api_keys.clone(),
        metrics_handle,
    };
    let app = archiver_api::build_router(app_state);

    let addr = format!("{}:{}", config.listen_addr, config.listen_port);
    let listener = tokio::net::TcpListener::bind(&addr).await?;
    info!(addr, "REST API listening");

    // Graceful shutdown on Ctrl+C.
    let server = axum::serve(listener, app).with_graceful_shutdown(async move {
        tokio::signal::ctrl_c().await.ok();
        info!("Shutdown signal received");
        let _ = shutdown_tx.send(true);
    });

    server.await?;
    info!("Archiver stopped");
    Ok(())
}
