//! Live save-path smoke over Channel Access: an in-process CA server
//! serving a DOUBLE waveform record → `ChannelManager` CA monitor →
//! sharded write pool → PlainPB on disk + on-disk SQLite registry →
//! read-back through `query_data`.
//!
//! The archiver's own `CaClient` is built from the environment, so the
//! server is reached through `EPICS_CA_ADDR_LIST`. Sets process env;
//! run under nextest (one process per test).

use std::sync::Arc;
use std::time::{Duration, SystemTime};

use archiver_core::registry::{Protocol, PvRegistry, SampleMode};
use archiver_core::retrieval::query::query_data;
use archiver_core::storage::partition::PartitionGranularity;
use archiver_core::storage::plainpb::PlainPbStoragePlugin;
use archiver_core::storage::traits::StoragePlugin;
use archiver_core::types::{ArchDbType, ArchiverValue};
use archiver_engine::channel_manager::{
    ChannelManager, PvCountersSnapshot, ShardedWritePoolConfig, WriteLoopConfig,
    run_sharded_write_pool,
};
use epics_rs::base::server::records::waveform::WaveformRecord;
use epics_rs::base::types::{DbFieldType, EpicsValue};
use epics_rs::ca::client::CaClient;
use epics_rs::ca::server::CaServer;

const PV: &str = "SMOKE:CA:WF";
const NELM: i32 = 3;

fn counters(mgr: &ChannelManager) -> PvCountersSnapshot {
    mgr.all_pv_counters()
        .into_iter()
        .find(|(n, _)| n == PV)
        .map(|(_, c)| c)
        .expect("counters for PV")
}

/// A CA array channel registers as the waveform type and its array
/// samples reach the disk as `VectorDouble`, with no type-change drops.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn ca_waveform_samples_land_as_waveform_double() {
    // Quiet unless RUST_LOG is set (e.g. archiver_engine=debug).
    let _ = tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .with_test_writer()
        .try_init();
    let dir = tempfile::tempdir().unwrap();
    let storage: Arc<dyn StoragePlugin> = Arc::new(PlainPbStoragePlugin::new(
        "sts",
        dir.path().join("sts"),
        PartitionGranularity::Year,
    ));
    let registry = Arc::new(PvRegistry::open(&dir.path().join("registry.db")).unwrap());

    let server = CaServer::builder()
        .port(0)
        .record(PV, WaveformRecord::new(NELM, DbFieldType::Double))
        .build()
        .await
        .expect("in-process CA server");
    let port = server.udp_port();
    let server_task = tokio::spawn(async move { server.run().await });

    // SAFETY: nextest runs each test in its own process; nothing else
    // reads these variables concurrently, and they are set before any
    // CaClient snapshots its resolver configuration.
    unsafe {
        std::env::set_var("EPICS_CA_ADDR_LIST", format!("127.0.0.1:{port}"));
        std::env::set_var("EPICS_CA_AUTO_ADDR_LIST", "NO");
        std::env::set_var("EPICS_CA_SERVER_PORT", port.to_string());
    }

    let (mgr, rx) = ChannelManager::new(storage.clone(), registry.clone(), None)
        .await
        .unwrap();
    let mgr = Arc::new(mgr);
    let (shutdown_tx, shutdown_rx) = tokio::sync::watch::channel(false);
    let pool = tokio::spawn(run_sharded_write_pool(
        storage.clone(),
        registry.clone(),
        rx,
        shutdown_rx,
        ShardedWritePoolConfig {
            shards: 1,
            per_shard_buffer: 1024,
            write_loop: WriteLoopConfig {
                flush_period: Duration::from_millis(300),
                ..Default::default()
            },
        },
    ));

    // Process the record once before archiving: a never-processed
    // record reports TIME = 0, which the drift filter rejects, so the
    // connect-time event would otherwise be a timestamp drop rather
    // than the first stored sample.
    let writer = CaClient::new().await.expect("writer client");
    let ch = writer.create_channel(PV);
    ch.wait_connected(Duration::from_secs(10))
        .await
        .expect("writer connects");
    ch.put(&EpicsValue::DoubleArray(vec![0.0; NELM as usize]))
        .await
        .expect("seed caput");

    let t0 = SystemTime::now();
    mgr.archive_pv(PV, &SampleMode::Monitor, Protocol::Ca)
        .await
        .expect("archive_pv over CA");
    let rec = registry.get_pv(PV).unwrap().expect("registry row");
    assert_eq!(rec.dbr_type, ArchDbType::WaveformDouble, "{rec:?}");
    assert_eq!(rec.element_count, NELM, "{rec:?}");
    assert_eq!(rec.protocol, Protocol::Ca);

    // Each put processes the record and stamps the value; the
    // archiver's monitor sees it as a DoubleArray event.
    let arrays: [[f64; 3]; 3] = [[1.0, 2.0, 3.0], [4.0, 5.0, 6.0], [7.0, 8.0, 9.0]];
    for a in &arrays {
        ch.put(&EpicsValue::DoubleArray(a.to_vec()))
            .await
            .expect("caput");
        tokio::time::sleep(Duration::from_millis(100)).await;
    }

    let from = t0 - Duration::from_secs(3600);
    let to = t0 + Duration::from_secs(3600);
    let deadline = tokio::time::Instant::now() + Duration::from_secs(20);
    let stored = loop {
        let mut stream = query_data(&*storage, PV, from, to, None)
            .await
            .expect("query_data");
        let mut got: Vec<Vec<f64>> = Vec::new();
        while let Some(s) = stream.next_event().expect("next_event") {
            match s.value {
                ArchiverValue::VectorDouble(v) => got.push(v),
                other => panic!("stored value is not a waveform: {other:?}"),
            }
        }
        if arrays.iter().all(|a| got.iter().any(|g| g == a)) {
            break got;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "timed out waiting for {arrays:?} on disk; have {got:?}; counters {:?}",
            counters(&mgr)
        );
        tokio::time::sleep(Duration::from_millis(200)).await;
    };
    // Stream order is timestamp order; the connect-time value may
    // precede the posted arrays.
    let posted: Vec<Vec<f64>> = stored
        .into_iter()
        .filter(|g| arrays.iter().any(|a| a == g.as_slice()))
        .collect();
    let want: Vec<Vec<f64>> = arrays.iter().map(|a| a.to_vec()).collect();
    assert_eq!(posted, want, "on-disk sample order/content");

    let c = counters(&mgr);
    assert_eq!(c.type_change_drops, 0, "{c:?}");
    assert_eq!(c.storage_write_errors, 0, "{c:?}");
    assert_eq!(c.timestamp_drops, 0, "{c:?}");
    assert!(c.events_stored >= arrays.len() as u64, "{c:?}");

    mgr.stop_pv(PV).await.unwrap();
    shutdown_tx.send(true).unwrap();
    tokio::time::timeout(Duration::from_secs(10), pool)
        .await
        .expect("write pool exits on shutdown")
        .unwrap();
    server_task.abort();
}
