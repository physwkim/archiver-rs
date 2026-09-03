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

use archiver_core::registry::{Protocol, PvRegistry, PvStatus, SampleMode};
use archiver_core::retrieval::query::query_data;
use archiver_core::storage::partition::PartitionGranularity;
use archiver_core::storage::plainpb::PlainPbStoragePlugin;
use archiver_core::storage::traits::StoragePlugin;
use archiver_core::types::{ArchDbType, ArchiverValue};
use archiver_engine::channel_manager::{
    ChannelManager, PvCountersSnapshot, PvSample, ShardedWritePoolConfig, WriteLoopConfig,
    run_sharded_write_pool,
};
use epics_rs::base::server::records::waveform::WaveformRecord;
use epics_rs::base::types::{DbFieldType, EpicsValue};
use epics_rs::ca::client::{CaChannel, CaClient};
use epics_rs::ca::server::CaServer;

const PV: &str = "SMOKE:CA:WF";
const OTHER_PV: &str = "SMOKE:CA:OTHER";
const NELM: i32 = 3;

fn counters(mgr: &ChannelManager, pv: &str) -> PvCountersSnapshot {
    mgr.all_pv_counters()
        .into_iter()
        .find(|(n, _)| n == pv)
        .map(|(_, c)| c)
        .expect("counters for PV")
}

/// In-process CA server, archiver manager, write pool, and a writer
/// client connected to `PV` (seeded once so the record carries a real
/// timestamp: a never-processed record reports TIME = 0, which the
/// drift filter rejects).
struct Stack {
    _dir: tempfile::TempDir,
    storage: Arc<dyn StoragePlugin>,
    registry: Arc<PvRegistry>,
    mgr: Arc<ChannelManager>,
    /// The write pool's input, until `start_pool` spawns the pool.
    pool_input: Option<tokio::sync::mpsc::Receiver<PvSample>>,
    pool: Option<tokio::task::JoinHandle<()>>,
    pool_shutdown: tokio::sync::watch::Sender<bool>,
    server_task: tokio::task::JoinHandle<()>,
    _writer: CaClient,
    ch: CaChannel,
}

impl Stack {
    async fn start() -> Self {
        let mut s = Self::start_without_pool().await;
        s.start_pool();
        s
    }

    /// Everything but the write pool: the samples the producers queue
    /// stay in the channel until `start_pool`.
    async fn start_without_pool() -> Self {
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
            .record(OTHER_PV, WaveformRecord::new(1, DbFieldType::Double))
            .build()
            .await
            .expect("in-process CA server");
        let port = server.udp_port();
        let server_task = tokio::spawn(async move {
            let _ = server.run().await;
        });

        // SAFETY: nextest runs each test in its own process; nothing
        // else reads these variables concurrently, and they are set
        // before any CaClient snapshots its resolver configuration.
        unsafe {
            std::env::set_var("EPICS_CA_ADDR_LIST", format!("127.0.0.1:{port}"));
            std::env::set_var("EPICS_CA_AUTO_ADDR_LIST", "NO");
            std::env::set_var("EPICS_CA_SERVER_PORT", port.to_string());
        }

        let (mgr, rx) = ChannelManager::new(storage.clone(), registry.clone(), None)
            .await
            .unwrap();
        let mgr = Arc::new(mgr);
        let (pool_shutdown, _) = tokio::sync::watch::channel(false);

        let writer = CaClient::new().await.expect("writer client");
        let ch = writer.create_channel(PV);
        ch.wait_connected(Duration::from_secs(10))
            .await
            .expect("writer connects");
        ch.put(&EpicsValue::DoubleArray(vec![0.0; NELM as usize]))
            .await
            .expect("seed caput");

        Self {
            _dir: dir,
            storage,
            registry,
            mgr,
            pool_input: Some(rx),
            pool: None,
            pool_shutdown,
            server_task,
            _writer: writer,
            ch,
        }
    }

    fn start_pool(&mut self) {
        let rx = self.pool_input.take().expect("write pool already started");
        self.pool = Some(tokio::spawn(run_sharded_write_pool(
            self.storage.clone(),
            self.registry.clone(),
            rx,
            self.pool_shutdown.subscribe(),
            ShardedWritePoolConfig {
                shards: 1,
                per_shard_buffer: 1024,
                write_loop: WriteLoopConfig {
                    flush_period: Duration::from_millis(300),
                    ..Default::default()
                },
            },
        )));
    }

    /// Start archiving `PV` and wait for the connect-time event, which
    /// proves the monitor subscription is active. A put that lands
    /// while the subscription is still being set up can be reported by
    /// the in-process server as the new value under the previous
    /// timestamp, followed by the properly stamped event.
    async fn archive(&self) {
        self.mgr
            .archive_pv(PV, &SampleMode::Monitor, Protocol::Ca)
            .await
            .expect("archive_pv over CA");
        let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
        while counters(&self.mgr, PV).events_received == 0 {
            assert!(
                tokio::time::Instant::now() < deadline,
                "connect-time event never delivered"
            );
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    }

    async fn finish(self) {
        if let Some(pool) = self.pool {
            self.pool_shutdown.send(true).unwrap();
            tokio::time::timeout(Duration::from_secs(10), pool)
                .await
                .expect("write pool exits on shutdown")
                .unwrap();
        }
        self.server_task.abort();
    }
}

/// A CA array channel registers as the waveform type and its array
/// samples reach the disk as `VectorDouble`, with no type-change drops.
/// The one-element put sets NORD to 1, which CA delivers as a scalar
/// event; it must still be stored as a one-element waveform.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn ca_waveform_samples_land_as_waveform_double() {
    let s = Stack::start().await;

    let t0 = SystemTime::now();
    s.archive().await;
    let rec = s.registry.get_pv(PV).unwrap().expect("registry row");
    assert_eq!(rec.dbr_type, ArchDbType::WaveformDouble, "{rec:?}");
    assert_eq!(rec.element_count, NELM, "{rec:?}");
    assert_eq!(rec.protocol, Protocol::Ca);

    // Each put processes the record and stamps the value; the
    // archiver's monitor sees it as a DoubleArray event.
    let arrays: Vec<Vec<f64>> = vec![vec![1.0, 2.0, 3.0], vec![5.0], vec![7.0, 8.0, 9.0]];
    for a in &arrays {
        s.ch.put(&EpicsValue::DoubleArray(a.clone()))
            .await
            .expect("caput");
        tokio::time::sleep(Duration::from_millis(100)).await;
    }

    let from = t0 - Duration::from_secs(3600);
    let to = t0 + Duration::from_secs(3600);
    let deadline = tokio::time::Instant::now() + Duration::from_secs(20);
    let stored = loop {
        let mut stream = query_data(&*s.storage, PV, from, to, None)
            .await
            .expect("query_data");
        let mut got: Vec<Vec<f64>> = Vec::new();
        while let Some(e) = stream.next_event().expect("next_event") {
            match e.value {
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
            counters(&s.mgr, PV)
        );
        tokio::time::sleep(Duration::from_millis(200)).await;
    };
    // Stream order is timestamp order; the connect-time value may
    // precede the posted arrays.
    let posted: Vec<Vec<f64>> = stored.into_iter().filter(|g| arrays.contains(g)).collect();
    assert_eq!(posted, arrays, "on-disk sample order/content");

    let c = counters(&s.mgr, PV);
    assert_eq!(c.type_change_drops, 0, "{c:?}");
    assert_eq!(c.storage_write_errors, 0, "{c:?}");
    assert_eq!(c.timestamp_drops, 0, "{c:?}");
    assert!(c.events_stored >= arrays.len() as u64, "{c:?}");

    s.mgr.stop_pv(PV).await.unwrap();
    s.finish().await;
}

/// Every `VectorDouble` sample on disk for `PV`, in stream order.
async fn stored_arrays(s: &Stack, t0: SystemTime) -> Vec<Vec<f64>> {
    let from = t0 - Duration::from_secs(3600);
    let to = t0 + Duration::from_secs(3600);
    let mut stream = query_data(&*s.storage, PV, from, to, None)
        .await
        .expect("query_data");
    let mut got = Vec::new();
    while let Some(e) = stream.next_event().expect("next_event") {
        match e.value {
            ArchiverValue::VectorDouble(v) => got.push(v),
            other => panic!("stored value is not a waveform: {other:?}"),
        }
    }
    got
}

/// Poll the disk until `want` is present; panic with the counters
/// after the deadline.
async fn wait_on_disk(s: &Stack, t0: SystemTime, want: &[f64]) -> Vec<Vec<f64>> {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(20);
    loop {
        let got = stored_arrays(s, t0).await;
        if got.iter().any(|g| g == want) {
            return got;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "timed out waiting for {want:?} on disk; have {got:?}; counters {:?}",
            counters(&s.mgr, PV)
        );
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
}

/// Pause and resume start a new archiving task, and the connect-time
/// event redelivers the current value with the timestamp already on
/// disk. The PV's counters, and with them the ordering gate, outlive
/// the task, so the redelivery is dropped instead of stored a second
/// time.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn resume_does_not_re_store_the_current_value() {
    let s = Stack::start().await;
    let t0 = SystemTime::now();
    s.archive().await;
    let first = vec![1.0, 2.0, 3.0];
    s.ch.put(&EpicsValue::DoubleArray(first.clone()))
        .await
        .expect("caput");
    wait_on_disk(&s, t0, &first).await;

    let before = counters(&s.mgr, PV).events_received;
    s.mgr.pause_pv(PV).await.unwrap();
    s.mgr.resume_pv(PV).await.unwrap();
    // The resumed task receives the connect-time redelivery of `first`.
    let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
    while counters(&s.mgr, PV).events_received == before {
        assert!(
            tokio::time::Instant::now() < deadline,
            "resumed monitor never delivered"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }

    let second = vec![4.0, 5.0, 6.0];
    s.ch.put(&EpicsValue::DoubleArray(second.clone()))
        .await
        .expect("caput");
    let got = wait_on_disk(&s, t0, &second).await;
    let copies = got.iter().filter(|g| **g == first).count();
    assert_eq!(
        copies, 1,
        "the redelivered current value was stored again: {got:?}"
    );
    let c = counters(&s.mgr, PV);
    assert_eq!(c.timestamp_drops, 1, "{c:?}");
    assert_eq!(c.storage_write_errors, 0, "{c:?}");

    s.mgr.stop_pv(PV).await.unwrap();
    s.finish().await;
}

/// A restart runs the PV's first task of a new process, whose ordering
/// gate is seeded from the registry's committed `last_timestamp`. The
/// connect-time redelivery of the value already on disk is dropped,
/// not stored again.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn restart_does_not_re_store_the_committed_value() {
    let s = Stack::start().await;
    let t0 = SystemTime::now();
    s.archive().await;
    let first = vec![1.0, 2.0, 3.0];
    s.ch.put(&EpicsValue::DoubleArray(first.clone()))
        .await
        .expect("caput");
    wait_on_disk(&s, t0, &first).await;
    // The flush owner commits `last_timestamp` on its next flush.
    let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
    loop {
        let rec = s.registry.get_pv(PV).unwrap().expect("registry row");
        if rec.last_timestamp.is_some_and(|t| t >= t0) {
            break;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "last_timestamp never committed: {rec:?}"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }

    // Restart: the first manager's producers stop, and a second
    // manager with its own write pool restores from the same registry
    // and store.
    tokio::time::timeout(Duration::from_secs(10), s.mgr.shutdown())
        .await
        .expect("producers stop");
    let (mgr2, rx2) = ChannelManager::new(s.storage.clone(), s.registry.clone(), None)
        .await
        .unwrap();
    let (pool2_shutdown, shutdown_rx2) = tokio::sync::watch::channel(false);
    let pool2 = tokio::spawn(run_sharded_write_pool(
        s.storage.clone(),
        s.registry.clone(),
        rx2,
        shutdown_rx2,
        ShardedWritePoolConfig {
            shards: 1,
            per_shard_buffer: 1024,
            write_loop: WriteLoopConfig {
                flush_period: Duration::from_millis(300),
                ..Default::default()
            },
        },
    ));
    assert_eq!(mgr2.restore_from_registry().await.unwrap(), 1);
    let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
    while counters(&mgr2, PV).events_received == 0 {
        assert!(
            tokio::time::Instant::now() < deadline,
            "restored monitor never delivered"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }

    let second = vec![4.0, 5.0, 6.0];
    s.ch.put(&EpicsValue::DoubleArray(second.clone()))
        .await
        .expect("caput");
    let got = wait_on_disk(&s, t0, &second).await;
    let copies = got.iter().filter(|g| **g == first).count();
    assert_eq!(
        copies, 1,
        "the redelivered committed value was stored again: {got:?}"
    );
    let c = counters(&mgr2, PV);
    assert_eq!(c.timestamp_drops, 1, "{c:?}");
    assert_eq!(c.events_stored, 1, "{c:?}");
    assert_eq!(c.storage_write_errors, 0, "{c:?}");

    mgr2.stop_pv(PV).await.unwrap();
    pool2_shutdown.send(true).unwrap();
    tokio::time::timeout(Duration::from_secs(10), pool2)
        .await
        .expect("second write pool exits on shutdown")
        .unwrap();
    s.finish().await;
}

/// `ChannelManager::shutdown` stops every producer and refuses new
/// starts, so the write pool's drain that follows sees a fixed queue
/// tail. The channel keeps posting; nothing may be received after
/// shutdown returned.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn shutdown_stops_producers_and_refuses_new_starts() {
    let s = Stack::start().await;
    s.archive().await;
    s.ch.put(&EpicsValue::DoubleArray(vec![1.0, 2.0, 3.0]))
        .await
        .expect("caput");
    let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
    while counters(&s.mgr, PV).events_received < 2 {
        assert!(
            tokio::time::Instant::now() < deadline,
            "monitor never delivered the put"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }

    tokio::time::timeout(Duration::from_secs(10), s.mgr.shutdown())
        .await
        .expect("producers stop");

    let before = counters(&s.mgr, PV).events_received;
    s.ch.put(&EpicsValue::DoubleArray(vec![4.0, 5.0, 6.0]))
        .await
        .expect("caput after shutdown");
    tokio::time::sleep(Duration::from_millis(500)).await;
    assert_eq!(
        counters(&s.mgr, PV).events_received,
        before,
        "a producer received a sample after shutdown"
    );

    let err = s
        .mgr
        .archive_pv(OTHER_PV, &SampleMode::Monitor, Protocol::Ca)
        .await
        .expect_err("start after shutdown must be refused");
    assert!(err.to_string().contains("shut down"), "{err}");

    s.finish().await;
}

/// `pause_pv` returns only once every sample the PV produced has
/// reached the store: with no write pool running, the connect-time
/// sample sits in the queue and the pause must wait for it. A
/// `Paused` PV therefore has nothing in flight, and the mgmt
/// operations that require `Paused` (rename, type change, reassign,
/// delete) act on the whole data set.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn pause_waits_for_the_queued_sample_to_land() {
    let mut s = Stack::start_without_pool().await;
    let t0 = SystemTime::now();
    s.archive().await;

    let mgr = s.mgr.clone();
    let pause = tokio::spawn(async move { mgr.pause_pv(PV).await });
    tokio::time::sleep(Duration::from_millis(500)).await;
    assert!(
        !pause.is_finished(),
        "pause returned while the connect-time sample was still queued"
    );
    assert!(stored_arrays(&s, t0).await.is_empty());

    s.start_pool();
    tokio::time::timeout(Duration::from_secs(10), pause)
        .await
        .expect("pause completes once the queued sample lands")
        .unwrap()
        .expect("pause_pv");
    let rec = s.registry.get_pv(PV).unwrap().expect("registry row");
    assert_eq!(rec.status, PvStatus::Paused);

    // The paused PV's counters are no longer reported, so the
    // evidence is the disk: the ticker flush lands the seed value.
    let deadline = tokio::time::Instant::now() + Duration::from_secs(20);
    loop {
        let got = stored_arrays(&s, t0).await;
        if got == vec![vec![0.0; NELM as usize]] {
            break;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "timed out waiting for the seed value on disk; have {got:?}"
        );
        tokio::time::sleep(Duration::from_millis(200)).await;
    }

    s.finish().await;
}
