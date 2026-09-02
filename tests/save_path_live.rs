//! Live save-path smoke against the epics-rs PVA stack: an in-process
//! `SharedPV` NTScalar served by an isolated PVA server →
//! `ChannelManager` PVA monitor → sharded write pool → PlainPB on disk
//! + on-disk SQLite registry → read-back through `query_data`.
//!
//! The archiver's own `PvaClient` is built from the environment, so the
//! isolated server is reached through `EPICS_PVA_NAME_SERVERS`. Each test
//! sets process env; run under nextest (one process per test).

use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use archiver_core::registry::{Protocol, PvRegistry, PvStatus, SampleMode};
use archiver_core::retrieval::query::query_data;
use archiver_core::storage::partition::PartitionGranularity;
use archiver_core::storage::plainpb::PlainPbStoragePlugin;
use archiver_core::storage::traits::StoragePlugin;
use archiver_core::types::{ArchDbType, ArchiverValue};
use archiver_engine::channel_manager::{
    ChannelManager, PvCountersSnapshot, ShardedWritePoolConfig, WriteLoopConfig,
    run_sharded_write_pool,
};
use epics_rs::pva::nt::NTScalar;
use epics_rs::pva::pvdata::{FieldDesc, PvField, ScalarType, ScalarValue};
use epics_rs::pva::server_native::{PvaServer, SharedPV, SharedSource};

const PV: &str = "SMOKE:PVA:DBL";

fn set_scalar(field: &mut PvField, path: &[&str], val: ScalarValue) {
    match path {
        [] => *field = PvField::Scalar(val),
        [head, rest @ ..] => {
            let PvField::Structure(s) = field else {
                panic!("{head}: parent is not a structure");
            };
            let child = s
                .get_field_mut(head)
                .unwrap_or_else(|| panic!("no field {head}"));
            set_scalar(child, rest, val);
        }
    }
}

fn nt_desc() -> FieldDesc {
    NTScalar::new(ScalarType::Double).with_display().build()
}

fn nt_double(value: f64, ts: SystemTime) -> PvField {
    let mut v = NTScalar::new(ScalarType::Double).with_display().create();
    set_scalar(&mut v, &["value"], ScalarValue::Double(value));
    let d = ts.duration_since(UNIX_EPOCH).unwrap();
    set_scalar(
        &mut v,
        &["timeStamp", "secondsPastEpoch"],
        ScalarValue::Long(d.as_secs() as i64),
    );
    set_scalar(
        &mut v,
        &["timeStamp", "nanoseconds"],
        ScalarValue::Int(d.subsec_nanos() as i32),
    );
    v
}

fn secs(ts: SystemTime) -> u64 {
    ts.duration_since(UNIX_EPOCH).unwrap().as_secs()
}

struct Live {
    _dir: tempfile::TempDir,
    storage: Arc<dyn StoragePlugin>,
    registry: Arc<PvRegistry>,
    mgr: Arc<ChannelManager>,
    pv: SharedPV,
    server: PvaServer,
    shutdown_tx: tokio::sync::watch::Sender<bool>,
    pool: tokio::task::JoinHandle<()>,
}

impl Live {
    async fn start(initial: PvField) -> Self {
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

        let pv = SharedPV::build_readonly();
        pv.open(nt_desc(), initial).unwrap();
        let source = SharedSource::new();
        source.add(PV, pv.clone());
        let server = PvaServer::isolated(Arc::new(source)).expect("isolated PVA server");

        // SAFETY: nextest runs each test in its own process; nothing else
        // reads these variables concurrently.
        unsafe {
            std::env::set_var("EPICS_PVA_NAME_SERVERS", server.tcp_addr().to_string());
            std::env::set_var("EPICS_PVA_AUTO_ADDR_LIST", "NO");
            std::env::set_var("EPICS_PVA_ADDR_LIST", "");
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
                shards: 2,
                per_shard_buffer: 1024,
                write_loop: WriteLoopConfig {
                    flush_period: Duration::from_millis(300),
                    ..Default::default()
                },
            },
        ));
        Self {
            _dir: dir,
            storage,
            registry,
            mgr,
            pv,
            server,
            shutdown_tx,
            pool,
        }
    }

    /// Doubles stored on disk for `PV` in `[start, end)`, in stream order.
    async fn stored(&self, start: SystemTime, end: SystemTime) -> Vec<(u64, f64)> {
        let mut stream = query_data(&*self.storage, PV, start, end, None)
            .await
            .expect("query_data");
        let mut out = Vec::new();
        while let Some(s) = stream.next_event().expect("next_event") {
            if let ArchiverValue::ScalarDouble(d) = s.value {
                out.push((secs(s.timestamp), d));
            } else {
                panic!("unexpected stored value {:?}", s.value);
            }
        }
        out
    }

    async fn wait_stored(
        &self,
        start: SystemTime,
        end: SystemTime,
        want: &[f64],
        budget: Duration,
    ) -> Vec<(u64, f64)> {
        let deadline = tokio::time::Instant::now() + budget;
        loop {
            let got = self.stored(start, end).await;
            if want.iter().all(|w| got.iter().any(|(_, v)| v == w)) {
                return got;
            }
            assert!(
                tokio::time::Instant::now() < deadline,
                "timed out waiting for {want:?} on disk; have {got:?}; counters {:?}",
                self.counters()
            );
            tokio::time::sleep(Duration::from_millis(200)).await;
        }
    }

    fn counters(&self) -> PvCountersSnapshot {
        self.mgr
            .all_pv_counters()
            .into_iter()
            .find(|(n, _)| n == PV)
            .map(|(_, c)| c)
            .expect("counters for PV")
    }

    async fn wait_connected(&self, want: bool, budget: Duration) {
        let deadline = tokio::time::Instant::now() + budget;
        loop {
            let ci = self.mgr.get_connection_info(PV).expect("conn info");
            if ci.is_connected == want {
                return;
            }
            assert!(
                tokio::time::Instant::now() < deadline,
                "timed out waiting for is_connected == {want}; state {:?}",
                ci.state
            );
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    }

    async fn shutdown(self) {
        self.mgr.stop_pv(PV).await.unwrap();
        self.shutdown_tx.send(true).unwrap();
        tokio::time::timeout(Duration::from_secs(10), self.pool)
            .await
            .expect("write pool exits on shutdown")
            .unwrap();
        self.server.stop();
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn pva_monitor_samples_land_in_plainpb_and_registry() {
    let t0 = SystemTime::now();
    let live = Live::start(nt_double(0.0, t0)).await;

    live.mgr
        .archive_pv(PV, &SampleMode::Monitor, Protocol::Pva)
        .await
        .expect("archive_pv over PVA");

    let values = [1.0, 2.0, 3.0, 4.0, 5.0];
    let mut last_ts = t0;
    for (i, v) in values.iter().enumerate() {
        last_ts = t0 + Duration::from_secs(i as u64 + 1);
        let delivered = live.pv.try_post(nt_double(*v, last_ts));
        assert!(delivered > 0 || i == 0, "post {v} reached no subscriber");
        tokio::time::sleep(Duration::from_millis(50)).await;
    }

    let got = live
        .wait_stored(
            t0 - Duration::from_secs(60),
            t0 + Duration::from_secs(60),
            &values,
            Duration::from_secs(20),
        )
        .await;
    // Stream order is timestamp order; the connect-time value 0.0 at t0
    // may or may not precede them depending on when the monitor attached.
    let doubles: Vec<f64> = got.iter().map(|(_, v)| *v).filter(|v| *v > 0.0).collect();
    assert_eq!(doubles, values, "on-disk sample order/content");
    for (ts, v) in &got {
        if *v > 0.0 {
            assert_eq!(*ts, secs(t0) + *v as u64, "IOC timestamp preserved for {v}");
        }
    }

    // Registry (on-disk SQLite) reflects the PV and its newest committed sample.
    let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
    let rec = loop {
        let rec = live.registry.get_pv(PV).unwrap().expect("registry row");
        if rec.last_timestamp.map(secs) == Some(secs(last_ts)) {
            break rec;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "registry last_timestamp never reached {}: {:?}",
            secs(last_ts),
            rec.last_timestamp.map(secs)
        );
        tokio::time::sleep(Duration::from_millis(200)).await;
    };
    assert_eq!(rec.status, PvStatus::Active);
    assert_eq!(rec.protocol, Protocol::Pva);
    assert_eq!(rec.dbr_type, ArchDbType::ScalarDouble);

    let c = live.counters();
    assert_eq!(c.transient_error_count, 0, "{c:?}");
    assert_eq!(c.type_change_drops, 0, "{c:?}");
    assert_eq!(c.timestamp_drops, 0, "{c:?}");
    assert_eq!(c.buffer_overflow_drops, 0, "{c:?}");
    assert_eq!(c.storage_append_timeouts, 0, "{c:?}");
    assert_eq!(c.disconnect_count, 0, "{c:?}");
    assert!(c.events_stored >= values.len() as u64, "{c:?}");

    live.shutdown().await;
}

/// Server-side close/reopen against an IOC whose clock runs two hours
/// behind. The monitor's `MonitorConnEvent::Finished` must flip the PV
/// to disconnected without waiting for the 60 s watchdog, the fast
/// path must re-subscribe on its own, and the first sample after the
/// reconnect — outside the 30-minute drift window like every sample
/// from this IOC — must be stored through the `first_after_connect`
/// bypass instead of dropped.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn pva_reconnect_first_sample_bypasses_drift_filter() {
    let t0 = SystemTime::now();
    let ioc_t0 = t0 - Duration::from_secs(2 * 3600);
    let live = Live::start(nt_double(0.0, ioc_t0)).await;
    live.mgr
        .archive_pv(PV, &SampleMode::Monitor, Protocol::Pva)
        .await
        .expect("archive_pv over PVA");

    let from = t0 - Duration::from_secs(3 * 3600);
    let to = t0 + Duration::from_secs(60);
    live.wait_stored(from, to, &[0.0], Duration::from_secs(20))
        .await;
    live.wait_connected(true, Duration::from_secs(5)).await;

    live.pv.close();
    live.wait_connected(false, Duration::from_secs(15)).await;
    let c = live.counters();
    assert_eq!(c.disconnect_count, 1, "{c:?}");

    let ioc_t1 = ioc_t0 + Duration::from_secs(10);
    live.pv.open(nt_desc(), nt_double(100.0, ioc_t1)).unwrap();
    live.wait_connected(true, Duration::from_secs(30)).await;

    let got = live
        .wait_stored(from, to, &[0.0, 100.0], Duration::from_secs(20))
        .await;
    assert!(
        got.iter().any(|(ts, v)| *v == 100.0 && *ts == secs(ioc_t1)),
        "reconnect sample stored with its own IOC timestamp: {got:?}"
    );
    let c = live.counters();
    assert_eq!(c.timestamp_drops, 0, "{c:?}");
    assert_eq!(c.disconnect_count, 1, "{c:?}");

    live.shutdown().await;
}

/// An IOC that connects with a far-future clock (booted before NTP
/// sync) and is corrected afterwards. The first-after-connect waiver
/// covers only the past side of the drift window; a future stamp is
/// dropped even on the first sample, because the write pool's per-PV
/// monotonic guard would otherwise reject every later, correctly
/// stamped sample until restart.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn future_first_sample_must_not_block_corrected_clock() {
    let t0 = SystemTime::now();
    let future = t0 + Duration::from_secs(2 * 3600);
    let live = Live::start(nt_double(0.0, future)).await;
    live.mgr
        .archive_pv(PV, &SampleMode::Monitor, Protocol::Pva)
        .await
        .expect("archive_pv over PVA");
    live.wait_connected(true, Duration::from_secs(5)).await;
    tokio::time::sleep(Duration::from_secs(1)).await;

    live.pv
        .try_post(nt_double(1.0, t0 + Duration::from_secs(1)));
    let from = t0 - Duration::from_secs(60);
    let to = t0 + Duration::from_secs(3 * 3600);
    let got = live
        .wait_stored(from, to, &[1.0], Duration::from_secs(15))
        .await;
    assert!(
        !got.iter().any(|(_, v)| *v == 0.0),
        "future-stamped sample must not be archived: {got:?}"
    );
    let c = live.counters();
    assert_eq!(c.timestamp_drops, 1, "{c:?}");

    live.shutdown().await;
}

/// Library contract the fast path's re-subscribe relies on: a
/// server-side `SharedPV::close()` ends the `pvmonitor_handle` stream
/// with `MonitorConnEvent::Finished` (pvxs parity: `SharedPV::close()`
/// destroys its `MonitorControlOp`s, whose destructor sends FINISH
/// before DESTROY_CHANNEL). The handle does not re-subscribe on its
/// own after `open()`; `monitor_loop_pva` does.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn library_pvmonitor_handle_finishes_on_server_close() {
    use epics_rs::pva::client_native::ops_v2::MonitorConnEvent;
    use std::sync::Mutex;

    let t0 = SystemTime::now();
    let pv = SharedPV::build_readonly();
    pv.open(nt_desc(), nt_double(0.0, t0)).unwrap();
    let source = SharedSource::new();
    source.add(PV, pv.clone());
    let server = PvaServer::isolated(Arc::new(source)).expect("isolated PVA server");
    let client = server.client_config();

    let events: Arc<Mutex<Vec<MonitorConnEvent>>> = Arc::new(Mutex::new(Vec::new()));
    let ev = events.clone();
    let handle = client
        .pvmonitor_handle(PV, |_desc, _field| {}, move |e| ev.lock().unwrap().push(e))
        .await
        .expect("pvmonitor_handle");

    let wait_for = |pred: fn(&[MonitorConnEvent]) -> bool, budget: Duration, what: &'static str| {
        let events = events.clone();
        async move {
            let deadline = tokio::time::Instant::now() + budget;
            while !pred(&events.lock().unwrap()) {
                assert!(
                    tokio::time::Instant::now() < deadline,
                    "timed out waiting for {what}; events {:?}",
                    events.lock().unwrap()
                );
                tokio::time::sleep(Duration::from_millis(100)).await;
            }
        }
    };
    wait_for(
        |e| {
            e.iter()
                .any(|x| matches!(x, MonitorConnEvent::Connected { .. }))
        },
        Duration::from_secs(10),
        "Connected",
    )
    .await;
    pv.close();
    wait_for(
        |e| e.iter().any(|x| matches!(x, MonitorConnEvent::Finished)),
        Duration::from_secs(15),
        "Finished after close",
    )
    .await;
    drop(handle);
    server.stop();
}
