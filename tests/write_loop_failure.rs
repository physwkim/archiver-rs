//! Failure-injection tests for the engine `write_loop`.
//!
//! These exercise the storage-side failure modes the production
//! PlainPB plugin can produce — flush returning per-PV failures,
//! flush hanging, append hanging — and assert that `write_loop`'s
//! response (filtering ts_updates, deferring registry commits,
//! continuing past timeouts) matches the contract documented in
//! `WriteLoopConfig`. Uses an in-process `InjectingStorage` so we
//! can inject hangs and failures without touching any real
//! filesystem.

use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, SystemTime};

use archiver_core::registry::{PvRegistry, SampleMode};
use archiver_core::storage::partition::PartitionGranularity;
use archiver_core::storage::traits::{AppendMeta, EventStream, StoragePlugin, StoreSummary};
use archiver_core::types::{ArchDbType, ArchiverSample, ArchiverValue};
use archiver_engine::channel_manager::{
    PvCounters, PvSample, ShardedWritePoolConfig, WriteLoopConfig, run_sharded_write_pool,
    write_loop_with_config,
};
use tokio::sync::{mpsc, watch};

#[derive(Debug, Clone)]
struct AppendRecord {
    pv: String,
    timestamp: SystemTime,
}

/// In-memory `StoragePlugin` with knobs for flush failure, flush
/// hang, and per-PV append hang. Records every successful append
/// (with completion time) so tests can assert ordering even across
/// timed-out-but-late-success cases.
struct InjectingStorage {
    /// PV-name → tokio::sync::Mutex serialising appends for that PV
    /// (the same role PlainPB's `PvWriterSlot` plays in production).
    /// `tokio::sync::Mutex` so the lock can be held across the
    /// `tokio::time::sleep` we use to simulate slow I/O.
    pv_locks: Mutex<HashMap<String, Arc<tokio::sync::Mutex<()>>>>,
    /// PVs whose `flush_ingest_writes` should report as failed.
    flush_failed_pvs: Mutex<Vec<String>>,
    /// When set, `flush_ingest_writes` blocks for `flush_hang_for`
    /// before returning Ok([]).
    flush_hang: AtomicBool,
    flush_hang_for: Mutex<Duration>,
    /// PVs whose `append_event_with_meta` should sleep for
    /// `append_hang_for` before recording / returning. A test that
    /// wants a "hang past write_loop's append_timeout" effect sets
    /// `append_hang_for` larger than the configured `append_timeout`.
    append_hang_pvs: Mutex<HashMap<String, Duration>>,
    /// Successful append log (in completion order).
    appends: Mutex<Vec<AppendRecord>>,
    flush_calls: AtomicUsize,
    flush_ingest_calls: AtomicUsize,
}

impl InjectingStorage {
    fn new() -> Arc<Self> {
        Arc::new(Self {
            pv_locks: Mutex::new(HashMap::new()),
            flush_failed_pvs: Mutex::new(Vec::new()),
            flush_hang: AtomicBool::new(false),
            flush_hang_for: Mutex::new(Duration::from_secs(0)),
            append_hang_pvs: Mutex::new(HashMap::new()),
            appends: Mutex::new(Vec::new()),
            flush_calls: AtomicUsize::new(0),
            flush_ingest_calls: AtomicUsize::new(0),
        })
    }

    fn set_flush_failed(&self, pvs: Vec<String>) {
        *self.flush_failed_pvs.lock().unwrap() = pvs;
    }

    fn set_flush_hang(&self, hang_for: Duration) {
        *self.flush_hang_for.lock().unwrap() = hang_for;
        self.flush_hang.store(true, Ordering::SeqCst);
    }

    fn clear_flush_hang(&self) {
        self.flush_hang.store(false, Ordering::SeqCst);
    }

    fn set_append_hang(&self, pv: &str, hang_for: Duration) {
        self.append_hang_pvs
            .lock()
            .unwrap()
            .insert(pv.to_string(), hang_for);
    }

    fn clear_append_hang(&self, pv: &str) {
        self.append_hang_pvs.lock().unwrap().remove(pv);
    }

    fn appends_snapshot(&self) -> Vec<AppendRecord> {
        self.appends.lock().unwrap().clone()
    }

    fn flush_ingest_call_count(&self) -> usize {
        self.flush_ingest_calls.load(Ordering::SeqCst)
    }

    fn pv_lock(&self, pv: &str) -> Arc<tokio::sync::Mutex<()>> {
        let mut locks = self.pv_locks.lock().unwrap();
        locks
            .entry(pv.to_string())
            .or_insert_with(|| Arc::new(tokio::sync::Mutex::new(())))
            .clone()
    }
}

#[async_trait::async_trait]
impl StoragePlugin for InjectingStorage {
    fn name(&self) -> &str {
        "inject"
    }

    fn partition_granularity(&self) -> PartitionGranularity {
        PartitionGranularity::Hour
    }

    async fn append_event(
        &self,
        pv: &str,
        dbr_type: ArchDbType,
        sample: &ArchiverSample,
    ) -> anyhow::Result<()> {
        self.append_event_with_meta(pv, dbr_type, sample, &AppendMeta::default())
            .await
    }

    async fn append_event_with_meta(
        &self,
        pv: &str,
        _dbr_type: ArchDbType,
        sample: &ArchiverSample,
        _meta: &AppendMeta,
    ) -> anyhow::Result<()> {
        // Per-PV serialization mimicking PlainPB's `PvWriterSlot`
        // mutex. Without this, a timed-out-then-late append could
        // race with the next sample's append for the same PV and
        // give the test a false negative on ordering.
        let pv_lock = self.pv_lock(pv);
        let _g = pv_lock.lock().await;

        let hang = self
            .append_hang_pvs
            .lock()
            .unwrap()
            .get(pv)
            .copied()
            .unwrap_or_default();
        if !hang.is_zero() {
            tokio::time::sleep(hang).await;
        }

        self.appends.lock().unwrap().push(AppendRecord {
            pv: pv.to_string(),
            timestamp: sample.timestamp,
        });
        Ok(())
    }

    async fn get_data(
        &self,
        _pv: &str,
        _start: SystemTime,
        _end: SystemTime,
    ) -> anyhow::Result<Vec<Box<dyn EventStream>>> {
        Ok(Vec::new())
    }

    async fn get_last_known_event(&self, _pv: &str) -> anyhow::Result<Option<ArchiverSample>> {
        Ok(None)
    }

    async fn flush_writes(&self) -> anyhow::Result<()> {
        self.flush_calls.fetch_add(1, Ordering::SeqCst);
        Ok(())
    }

    async fn flush_ingest_writes(&self) -> anyhow::Result<Vec<String>> {
        self.flush_ingest_calls.fetch_add(1, Ordering::SeqCst);
        if self.flush_hang.load(Ordering::SeqCst) {
            let hang_for = *self.flush_hang_for.lock().unwrap();
            tokio::time::sleep(hang_for).await;
        }
        let failed = self.flush_failed_pvs.lock().unwrap().clone();
        Ok(failed)
    }

    fn stores_for_pv(&self, _pv: &str) -> anyhow::Result<Vec<StoreSummary>> {
        Ok(Vec::new())
    }

    fn appliance_metrics(&self) -> anyhow::Result<Vec<StoreSummary>> {
        Ok(Vec::new())
    }
}

fn sample_at(ts: SystemTime, value: f64) -> ArchiverSample {
    ArchiverSample::new(ts, ArchiverValue::ScalarDouble(value))
}

fn pv_sample(pv: &str, ts: SystemTime, value: f64, counters: &Arc<PvCounters>) -> PvSample {
    PvSample {
        pv_name: pv.to_string(),
        dbr_type: ArchDbType::ScalarDouble,
        sample: sample_at(ts, value),
        element_count: Some(1),
        counters: Some(counters.clone()),
    }
}

fn ts(secs_since_2020: u64) -> SystemTime {
    // Comfortably past the 1991 archiver cutoff and within any
    // reasonable IOC drift window — using a fixed base keeps test
    // assertions stable regardless of when they run.
    SystemTime::UNIX_EPOCH + Duration::from_secs(1_577_836_800 + secs_since_2020)
}

fn fast_cfg() -> WriteLoopConfig {
    WriteLoopConfig {
        flush_period: Duration::from_millis(100),
        append_timeout: Duration::from_millis(300),
        flush_timeout: Duration::from_millis(300),
        drain_per_sample_timeout: Duration::from_millis(300),
        drain_total_budget: Duration::from_secs(2),
        shutdown_flush_timeout: Duration::from_millis(500),
    }
}

/// Spin up a write_loop on the current tokio runtime and return
/// (sample_tx, shutdown_tx, JoinHandle) so tests drive it directly.
fn spawn_loop(
    storage: Arc<dyn StoragePlugin>,
    registry: Arc<PvRegistry>,
    cfg: WriteLoopConfig,
) -> (
    mpsc::Sender<PvSample>,
    watch::Sender<bool>,
    tokio::task::JoinHandle<()>,
) {
    let (tx, rx) = mpsc::channel::<PvSample>(64);
    let (sd_tx, sd_rx) = watch::channel(false);
    let join = tokio::spawn(write_loop_with_config(storage, registry, rx, sd_rx, cfg));
    (tx, sd_tx, join)
}

async fn shutdown(sd_tx: watch::Sender<bool>, join: tokio::task::JoinHandle<()>) {
    let _ = sd_tx.send(true);
    let _ = tokio::time::timeout(Duration::from_secs(5), join).await;
}

/// PV "B" reports a flush failure → write_loop must commit registry
/// timestamps for A and C but NOT for B (its bytes never reached
/// disk, so the registry's `last_event` would lie if it advanced).
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn flush_failed_pv_drops_only_that_pv_from_ts_updates() {
    let storage = InjectingStorage::new();
    storage.set_flush_failed(vec!["B".to_string()]);

    let registry = Arc::new(PvRegistry::in_memory().unwrap());
    for pv in ["A", "B", "C"] {
        registry
            .register_pv(pv, ArchDbType::ScalarDouble, &SampleMode::Monitor, 1)
            .unwrap();
    }

    // Register a trigger PV ("T") that's guaranteed not to be in
    // the failed list — used purely to wake the sample-recv branch
    // so it sees `flush_period` elapsed and runs the flush.
    registry
        .register_pv("T", ArchDbType::ScalarDouble, &SampleMode::Monitor, 1)
        .unwrap();

    let counters = Arc::new(PvCounters::default());
    let cfg = fast_cfg();
    let (tx, sd_tx, join) = spawn_loop(storage.clone(), registry.clone(), cfg.clone());

    let t_a = ts(10);
    let t_b = ts(20);
    let t_c = ts(30);
    tx.send(pv_sample("A", t_a, 1.0, &counters)).await.unwrap();
    tx.send(pv_sample("B", t_b, 2.0, &counters)).await.unwrap();
    tx.send(pv_sample("C", t_c, 3.0, &counters)).await.unwrap();

    // Wait past flush_period, then send a trigger sample on a
    // separate PV so the sample-recv branch fires the flush check
    // without disturbing A/B/C's expected timestamps.
    tokio::time::sleep(cfg.flush_period + Duration::from_millis(50)).await;
    tx.send(pv_sample("T", ts(40), 9.0, &counters))
        .await
        .unwrap();
    tokio::time::sleep(Duration::from_millis(200)).await;

    let a_rec = registry.get_pv("A").unwrap().unwrap();
    let b_rec = registry.get_pv("B").unwrap().unwrap();
    let c_rec = registry.get_pv("C").unwrap().unwrap();

    assert_eq!(
        a_rec.last_timestamp,
        Some(t_a),
        "PV A should have its timestamp committed"
    );
    assert_eq!(
        c_rec.last_timestamp,
        Some(t_c),
        "PV C should have its timestamp committed"
    );
    assert_eq!(
        b_rec.last_timestamp, None,
        "PV B was reported as flush-failed; its timestamp must NOT be committed"
    );

    shutdown(sd_tx, join).await;
}

/// Flush hangs past `flush_timeout` → write_loop must NOT block on
/// it; subsequent appends still flow, and once the hang is released
/// the registry catches up on the next flush cycle.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn flush_hang_does_not_stall_write_loop() {
    let cfg = fast_cfg();
    let storage = InjectingStorage::new();
    // Hang long enough to outlast flush_timeout but not so long
    // that an abandoned tokio::sleep is still parked when the
    // test runtime tears down (would panic with "context was
    // found, but it is being shutdown").
    storage.set_flush_hang(cfg.flush_timeout + Duration::from_millis(200));

    let registry = Arc::new(PvRegistry::in_memory().unwrap());
    registry
        .register_pv("A", ArchDbType::ScalarDouble, &SampleMode::Monitor, 1)
        .unwrap();

    let counters = Arc::new(PvCounters::default());
    let (tx, sd_tx, join) = spawn_loop(storage.clone(), registry.clone(), cfg.clone());

    let t_first = ts(100);
    tx.send(pv_sample("A", t_first, 1.0, &counters))
        .await
        .unwrap();

    // Trigger the flush by sending a second sample after
    // flush_period elapses. write_loop will attempt the flush
    // (call_count++ inside the fake's async body), see it hang
    // past flush_timeout, log, and continue.
    tokio::time::sleep(cfg.flush_period + Duration::from_millis(50)).await;
    let t_second = ts(200);
    tx.send(pv_sample("A", t_second, 2.0, &counters))
        .await
        .unwrap();

    // Poll until the flush attempt is registered (call_count++
    // happens at the start of the fake's async body, before the
    // sleep). Bounded so a regression fails fast.
    let deadline = std::time::Instant::now() + Duration::from_secs(2);
    while storage.flush_ingest_call_count() == 0 {
        if std::time::Instant::now() > deadline {
            panic!("write_loop never attempted a flush");
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }

    // Subsequent appends still land — the hung flush task is parked
    // but write_loop's main path is alive. Wait for the timeout to
    // fire so write_loop is back on the recv branch.
    tokio::time::sleep(cfg.flush_timeout + Duration::from_millis(100)).await;
    let t_third = ts(300);
    tx.send(pv_sample("A", t_third, 3.0, &counters))
        .await
        .unwrap();
    // Poll for t_third's append rather than fixed-sleep so the
    // assertion isn't a race against scheduler jitter.
    let deadline = std::time::Instant::now() + Duration::from_secs(2);
    loop {
        let snap = storage.appends_snapshot();
        if snap.iter().any(|r| r.timestamp == t_third) {
            break;
        }
        if std::time::Instant::now() > deadline {
            panic!(
                "third sample never appended; only got: {:?}",
                snap.iter().map(|r| r.timestamp).collect::<Vec<_>>()
            );
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    let appends = storage.appends_snapshot();
    assert!(
        appends.iter().any(|r| r.timestamp == t_first),
        "first sample should have been appended"
    );
    assert!(
        appends.iter().any(|r| r.timestamp == t_second),
        "second sample should have been appended even while flush is hung"
    );

    // Release the hang. Repeatedly trigger flush attempts (one
    // sample per flush_period) until the registry's `last_event`
    // for PV A advances out of None — proves write_loop both
    // recovered from the hang AND committed pending ts_updates.
    storage.clear_flush_hang();
    let mut latest = 400u64;
    let deadline = std::time::Instant::now() + Duration::from_secs(5);
    loop {
        if let Some(rec) = registry.get_pv("A").unwrap()
            && rec.last_timestamp.is_some()
        {
            break;
        }
        if std::time::Instant::now() > deadline {
            panic!(
                "registry never received a commit after flush hang cleared \
                 (sent {} kicker samples)",
                latest - 400
            );
        }
        latest += 1;
        tx.send(pv_sample("A", ts(latest), latest as f64, &counters))
            .await
            .unwrap();
        tokio::time::sleep(cfg.flush_period + Duration::from_millis(50)).await;
    }

    shutdown(sd_tx, join).await;
}

/// An append that exceeds `append_timeout` is logged as abandoned,
/// but the spawn_blocking task still completes later. With per-PV
/// serialization (mirrored in this test fake by `pv_locks`), the
/// late-success bytes land BEFORE any subsequent same-PV sample's
/// bytes — so on-disk order stays monotonic and the
/// `events_stored` counter still ticks for the late completion.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn append_timeout_late_success_preserves_order_and_counter() {
    let storage = InjectingStorage::new();
    // First append for A hangs longer than append_timeout. The
    // second sample for A will block in the fake's per-PV lock
    // until the first eventually completes — same shape as
    // PlainPB's PvWriterSlot serialization.
    let cfg = fast_cfg();
    let hang_for = cfg.append_timeout + Duration::from_millis(500);
    storage.set_append_hang("A", hang_for);

    let registry = Arc::new(PvRegistry::in_memory().unwrap());
    registry
        .register_pv("A", ArchDbType::ScalarDouble, &SampleMode::Monitor, 1)
        .unwrap();

    let counters = Arc::new(PvCounters::default());
    let (tx, sd_tx, join) = spawn_loop(storage.clone(), registry.clone(), cfg.clone());

    let t1 = ts(1000);
    let t2 = ts(2000);
    tx.send(pv_sample("A", t1, 1.0, &counters)).await.unwrap();
    // Send the second sample while the first one is still hung —
    // write_loop will spawn_blocking it, the task waits on the
    // per-PV lock, and the timeout fires for that one too if it
    // can't lock in time. Once the first hang clears, both land
    // in order.
    tokio::time::sleep(Duration::from_millis(50)).await;
    tx.send(pv_sample("A", t2, 2.0, &counters)).await.unwrap();

    // Wait for both appends to reach completion. Bound the wait so
    // a regression doesn't hang the test forever.
    let deadline = std::time::Instant::now() + Duration::from_secs(5);
    while std::time::Instant::now() < deadline {
        if storage
            .appends_snapshot()
            .iter()
            .filter(|r| r.pv == "A")
            .count()
            == 2
        {
            break;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    // Drop the hang so the second append can proceed if it was
    // also caught in the lock-wait timeout chain.
    storage.clear_append_hang("A");
    tokio::time::sleep(Duration::from_millis(800)).await;

    let appends: Vec<_> = storage
        .appends_snapshot()
        .into_iter()
        .filter(|r| r.pv == "A")
        .collect();
    assert!(
        appends.len() >= 2,
        "both samples for PV A should eventually complete (got {} append(s))",
        appends.len()
    );
    // Ordering check: t1's append must complete BEFORE t2's, since
    // they go through the same per-PV lock.
    let pos_t1 = appends.iter().position(|r| r.timestamp == t1).unwrap();
    let pos_t2 = appends.iter().position(|r| r.timestamp == t2).unwrap();
    assert!(
        pos_t1 < pos_t2,
        "late-completing first sample must still land before second \
         sample (got positions t1={pos_t1} t2={pos_t2})"
    );

    // Counter consistency: events_stored should be bumped for BOTH
    // appends (the first by the late-success path inside the
    // spawn_blocking task, the second by either path depending on
    // whether write_loop saw it via timeout or fast success).
    // storage_append_timeouts must be at least 1 (the first hung
    // past write_loop's view).
    assert!(
        counters.events_stored.load(Ordering::Relaxed) >= 2,
        "events_stored should count both samples (got {})",
        counters.events_stored.load(Ordering::Relaxed)
    );
    assert!(
        counters.storage_append_timeouts.load(Ordering::Relaxed) >= 1,
        "at least one append should have been observed as timed-out (got {})",
        counters.storage_append_timeouts.load(Ordering::Relaxed)
    );

    shutdown(sd_tx, join).await;
}

/// Sharded pool: many PVs across shards = 4 should ALL get their
/// samples appended and timestamps committed. Verifies that the
/// dispatcher routes correctly (no PV's samples get lost) and that
/// per-PV ordering survives the hash routing (samples for a given
/// PV always land in the same shard).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn sharded_pool_routes_all_pvs_correctly() {
    let storage = InjectingStorage::new();
    let registry = Arc::new(PvRegistry::in_memory().unwrap());

    const N_PVS: usize = 32;
    const SAMPLES_PER_PV: usize = 5;
    let pv_names: Vec<String> = (0..N_PVS).map(|i| format!("pv{i}")).collect();
    for name in &pv_names {
        registry
            .register_pv(name, ArchDbType::ScalarDouble, &SampleMode::Monitor, 1)
            .unwrap();
    }

    let cfg = ShardedWritePoolConfig {
        shards: 4,
        per_shard_buffer: 64,
        write_loop: fast_cfg(),
    };

    let (tx, rx) = mpsc::channel::<PvSample>(256);
    let (sd_tx, sd_rx) = watch::channel(false);
    let storage_for_pool = storage.clone();
    let registry_for_pool = registry.clone();
    let join = tokio::spawn(async move {
        run_sharded_write_pool(storage_for_pool, registry_for_pool, rx, sd_rx, cfg).await
    });

    let counters = Arc::new(PvCounters::default());
    // Send 5 samples per PV with strictly increasing timestamps.
    for s in 0..SAMPLES_PER_PV {
        for (i, name) in pv_names.iter().enumerate() {
            let t = ts(10_000 + (i as u64) * 100 + s as u64);
            tx.send(pv_sample(name, t, s as f64, &counters))
                .await
                .unwrap();
        }
    }

    // Poll until every PV's last sample reaches the registry.
    let expected_last: HashMap<&String, SystemTime> = pv_names
        .iter()
        .enumerate()
        .map(|(i, name)| {
            (
                name,
                ts(10_000 + (i as u64) * 100 + (SAMPLES_PER_PV - 1) as u64),
            )
        })
        .collect();
    let deadline = std::time::Instant::now() + Duration::from_secs(8);
    loop {
        let mut all_committed = true;
        for (name, expected) in &expected_last {
            let rec = registry.get_pv(name).unwrap().unwrap();
            if rec.last_timestamp != Some(*expected) {
                all_committed = false;
                break;
            }
        }
        if all_committed {
            break;
        }
        if std::time::Instant::now() > deadline {
            // Diagnostic: which PVs lag?
            let lagging: Vec<String> = expected_last
                .iter()
                .filter_map(|(name, expected)| {
                    let rec = registry.get_pv(name).unwrap().unwrap();
                    if rec.last_timestamp != Some(*expected) {
                        Some(format!(
                            "{}: have={:?} want={:?}",
                            name, rec.last_timestamp, expected
                        ))
                    } else {
                        None
                    }
                })
                .collect();
            panic!(
                "sharded pool didn't commit all PVs in time. Lagging:\n{}",
                lagging.join("\n")
            );
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }

    // Append-side check: total appends == N_PVS * SAMPLES_PER_PV
    // (every sample reached storage; nothing was dropped en route
    // through the dispatcher or shard channels).
    let appends = storage.appends_snapshot();
    assert_eq!(
        appends.len(),
        N_PVS * SAMPLES_PER_PV,
        "expected {} total appends, got {}",
        N_PVS * SAMPLES_PER_PV,
        appends.len()
    );

    // Per-PV ordering: samples for any single PV must appear in
    // strictly increasing-timestamp order in the storage append
    // log (consistent-hash routing keeps a PV pinned to one shard,
    // and the per-PV mutex inside the fake matches PlainPB's
    // ordering guarantee).
    for name in &pv_names {
        let pv_appends: Vec<_> = appends
            .iter()
            .filter(|r| &r.pv == name)
            .map(|r| r.timestamp)
            .collect();
        assert_eq!(
            pv_appends.len(),
            SAMPLES_PER_PV,
            "PV {name} should have {SAMPLES_PER_PV} appends"
        );
        for w in pv_appends.windows(2) {
            assert!(
                w[0] < w[1],
                "PV {name} appends out of order: {:?} >= {:?}",
                w[0],
                w[1]
            );
        }
    }

    let _ = sd_tx.send(true);
    let _ = tokio::time::timeout(Duration::from_secs(5), join).await;
}

/// Periodic flush must fire from the ticker even when no further
/// samples arrive — without this, a PV that goes silent (IOC down,
/// scan period missed) keeps its buffered bytes hostage in the
/// BufWriter until shutdown. Send one sample, then go quiet, and
/// expect the registry to advance on its own.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn ticker_flush_fires_without_sample_arrival() {
    let storage = InjectingStorage::new();
    let registry = Arc::new(PvRegistry::in_memory().unwrap());
    registry
        .register_pv("A", ArchDbType::ScalarDouble, &SampleMode::Monitor, 1)
        .unwrap();

    let counters = Arc::new(PvCounters::default());
    let cfg = fast_cfg();
    let (tx, sd_tx, join) = spawn_loop(storage.clone(), registry.clone(), cfg.clone());

    let t = ts(700);
    tx.send(pv_sample("A", t, 1.0, &counters)).await.unwrap();
    // No more samples. The old write_loop would never flush.

    let deadline = std::time::Instant::now() + Duration::from_secs(3);
    loop {
        if let Some(rec) = registry.get_pv("A").unwrap()
            && rec.last_timestamp == Some(t)
        {
            break;
        }
        if std::time::Instant::now() > deadline {
            panic!(
                "ticker never committed PV A's timestamp despite no further samples \
                 (flush_period={:?})",
                cfg.flush_period
            );
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    assert!(
        storage.flush_ingest_call_count() >= 1,
        "ticker branch must have invoked flush_ingest_writes at least once"
    );

    shutdown(sd_tx, join).await;
}

/// `flush_ingest_writes` returning Ok([]) (every PV flushed cleanly)
/// must clear `ts_updates` and persist every pending timestamp —
/// guards the success path so the failure tests above can isolate
/// the failure-only deltas.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn clean_flush_persists_all_timestamps() {
    let storage = InjectingStorage::new();
    let registry = Arc::new(PvRegistry::in_memory().unwrap());
    for pv in ["A", "B", "T"] {
        registry
            .register_pv(pv, ArchDbType::ScalarDouble, &SampleMode::Monitor, 1)
            .unwrap();
    }
    let counters = Arc::new(PvCounters::default());
    let cfg = fast_cfg();
    let (tx, sd_tx, join) = spawn_loop(storage.clone(), registry.clone(), cfg.clone());

    let ta = ts(500);
    let tb = ts(600);
    tx.send(pv_sample("A", ta, 1.0, &counters)).await.unwrap();
    tx.send(pv_sample("B", tb, 2.0, &counters)).await.unwrap();

    // Trigger flush via a separate-PV trigger sample after
    // flush_period elapses.
    tokio::time::sleep(cfg.flush_period + Duration::from_millis(50)).await;
    tx.send(pv_sample("T", ts(700), 9.0, &counters))
        .await
        .unwrap();
    tokio::time::sleep(Duration::from_millis(200)).await;

    let a = registry.get_pv("A").unwrap().unwrap();
    let b = registry.get_pv("B").unwrap().unwrap();
    assert_eq!(a.last_timestamp, Some(ta));
    assert_eq!(b.last_timestamp, Some(tb));

    shutdown(sd_tx, join).await;
}
