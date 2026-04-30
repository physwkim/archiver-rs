use std::sync::atomic::{AtomicI32, AtomicI64, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, SystemTime};

use dashmap::DashMap;
use epics_rs::base::types::{DbFieldType, EpicsValue};
use epics_rs::ca::client::{CaChannel, CaClient, ConnectionEvent};
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;
use tracing::{debug, error, info, warn};

use archiver_core::registry::{PvRecord, PvRegistry, PvStatus, SampleMode};
use archiver_core::storage::traits::{AppendMeta, StoragePlugin};
use archiver_core::types::{ArchDbType, ArchiverSample, ArchiverValue};

use crate::policy::PolicyConfig;

/// Timeout for initial CA channel connection.
const CA_CONNECT_TIMEOUT: Duration = Duration::from_secs(10);
/// Timeout for CA reconnection attempts in the monitor loop.
const CA_RECONNECT_TIMEOUT: Duration = Duration::from_secs(30);
/// Delay before retrying a failed CA subscription.
const CA_RETRY_DELAY: Duration = Duration::from_secs(5);

/// Hard floor on accepted timestamps. Mirrors Java's `PAST_CUTOFF_TIMESTAMP`
/// of 1991-01-01 — earlier than that, the timestamp is almost certainly a
/// stale uninitialised IOC clock or a sentinel.
const PAST_CUTOFF_UNIX_SECS: i64 = 662_688_000; // 1991-01-01 00:00:00 UTC

/// Filter a freshly-received sample timestamp against the wall clock and
/// the floor. Returns `Some(ts)` if accepted, `None` if it should be
/// dropped (caller bumps `timestamp_drops`).
///
/// `drift_secs` is the configured `server_ioc_drift_secs` (Java parity
/// 6538631), so per-site tuning doesn't require recompiling. `now` is
/// passed in (rather than calling `SystemTime::now()` here) so the test
/// suite can pin time deterministically.
fn ioc_timestamp_in_window(ts: SystemTime, now: SystemTime, drift_secs: u64) -> bool {
    let unix = ts
        .duration_since(SystemTime::UNIX_EPOCH)
        .map(|d| d.as_secs() as i64)
        .unwrap_or(i64::MIN);
    if unix < PAST_CUTOFF_UNIX_SECS {
        return false;
    }
    // Within ±drift_secs of `now`?
    let now_unix = now
        .duration_since(SystemTime::UNIX_EPOCH)
        .map(|d| d.as_secs() as i64)
        .unwrap_or(0);
    let delta = (unix - now_unix).unsigned_abs();
    delta <= drift_secs
}

/// Discrete connection state for `getPVDetails` (Java parity dea7acb).
/// Distinguishes never-connected from connecting from confirmed-down.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum PvConnectionState {
    /// No connection attempt has been made (or none has progressed
    /// past `wait_connected` yet).
    #[default]
    Idle,
    /// Currently waiting on `wait_connected` for the first time on
    /// this channel handle.
    Connecting,
    /// Channel has reported a successful connect; samples are flowing
    /// or the channel is otherwise live.
    Connected,
    /// Channel reported connect at least once but the monitor loop has
    /// since dropped — distinct from `Idle` so operators can spot a
    /// regression vs a never-resolved name.
    Disconnected,
}

impl PvConnectionState {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Idle => "Idle",
            Self::Connecting => "Connecting",
            Self::Connected => "Connected",
            Self::Disconnected => "Disconnected",
        }
    }
}

/// Connection state tracked per PV.
#[derive(Debug, Clone, Default)]
pub struct ConnectionInfo {
    pub connected_since: Option<SystemTime>,
    pub last_event_time: Option<SystemTime>,
    pub is_connected: bool,
    /// Java parity (dea7acb): discrete connection state for the
    /// PVDetails report. Operators can distinguish never-connected,
    /// currently-connecting, and previously-connected-now-down.
    pub state: PvConnectionState,
}

/// Per-PV diagnostic counters for the BPL drop / rate / connection
/// reports. All counts are monotonic across the PV's lifetime; the
/// rate handlers compute deltas against `first_event_unix_secs`.
///
/// Tracked here (not in ConnectionInfo) so they survive transient
/// disconnects and are read lock-free from the report endpoints.
#[derive(Debug)]
pub struct PvCounters {
    /// Total events produced by monitor/scan, including those that
    /// later got dropped before write.
    pub events_received: AtomicU64,
    /// Total events successfully written to storage.
    pub events_stored: AtomicU64,
    /// Unix-epoch seconds of the first event we ever saw for this PV.
    /// 0 = "no event yet" (i64 because Atomic<Option<...>> doesn't exist).
    pub first_event_unix_secs: AtomicI64,
    /// Number of events the bounded sample channel rejected (write
    /// loop falling behind producer). Surfaces as
    /// `getDroppedEventsBufferOverflowReport`.
    pub buffer_overflow_drops: AtomicU64,
    /// Number of events whose timestamp went backwards relative to the
    /// previously-stored event. Java archiver's
    /// `DroppedEventsTimestampReport`.
    pub timestamp_drops: AtomicU64,
    /// Number of events whose runtime DBR type didn't match the
    /// PvRecord's stored type — the engine drops these because mixing
    /// types in one PB partition would corrupt downstream readers.
    pub type_change_drops: AtomicU64,
    /// Number of disconnect transitions seen on this PV's CA channel.
    /// `LostConnectionsReport`.
    pub disconnect_count: AtomicU64,
    /// Last unix-epoch seconds of disconnect transition.
    pub last_disconnect_unix_secs: AtomicI64,
    /// Number of transient subscribe / monitor-recv / scan-read errors
    /// (Java parity 8fe73eb). Distinct from `disconnect_count` —
    /// these are recoverable per-attempt failures rather than confirmed
    /// link drops.
    pub transient_error_count: AtomicU64,
    /// Latest DBR type observed from CA that did not match the
    /// archive-time recorded type (Java parity 9f2234f). `-1` = no
    /// mismatch ever seen.
    pub latest_observed_dbr: AtomicI32,
}

impl Default for PvCounters {
    fn default() -> Self {
        Self {
            events_received: AtomicU64::new(0),
            events_stored: AtomicU64::new(0),
            first_event_unix_secs: AtomicI64::new(0),
            buffer_overflow_drops: AtomicU64::new(0),
            timestamp_drops: AtomicU64::new(0),
            type_change_drops: AtomicU64::new(0),
            disconnect_count: AtomicU64::new(0),
            last_disconnect_unix_secs: AtomicI64::new(0),
            transient_error_count: AtomicU64::new(0),
            // -1 sentinel = no type mismatch ever observed.
            latest_observed_dbr: AtomicI32::new(-1),
        }
    }
}

/// Read-only snapshot of `PvCounters` — owned values so callers can
/// move them across threads / serialise without reaching into Atomics.
#[derive(Debug, Clone)]
pub struct PvCountersSnapshot {
    pub events_received: u64,
    pub events_stored: u64,
    pub first_event_unix_secs: Option<i64>,
    pub buffer_overflow_drops: u64,
    pub timestamp_drops: u64,
    pub type_change_drops: u64,
    pub disconnect_count: u64,
    pub last_disconnect_unix_secs: Option<i64>,
    pub transient_error_count: u64,
    /// `Some(dbr_type as i32)` if a type-change mismatch has been
    /// observed; `None` otherwise (Java parity 9f2234f).
    pub latest_observed_dbr: Option<i32>,
}

impl From<&PvCounters> for PvCountersSnapshot {
    fn from(c: &PvCounters) -> Self {
        let first = c.first_event_unix_secs.load(Ordering::Relaxed);
        let last_disc = c.last_disconnect_unix_secs.load(Ordering::Relaxed);
        Self {
            events_received: c.events_received.load(Ordering::Relaxed),
            events_stored: c.events_stored.load(Ordering::Relaxed),
            first_event_unix_secs: if first == 0 { None } else { Some(first) },
            buffer_overflow_drops: c.buffer_overflow_drops.load(Ordering::Relaxed),
            timestamp_drops: c.timestamp_drops.load(Ordering::Relaxed),
            type_change_drops: c.type_change_drops.load(Ordering::Relaxed),
            disconnect_count: c.disconnect_count.load(Ordering::Relaxed),
            last_disconnect_unix_secs: if last_disc == 0 {
                None
            } else {
                Some(last_disc)
            },
            transient_error_count: c.transient_error_count.load(Ordering::Relaxed),
            latest_observed_dbr: match c.latest_observed_dbr.load(Ordering::Relaxed) {
                -1 => None,
                v => Some(v),
            },
        }
    }
}

/// Handle for a running PV archiving task.
struct PvHandle {
    #[allow(dead_code)]
    channel: CaChannel,
    cancel_token: CancellationToken,
    #[allow(dead_code)]
    dbr_type: ArchDbType,
    conn_info: Arc<Mutex<ConnectionInfo>>,
    /// Latest values of metadata fields (.HIHI, .LOLO, .EGU, ...) attached to
    /// every sample emitted for this PV. Populated by per-field monitor tasks
    /// owned by `cancel_token` (and per-field child tokens in `field_tokens`),
    /// so stopping the PV stops all of them.
    extras: Arc<ExtraFieldsCache>,
    /// Per-field cancellation tokens — child tokens of `cancel_token`. Keyed
    /// by field name (e.g. "HIHI"). Lets `update_archive_fields` cancel one
    /// field's task without disturbing the others or the main PV.
    field_tokens: Arc<DashMap<String, CancellationToken>>,
    /// Serialises concurrent `update_archive_fields` calls for this PV so
    /// add/remove/respawn never race with itself.
    update_lock: Arc<tokio::sync::Mutex<()>>,
    /// Diagnostic counters surfaced through the BPL drop / rate /
    /// connection reports. Lock-free reads; updates from the producer
    /// (monitor/scan) and the writer happen on different threads.
    counters: Arc<PvCounters>,
}

/// Thread-safe cache of latest extra-field values for one PV.
/// Each map entry is `(field_name, stringified_value)`.
type ExtraFieldsCache = DashMap<String, String>;

/// Default capacity for the bounded sample channel.
/// This limits memory usage when producers outpace the storage writer.
/// At ~200 bytes per sample, 500K entries ≈ 100 MB worst-case.
const SAMPLE_CHANNEL_CAPACITY: usize = 500_000;

/// RAII guard that removes a key from `pending_archives` on drop,
/// ensuring cleanup even if the owning future is cancelled.
struct PendingGuard<'a> {
    map: &'a DashMap<String, ()>,
    key: String,
}

impl Drop for PendingGuard<'_> {
    fn drop(&mut self) {
        self.map.remove(&self.key);
    }
}

/// Manages EPICS Channel Access connections and dispatches archived samples to storage.
pub struct ChannelManager {
    /// The CA client context.
    ca_client: CaClient,
    /// Active channels: PV name → handle with cancellation.
    channels: DashMap<String, PvHandle>,
    /// PVs currently being archived (in-progress CA connect). Prevents TOCTOU races
    /// where two concurrent `archive_pv` calls could double-subscribe the same PV.
    pending_archives: DashMap<String, ()>,
    /// Per-PV mutex serialising archive/pause/resume/stop/destroy on a single
    /// PV. Without this, e.g. `pause_pv` racing with `resume_pv` can leave
    /// the registry status and the channel map disagreeing.
    op_locks: DashMap<String, Arc<tokio::sync::Mutex<()>>>,
    /// Storage backend.
    #[allow(dead_code)]
    storage: Arc<dyn StoragePlugin>,
    /// PV metadata registry.
    registry: Arc<PvRegistry>,
    /// Sample sender for the write thread.
    sample_tx: mpsc::Sender<PvSample>,
    /// Optional policy configuration.
    policy: Option<PolicyConfig>,
    /// Per-site IOC drift bound (Java parity 6538631).
    server_ioc_drift_secs: u64,
}

/// A sample ready to be written to storage.
pub struct PvSample {
    pub pv_name: String,
    pub dbr_type: ArchDbType,
    pub sample: ArchiverSample,
    pub element_count: Option<i32>,
    /// Counter handle used by the write loop to record timestamp /
    /// type-change drops. None on samples produced before counter
    /// support was wired up — write_loop tolerates the absence.
    pub counters: Option<Arc<PvCounters>>,
}

impl ChannelManager {
    pub async fn new(
        storage: Arc<dyn StoragePlugin>,
        registry: Arc<PvRegistry>,
        policy: Option<PolicyConfig>,
    ) -> anyhow::Result<(Self, mpsc::Receiver<PvSample>)> {
        Self::new_with_drift(storage, registry, policy, 30 * 60).await
    }

    /// Construct with an explicit IOC drift bound. Java parity (6538631):
    /// keeps tests + sites that don't surface `EngineConfig` on the
    /// existing default while letting the daemon plumb a configured value.
    pub async fn new_with_drift(
        storage: Arc<dyn StoragePlugin>,
        registry: Arc<PvRegistry>,
        policy: Option<PolicyConfig>,
        server_ioc_drift_secs: u64,
    ) -> anyhow::Result<(Self, mpsc::Receiver<PvSample>)> {
        let ca_client = CaClient::new().await.map_err(|e| anyhow::anyhow!("{e}"))?;
        let (tx, rx) = mpsc::channel(SAMPLE_CHANNEL_CAPACITY);

        let mgr = Self {
            ca_client,
            channels: DashMap::new(),
            pending_archives: DashMap::new(),
            op_locks: DashMap::new(),
            storage,
            registry,
            sample_tx: tx,
            policy,
            server_ioc_drift_secs,
        };

        Ok((mgr, rx))
    }

    /// Get-or-insert the per-PV operation mutex. The returned `Arc<Mutex>`
    /// is what callers should `.lock().await` on; holding the entry guard
    /// (via `entry().or_insert_with`) across the await would deadlock the
    /// DashMap shard.
    fn op_lock(&self, pv_name: &str) -> Arc<tokio::sync::Mutex<()>> {
        if let Some(existing) = self.op_locks.get(pv_name) {
            return existing.clone();
        }
        self.op_locks
            .entry(pv_name.to_string())
            .or_insert_with(|| Arc::new(tokio::sync::Mutex::new(())))
            .clone()
    }

    /// Restore all active PVs from the registry (called on startup).
    ///
    /// `pvs_by_status(Active)` already filters out alias rows (they carry
    /// `status='alias'`), but we re-check `alias_for.is_some()` here so a
    /// future schema change can't silently re-introduce double-archiving
    /// of the underlying IOC PV.
    pub async fn restore_from_registry(&self) -> anyhow::Result<u64> {
        let active_pvs = self.registry.pvs_by_status(PvStatus::Active)?;
        let total = active_pvs.len() as u64;
        info!(total, "Restoring PVs from registry");

        let mut restored = 0u64;
        for record in active_pvs {
            if record.alias_for.is_some() {
                warn!(
                    pv = record.pv_name,
                    target = record.alias_for.as_deref(),
                    "Skipping alias row in restore; aliases are routed, not archived"
                );
                continue;
            }
            if let Err(e) = self.start_archiving_internal(&record).await {
                warn!(pv = record.pv_name, "Failed to restore PV: {e}");
                self.registry.set_status(&record.pv_name, PvStatus::Error)?;
            } else {
                restored += 1;
            }
        }
        metrics::gauge!("archiver_pvs_active").set(restored as f64);
        if restored < total {
            warn!(
                restored,
                failed = total - restored,
                "Some PVs failed to restore"
            );
        }

        Ok(restored)
    }

    /// Start archiving a new PV.
    pub async fn archive_pv(&self, pv_name: &str, sample_mode: &SampleMode) -> anyhow::Result<()> {
        // Serialise with pause/resume/stop/destroy on the same PV.
        let lock = self.op_lock(pv_name);
        let _g = lock.lock().await;

        if self.channels.contains_key(pv_name) {
            anyhow::bail!("PV {pv_name} is already being archived");
        }

        // Atomically claim the PV to prevent concurrent archive_pv races.
        // The guard ensures cleanup even if this future is cancelled.
        if self
            .pending_archives
            .insert(pv_name.to_string(), ())
            .is_some()
        {
            anyhow::bail!("PV {pv_name} archive operation already in progress");
        }
        let _guard = PendingGuard {
            map: &self.pending_archives,
            key: pv_name.to_string(),
        };

        self.archive_pv_inner(pv_name, sample_mode).await
    }

    /// Inner implementation of archive_pv, separated for cleanup safety.
    async fn archive_pv_inner(
        &self,
        pv_name: &str,
        sample_mode: &SampleMode,
    ) -> anyhow::Result<()> {
        // Re-check after acquiring the pending slot (another task may have completed).
        if self.channels.contains_key(pv_name) {
            anyhow::bail!("PV {pv_name} is already being archived");
        }

        // Check policy override.
        let (effective_mode, matched_policy_name) = if let Some(ref policy) = self.policy {
            if let Some(p) = policy.find_policy(pv_name) {
                (p.to_sample_mode(), Some(p.policy_name().to_string()))
            } else {
                (sample_mode.clone(), None)
            }
        } else {
            (sample_mode.clone(), None)
        };

        // Connect to discover the native type.
        let channel = self.ca_client.create_channel(pv_name);
        channel
            .wait_connected(CA_CONNECT_TIMEOUT)
            .await
            .map_err(|e| anyhow::anyhow!("Failed to connect to {pv_name}: {e}"))?;

        let info = self
            .ca_client
            .cainfo(pv_name)
            .await
            .map_err(|e| anyhow::anyhow!("Failed to get info for {pv_name}: {e}"))?;

        let dbr_type = dbr_field_to_arch_type(info.native_type);
        let element_count = info.element_count as i32;

        // Register in SQLite.
        self.registry
            .register_pv(pv_name, dbr_type, &effective_mode, element_count)?;
        // Java parity (b30f1a6): persist the matched policy's stable name so
        // audit / metrics paths know which policy governed this archive.
        if let Some(ref name) = matched_policy_name {
            self.registry.update_policy_name(pv_name, Some(name))?;
        }

        let record = self
            .registry
            .get_pv(pv_name)?
            .ok_or_else(|| anyhow::anyhow!("PV {pv_name} not found in registry"))?;
        self.start_archiving_internal(&record).await?;

        metrics::gauge!("archiver_pvs_active").increment(1.0);
        info!(pv = pv_name, ?dbr_type, element_count, "Started archiving");
        Ok(())
    }

    /// Internal: start CA subscription for a PV record.
    async fn start_archiving_internal(&self, record: &PvRecord) -> anyhow::Result<()> {
        let pv_name = record.pv_name.clone();
        let dbr_type = record.dbr_type;
        let element_count = record.element_count;
        let channel = self.ca_client.create_channel(&pv_name);
        let cancel_token = CancellationToken::new();
        let conn_info = Arc::new(Mutex::new(ConnectionInfo::default()));
        let extras: Arc<ExtraFieldsCache> = Arc::new(DashMap::new());
        let field_tokens: Arc<DashMap<String, CancellationToken>> = Arc::new(DashMap::new());
        let update_lock = Arc::new(tokio::sync::Mutex::new(()));
        let counters = Arc::new(PvCounters::default());

        // Hold update_lock around the whole insert+spawn block so a
        // concurrent update_archive_fields can't observe the empty
        // field_tokens map and spawn its own copies of the same fields
        // before we get to the spawn loop. update_archive_fields acquires
        // the same update_lock before mutating tokens.
        let _guard = update_lock.lock().await;

        self.channels.insert(
            pv_name.clone(),
            PvHandle {
                channel: channel.clone(),
                cancel_token: cancel_token.clone(),
                dbr_type,
                conn_info: conn_info.clone(),
                extras: extras.clone(),
                field_tokens: field_tokens.clone(),
                update_lock: update_lock.clone(),
                counters: counters.clone(),
            },
        );

        // Start one monitor task per archive_field with a child cancel token,
        // tracked so update_archive_fields can stop individual fields.
        for field in &record.archive_fields {
            let child = cancel_token.child_token();
            field_tokens.insert(field.clone(), child.clone());
            spawn_extra_field_monitor(
                &self.ca_client,
                &pv_name,
                field,
                extras.clone(),
                child,
                counters.clone(),
            );
        }
        metrics::gauge!("archiver_extra_field_tasks").increment(record.archive_fields.len() as f64);
        drop(_guard);

        let tx = self.sample_tx.clone();
        let token = cancel_token.clone();
        let ci = conn_info.clone();
        let extras_for_loop = extras.clone();
        let counters_for_loop = counters.clone();

        let drift = self.server_ioc_drift_secs;
        match &record.sample_mode {
            SampleMode::Monitor => {
                tokio::spawn(async move {
                    monitor_loop(
                        pv_name,
                        dbr_type,
                        element_count,
                        channel,
                        tx,
                        token,
                        ci,
                        extras_for_loop,
                        counters_for_loop,
                        drift,
                    )
                    .await;
                });
            }
            SampleMode::Scan { period_secs } => {
                let period = *period_secs;
                tokio::spawn(async move {
                    scan_loop(
                        pv_name,
                        dbr_type,
                        element_count,
                        channel,
                        tx,
                        token,
                        period,
                        ci,
                        extras_for_loop,
                        counters_for_loop,
                    )
                    .await;
                });
            }
        }

        Ok(())
    }

    /// Replace the archive_fields list for a running PV. Cancels per-field
    /// monitor tasks for fields that left the set, spawns fresh ones for
    /// fields that joined, and leaves unchanged fields running. Serialised
    /// per-PV by an async mutex so concurrent callers can't double-spawn.
    /// The main PV keeps running.
    pub async fn update_archive_fields(
        &self,
        pv_name: &str,
        fields: &[String],
    ) -> anyhow::Result<()> {
        // Persist first so a restart sees the new set even if the engine
        // half of the update fails partway through.
        self.registry.update_archive_fields(pv_name, fields)?;

        // If the PV isn't currently active there's nothing more to do —
        // start_archiving_internal will pick up the new fields on resume.
        let (parent_token, extras, field_tokens, update_lock, counters) = {
            let Some(handle) = self.channels.get(pv_name) else {
                return Ok(());
            };
            (
                handle.cancel_token.clone(),
                handle.extras.clone(),
                handle.field_tokens.clone(),
                handle.update_lock.clone(),
                handle.counters.clone(),
            )
        };

        // Serialise so two concurrent updates can't both decide the same
        // field is missing and spawn it twice.
        let _guard = update_lock.lock().await;

        let wanted: std::collections::HashSet<&str> = fields.iter().map(|s| s.as_str()).collect();

        // Cancel + drop tasks for fields that left the set. Removing the
        // entry from `field_tokens` also drops our handle on the child
        // token; the spawned task observes `cancelled()` and exits.
        let to_remove: Vec<String> = field_tokens
            .iter()
            .filter(|e| !wanted.contains(e.key().as_str()))
            .map(|e| e.key().clone())
            .collect();
        let removed_count = to_remove.len();
        for key in to_remove {
            if let Some((_, token)) = field_tokens.remove(&key) {
                token.cancel();
            }
            extras.remove(&key);
        }

        // Spawn tasks for fields newly added. Existing fields keep their
        // task and their last cached value.
        let mut added_count = 0usize;
        for f in fields {
            if !field_tokens.contains_key(f) {
                let child = parent_token.child_token();
                field_tokens.insert(f.clone(), child.clone());
                spawn_extra_field_monitor(
                    &self.ca_client,
                    pv_name,
                    f,
                    extras.clone(),
                    child,
                    counters.clone(),
                );
                added_count += 1;
            }
        }
        let net = added_count as i64 - removed_count as i64;
        if net != 0 {
            metrics::gauge!("archiver_extra_field_tasks").increment(net as f64);
        }
        Ok(())
    }

    /// Pause archiving for a PV.
    pub async fn pause_pv(&self, pv_name: &str) -> anyhow::Result<()> {
        let lock = self.op_lock(pv_name);
        let _g = lock.lock().await;
        if let Some((_key, handle)) = self.channels.remove(pv_name) {
            let extra_count = handle.field_tokens.len() as f64;
            handle.cancel_token.cancel();
            metrics::gauge!("archiver_pvs_active").decrement(1.0);
            if extra_count > 0.0 {
                metrics::gauge!("archiver_extra_field_tasks").decrement(extra_count);
            }
        }
        self.registry.set_status(pv_name, PvStatus::Paused)?;
        info!(pv = pv_name, "Paused archiving");
        Ok(())
    }

    /// Resume a paused PV. Only paused or error PVs can be resumed;
    /// calling resume on an already-active PV is a no-op (returns Ok).
    pub async fn resume_pv(&self, pv_name: &str) -> anyhow::Result<()> {
        let lock = self.op_lock(pv_name);
        let _g = lock.lock().await;

        let record = self
            .registry
            .get_pv(pv_name)?
            .ok_or_else(|| anyhow::anyhow!("PV {pv_name} not found in registry"))?;

        // Guard: if already active with a live task, nothing to do.
        if record.status == PvStatus::Active && self.channels.contains_key(pv_name) {
            info!(
                pv = pv_name,
                "PV is already actively archived, skipping resume"
            );
            return Ok(());
        }

        // Cancel any orphaned task to prevent duplicate subscriptions.
        if let Some((_key, handle)) = self.channels.remove(pv_name) {
            let extra_count = handle.field_tokens.len() as f64;
            handle.cancel_token.cancel();
            if extra_count > 0.0 {
                metrics::gauge!("archiver_extra_field_tasks").decrement(extra_count);
            }
        }

        // Start the task first; only mark Active if it succeeds.
        self.start_archiving_internal(&record).await?;
        self.registry.set_status(pv_name, PvStatus::Active)?;
        metrics::gauge!("archiver_pvs_active").increment(1.0);
        info!(pv = pv_name, "Resumed archiving");
        Ok(())
    }

    /// Stop archiving a PV without removing it from the registry.
    /// Sets the PV status to Inactive (data retained, monitoring stopped).
    pub async fn stop_pv(&self, pv_name: &str) -> anyhow::Result<()> {
        let lock = self.op_lock(pv_name);
        let _g = lock.lock().await;
        if let Some((_key, handle)) = self.channels.remove(pv_name) {
            let extra_count = handle.field_tokens.len() as f64;
            handle.cancel_token.cancel();
            metrics::gauge!("archiver_pvs_active").decrement(1.0);
            if extra_count > 0.0 {
                metrics::gauge!("archiver_extra_field_tasks").decrement(extra_count);
            }
        }
        self.registry.set_status(pv_name, PvStatus::Inactive)?;
        info!(pv = pv_name, "Stopped archiving (inactive)");
        Ok(())
    }

    /// Remove a PV from archiving entirely.
    pub async fn destroy_pv(&self, pv_name: &str) -> anyhow::Result<()> {
        let lock = self.op_lock(pv_name);
        let _g = lock.lock().await;
        if let Some((_key, handle)) = self.channels.remove(pv_name) {
            let extra_count = handle.field_tokens.len() as f64;
            handle.cancel_token.cancel();
            metrics::gauge!("archiver_pvs_active").decrement(1.0);
            if extra_count > 0.0 {
                metrics::gauge!("archiver_extra_field_tasks").decrement(extra_count);
            }
        }
        self.registry.remove_pv(pv_name)?;
        // Don't remove the op_locks entry here: a concurrent caller may have
        // already taken a clone of this Arc and be queued on it; removing
        // would let a fresh caller obtain a new mutex and the queued one
        // would race them. The map is bounded by the lifetime universe of
        // PV names, which is acceptable.
        info!(pv = pv_name, "Destroyed archiving channel");
        Ok(())
    }

    /// List all currently archived PV names (from registry).
    pub fn list_pvs(&self) -> Vec<String> {
        self.registry.all_pv_names().unwrap_or_else(|e| {
            warn!("Failed to list PVs: {e}");
            Vec::new()
        })
    }

    /// Match PVs by glob pattern (from registry).
    pub fn matching_pvs(&self, pattern: &str) -> Vec<String> {
        self.registry.matching_pvs(pattern).unwrap_or_else(|e| {
            warn!("Failed to match PVs: {e}");
            Vec::new()
        })
    }

    /// Get the registry reference.
    pub fn registry(&self) -> &Arc<PvRegistry> {
        &self.registry
    }

    /// Get connection info for a PV.
    pub fn get_connection_info(&self, pv: &str) -> Option<ConnectionInfo> {
        self.channels.get(pv).map(|h| {
            h.conn_info
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .clone()
        })
    }

    /// Get PV names that have never received any event (connected_since == None).
    pub fn get_never_connected_pvs(&self) -> Vec<String> {
        self.channels
            .iter()
            .filter(|entry| {
                let ci = entry
                    .value()
                    .conn_info
                    .lock()
                    .unwrap_or_else(|e| e.into_inner());
                ci.connected_since.is_none()
            })
            .map(|entry| entry.key().clone())
            .collect()
    }

    /// Snapshot the diagnostic counters for one PV. Returns None if the
    /// PV isn't actively archived. The returned Arc is the live counter
    /// — callers read with `Ordering::Relaxed`.
    pub fn pv_counters(&self, pv_name: &str) -> Option<Arc<PvCounters>> {
        self.channels.get(pv_name).map(|h| h.counters.clone())
    }

    /// Snapshot every active PV's counters. Returns `(pv_name,
    /// PvCountersSnapshot)` so callers don't have to handle Arc.
    pub fn all_pv_counters(&self) -> Vec<(String, PvCountersSnapshot)> {
        self.channels
            .iter()
            .map(|e| {
                (
                    e.key().clone(),
                    PvCountersSnapshot::from(&*e.value().counters),
                )
            })
            .collect()
    }

    /// One-shot CA `get` against the running channel for `pv`. Returns
    /// `None` if the PV isn't actively archived. The timeout caps how
    /// long the caller will wait for a value when the IOC is slow.
    /// Powers `getEngineDataAction` / `getDataAtTimeEngine`.
    pub async fn live_value(
        &self,
        pv_name: &str,
        timeout: Duration,
    ) -> Option<anyhow::Result<ArchiverValue>> {
        let channel = self.channels.get(pv_name)?.channel.clone();
        // Wait briefly for connection — channel.get on a disconnected
        // channel would otherwise return an error from deep in the CA
        // stack. Capped by the same timeout the caller chose.
        if channel.wait_connected(timeout).await.is_err() {
            return Some(Err(anyhow::anyhow!(
                "channel not connected within {timeout:?}"
            )));
        }
        match tokio::time::timeout(timeout, channel.get()).await {
            Ok(Ok((_dbr_type, val))) => Some(Ok(epics_value_to_archiver(&val))),
            Ok(Err(e)) => Some(Err(anyhow::anyhow!("CA get failed: {e}"))),
            Err(_) => Some(Err(anyhow::anyhow!("CA get timed out after {timeout:?}"))),
        }
    }

    /// Snapshot the latest cached extra-field values for `pv` —
    /// `(field_name, stringified_value)` pairs. Empty map when the PV
    /// isn't archived or has no archive_fields configured.
    pub fn extras_snapshot(&self, pv_name: &str) -> std::collections::HashMap<String, String> {
        match self.channels.get(pv_name) {
            Some(handle) => handle
                .extras
                .iter()
                .map(|e| (e.key().clone(), e.value().clone()))
                .collect(),
            None => std::collections::HashMap::new(),
        }
    }

    /// Get PV names that are currently disconnected (is_connected == false).
    pub fn get_currently_disconnected_pvs(&self) -> Vec<String> {
        self.channels
            .iter()
            .filter(|entry| {
                let ci = entry
                    .value()
                    .conn_info
                    .lock()
                    .unwrap_or_else(|e| e.into_inner());
                !ci.is_connected
            })
            .map(|entry| entry.key().clone())
            .collect()
    }
}

/// Monitor loop: subscribe to a channel and forward values.
#[allow(clippy::too_many_arguments)]
async fn monitor_loop(
    pv_name: String,
    dbr_type: ArchDbType,
    element_count: i32,
    channel: CaChannel,
    tx: mpsc::Sender<PvSample>,
    cancel_token: CancellationToken,
    conn_info: Arc<Mutex<ConnectionInfo>>,
    extras: Arc<ExtraFieldsCache>,
    counters: Arc<PvCounters>,
    server_ioc_drift_secs: u64,
) {
    loop {
        // Wait for connection, respecting cancellation.
        tokio::select! {
            _ = cancel_token.cancelled() => return,
            result = channel.wait_connected(CA_RECONNECT_TIMEOUT) => {
                if result.is_err() {
                    let was_connected = {
                        let mut ci = conn_info.lock().unwrap_or_else(|e| e.into_inner());
                        let prev_connected = ci.is_connected;
                        ci.is_connected = false;
                        ci.last_event_time = None;
                        // Connecting if we've never seen a connect, otherwise
                        // demote from Connected to Disconnected on a real loss.
                        ci.state = match ci.state {
                            PvConnectionState::Connected => PvConnectionState::Disconnected,
                            PvConnectionState::Disconnected => PvConnectionState::Disconnected,
                            _ => PvConnectionState::Connecting,
                        };
                        prev_connected
                    };
                    // A search-failure timeout that demotes us out of
                    // Connected counts as a fresh disconnect — without
                    // this, only the post-monitor `break` path bumps the
                    // counters and a flapping PV silently under-reports
                    // its drops while subsequent `attach_cnx_lost_headers`
                    // calls carry a stale `cnxlostepsecs`.
                    if was_connected {
                        counters.disconnect_count.fetch_add(1, Ordering::Relaxed);
                        counters
                            .last_disconnect_unix_secs
                            .store(unix_secs(SystemTime::now()), Ordering::Relaxed);
                    }
                    // Subscribe BEFORE re-checking the current state. If we
                    // subscribed after a state check, a Connected event
                    // firing in between would be lost forever, leaving this
                    // loop hung until the next disconnect/reconnect cycle.
                    let mut conn_rx = channel.connection_events();

                    // Close the remaining race: the channel may have become
                    // connected between the outer `wait_connected` timeout
                    // and our `subscribe()` above, in which case no new event
                    // will arrive. A short re-probe catches that case.
                    if channel
                        .wait_connected(Duration::from_millis(100))
                        .await
                        .is_err()
                    {
                        loop {
                            tokio::select! {
                                _ = cancel_token.cancelled() => return,
                                event = conn_rx.recv() => {
                                    use tokio::sync::broadcast::error::RecvError;
                                    match event {
                                        Ok(ConnectionEvent::Connected) => break,
                                        Ok(_) => continue,
                                        // Lagged: we missed some events but
                                        // the channel is still live. Re-probe
                                        // state and otherwise keep waiting.
                                        Err(RecvError::Lagged(_)) => {
                                            if channel
                                                .wait_connected(Duration::from_millis(100))
                                                .await
                                                .is_ok()
                                            {
                                                break;
                                            }
                                            continue;
                                        }
                                        Err(RecvError::Closed) => return,
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }

        // Subscribe.
        let mut monitor = match channel.subscribe().await {
            Ok(m) => m,
            Err(e) => {
                counters
                    .transient_error_count
                    .fetch_add(1, Ordering::Relaxed);
                warn!(pv = pv_name, "Subscribe failed: {e}, retrying...");
                tokio::select! {
                    _ = cancel_token.cancelled() => return,
                    _ = tokio::time::sleep(CA_RETRY_DELAY) => continue,
                }
            }
        };

        debug!(pv = pv_name, "Monitor subscription active");

        // Receive values with cancellation.
        loop {
            tokio::select! {
                _ = cancel_token.cancelled() => return,
                result = monitor.recv() => {
                    match result {
                        Some(Ok(snapshot)) => {
                            let now = SystemTime::now();
                            // Java parity (11e554d0): use the IOC-reported
                            // timestamp, not receive-time, so latency
                            // doesn't smear sample times. First sample
                            // after connect is accepted unconditionally
                            // — legitimate backfill on reconnect can
                            // include older timestamps. Subsequent
                            // samples whose IOC clock is more than
                            // SERVER_IOC_DRIFT_SECS away from wall clock,
                            // or earlier than the 1991 floor, are
                            // dropped + counted.
                            let first_after_connect = {
                                let mut ci = conn_info.lock().unwrap_or_else(|e| e.into_inner());
                                let first = ci.last_event_time.is_none();
                                if ci.connected_since.is_none() {
                                    ci.connected_since = Some(now);
                                }
                                ci.is_connected = true;
                                ci.last_event_time = Some(now);
                                ci.state = PvConnectionState::Connected;
                                first
                            };
                            if !first_after_connect
                                && !ioc_timestamp_in_window(
                                    snapshot.timestamp,
                                    now,
                                    server_ioc_drift_secs,
                                )
                            {
                                counters.timestamp_drops.fetch_add(1, Ordering::Relaxed);
                                debug!(
                                    pv = pv_name,
                                    ?snapshot.timestamp,
                                    "Dropping sample with out-of-window IOC timestamp"
                                );
                                continue;
                            }
                            counters.events_received.fetch_add(1, Ordering::Relaxed);
                            // CAS the first-event timestamp once. 0 sentinel
                            // means "no event yet"; replace with now.
                            let now_secs = unix_secs(now);
                            let _ = counters.first_event_unix_secs.compare_exchange(
                                0,
                                now_secs,
                                Ordering::Relaxed,
                                Ordering::Relaxed,
                            );
                            let archiver_val = epics_value_to_archiver(&snapshot.value);
                            let mut sample = ArchiverSample::new(snapshot.timestamp, archiver_val);
                            attach_extras(&extras, &mut sample);
                            if first_after_connect {
                                let lost_secs = counters
                                    .last_disconnect_unix_secs
                                    .load(Ordering::Relaxed);
                                attach_cnx_lost_headers(&mut sample, lost_secs, now_secs);
                            }
                            let pv_sample = PvSample {
                                pv_name: pv_name.clone(),
                                dbr_type,
                                sample,
                                element_count: Some(element_count),
                                counters: Some(counters.clone()),
                            };
                            if let Err(pv_sample) = try_send_with_overflow_count(
                                &tx,
                                pv_sample,
                                &counters,
                            )
                            .await
                            {
                                let _ = pv_sample;
                                return; // Write loop shut down
                            }
                        }
                        Some(Err(e)) => {
                            counters.transient_error_count.fetch_add(1, Ordering::Relaxed);
                            warn!(pv = pv_name, "Monitor error: {e}");
                        }
                        None => break, // Monitor ended, reconnect
                    }
                }
            }
        }

        // Monitor ended (disconnect) — loop back to reconnect. Reset
        // `last_event_time` so the first sample after reconnect is
        // treated as `first_after_connect` and bypasses the drift
        // filter; without this, an IOC that comes back with a
        // legitimate backfill timestamp older than the 30 min window
        // would be silently dropped.
        {
            let mut ci = conn_info.lock().unwrap_or_else(|e| e.into_inner());
            ci.is_connected = false;
            ci.last_event_time = None;
            ci.state = PvConnectionState::Disconnected;
        }
        counters.disconnect_count.fetch_add(1, Ordering::Relaxed);
        counters
            .last_disconnect_unix_secs
            .store(unix_secs(SystemTime::now()), Ordering::Relaxed);
        debug!(pv = pv_name, "Monitor ended, waiting for reconnection");
    }
}

fn unix_secs(t: SystemTime) -> i64 {
    t.duration_since(SystemTime::UNIX_EPOCH)
        .map(|d| d.as_secs() as i64)
        .unwrap_or(0)
}

/// Send a sample and count buffer-overflow events. Tries non-blocking
/// first; if the bounded channel is full we increment the counter and
/// fall back to a blocking send so backpressure still works.
async fn try_send_with_overflow_count(
    tx: &mpsc::Sender<PvSample>,
    pv_sample: PvSample,
    counters: &PvCounters,
) -> Result<(), PvSample> {
    match tx.try_send(pv_sample) {
        Ok(()) => Ok(()),
        Err(tokio::sync::mpsc::error::TrySendError::Full(pv_sample)) => {
            counters
                .buffer_overflow_drops
                .fetch_add(1, Ordering::Relaxed);
            // Backpressure to the producer; this awaits until the writer
            // drains some space. We count the saturation event but don't
            // actually drop the sample.
            tx.send(pv_sample).await.map_err(|e| e.0)
        }
        Err(tokio::sync::mpsc::error::TrySendError::Closed(pv_sample)) => Err(pv_sample),
    }
}

/// Scan loop: periodically read a channel value.
#[allow(clippy::too_many_arguments)]
async fn scan_loop(
    pv_name: String,
    dbr_type: ArchDbType,
    element_count: i32,
    channel: CaChannel,
    tx: mpsc::Sender<PvSample>,
    cancel_token: CancellationToken,
    period_secs: f64,
    conn_info: Arc<Mutex<ConnectionInfo>>,
    extras: Arc<ExtraFieldsCache>,
    counters: Arc<PvCounters>,
) {
    let period = Duration::from_secs_f64(period_secs);
    let mut interval = tokio::time::interval(period);

    loop {
        tokio::select! {
            _ = cancel_token.cancelled() => return,
            _ = interval.tick() => {}
        }

        if channel.wait_connected(CA_RETRY_DELAY).await.is_err() {
            let was_connected = {
                let mut ci = conn_info.lock().unwrap_or_else(|e| e.into_inner());
                let prev = ci.is_connected;
                ci.is_connected = false;
                ci.last_event_time = None;
                ci.state = match ci.state {
                    PvConnectionState::Connected => PvConnectionState::Disconnected,
                    PvConnectionState::Disconnected => PvConnectionState::Disconnected,
                    _ => PvConnectionState::Connecting,
                };
                prev
            };
            if was_connected {
                counters.disconnect_count.fetch_add(1, Ordering::Relaxed);
                counters
                    .last_disconnect_unix_secs
                    .store(unix_secs(SystemTime::now()), Ordering::Relaxed);
            }
            continue;
        }

        match channel.get().await {
            Ok((_dbr_type, epics_val)) => {
                let now = SystemTime::now();
                let first_after_connect = {
                    let mut ci = conn_info.lock().unwrap_or_else(|e| e.into_inner());
                    let first = ci.last_event_time.is_none();
                    if ci.connected_since.is_none() {
                        ci.connected_since = Some(now);
                    }
                    ci.is_connected = true;
                    ci.last_event_time = Some(now);
                    ci.state = PvConnectionState::Connected;
                    first
                };
                counters.events_received.fetch_add(1, Ordering::Relaxed);
                let now_secs = unix_secs(now);
                let _ = counters.first_event_unix_secs.compare_exchange(
                    0,
                    now_secs,
                    Ordering::Relaxed,
                    Ordering::Relaxed,
                );
                let archiver_val = epics_value_to_archiver(&epics_val);
                let mut sample = ArchiverSample::new(now, archiver_val);
                attach_extras(&extras, &mut sample);
                if first_after_connect {
                    let lost_secs = counters.last_disconnect_unix_secs.load(Ordering::Relaxed);
                    attach_cnx_lost_headers(&mut sample, lost_secs, now_secs);
                }
                let pv_sample = PvSample {
                    pv_name: pv_name.clone(),
                    dbr_type,
                    sample,
                    element_count: Some(element_count),
                    counters: Some(counters.clone()),
                };
                if try_send_with_overflow_count(&tx, pv_sample, &counters)
                    .await
                    .is_err()
                {
                    return;
                }
            }
            Err(e) => {
                counters
                    .transient_error_count
                    .fetch_add(1, Ordering::Relaxed);
                debug!(pv = pv_name, "Scan read error: {e}");
            }
        }
    }
}

/// Java parity (ed07feb): tag the first sample after (re)connect with
/// `cnxlostepsecs` / `cnxregainedepsecs` / `startup` so consumers can
/// detect archiver restarts and PV resumes. Unconditional emission —
/// on a clean startup `lost_secs` is 0, which is itself a valid value
/// for downstream gap detection.
fn attach_cnx_lost_headers(sample: &mut ArchiverSample, lost_secs: i64, now_secs: i64) {
    sample
        .field_values
        .push(("cnxlostepsecs".to_string(), lost_secs.to_string()));
    sample
        .field_values
        .push(("cnxregainedepsecs".to_string(), now_secs.to_string()));
    sample
        .field_values
        .push(("startup".to_string(), "true".to_string()));
}

/// Snapshot the extras cache into `sample.field_values`. We sort for stable
/// PB output across runs (the protobuf field is repeated; consumers that
/// diff/compare files appreciate determinism).
fn attach_extras(extras: &ExtraFieldsCache, sample: &mut ArchiverSample) {
    if extras.is_empty() {
        return;
    }
    let mut entries: Vec<(String, String)> = extras
        .iter()
        .map(|e| (e.key().clone(), e.value().clone()))
        .collect();
    entries.sort_by(|a, b| a.0.cmp(&b.0));
    sample.field_values = entries;
}

/// Render an EpicsValue as the string we'll persist in the metadata field
/// slot. Numeric scalars get `Display`-format; strings pass through; arrays
/// fall back to JSON-ish bracket notation. Stays in sync with what Java
/// archiver writes into PVTypeInfo's `archiveFields` blob (a string map).
fn epics_value_to_field_string(val: &EpicsValue) -> String {
    match val {
        EpicsValue::String(s) => s.clone(),
        EpicsValue::Short(v) => v.to_string(),
        EpicsValue::Float(v) => v.to_string(),
        EpicsValue::Enum(v) => v.to_string(),
        EpicsValue::Char(v) => v.to_string(),
        EpicsValue::Long(v) => v.to_string(),
        EpicsValue::Double(v) => v.to_string(),
        EpicsValue::ShortArray(v) => format!("{v:?}"),
        EpicsValue::FloatArray(v) => format!("{v:?}"),
        EpicsValue::EnumArray(v) => format!("{v:?}"),
        EpicsValue::DoubleArray(v) => format!("{v:?}"),
        EpicsValue::LongArray(v) => format!("{v:?}"),
        EpicsValue::CharArray(v) => String::from_utf8_lossy(v).into_owned(),
        EpicsValue::StringArray(v) => format!("{v:?}"),
    }
}

/// Spawn a long-running task that subscribes to `<pv>.<field>` and updates
/// `extras` with each event. Owned by `parent_token` so pause/destroy cleans
/// it up alongside the main PV.
fn spawn_extra_field_monitor(
    ca_client: &CaClient,
    pv_name: &str,
    field: &str,
    extras: Arc<ExtraFieldsCache>,
    parent_token: CancellationToken,
    counters: Arc<PvCounters>,
) {
    let full_name = format!("{pv_name}.{field}");
    let channel = ca_client.create_channel(&full_name);
    let field_owned = field.to_string();
    let pv_owned = pv_name.to_string();

    // Catch-unwind boundary: a panic from the CA client (e.g. malformed
    // wire frame, allocation failure inside epics_rs) would propagate to
    // the runtime and abort sibling tasks of this worker thread. Trap it
    // here, log with PV+field context, and return normally so the runtime
    // remains healthy.
    let panic_pv = pv_owned.clone();
    let panic_field = field_owned.clone();
    tokio::spawn(async move {
        let body = std::panic::AssertUnwindSafe(extra_field_monitor_body(
            channel,
            pv_owned,
            field_owned,
            extras,
            parent_token,
            counters,
        ));
        if let Err(payload) = futures::FutureExt::catch_unwind(body).await {
            let msg = panic_payload_msg(&payload);
            error!(
                pv = panic_pv,
                field = panic_field,
                "Extra-field monitor panicked: {msg}"
            );
        }
    });
}

/// Body of the spawned extra-field monitor. Split out so the spawn site
/// can wrap it in `catch_unwind`.
async fn extra_field_monitor_body(
    channel: CaChannel,
    pv_owned: String,
    field_owned: String,
    extras: Arc<ExtraFieldsCache>,
    parent_token: CancellationToken,
    counters: Arc<PvCounters>,
) {
    // Initial connect attempt — failure here is non-fatal (the field may
    // not exist on every IOC; we just leave the cache empty).
    if channel.wait_connected(CA_CONNECT_TIMEOUT).await.is_err() {
        debug!(
            pv = pv_owned,
            field = field_owned,
            "Extra-field channel did not connect within timeout (will keep retrying via subscribe)"
        );
    }

    // Exponential backoff for misconfigured fields (e.g. operator
    // listed `.HIHI` on a PV that doesn't expose it). Without this
    // we'd retry every 5s forever, churning CA search packets and
    // file descriptors. The cap is 60s; one warn at the cap so
    // ops know to fix archive_fields.
    let mut backoff = CA_RETRY_DELAY;
    let max_backoff = Duration::from_secs(60);
    let mut warned_at_cap = false;

    loop {
        // Cancel-aware subscribe attempt.
        tokio::select! {
            _ = parent_token.cancelled() => return,
            sub = channel.subscribe() => {
                let mut monitor = match sub {
                    Ok(m) => m,
                    Err(e) => {
                        // Java parity (8fe73eb): bump the transient
                        // counter so a misconfigured `.HIHI` field
                        // shows up in the rate / drop reports.
                        counters.transient_error_count.fetch_add(1, Ordering::Relaxed);
                        debug!(
                            pv = pv_owned,
                            field = field_owned,
                            ?backoff,
                            "Extra-field subscribe failed: {e}; retrying"
                        );
                        if backoff >= max_backoff && !warned_at_cap {
                            warn!(
                                pv = pv_owned,
                                field = field_owned,
                                "Extra-field repeatedly fails to subscribe; \
                                 check archive_fields config (now retrying every 60s)"
                            );
                            warned_at_cap = true;
                        }
                        let sleep_for = backoff;
                        backoff = (backoff * 2).min(max_backoff);
                        tokio::select! {
                            _ = parent_token.cancelled() => return,
                            _ = tokio::time::sleep(sleep_for) => continue,
                        }
                    }
                };
                // Subscribe succeeded — reset backoff so the next
                // failure starts at the short delay again.
                backoff = CA_RETRY_DELAY;
                warned_at_cap = false;
                loop {
                    tokio::select! {
                        _ = parent_token.cancelled() => return,
                        ev = monitor.recv() => match ev {
                            Some(Ok(snapshot)) => {
                                extras.insert(
                                    field_owned.clone(),
                                    epics_value_to_field_string(&snapshot.value),
                                );
                            }
                            Some(Err(e)) => {
                                counters.transient_error_count.fetch_add(1, Ordering::Relaxed);
                                debug!(
                                    pv = pv_owned,
                                    field = field_owned,
                                    "Extra-field monitor error: {e}"
                                );
                            }
                            None => break, // resubscribe
                        }
                    }
                }
            }
        }
    }
}

/// Format a panic payload (Box<dyn Any>) as a printable string. Mirrors
/// what `std::panicking::default_hook` extracts for the message.
fn panic_payload_msg(payload: &Box<dyn std::any::Any + Send>) -> String {
    if let Some(s) = payload.downcast_ref::<&'static str>() {
        (*s).to_string()
    } else if let Some(s) = payload.downcast_ref::<String>() {
        s.clone()
    } else {
        "<non-string panic payload>".to_string()
    }
}

/// Convert epics-base-rs DbFieldType to archiver ArchDbType.
fn dbr_field_to_arch_type(field_type: DbFieldType) -> ArchDbType {
    match field_type {
        DbFieldType::String => ArchDbType::ScalarString,
        DbFieldType::Short => ArchDbType::ScalarShort,
        DbFieldType::Float => ArchDbType::ScalarFloat,
        DbFieldType::Enum => ArchDbType::ScalarEnum,
        DbFieldType::Char => ArchDbType::ScalarByte,
        DbFieldType::Long => ArchDbType::ScalarInt,
        DbFieldType::Double => ArchDbType::ScalarDouble,
    }
}

/// Convert epics-base-rs EpicsValue to archiver ArchiverValue.
fn epics_value_to_archiver(val: &EpicsValue) -> ArchiverValue {
    match val {
        EpicsValue::String(s) => ArchiverValue::ScalarString(s.clone()),
        EpicsValue::Short(v) => ArchiverValue::ScalarShort(*v as i32),
        EpicsValue::Float(v) => ArchiverValue::ScalarFloat(*v),
        EpicsValue::Enum(v) => ArchiverValue::ScalarEnum(*v as i32),
        EpicsValue::Char(v) => ArchiverValue::ScalarByte(vec![*v]),
        EpicsValue::Long(v) => ArchiverValue::ScalarInt(*v),
        EpicsValue::Double(v) => ArchiverValue::ScalarDouble(*v),
        EpicsValue::ShortArray(v) => {
            ArchiverValue::VectorShort(v.iter().map(|x| *x as i32).collect())
        }
        EpicsValue::FloatArray(v) => ArchiverValue::VectorFloat(v.clone()),
        EpicsValue::EnumArray(v) => {
            ArchiverValue::VectorEnum(v.iter().map(|x| *x as i32).collect())
        }
        EpicsValue::DoubleArray(v) => ArchiverValue::VectorDouble(v.clone()),
        EpicsValue::LongArray(v) => ArchiverValue::VectorInt(v.clone()),
        EpicsValue::CharArray(v) => ArchiverValue::VectorChar(v.clone()),
        EpicsValue::StringArray(v) => ArchiverValue::VectorString(v.clone()),
    }
}

/// Background writer task — drains samples and writes to storage.
pub async fn write_loop(
    storage: Arc<dyn StoragePlugin>,
    registry: Arc<PvRegistry>,
    mut rx: mpsc::Receiver<PvSample>,
    mut shutdown: tokio::sync::watch::Receiver<bool>,
    flush_period: Duration,
) {
    info!("Write loop started");
    // HashMap keyed by PV name → latest timestamp. Avoids duplicate entries
    // when the same PV receives many samples between flushes, and feeds
    // the per-PV out-of-order detector below.
    let mut ts_updates: std::collections::HashMap<String, SystemTime> =
        std::collections::HashMap::new();
    // Last-observed (registry-record) DBR type per PV, used to detect
    // mid-stream type changes that we must drop to avoid corrupting the
    // PB partition.
    let mut last_dbr_type: std::collections::HashMap<String, ArchDbType> =
        std::collections::HashMap::new();
    let mut last_flush = std::time::Instant::now();

    loop {
        tokio::select! {
            Some(pv_sample) = rx.recv() => {
                let ts = pv_sample.sample.timestamp;

                // Out-of-order timestamp drop. Storage requires monotonic
                // appends per PV; an older timestamp would produce a
                // corrupt partition.
                if let Some(prev_ts) = ts_updates.get(&pv_sample.pv_name)
                    && ts < *prev_ts
                {
                    if let Some(ref c) = pv_sample.counters {
                        c.timestamp_drops.fetch_add(1, Ordering::Relaxed);
                    }
                    debug!(
                        pv = pv_sample.pv_name,
                        ?ts,
                        ?prev_ts,
                        "Dropping out-of-order sample"
                    );
                    continue;
                }

                // Type-change drop. The first sample defines the PV's
                // wire type; later samples with a different DBR get
                // dropped (operator must changeTypeForPV first).
                let prev_type = last_dbr_type
                    .insert(pv_sample.pv_name.clone(), pv_sample.dbr_type);
                if let Some(prev) = prev_type
                    && prev != pv_sample.dbr_type
                {
                    if let Some(ref c) = pv_sample.counters {
                        c.type_change_drops.fetch_add(1, Ordering::Relaxed);
                        // Java parity (9f2234f): record the latest observed
                        // DBR so the dropped-events report can show what
                        // the IOC is now sending vs the archived type.
                        c.latest_observed_dbr
                            .store(pv_sample.dbr_type as i32, Ordering::Relaxed);
                    }
                    debug!(
                        pv = pv_sample.pv_name,
                        ?prev,
                        new = ?pv_sample.dbr_type,
                        "Dropping type-changed sample"
                    );
                    // Restore prev_type in the map so a single
                    // mismatched sample doesn't permanently flip our
                    // recorded type.
                    last_dbr_type.insert(pv_sample.pv_name.clone(), prev);
                    continue;
                }

                let meta = AppendMeta {
                    element_count: pv_sample.element_count,
                    ..Default::default()
                };
                if let Err(e) = storage
                    .append_event_with_meta(
                        &pv_sample.pv_name,
                        pv_sample.dbr_type,
                        &pv_sample.sample,
                        &meta,
                    )
                    .await
                {
                    error!(pv = pv_sample.pv_name, "Write error: {e}");
                } else {
                    metrics::counter!("archiver_events_stored_total").increment(1);
                    if let Some(ref c) = pv_sample.counters {
                        c.events_stored.fetch_add(1, Ordering::Relaxed);
                    }
                    ts_updates.insert(pv_sample.pv_name, ts);
                }

                // Periodic flush: timestamps to SQLite + buffered writes to disk.
                if last_flush.elapsed() > flush_period && !ts_updates.is_empty() {
                    let refs: Vec<(&str, SystemTime)> = ts_updates
                        .iter()
                        .map(|(name, ts)| (name.as_str(), *ts))
                        .collect();
                    if let Err(e) = registry.batch_update_timestamps(&refs) {
                        error!("Failed to flush timestamps: {e}");
                    }
                    if let Err(e) = storage.flush_writes().await {
                        error!("Failed to flush storage writes: {e}");
                    }
                    ts_updates.clear();
                    last_flush = std::time::Instant::now();
                }
            }
            _ = shutdown.changed() => {
                // Drain remaining samples.
                while let Ok(pv_sample) = rx.try_recv() {
                    let meta = AppendMeta {
                        element_count: pv_sample.element_count,
                        ..Default::default()
                    };
                    if let Err(e) = storage
                        .append_event_with_meta(
                            &pv_sample.pv_name,
                            pv_sample.dbr_type,
                            &pv_sample.sample,
                            &meta,
                        )
                        .await
                    {
                        warn!(pv = pv_sample.pv_name, "Shutdown drain write error: {e}");
                    }
                }
                // Final flush: timestamps + buffered writes.
                if !ts_updates.is_empty() {
                    let refs: Vec<(&str, SystemTime)> = ts_updates
                        .iter()
                        .map(|(name, ts)| (name.as_str(), *ts))
                        .collect();
                    if let Err(e) = registry.batch_update_timestamps(&refs) {
                        warn!("Shutdown timestamp flush failed: {e}");
                    }
                }
                if let Err(e) = storage.flush_writes().await {
                    warn!("Shutdown storage flush failed: {e}");
                }
                info!("Write loop shutting down");
                break;
            }
        }
    }
}
