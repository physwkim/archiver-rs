use std::sync::atomic::{AtomicI32, AtomicI64, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, SystemTime};

use dashmap::DashMap;
use epics_rs::base::server::snapshot::DbrClass;
use epics_rs::base::types::{DbFieldType, EpicsValue};
use epics_rs::ca::client::{CaChannel, CaClient, ConnectionEvent};
use epics_rs::pva::client_native::PvaClient;
use epics_rs::pva::pvdata::{PvField, ScalarValue};
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;
use tracing::{debug, error, info, warn};

use archiver_core::registry::{Protocol, PvRecord, PvRegistry, PvStatus, SampleMode};
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
    /// Number of DBR_CTRL metadata refresh attempts that failed (timeout,
    /// transport error, missing display info). Operators looking at PVs
    /// with empty PREC/EGU should check this.
    pub metadata_fetch_failures: AtomicU64,
    /// Number of `storage.append_event_with_meta` calls that exceeded
    /// the per-sample storage timeout. Distinct from
    /// `buffer_overflow_drops` (mpsc channel saturation) so an
    /// operator can tell "the storage tier is wedged" apart from
    /// "the writer can't keep up with the producer".
    pub storage_append_timeouts: AtomicU64,
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
            metadata_fetch_failures: AtomicU64::new(0),
            storage_append_timeouts: AtomicU64::new(0),
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
    pub metadata_fetch_failures: u64,
    pub storage_append_timeouts: u64,
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
            metadata_fetch_failures: c.metadata_fetch_failures.load(Ordering::Relaxed),
            storage_append_timeouts: c.storage_append_timeouts.load(Ordering::Relaxed),
        }
    }
}

/// Handle for a running PV archiving task.
struct PvHandle {
    /// `Some` for CA-acquired PVs (used by `try_get_value` for the
    /// live-value RPC); `None` for PVA-acquired PVs since PVA exposes
    /// no Clone-able channel handle — live_value for those routes
    /// through `pva_client` directly.
    channel: Option<CaChannel>,
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

/// Manages EPICS Channel Access + pvAccess connections and dispatches
/// archived samples to storage.
pub struct ChannelManager {
    /// The CA client context.
    ca_client: CaClient,
    /// The PVA client context (lazily used only when a PV's registry
    /// row carries `Protocol::Pva`).
    pva_client: PvaClient,
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
        let pva_client = PvaClient::new().map_err(|e| anyhow::anyhow!("{e}"))?;
        let (tx, rx) = mpsc::channel(SAMPLE_CHANNEL_CAPACITY);

        let mgr = Self {
            ca_client,
            pva_client,
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
    pub async fn archive_pv(
        &self,
        pv_name: &str,
        sample_mode: &SampleMode,
        protocol: Protocol,
    ) -> anyhow::Result<()> {
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

        match protocol {
            Protocol::Ca => self.archive_pv_inner(pv_name, sample_mode).await,
            Protocol::Pva => self.archive_pv_inner_pva(pv_name, sample_mode).await,
        }
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

        // Register in SQLite. (CA path — PVA acquisition uses a separate
        // entry point, so this branch is always Protocol::Ca.)
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

    /// PVA equivalent of [`Self::archive_pv_inner`]. Connects via the
    /// pvAccess client, infers archive type from a one-shot pvget, then
    /// hands off to a `monitor_loop_pva` that mirrors the CA monitor
    /// loop's lifecycle (samples → write_loop, cancel_token shutdown).
    async fn archive_pv_inner_pva(
        &self,
        pv_name: &str,
        sample_mode: &SampleMode,
    ) -> anyhow::Result<()> {
        if self.channels.contains_key(pv_name) {
            anyhow::bail!("PV {pv_name} is already being archived");
        }

        // Apply policy override (same surface as CA path).
        let (effective_mode, matched_policy_name) = if let Some(ref policy) = self.policy {
            if let Some(p) = policy.find_policy(pv_name) {
                (p.to_sample_mode(), Some(p.policy_name().to_string()))
            } else {
                (sample_mode.clone(), None)
            }
        } else {
            (sample_mode.clone(), None)
        };

        // Connect + introspect: a one-shot pvget tells us the value
        // shape. We rely on it instead of a `cainfo` analogue because
        // PVA channels carry their type only via fetched data.
        let connect = tokio::time::timeout(
            CA_CONNECT_TIMEOUT,
            self.pva_client.pvconnect(pv_name),
        )
        .await
        .map_err(|_| anyhow::anyhow!("PVA connect to {pv_name} timed out"))?
        .map_err(|e| anyhow::anyhow!("Failed to connect to {pv_name} via PVA: {e}"))?;
        debug!(pv = pv_name, server = %connect, "PVA channel connected");

        let initial = tokio::time::timeout(CA_CONNECT_TIMEOUT, self.pva_client.pvget(pv_name))
            .await
            .map_err(|_| anyhow::anyhow!("PVA pvget for {pv_name} timed out"))?
            .map_err(|e| anyhow::anyhow!("Failed to pvget {pv_name}: {e}"))?;
        let (dbr_type, element_count) = pv_field_to_arch_db_type(&initial).ok_or_else(|| {
            anyhow::anyhow!(
                "PV {pv_name}: PVA value is not a scalar/scalar-array; structured types not yet supported"
            )
        })?;

        // Register with Pva flag.
        self.registry.register_pv_with_protocol(
            pv_name,
            dbr_type,
            &effective_mode,
            element_count,
            Protocol::Pva,
        )?;
        if let Some(ref name) = matched_policy_name {
            self.registry.update_policy_name(pv_name, Some(name))?;
        }
        // Persist PREC/EGU extracted from the same pvget — equivalent
        // to the CA path's DBR_CTRL refresh. Non-fatal on error.
        let (prec, egu) = pv_field_extract_display(&initial);
        if (prec.is_some() || egu.is_some())
            && let Err(e) =
                self.registry
                    .update_metadata(pv_name, prec.as_deref(), egu.as_deref())
        {
            debug!(pv = pv_name, "Failed to persist PVA PREC/EGU: {e}");
        }

        let record = self
            .registry
            .get_pv(pv_name)?
            .ok_or_else(|| anyhow::anyhow!("PV {pv_name} not found in registry"))?;
        self.start_archiving_internal(&record).await?;

        metrics::gauge!("archiver_pvs_active").increment(1.0);
        info!(
            pv = pv_name,
            ?dbr_type,
            element_count,
            protocol = "pva",
            "Started archiving"
        );
        Ok(())
    }

    /// Internal: start a subscription for a PV record. Routes by
    /// `record.protocol`; the CA branch keeps the historical behaviour,
    /// the PVA branch goes through `start_archiving_internal_pva`.
    async fn start_archiving_internal(&self, record: &PvRecord) -> anyhow::Result<()> {
        if record.protocol == Protocol::Pva {
            return self.start_archiving_internal_pva(record).await;
        }
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
                channel: Some(channel.clone()),
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
        let registry_for_loop = self.registry.clone();

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
                        registry_for_loop,
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
                        registry_for_loop,
                        extras_for_loop,
                        counters_for_loop,
                    )
                    .await;
                });
            }
        }

        Ok(())
    }

    /// PVA equivalent of `start_archiving_internal`. Spawns a single
    /// callback-driven monitor task; pvAccess auto-reconnect inside
    /// `pvmonitor_handle` removes the explicit reconnect loop the CA
    /// path needs. PVA records use `channel: None` on the [`PvHandle`]
    /// and skip per-field "extras" subscriptions (`.HIHI`/`.LOLO`/…)
    /// since pvAccess wraps those in the NTScalar value structure
    /// rather than separate channels.
    async fn start_archiving_internal_pva(&self, record: &PvRecord) -> anyhow::Result<()> {
        let pv_name = record.pv_name.clone();
        let dbr_type = record.dbr_type;
        let element_count = record.element_count;
        let cancel_token = CancellationToken::new();
        let conn_info = Arc::new(Mutex::new(ConnectionInfo::default()));
        let extras: Arc<ExtraFieldsCache> = Arc::new(DashMap::new());
        let field_tokens: Arc<DashMap<String, CancellationToken>> = Arc::new(DashMap::new());
        let update_lock = Arc::new(tokio::sync::Mutex::new(()));
        let counters = Arc::new(PvCounters::default());

        self.channels.insert(
            pv_name.clone(),
            PvHandle {
                channel: None,
                cancel_token: cancel_token.clone(),
                dbr_type,
                conn_info: conn_info.clone(),
                extras: extras.clone(),
                field_tokens: field_tokens.clone(),
                update_lock,
                counters: counters.clone(),
            },
        );

        let tx = self.sample_tx.clone();
        let pva_client = self.pva_client.clone();
        let token = cancel_token.clone();
        let ci = conn_info.clone();
        let counters_for_loop = counters.clone();
        let drift = self.server_ioc_drift_secs;

        match &record.sample_mode {
            SampleMode::Monitor => {
                let pv_name_loop = pv_name.clone();
                let archive_fields_loop = record.archive_fields.clone();
                let extras_for_loop = extras.clone();
                tokio::spawn(async move {
                    monitor_loop_pva(
                        pv_name_loop,
                        dbr_type,
                        element_count,
                        pva_client,
                        tx,
                        token,
                        ci,
                        counters_for_loop,
                        drift,
                        archive_fields_loop,
                        extras_for_loop,
                    )
                    .await;
                });
            }
            SampleMode::Scan { period_secs } => {
                let period = *period_secs;
                let pv_name_loop = pv_name.clone();
                let archive_fields_loop = record.archive_fields.clone();
                let extras_for_loop = extras.clone();
                tokio::spawn(async move {
                    scan_loop_pva(
                        pv_name_loop,
                        dbr_type,
                        pva_client,
                        tx,
                        token,
                        period,
                        ci,
                        counters_for_loop,
                        drift,
                        archive_fields_loop,
                        extras_for_loop,
                    )
                    .await;
                });
            }
        }

        // Auxiliary periodic refreshers: keep PREC/EGU current after
        // IOC restarts, and approximate disconnect events from a long
        // sample gap. Both bind to the same cancel_token so a stop /
        // delete reaps every PVA-side task in one go.
        {
            let pv_name = pv_name.clone();
            let pva_client = self.pva_client.clone();
            let registry = self.registry.clone();
            let cancel = cancel_token.clone();
            let counters_for_refresh = counters.clone();
            tokio::spawn(async move {
                pva_metadata_refresh_loop(
                    pv_name,
                    pva_client,
                    registry,
                    cancel,
                    counters_for_refresh,
                )
                .await;
            });
        }
        {
            let pv_name = pv_name.clone();
            let conn_info = conn_info.clone();
            let counters_for_watch = counters.clone();
            let cancel = cancel_token.clone();
            let sample_mode = record.sample_mode.clone();
            tokio::spawn(async move {
                pva_state_watchdog(
                    pv_name,
                    conn_info,
                    counters_for_watch,
                    cancel,
                    sample_mode,
                )
                .await;
            });
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
        let channel_opt = self.channels.get(pv_name)?.channel.clone();
        let Some(channel) = channel_opt else {
            // PVA-acquired PV — live_value via pva_client.pvget.
            let pva = self.pva_client.clone();
            let name = pv_name.to_string();
            let res = tokio::time::timeout(timeout, pva.pvget(&name)).await;
            return Some(match res {
                Ok(Ok(field)) => pv_field_scalar_to_archiver(&field)
                    .ok_or_else(|| anyhow::anyhow!("PVA value not a scalar")),
                Ok(Err(e)) => Err(anyhow::anyhow!("PVA get failed: {e}")),
                Err(_) => Err(anyhow::anyhow!("PVA get timed out after {timeout:?}")),
            });
        };
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

/// Read DBR_CTRL metadata from the channel and persist `precision`/`units`
/// to the registry when they differ from stored values. Best-effort: any
/// failure (timeout, transport error, missing display info, non-numeric
/// type) is logged at debug and silently ignored. Skips the SQL write
/// entirely when the in-memory record already matches, so reconnect
/// storms don't churn the database.
async fn refresh_ctrl_metadata(
    channel: &CaChannel,
    registry: &PvRegistry,
    pv_name: &str,
    counters: &PvCounters,
) {
    // 15s — long enough for slow-network IOCs (Java's default `epicsTimeout`
    // is 30s; 15s splits the difference). Failures count toward
    // `metadata_fetch_failures` so operators can spot PVs that never get
    // PREC/EGU populated.
    const FETCH_TIMEOUT: Duration = Duration::from_secs(15);
    let snapshot = match tokio::time::timeout(
        FETCH_TIMEOUT,
        channel.get_with_metadata(DbrClass::Ctrl),
    )
    .await
    {
        Ok(Ok(s)) => s,
        Ok(Err(e)) => {
            counters
                .metadata_fetch_failures
                .fetch_add(1, Ordering::Relaxed);
            debug!(pv = pv_name, "Ctrl metadata fetch failed: {e}");
            return;
        }
        Err(_) => {
            counters
                .metadata_fetch_failures
                .fetch_add(1, Ordering::Relaxed);
            debug!(pv = pv_name, "Ctrl metadata fetch timed out");
            return;
        }
    };
    let Some(display) = snapshot.display else {
        // Non-numeric type (string, enum, …) — DBR_CTRL has no
        // DisplayInfo. Not a failure; just nothing to persist.
        return;
    };

    // Negative precision is the Java EAA "no precision info" sentinel —
    // persisting "-1" as the PREC string would surface as a literal -1
    // in the UI / API. Treat it the same as missing.
    let new_prec_opt: Option<String> = if display.precision < 0 {
        None
    } else {
        Some(display.precision.to_string())
    };
    let new_egu_trimmed = display.units.trim();
    let new_egu_opt: Option<&str> = if new_egu_trimmed.is_empty() {
        None
    } else {
        Some(new_egu_trimmed)
    };

    let stored = match registry.get_pv(pv_name) {
        Ok(Some(r)) => r,
        Ok(None) => return,
        Err(e) => {
            debug!(pv = pv_name, "Registry read for metadata compare failed: {e}");
            return;
        }
    };

    let prec_changed = match (stored.prec.as_deref(), new_prec_opt.as_deref()) {
        (Some(s), Some(n)) => s != n,
        (None, Some(_)) => true,
        // Don't overwrite a populated PREC with the "no info" sentinel.
        _ => false,
    };
    let egu_changed = match (stored.egu.as_deref(), new_egu_opt) {
        (Some(s), Some(n)) => s != n,
        (None, Some(_)) => true,
        // Don't overwrite a populated EGU with empty, and don't
        // bother writing None over None.
        _ => false,
    };
    if !prec_changed && !egu_changed {
        return;
    }

    let prec_arg = if prec_changed {
        new_prec_opt.as_deref()
    } else {
        None
    };
    let egu_arg = if egu_changed { new_egu_opt } else { None };
    if let Err(e) = registry.update_metadata(pv_name, prec_arg, egu_arg) {
        warn!(pv = pv_name, "Failed to persist PREC/EGU: {e}");
    } else {
        debug!(
            pv = pv_name,
            prec = ?prec_arg, egu = ?egu_arg,
            "Refreshed PREC/EGU from DBR_CTRL"
        );
    }
}

/// Body shared by the two PVA monitor callback variants
/// (`pvmonitor_handle` takes `(FieldDesc, PvField)`,
/// `pvmonitor_with_request` takes `(PvField)`). Lifted out so we
/// can write the once-per-event logic in one place.
#[allow(clippy::too_many_arguments)]
fn pva_handle_event(
    field: &PvField,
    pv_name: &str,
    dbr_type: ArchDbType,
    tx: &mpsc::Sender<PvSample>,
    conn_info: &Mutex<ConnectionInfo>,
    counters: &Arc<PvCounters>,
    extras: &ExtraFieldsCache,
    extras_paths: &[(String, &'static str)],
    drift_secs: u64,
) {
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
    if first_after_connect {
        counters
            .first_event_unix_secs
            .compare_exchange(0, unix_secs(now), Ordering::Relaxed, Ordering::Relaxed)
            .ok();
    }
    counters.events_received.fetch_add(1, Ordering::Relaxed);

    let Some(value) = pv_field_scalar_to_archiver(field) else {
        counters.type_change_drops.fetch_add(1, Ordering::Relaxed);
        debug!(pv = pv_name, "PVA event has non-scalar value; dropping");
        return;
    };

    let ts = pv_field_extract_timestamp(field);
    if !first_after_connect && !ioc_timestamp_in_window(ts, now, drift_secs) {
        counters.timestamp_drops.fetch_add(1, Ordering::Relaxed);
        debug!(pv = pv_name, ?ts, "Dropping PVA sample with out-of-window timestamp");
        return;
    }
    let elem_count = match pv_field_element_count(field) {
        0 => 1,
        n => n,
    };
    for (field_name, path) in extras_paths {
        if let Some(PvField::Scalar(s)) = pv_field_walk_path(field, path) {
            extras.insert(field_name.clone(), scalar_value_to_string(s));
        }
    }
    let mut sample = ArchiverSample::new(ts, value);
    attach_extras(extras, &mut sample);
    let pv_sample = PvSample {
        pv_name: pv_name.to_string(),
        dbr_type,
        sample,
        element_count: Some(elem_count),
        counters: Some(counters.clone()),
    };
    if let Err(tokio::sync::mpsc::error::TrySendError::Full(_)) = tx.try_send(pv_sample) {
        counters.buffer_overflow_drops.fetch_add(1, Ordering::Relaxed);
    }
}

/// PVA monitor loop: subscribes once and parks until cancellation.
/// Each fan-in event is decoded inline, packaged as a [`PvSample`],
/// and non-blocking-pushed into the storage write_loop's channel —
/// blocking would stall the pvAccess reactor thread.
///
/// When `archive_fields` is non-empty, the subscription requests an
/// explicit pvRequest with the mapped sub-field paths so the IOC
/// includes them in every monitor event; the callback then mirrors
/// the CA path's "extras" cache by stringifying each requested field.
#[allow(clippy::too_many_arguments)]
async fn monitor_loop_pva(
    pv_name: String,
    dbr_type: ArchDbType,
    element_count: i32,
    pva_client: PvaClient,
    tx: mpsc::Sender<PvSample>,
    cancel_token: CancellationToken,
    conn_info: Arc<Mutex<ConnectionInfo>>,
    counters: Arc<PvCounters>,
    server_ioc_drift_secs: u64,
    archive_fields: Vec<String>,
    extras: Arc<ExtraFieldsCache>,
) {
    let drift_secs = server_ioc_drift_secs;
    // Static element_count is unused — each callback derives the
    // current array length from the live PvField, so an NTScalarArray
    // whose size changes between events still gets tagged correctly.
    let _ = element_count;

    // Build the (CA name, PVA path) pairs once. Skip CA fields with
    // no PVA equivalent so a misconfigured archive_fields list
    // doesn't poison the pvRequest.
    let extras_paths: Vec<(String, &'static str)> = archive_fields
        .iter()
        .filter_map(|f| ca_archive_field_to_pva_path(f).map(|p| (f.clone(), p)))
        .collect();
    let request_expr = if extras_paths.is_empty() {
        None
    } else {
        let mut builder = epics_rs::pva::pv_request::PvRequestBuilder::new()
            .field("value")
            .field("alarm")
            .field("timeStamp");
        for (_, path) in &extras_paths {
            builder = builder.field(*path);
        }
        Some(builder.build())
    };

    // Subscribe loop with retry. The custom-request path uses
    // `pvmonitor_with_request` (no SubscriptionHandle) inside a
    // tokio::select! so cancellation drops the future cleanly; the
    // empty-request path keeps the original SubscriptionHandle form
    // since that's the simpler path for the common case.
    loop {
        if let Some(ref req) = request_expr {
            // Custom-request path: 1-arg callback. pvmonitor_with_request
            // returns a future that runs to completion; race it against
            // the cancel signal.
            let pv_name_cb = pv_name.clone();
            let tx_cb = tx.clone();
            let conn_info_cb = conn_info.clone();
            let counters_cb = counters.clone();
            let extras_cb = extras.clone();
            let extras_paths_cb = extras_paths.clone();
            let cb = move |field: &PvField| {
                pva_handle_event(
                    field,
                    &pv_name_cb,
                    dbr_type,
                    &tx_cb,
                    &conn_info_cb,
                    &counters_cb,
                    &extras_cb,
                    &extras_paths_cb,
                    drift_secs,
                );
            };
            tokio::select! {
                _ = cancel_token.cancelled() => {
                    debug!(pv = pv_name, "PVA monitor (custom request) cancelled");
                    return;
                }
                res = pva_client.pvmonitor_with_request(&pv_name, req, cb) => {
                    match res {
                        Ok(()) => {
                            debug!(pv = pv_name, "PVA pvmonitor_with_request returned Ok; resubscribing");
                        }
                        Err(e) => {
                            counters
                                .transient_error_count
                                .fetch_add(1, Ordering::Relaxed);
                            warn!(pv = pv_name, "PVA pvmonitor_with_request failed: {e}; retrying");
                        }
                    }
                    tokio::select! {
                        _ = cancel_token.cancelled() => return,
                        _ = tokio::time::sleep(CA_RETRY_DELAY) => continue,
                    }
                }
            }
        } else {
            // Empty-request fast path: 2-arg callback. Keep the
            // SubscriptionHandle so the inner reactor task lives
            // until our explicit drop.
            let pv_name_cb = pv_name.clone();
            let tx_cb = tx.clone();
            let conn_info_cb = conn_info.clone();
            let counters_cb = counters.clone();
            let extras_cb = extras.clone();
            let extras_paths_cb = extras_paths.clone();
            let cb = move |_desc: &epics_rs::pva::pvdata::FieldDesc, field: &PvField| {
                pva_handle_event(
                    field,
                    &pv_name_cb,
                    dbr_type,
                    &tx_cb,
                    &conn_info_cb,
                    &counters_cb,
                    &extras_cb,
                    &extras_paths_cb,
                    drift_secs,
                );
            };
            let handle = match pva_client.pvmonitor_handle(&pv_name, cb).await {
                Ok(h) => h,
                Err(e) => {
                    counters
                        .transient_error_count
                        .fetch_add(1, Ordering::Relaxed);
                    warn!(pv = pv_name, "PVA pvmonitor failed: {e}; retrying");
                    tokio::select! {
                        _ = cancel_token.cancelled() => return,
                        _ = tokio::time::sleep(CA_RETRY_DELAY) => continue,
                    }
                }
            };
            debug!(pv = pv_name, "PVA monitor active");
            cancel_token.cancelled().await;
            debug!(pv = pv_name, "PVA monitor cancelled; dropping subscription");
            drop(handle);
            return;
        }
    }
}

/// PVA Scan loop: periodic `pvget` instead of streaming monitor.
/// Mirrors the CA scan_loop's per-tick connection check + sample emit.
#[allow(clippy::too_many_arguments)]
async fn scan_loop_pva(
    pv_name: String,
    dbr_type: ArchDbType,
    pva_client: PvaClient,
    tx: mpsc::Sender<PvSample>,
    cancel_token: CancellationToken,
    period_secs: f64,
    conn_info: Arc<Mutex<ConnectionInfo>>,
    counters: Arc<PvCounters>,
    server_ioc_drift_secs: u64,
    archive_fields: Vec<String>,
    extras: Arc<ExtraFieldsCache>,
) {
    let extras_paths: Vec<(String, &'static str)> = archive_fields
        .iter()
        .filter_map(|f| ca_archive_field_to_pva_path(f).map(|p| (f.clone(), p)))
        .collect();
    let period = Duration::from_secs_f64(period_secs);
    let mut interval = tokio::time::interval(period);
    let drift_secs = server_ioc_drift_secs;
    // Hard timeout per pvget — at most one tick of slack so a stuck
    // PVA server can't accumulate pending requests.
    let pvget_timeout = period.max(Duration::from_secs(5));

    loop {
        tokio::select! {
            _ = cancel_token.cancelled() => return,
            _ = interval.tick() => {}
        }

        let res = tokio::time::timeout(pvget_timeout, pva_client.pvget(&pv_name)).await;
        let field = match res {
            Ok(Ok(f)) => f,
            Ok(Err(e)) => {
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
                counters
                    .transient_error_count
                    .fetch_add(1, Ordering::Relaxed);
                debug!(pv = pv_name, "PVA scan pvget failed: {e}");
                continue;
            }
            Err(_) => {
                counters
                    .transient_error_count
                    .fetch_add(1, Ordering::Relaxed);
                debug!(pv = pv_name, "PVA scan pvget timed out");
                continue;
            }
        };

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
        if first_after_connect {
            counters
                .first_event_unix_secs
                .compare_exchange(0, unix_secs(now), Ordering::Relaxed, Ordering::Relaxed)
                .ok();
        }

        let Some(value) = pv_field_scalar_to_archiver(&field) else {
            counters.type_change_drops.fetch_add(1, Ordering::Relaxed);
            debug!(pv = pv_name, "PVA scan returned non-scalar value; dropping");
            continue;
        };
        // Scan mode: timestamp the sample at receive time (no IOC
        // timestamp available reliably for periodic pvget, mirroring
        // CA scan_loop's `now` policy).
        let _ = drift_secs; // referenced for symmetry; not used here
        let elem_count = match pv_field_element_count(&field) {
            0 => 1,
            n => n,
        };
        // Refresh extras from this scan's pvget result (the pvget
        // returns the full NTScalar so all sub-fields are available
        // for free — no extra channel spawn needed).
        for (field_name, path) in &extras_paths {
            if let Some(PvField::Scalar(s)) = pv_field_walk_path(&field, path) {
                extras.insert(field_name.clone(), scalar_value_to_string(s));
            }
        }
        let mut sample = ArchiverSample::new(now, value);
        attach_extras(&extras, &mut sample);
        let pv_sample = PvSample {
            pv_name: pv_name.clone(),
            dbr_type,
            sample,
            element_count: Some(elem_count),
            counters: Some(counters.clone()),
        };
        if let Err(rejected) =
            try_send_with_overflow_count(&tx, pv_sample, &counters).await
        {
            // Channel closed (write_loop down). Cooperative shutdown.
            let _ = rejected;
            return;
        }
    }
}

/// Periodic refresh task for PVA-acquired PVs: re-fetches DBR_CTRL-
/// equivalent metadata (`display.units`, `display.precision`) every
/// few minutes and persists when changed. PVA's monitor callback API
/// doesn't surface explicit reconnect events the way CA's
/// `ConnectionEvent` does, so a dedicated periodic poll is the
/// simplest way to keep PREC/EGU fresh after IOC restarts.
async fn pva_metadata_refresh_loop(
    pv_name: String,
    pva_client: PvaClient,
    registry: Arc<PvRegistry>,
    cancel_token: CancellationToken,
    counters: Arc<PvCounters>,
) {
    // 5 minutes — operators almost never change PREC/EGU at runtime;
    // shorter cadence wastes registry/network and longer leaves UI
    // stale across IOC restarts. Same magic number Java EAA uses
    // for its `metaFieldsRefresh` cron.
    const REFRESH_INTERVAL: Duration = Duration::from_secs(5 * 60);
    const FETCH_TIMEOUT: Duration = Duration::from_secs(15);
    let mut tick = tokio::time::interval(REFRESH_INTERVAL);
    // Skip the immediate first tick — initial pvget already populated
    // PREC/EGU at archive_pv time.
    tick.tick().await;
    loop {
        tokio::select! {
            _ = cancel_token.cancelled() => return,
            _ = tick.tick() => {}
        }

        let res = tokio::time::timeout(FETCH_TIMEOUT, pva_client.pvget(&pv_name)).await;
        let field = match res {
            Ok(Ok(f)) => f,
            _ => {
                counters
                    .metadata_fetch_failures
                    .fetch_add(1, Ordering::Relaxed);
                continue;
            }
        };
        let (new_prec, new_egu) = pv_field_extract_display(&field);
        let stored = match registry.get_pv(&pv_name) {
            Ok(Some(r)) => r,
            _ => continue,
        };
        let prec_changed = match (stored.prec.as_deref(), new_prec.as_deref()) {
            (Some(s), Some(n)) => s != n,
            (None, Some(_)) => true,
            _ => false,
        };
        let egu_changed = match (stored.egu.as_deref(), new_egu.as_deref()) {
            (Some(s), Some(n)) => s != n,
            (None, Some(_)) => true,
            _ => false,
        };
        if !prec_changed && !egu_changed {
            continue;
        }
        let prec_arg = if prec_changed { new_prec.as_deref() } else { None };
        let egu_arg = if egu_changed { new_egu.as_deref() } else { None };
        if let Err(e) = registry.update_metadata(&pv_name, prec_arg, egu_arg) {
            warn!(pv = pv_name, "Failed to persist PVA PREC/EGU: {e}");
        } else {
            debug!(pv = pv_name, prec = ?prec_arg, egu = ?egu_arg, "Refreshed PVA display");
        }
    }
}

/// State watchdog for PVA-acquired PVs: a dedicated task that flips
/// `conn_info.state` to `Disconnected` when no event has arrived
/// within `STALE_THRESHOLD`. PVA's callback API doesn't surface
/// explicit channel-state changes — we approximate by treating a
/// long event gap as a disconnect. Trade-off: a genuinely silent PV
/// (alarm-only, low-rate scan) appears disconnected. Operators can
/// raise the threshold by env var if their PV cadence is slower.
async fn pva_state_watchdog(
    pv_name: String,
    conn_info: Arc<Mutex<ConnectionInfo>>,
    counters: Arc<PvCounters>,
    cancel_token: CancellationToken,
    sample_mode: SampleMode,
) {
    const POLL_INTERVAL: Duration = Duration::from_secs(5);
    // Threshold scales with the expected event cadence so a slow Scan
    // PV (period=300 s) doesn't flap on every tick. For Monitor mode
    // the floor is 60 s — most production PVs change at least that
    // often. Operators who archive truly silent PVs (alarm-only) will
    // see stale disconnected status; that's a known limitation.
    let stale_threshold = match sample_mode {
        SampleMode::Monitor => Duration::from_secs(60),
        SampleMode::Scan { period_secs } => {
            Duration::from_secs_f64((period_secs * 3.0).max(60.0))
        }
    };
    let mut interval = tokio::time::interval(POLL_INTERVAL);
    interval.tick().await;
    loop {
        tokio::select! {
            _ = cancel_token.cancelled() => return,
            _ = interval.tick() => {}
        }
        let stale_now = {
            let ci = conn_info.lock().unwrap_or_else(|e| e.into_inner());
            if !ci.is_connected {
                continue;
            }
            match ci.last_event_time {
                Some(t) => SystemTime::now()
                    .duration_since(t)
                    .map(|d| d > stale_threshold)
                    .unwrap_or(false),
                None => false,
            }
        };
        if stale_now {
            let was_connected = {
                let mut ci = conn_info.lock().unwrap_or_else(|e| e.into_inner());
                let prev = ci.is_connected;
                ci.is_connected = false;
                ci.state = PvConnectionState::Disconnected;
                prev
            };
            if was_connected {
                counters.disconnect_count.fetch_add(1, Ordering::Relaxed);
                counters
                    .last_disconnect_unix_secs
                    .store(unix_secs(SystemTime::now()), Ordering::Relaxed);
                debug!(
                    pv = pv_name,
                    "PVA watchdog: marking disconnected after stale heartbeat"
                );
            }
        }
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
    registry: Arc<PvRegistry>,
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

        // Refresh DBR_CTRL metadata once per (re)connect so the registry's
        // PREC/EGU stay in sync with the IOC. Fire-and-forget so a slow
        // CA stack can't gate `subscribe()` (and therefore the first
        // sample) on a metadata round-trip. The spawned task is bound to
        // `cancel_token` so it terminates promptly when the PV is
        // stopped/deleted (otherwise a late metadata write could land on
        // a deleted-or-re-registered PV row).
        {
            let channel = channel.clone();
            let registry = registry.clone();
            let counters = counters.clone();
            let pv_name = pv_name.clone();
            let cancel = cancel_token.clone();
            tokio::spawn(async move {
                tokio::select! {
                    _ = cancel.cancelled() => {}
                    _ = refresh_ctrl_metadata(&channel, &registry, &pv_name, &counters) => {}
                }
            });
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
    registry: Arc<PvRegistry>,
    extras: Arc<ExtraFieldsCache>,
    counters: Arc<PvCounters>,
) {
    let period = Duration::from_secs_f64(period_secs);
    let mut interval = tokio::time::interval(period);
    // Reset on every detected disconnect so we re-fetch metadata after
    // each reconnect (mirrors monitor_loop's once-per-(re)connect cadence).
    let mut metadata_done = false;

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
            metadata_done = false;
            continue;
        }

        if !metadata_done {
            // Fire-and-forget; matches monitor_loop's once-per-(re)connect
            // semantics. The local `metadata_done` flag exists because
            // scan_loop's outer iteration is per-tick (vs monitor_loop's
            // per-reconnect), so we'd otherwise spawn a fetch every tick.
            // Bound to `cancel_token` so a stopped/deleted PV doesn't get
            // a stale metadata write from an in-flight task.
            let channel = channel.clone();
            let registry = registry.clone();
            let counters = counters.clone();
            let pv_name = pv_name.clone();
            let cancel = cancel_token.clone();
            tokio::spawn(async move {
                tokio::select! {
                    _ = cancel.cancelled() => {}
                    _ = refresh_ctrl_metadata(&channel, &registry, &pv_name, &counters) => {}
                }
            });
            metadata_done = true;
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
        EpicsValue::Int64(v) => v.to_string(),
        EpicsValue::Double(v) => v.to_string(),
        EpicsValue::ShortArray(v) => format!("{v:?}"),
        EpicsValue::FloatArray(v) => format!("{v:?}"),
        EpicsValue::EnumArray(v) => format!("{v:?}"),
        EpicsValue::DoubleArray(v) => format!("{v:?}"),
        EpicsValue::LongArray(v) => format!("{v:?}"),
        EpicsValue::Int64Array(v) => format!("{v:?}"),
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

/// Resolve the data-bearing sub-field of a PVA value. NTScalar /
/// NTScalarArray / NTEnum wrap the raw value in a `value` field; bare
/// scalars are passed through.
fn pv_value_field(field: &PvField) -> &PvField {
    match field {
        PvField::Structure(s) => s.get_field("value").unwrap_or(field),
        _ => field,
    }
}

/// Map a PVA `ScalarValue` to an archiver `ArchiverValue`.
fn scalar_value_to_archiver(s: &ScalarValue) -> ArchiverValue {
    match s {
        ScalarValue::Boolean(v) => ArchiverValue::ScalarEnum(*v as i32),
        ScalarValue::Byte(v) => ArchiverValue::ScalarByte(vec![*v as u8]),
        ScalarValue::UByte(v) => ArchiverValue::ScalarByte(vec![*v]),
        ScalarValue::Short(v) => ArchiverValue::ScalarShort(*v as i32),
        ScalarValue::UShort(v) => ArchiverValue::ScalarShort(*v as i32),
        ScalarValue::Int(v) => ArchiverValue::ScalarInt(*v),
        ScalarValue::UInt(v) => ArchiverValue::ScalarInt(*v as i32),
        ScalarValue::Long(v) => ArchiverValue::ScalarInt(*v as i32),
        ScalarValue::ULong(v) => ArchiverValue::ScalarInt(*v as i32),
        ScalarValue::Float(v) => ArchiverValue::ScalarFloat(*v),
        ScalarValue::Double(v) => ArchiverValue::ScalarDouble(*v),
        ScalarValue::String(s) => ArchiverValue::ScalarString(s.clone()),
    }
}

/// Extract a scalar `ArchiverValue` from a top-level PVA value (which
/// may be an NTScalar wrapper). Returns `None` for array/struct types
/// that don't reduce to a scalar.
fn pv_field_scalar_to_archiver(field: &PvField) -> Option<ArchiverValue> {
    match pv_value_field(field) {
        PvField::Scalar(s) => Some(scalar_value_to_archiver(s)),
        _ => None,
    }
}

/// Pick the `(ArchDbType, element_count)` to record at archive_pv time
/// from an NTScalar / NTScalarArray's introspection-pvget result.
/// Returns `None` for unsupported (struct / union / variant) shapes.
fn pv_field_to_arch_db_type(field: &PvField) -> Option<(ArchDbType, i32)> {
    let value = pv_value_field(field);
    Some(match value {
        PvField::Scalar(s) => (
            match s {
                ScalarValue::Boolean(_) => ArchDbType::ScalarEnum,
                ScalarValue::Byte(_) | ScalarValue::UByte(_) => ArchDbType::ScalarByte,
                ScalarValue::Short(_) | ScalarValue::UShort(_) => ArchDbType::ScalarShort,
                ScalarValue::Int(_)
                | ScalarValue::UInt(_)
                | ScalarValue::Long(_)
                | ScalarValue::ULong(_) => ArchDbType::ScalarInt,
                ScalarValue::Float(_) => ArchDbType::ScalarFloat,
                ScalarValue::Double(_) => ArchDbType::ScalarDouble,
                ScalarValue::String(_) => ArchDbType::ScalarString,
            },
            1,
        ),
        // Array variants — keep the corresponding scalar element type but
        // bump element_count. write_loop / PB encoder doesn't currently
        // serialise PVA arrays, so callers should reject these for now.
        PvField::ScalarArray(arr) => {
            let elem = arr.first()?;
            let inner = PvField::Scalar(elem.clone());
            let (t, _) = pv_field_to_arch_db_type(&inner)?;
            (t, arr.len() as i32)
        }
        _ => return None,
    })
}

/// Map a CA field name (`HIHI`, `LOLO`, `EGU`, …) to the dotted
/// pvAccess path under an NTScalar / NTScalarArray. Returns `None`
/// for fields that have no PVA equivalent — caller should drop them
/// silently rather than building an unsatisfiable pvRequest.
fn ca_archive_field_to_pva_path(field: &str) -> Option<&'static str> {
    Some(match field {
        // display sub-structure
        "EGU" => "display.units",
        "PREC" => "display.precision",
        "DESC" => "display.description",
        "HOPR" => "display.limitHigh",
        "LOPR" => "display.limitLow",
        // valueAlarm sub-structure (alarm thresholds)
        "HIHI" => "valueAlarm.highAlarmLimit",
        "LOLO" => "valueAlarm.lowAlarmLimit",
        "HIGH" => "valueAlarm.highWarningLimit",
        "LOW" => "valueAlarm.lowWarningLimit",
        "HHSV" => "valueAlarm.highAlarmSeverity",
        "LLSV" => "valueAlarm.lowAlarmSeverity",
        "HSV" => "valueAlarm.highWarningSeverity",
        "LSV" => "valueAlarm.lowWarningSeverity",
        "HYST" => "valueAlarm.hysteresis",
        // control sub-structure (drive limits)
        "DRVH" => "control.limitHigh",
        "DRVL" => "control.limitLow",
        _ => return None,
    })
}

/// Walk a dotted PVA field path (`valueAlarm.highAlarmLimit`) from
/// the root [`PvField`]. Returns `None` if any segment is missing or
/// the path lands on a non-structure intermediate.
fn pv_field_walk_path<'a>(root: &'a PvField, path: &str) -> Option<&'a PvField> {
    let mut current = root;
    for segment in path.split('.') {
        let PvField::Structure(s) = current else {
            return None;
        };
        current = s.get_field(segment)?;
    }
    Some(current)
}

/// Stringify a [`ScalarValue`] for storage in the per-sample extras
/// map. Java archiver's `archiveFields` blob is a string-string map,
/// so we mirror that wire shape.
fn scalar_value_to_string(s: &ScalarValue) -> String {
    match s {
        ScalarValue::Boolean(v) => v.to_string(),
        ScalarValue::Byte(v) => v.to_string(),
        ScalarValue::Short(v) => v.to_string(),
        ScalarValue::Int(v) => v.to_string(),
        ScalarValue::Long(v) => v.to_string(),
        ScalarValue::UByte(v) => v.to_string(),
        ScalarValue::UShort(v) => v.to_string(),
        ScalarValue::UInt(v) => v.to_string(),
        ScalarValue::ULong(v) => v.to_string(),
        ScalarValue::Float(v) => v.to_string(),
        ScalarValue::Double(v) => v.to_string(),
        ScalarValue::String(s) => s.clone(),
    }
}

/// Element count for a PVA value: 1 for scalar, length for arrays,
/// 0 for unsupported shapes (treated as "unknown" by callers).
fn pv_field_element_count(field: &PvField) -> i32 {
    match pv_value_field(field) {
        PvField::Scalar(_) => 1,
        PvField::ScalarArray(arr) => arr.len() as i32,
        PvField::ScalarArrayTyped(t) => t.len() as i32,
        _ => 0,
    }
}

/// Pull `(precision, units)` out of an NTScalar / NTScalarArray's
/// `display` sub-structure. Returns `(None, None)` for bare scalars
/// or structures missing `display` (some servers omit it).
/// Negative precision is the Java EAA "no precision info" sentinel —
/// treat it the same as missing so we don't surface "-1" to the UI.
fn pv_field_extract_display(field: &PvField) -> (Option<String>, Option<String>) {
    let PvField::Structure(s) = field else {
        return (None, None);
    };
    let Some(PvField::Structure(disp)) = s.get_field("display") else {
        return (None, None);
    };
    let prec = match disp.get_field("precision") {
        Some(PvField::Scalar(ScalarValue::Int(p))) if *p >= 0 => Some(p.to_string()),
        Some(PvField::Scalar(ScalarValue::Short(p))) if *p >= 0 => Some(p.to_string()),
        Some(PvField::Scalar(ScalarValue::Long(p))) if *p >= 0 => Some(p.to_string()),
        _ => None,
    };
    let egu = match disp.get_field("units") {
        Some(PvField::Scalar(ScalarValue::String(u))) => {
            let t = u.trim();
            if t.is_empty() { None } else { Some(t.to_string()) }
        }
        _ => None,
    };
    (prec, egu)
}

/// Pull the IOC-reported timestamp out of an NTScalar / NTScalarArray.
/// Falls back to `SystemTime::now()` when the structure lacks a usable
/// `timeStamp` sub-field, so a malformed server doesn't drop samples.
fn pv_field_extract_timestamp(field: &PvField) -> SystemTime {
    let PvField::Structure(s) = field else {
        return SystemTime::now();
    };
    let Some(PvField::Structure(ts)) = s.get_field("timeStamp") else {
        return SystemTime::now();
    };
    let secs = match ts.get_field("secondsPastEpoch") {
        Some(PvField::Scalar(ScalarValue::Long(v))) => *v as u64,
        Some(PvField::Scalar(ScalarValue::ULong(v))) => *v,
        Some(PvField::Scalar(ScalarValue::Int(v))) => *v as u64,
        Some(PvField::Scalar(ScalarValue::UInt(v))) => *v as u64,
        _ => return SystemTime::now(),
    };
    let nanos = match ts.get_field("nanoseconds") {
        Some(PvField::Scalar(ScalarValue::Int(v))) => *v as u32,
        Some(PvField::Scalar(ScalarValue::UInt(v))) => *v,
        _ => 0,
    };
    SystemTime::UNIX_EPOCH + Duration::new(secs, nanos)
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
        // PB PayloadType has no SCALAR_LONG (i64) — values outside i32 range
        // are truncated by the i32 cast in epics_value_to_archiver.
        DbFieldType::Int64 => ArchDbType::ScalarInt,
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
        EpicsValue::Int64(v) => ArchiverValue::ScalarInt(*v as i32),
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
        EpicsValue::Int64Array(v) => {
            ArchiverValue::VectorInt(v.iter().map(|x| *x as i32).collect())
        }
        EpicsValue::CharArray(v) => ArchiverValue::VectorChar(v.clone()),
        EpicsValue::StringArray(v) => ArchiverValue::VectorString(v.clone()),
    }
}

/// Tunables for [`write_loop_with_config`]. Production uses the
/// defaults via [`write_loop`]; tests dial the timeouts down to
/// sub-second values so failure-injection cases finish in a few
/// hundred milliseconds instead of minutes.
#[derive(Debug, Clone)]
pub struct WriteLoopConfig {
    /// How often to call `flush_ingest_writes` and commit the
    /// pending `last_event` timestamps to the registry.
    pub flush_period: Duration,
    /// Bound on how long write_loop waits for a single
    /// `append_event_with_meta` JoinHandle. On timeout the sample
    /// is logged as abandoned-but-may-succeed-late and the loop
    /// moves on (the spawn_blocking task remains parked on the
    /// blocking pool — per-PV serialization in PlainPB keeps any
    /// late completion ordered behind subsequent same-PV samples).
    pub append_timeout: Duration,
    /// Bound on how long write_loop waits for one
    /// `flush_ingest_writes` JoinHandle during the periodic flush.
    /// On timeout, every pending `ts_updates` entry is deferred to
    /// the next cycle (we don't know which PVs reached disk).
    pub flush_timeout: Duration,
    /// Bound on each individual append issued during the shutdown
    /// drain.
    pub drain_per_sample_timeout: Duration,
    /// Total wall-clock budget for the shutdown drain. Once
    /// exceeded, remaining buffered samples are abandoned without
    /// even attempting an append, so a wedged STS can't stretch an
    /// orderly stop into "kill -9".
    pub drain_total_budget: Duration,
    /// Bound on the final `flush_writes` issued at shutdown.
    pub shutdown_flush_timeout: Duration,
}

impl Default for WriteLoopConfig {
    fn default() -> Self {
        Self {
            flush_period: Duration::from_secs(10),
            append_timeout: Duration::from_secs(30),
            flush_timeout: Duration::from_secs(30),
            drain_per_sample_timeout: Duration::from_secs(5),
            drain_total_budget: Duration::from_secs(30),
            shutdown_flush_timeout: Duration::from_secs(15),
        }
    }
}

/// Tunables for [`run_sharded_write_pool`].
///
/// `shards = 1` is the legacy single-worker layout and incurs no
/// dispatcher overhead. `shards > 1` spawns a dispatcher that hashes
/// each sample's `pv_name` to a fixed shard (so any one PV's samples
/// always land in the same per-shard write_loop, preserving
/// timestamp ordering) and N parallel shard workers — different PVs
/// can append concurrently instead of queueing behind a single task.
/// Sites with many active PVs and a fast STS should set this to
/// `min(num_cpus, expected concurrent slow appends)`.
#[derive(Debug, Clone)]
pub struct ShardedWritePoolConfig {
    /// Number of parallel shard workers. Must be ≥ 1; `1` is the
    /// degenerate case (no dispatcher, behaves identically to a
    /// bare `write_loop_with_config`).
    pub shards: usize,
    /// mpsc capacity of each shard's input channel. The dispatcher
    /// `send().await`s into this — full shards back-pressure the
    /// dispatcher, which back-pressures the upstream main mpsc,
    /// which makes producer `try_send` fail with overflow drops.
    pub per_shard_buffer: usize,
    /// Per-worker config (timeouts, flush period). Cloned into
    /// each shard.
    pub write_loop: WriteLoopConfig,
}

impl Default for ShardedWritePoolConfig {
    fn default() -> Self {
        Self {
            shards: 1,
            per_shard_buffer: 4096,
            write_loop: WriteLoopConfig::default(),
        }
    }
}

/// Hash a PV name to a shard index in `0..n`. `DefaultHasher` is
/// stable enough for in-process partitioning — we don't care about
/// hash quality across process restarts (the file layout is keyed
/// by PV name on disk, not by shard).
fn shard_for_pv(pv: &str, n: usize) -> usize {
    use std::collections::hash_map::DefaultHasher;
    use std::hash::{Hash, Hasher};
    debug_assert!(n > 0);
    let mut h = DefaultHasher::new();
    pv.hash(&mut h);
    (h.finish() % n as u64) as usize
}

/// Run an N-shard write pool: 1 dispatcher (hashes pv_name → shard)
/// + N parallel `write_loop_with_config` workers. Each shard has
/// independent `ts_updates`, an independent flush ticker, and its
/// own per-PV writer slots inside the shared storage plugin (so
/// per-PV ordering is naturally preserved by the consistent-hash
/// routing — samples for one PV always go to the same shard).
///
/// `shards == 1` short-circuits the dispatcher and runs a single
/// `write_loop_with_config` directly so single-worker deployments
/// pay zero overhead.
pub async fn run_sharded_write_pool(
    storage: Arc<dyn StoragePlugin>,
    registry: Arc<PvRegistry>,
    rx: mpsc::Receiver<PvSample>,
    shutdown: tokio::sync::watch::Receiver<bool>,
    cfg: ShardedWritePoolConfig,
) {
    let n = cfg.shards.max(1);
    if n == 1 {
        write_loop_with_config(storage, registry, rx, shutdown, cfg.write_loop).await;
        return;
    }

    let mut shard_txs = Vec::with_capacity(n);
    let mut shard_handles = Vec::with_capacity(n);
    for shard_idx in 0..n {
        let (s_tx, s_rx) = mpsc::channel::<PvSample>(cfg.per_shard_buffer);
        shard_txs.push(s_tx);
        let storage = storage.clone();
        let registry = registry.clone();
        let shard_shutdown = shutdown.clone();
        let shard_cfg = cfg.write_loop.clone();
        shard_handles.push(tokio::spawn(async move {
            tracing::info!(shard = shard_idx, "Shard write loop started");
            write_loop_with_config(storage, registry, s_rx, shard_shutdown, shard_cfg)
                .await;
            tracing::info!(shard = shard_idx, "Shard write loop exited");
        }));
    }

    dispatch_loop(rx, shard_txs, shutdown).await;

    // Dispatcher exited → the shard senders it owned were dropped
    // → shard receivers see EOS on their next poll. The shard
    // workers should already have observed the same shutdown
    // signal via their cloned watch::Receiver and be running their
    // drain branches. Wait for them to finish.
    for h in shard_handles {
        let _ = h.await;
    }
}

/// Reads the upstream main mpsc, hashes each sample's `pv_name`,
/// and forwards to the appropriate shard channel via
/// `Sender::send().await` so a slow shard back-pressures the
/// upstream channel rather than silently dropping samples.
async fn dispatch_loop(
    mut rx: mpsc::Receiver<PvSample>,
    shard_txs: Vec<mpsc::Sender<PvSample>>,
    mut shutdown: tokio::sync::watch::Receiver<bool>,
) {
    let n = shard_txs.len();
    info!(shards = n, "Sharded write dispatcher started");
    loop {
        tokio::select! {
            biased;
            // `biased` so we always check shutdown before draining
            // another sample — keeps the shutdown latency bounded
            // even under a sustained sample storm.
            _ = shutdown.changed() => {
                if *shutdown.borrow() {
                    // Drain whatever's still in the upstream mpsc
                    // and route it to shards (try_send so a
                    // saturated shard doesn't extend our drain
                    // window past its own per-shard timeout).
                    while let Ok(sample) = rx.try_recv() {
                        let idx = shard_for_pv(&sample.pv_name, n);
                        let _ = shard_txs[idx].try_send(sample);
                    }
                    break;
                }
            }
            maybe = rx.recv() => {
                match maybe {
                    Some(sample) => {
                        let idx = shard_for_pv(&sample.pv_name, n);
                        if let Err(e) = shard_txs[idx].send(sample).await {
                            // Shard's receiver dropped → the shard
                            // worker exited (panic or shutdown
                            // race). We'll lose this sample, but
                            // we should keep the dispatcher alive
                            // for the OTHER shards. Log and
                            // continue — `e.0` would expose the
                            // sample we dropped.
                            warn!(
                                shard = idx,
                                pv = e.0.pv_name,
                                "Shard channel closed; sample dropped"
                            );
                        }
                    }
                    None => break, // Upstream closed.
                }
            }
        }
    }
    info!("Sharded write dispatcher exiting");
}

/// Background writer task — drains samples and writes to storage.
///
/// Thin wrapper that fills [`WriteLoopConfig`] from [`Default`] and
/// the caller-supplied `flush_period`. Production callers use this
/// form; the test suite uses [`write_loop_with_config`] directly to
/// dial timeouts down.
pub async fn write_loop(
    storage: Arc<dyn StoragePlugin>,
    registry: Arc<PvRegistry>,
    rx: mpsc::Receiver<PvSample>,
    shutdown: tokio::sync::watch::Receiver<bool>,
    flush_period: Duration,
) {
    let cfg = WriteLoopConfig {
        flush_period,
        ..Default::default()
    };
    write_loop_with_config(storage, registry, rx, shutdown, cfg).await
}

/// Run one flush + commit cycle: spawn the storage's
/// `flush_ingest_writes` on the blocking pool with a bounded
/// timeout, then commit the pending registry timestamps for every
/// PV the flush did NOT report as failed/deferred.
///
/// Mutates `ts_updates`:
///   * On clean flush: drops failed/deferred PVs and (on successful
///     batch update) clears the rest.
///   * On flush error / panic / timeout: leaves every entry intact
///     so the next cycle retries.
async fn run_flush_and_commit(
    storage: &Arc<dyn StoragePlugin>,
    registry: &Arc<PvRegistry>,
    ts_updates: &mut std::collections::HashMap<String, SystemTime>,
    flush_timeout: Duration,
) {
    let storage_for_flush = storage.clone();
    let flush_join = tokio::task::spawn_blocking(move || {
        let rt = tokio::runtime::Handle::current();
        rt.block_on(storage_for_flush.flush_ingest_writes())
    });
    match tokio::time::timeout(flush_timeout, flush_join).await {
        Ok(Ok(Ok(failed_pvs))) => {
            for pv in &failed_pvs {
                ts_updates.remove(pv);
            }
            if !failed_pvs.is_empty() {
                error!(
                    "STS flush dropped {} PV(s) from timestamp commit \
                     (their bytes never reached disk OR an in-flight \
                     append is still running): {:?}",
                    failed_pvs.len(),
                    failed_pvs
                );
            }
            if !ts_updates.is_empty() {
                let refs: Vec<(&str, SystemTime)> = ts_updates
                    .iter()
                    .map(|(name, ts)| (name.as_str(), *ts))
                    .collect();
                match registry.batch_update_timestamps(&refs) {
                    Ok(()) => ts_updates.clear(),
                    Err(e) => {
                        // Don't clear — retry on next cycle.
                        // Otherwise a transient SQLite error would
                        // orphan timestamps the disk has but the
                        // registry doesn't know about, with no
                        // retry path.
                        error!(
                            "Registry timestamp commit failed; \
                             keeping {} pending for retry: {e}",
                            ts_updates.len()
                        );
                    }
                }
            }
        }
        Ok(Ok(Err(e))) => {
            error!(
                "STS ingest flush errored; deferring all {} \
                 pending timestamp commits: {e}",
                ts_updates.len()
            );
        }
        Ok(Err(join_err)) => {
            error!(
                "STS ingest flush task panicked; deferring all {} \
                 pending timestamp commits: {join_err}",
                ts_updates.len()
            );
        }
        Err(_) => {
            // Flush wedged. Defer EVERY pending ts_updates rather
            // than commit any — we don't know which PVs reached
            // disk. The blocking task remains parked on the pool
            // until the OS unblocks it; further flush attempts on
            // the next cycle will queue another spawn_blocking,
            // with the same timeout safety net.
            metrics::counter!("archiver_storage_flush_timeouts_total").increment(1);
            error!(
                "STS ingest flush timed out after {flush_timeout:?}; \
                 deferring all {} pending timestamp commits \
                 (task remains on blocking pool)",
                ts_updates.len()
            );
        }
    }
}

/// Same as [`write_loop`] but accepts the full [`WriteLoopConfig`].
pub async fn write_loop_with_config(
    storage: Arc<dyn StoragePlugin>,
    registry: Arc<PvRegistry>,
    mut rx: mpsc::Receiver<PvSample>,
    mut shutdown: tokio::sync::watch::Receiver<bool>,
    cfg: WriteLoopConfig,
) {
    let WriteLoopConfig {
        flush_period,
        append_timeout,
        flush_timeout,
        drain_per_sample_timeout,
        drain_total_budget,
        shutdown_flush_timeout,
    } = cfg;
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

    // Periodic flush ticker: fires every `flush_period` regardless
    // of sample arrival. Without this, a PV that stops producing
    // (e.g. an IOC that goes silent) would keep its last buffered
    // bytes hostage in the BufWriter until shutdown — observable
    // only as missing data in the registry's `last_event` while the
    // disk has the bytes.
    //
    // `Skip` missed-tick behaviour: if write_loop blocks on a slow
    // append and several ticks queue up, fire only ONCE on resume
    // rather than spamming N flushes back-to-back.
    let mut flush_ticker = tokio::time::interval(flush_period);
    flush_ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    // Consume the immediate first tick so the first real flush
    // happens at `flush_period`, not at t=0.
    let _ = flush_ticker.tick().await;

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
                // Bound each append + isolate sync syscall hangs.
                // `spawn_blocking` moves the actual I/O to the
                // blocking thread pool, so a stuck NFS write parks
                // a blocking-pool thread instead of a runtime
                // worker. The `tokio::time::timeout` then bounds
                // how long write_loop waits for that join — a
                // stuck task is abandoned (its future is dropped)
                // and write_loop continues. NOTE: the abandoned
                // task remains parked on the blocking pool until
                // the OS unblocks it; under sustained hangs the
                // blocking pool eventually saturates, after which
                // newer spawn_blocking calls queue. mpsc back-
                // pressure then drops samples via overflow.
                //
                // Late-success ordering: a timed-out task may still
                // complete its write later. Per-PV serialization
                // inside PlainPB (`PvWriterSlot` mutex) guarantees
                // that any subsequent same-PV append blocks behind
                // the in-flight one, so on-disk timestamp order
                // stays monotonic even when write_loop has already
                // moved past the timed-out sample.
                let pv_name_for_post = pv_sample.pv_name.clone();
                let counters_for_post = pv_sample.counters.clone();
                let counters_in_task = pv_sample.counters.clone();
                let storage_for_task = storage.clone();
                let join = tokio::task::spawn_blocking(move || {
                    let rt = tokio::runtime::Handle::current();
                    let res = rt.block_on(storage_for_task.append_event_with_meta(
                        &pv_sample.pv_name,
                        pv_sample.dbr_type,
                        &pv_sample.sample,
                        &meta,
                    ));
                    // Bump counters from inside the task so a
                    // late-completing write (write_loop already
                    // gave up on the JoinHandle via timeout) still
                    // gets counted. Without this, every timed-out-
                    // but-late-success sample silently leaks from
                    // events_stored / archiver_events_stored_total
                    // even though the bytes ARE on disk.
                    if res.is_ok() {
                        metrics::counter!("archiver_events_stored_total").increment(1);
                        if let Some(c) = counters_in_task.as_ref() {
                            c.events_stored.fetch_add(1, Ordering::Relaxed);
                        }
                    }
                    res
                });
                let res = tokio::time::timeout(append_timeout, join).await;
                match res {
                    Ok(Ok(Ok(()))) => {
                        // Counters were already bumped inside the
                        // blocking task; here we only record the
                        // registry-level timestamp, which must stay
                        // on the loop task because ts_updates is a
                        // local HashMap.
                        ts_updates.insert(pv_name_for_post, ts);
                    }
                    Ok(Ok(Err(e))) => {
                        error!(pv = pv_name_for_post, "Write error: {e}");
                    }
                    Ok(Err(join_err)) => {
                        error!(
                            pv = pv_name_for_post,
                            "Storage write task panicked: {join_err}"
                        );
                    }
                    Err(_) => {
                        // Sample MAY still land on disk later —
                        // per-PV serialization keeps that ordered
                        // — but write_loop won't see the result,
                        // so registry's `last_event` for this PV
                        // will lag until the NEXT sample for the
                        // same PV catches it up.
                        error!(
                            pv = pv_name_for_post,
                            "Storage append timed out after {append_timeout:?}; \
                             write_loop abandoning (task remains on blocking pool, \
                             may still succeed late)"
                        );
                        if let Some(ref c) = counters_for_post {
                            c.storage_append_timeouts.fetch_add(1, Ordering::Relaxed);
                        }
                    }
                }

            }
            _ = flush_ticker.tick() => {
                // Periodic flush, decoupled from sample arrival so
                // a quiet PV's buffered bytes still reach disk.
                // Skip when there's nothing to commit — saves a
                // syscall per tick on idle systems.
                if !ts_updates.is_empty() {
                    run_flush_and_commit(
                        &storage,
                        &registry,
                        &mut ts_updates,
                        flush_timeout,
                    )
                    .await;
                }
            }
            _ = shutdown.changed() => {
                // Drain remaining samples. Apply the same
                // spawn_blocking + bounded-timeout discipline as the
                // main loop body so a wedged STS doesn't make
                // shutdown itself hang on the runtime worker.
                //
                // An overall `drain_deadline` further bounds total
                // shutdown time so any ONE stuck PV can't stretch
                // the orderly stop into "kill the process". Samples
                // past the deadline are abandoned (their bytes never
                // reach disk and registry won't claim them).
                let drain_start = std::time::Instant::now();
                let mut drained_ok = 0usize;
                let mut drained_err = 0usize;
                let mut drained_skipped = 0usize;

                while let Ok(pv_sample) = rx.try_recv() {
                    if drain_start.elapsed() > drain_total_budget {
                        drained_skipped += 1;
                        continue;
                    }
                    let meta = AppendMeta {
                        element_count: pv_sample.element_count,
                        ..Default::default()
                    };
                    let drain_ts = pv_sample.sample.timestamp;
                    let drain_pv = pv_sample.pv_name.clone();
                    let storage_for_drain = storage.clone();
                    let join = tokio::task::spawn_blocking(move || {
                        let rt = tokio::runtime::Handle::current();
                        rt.block_on(storage_for_drain.append_event_with_meta(
                            &pv_sample.pv_name,
                            pv_sample.dbr_type,
                            &pv_sample.sample,
                            &meta,
                        ))
                    });
                    match tokio::time::timeout(drain_per_sample_timeout, join).await {
                        Ok(Ok(Ok(()))) => {
                            // Track for the final ts_updates commit so
                            // restart-time recovery sees the drained
                            // samples reflected in `last_event`.
                            ts_updates.insert(drain_pv, drain_ts);
                            drained_ok += 1;
                        }
                        Ok(Ok(Err(e))) => {
                            warn!(pv = drain_pv, "Shutdown drain write error: {e}");
                            drained_err += 1;
                        }
                        Ok(Err(join_err)) => {
                            warn!(
                                pv = drain_pv,
                                "Shutdown drain write task panicked: {join_err}"
                            );
                            drained_err += 1;
                        }
                        Err(_) => {
                            warn!(
                                pv = drain_pv,
                                "Shutdown drain append timed out after \
                                 {drain_per_sample_timeout:?}; abandoning"
                            );
                            drained_err += 1;
                        }
                    }
                }
                if drained_skipped > 0 {
                    warn!(
                        "Shutdown drain budget ({drain_total_budget:?}) exhausted; \
                         {drained_skipped} buffered sample(s) abandoned without \
                         attempting append (ok={drained_ok}, err={drained_err})"
                    );
                }

                // Final flush: storage FIRST so the registry's
                // last_event timestamps don't claim more than what
                // actually reached disk. Use the all-tier flush_writes
                // here (not flush_ingest_writes) — at shutdown we
                // want every cached writer drained, including any
                // ETL-side caches. Same spawn_blocking + timeout
                // discipline as the periodic flush above so a stuck
                // mount doesn't make shutdown hang.
                let storage_for_flush = storage.clone();
                let flush_join = tokio::task::spawn_blocking(move || {
                    let rt = tokio::runtime::Handle::current();
                    rt.block_on(storage_for_flush.flush_writes())
                });
                let storage_ok =
                    match tokio::time::timeout(shutdown_flush_timeout, flush_join).await {
                        Ok(Ok(Ok(()))) => true,
                        Ok(Ok(Err(e))) => {
                            warn!("Shutdown storage flush failed: {e}");
                            false
                        }
                        Ok(Err(join_err)) => {
                            warn!("Shutdown storage flush task panicked: {join_err}");
                            false
                        }
                        Err(_) => {
                            warn!(
                                "Shutdown storage flush timed out after \
                                 {shutdown_flush_timeout:?}; abandoning ts_updates commit"
                            );
                            false
                        }
                    };
                if storage_ok && !ts_updates.is_empty() {
                    let refs: Vec<(&str, SystemTime)> = ts_updates
                        .iter()
                        .map(|(name, ts)| (name.as_str(), *ts))
                        .collect();
                    if let Err(e) = registry.batch_update_timestamps(&refs) {
                        warn!("Shutdown timestamp flush failed: {e}");
                    }
                }
                info!("Write loop shutting down");
                break;
            }
        }
    }
}
