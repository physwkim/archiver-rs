use std::sync::atomic::{AtomicI32, AtomicI64, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, SystemTime};

use dashmap::DashMap;
use epics_rs::base::server::snapshot::DbrClass;
use epics_rs::base::types::{DbFieldType, EpicsValue};
use epics_rs::ca::client::{CaChannel, CaClient, ConnectionEvent};
use epics_rs::pva::client_native::PvaClient;
use epics_rs::pva::proto::ByteOrder;
use epics_rs::pva::pvdata::encode::{encode_pv_field, encode_type_desc};
use epics_rs::pva::pvdata::{PvField, ScalarType, ScalarValue, TypedScalarArray};
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;
use tracing::{debug, error, info, warn};

use archiver_core::registry::{Protocol, PvRecord, PvRegistry, PvStatus, SampleMode};
use archiver_core::storage::traits::{AppendMeta, IngestFlushResult, StoragePlugin};
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
        let connect = tokio::time::timeout(CA_CONNECT_TIMEOUT, self.pva_client.pvconnect(pv_name))
            .await
            .map_err(|_| anyhow::anyhow!("PVA connect to {pv_name} timed out"))?
            .map_err(|e| anyhow::anyhow!("Failed to connect to {pv_name} via PVA: {e}"))?;
        debug!(pv = pv_name, server = %connect, "PVA channel connected");

        // pvget_full (not pvget) so the introspection round-trips
        // alongside the value: every PVA read in this crate goes
        // through a desc-aware API, matching the no-fallback invariant
        // enforced by `pv_field_scalar_to_archiver`. The same op_get
        // wire op underlies both, so this is a free strengthening.
        let initial = tokio::time::timeout(CA_CONNECT_TIMEOUT, self.pva_client.pvget_full(pv_name))
            .await
            .map_err(|_| anyhow::anyhow!("PVA pvget for {pv_name} timed out"))?
            .map_err(|e| anyhow::anyhow!("Failed to pvget {pv_name}: {e}"))?;
        let (dbr_type, element_count) =
            pv_field_to_arch_db_type(&initial.value).ok_or_else(|| {
                anyhow::anyhow!("PV {pv_name}: empty PVA scalar-array; cannot infer element type")
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
        let (prec, egu) = pv_field_extract_display(&initial.value);
        if (prec.is_some() || egu.is_some())
            && let Err(e) = self
                .registry
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
                pva_state_watchdog(pv_name, conn_info, counters_for_watch, cancel, sample_mode)
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
            // PVA-acquired PV — live_value via pvget_full so we get
            // the canonical channel descriptor alongside the value;
            // V4GenericBytes encoding then preserves Union / Variant
            // schemas instead of using lossy value-recovery.
            let pva = self.pva_client.clone();
            let name = pv_name.to_string();
            let res = tokio::time::timeout(timeout, pva.pvget_full(&name)).await;
            return Some(match res {
                Ok(Ok(result)) => pv_field_scalar_to_archiver(&result.value, &result.introspection)
                    .ok_or_else(|| anyhow::anyhow!("PVA value has no archiver mapping")),
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
    // `units` is a byte-preserving `PvString`; render it lossily for the
    // text EGU slot (the registry column is UTF-8 text). Bind the `Cow`
    // first so the trimmed `&str` borrow outlives this scope.
    let new_egu_units = display.units.as_str_lossy();
    let new_egu_trimmed = new_egu_units.trim();
    let new_egu_opt: Option<&str> = if new_egu_trimmed.is_empty() {
        None
    } else {
        Some(new_egu_trimmed)
    };

    let stored = match registry.get_pv(pv_name) {
        Ok(Some(r)) => r,
        Ok(None) => return,
        Err(e) => {
            debug!(
                pv = pv_name,
                "Registry read for metadata compare failed: {e}"
            );
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
///
/// `canonical_desc` is the channel-INIT introspection descriptor —
/// required at every callsite. The fast-path callback receives one
/// per event from the pvxs reactor; the custom-request path
/// pre-fetches it once via `pvinfo` and reuses the same `Arc`
/// across every event in a subscribe generation (a failed `pvinfo`
/// aborts subscribe + retries rather than degrading to lossy
/// value-recovery, so the on-disk invariant always holds).
#[allow(clippy::too_many_arguments)]
fn pva_handle_event(
    field: &PvField,
    canonical_desc: &epics_rs::pva::pvdata::FieldDesc,
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

    let Some(value) = pv_field_scalar_to_archiver(field, canonical_desc) else {
        counters.type_change_drops.fetch_add(1, Ordering::Relaxed);
        debug!(pv = pv_name, "PVA event has no archiver mapping; dropping");
        return;
    };

    let ts = pv_field_extract_timestamp(field);
    if !first_after_connect && !ioc_timestamp_in_window(ts, now, drift_secs) {
        counters.timestamp_drops.fetch_add(1, Ordering::Relaxed);
        debug!(
            pv = pv_name,
            ?ts,
            "Dropping PVA sample with out-of-window timestamp"
        );
        return;
    }
    let elem_count = match pv_field_element_count(field) {
        0 => 1,
        n => n,
    };
    refresh_nt_enum_extras(field, extras);
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
        counters
            .buffer_overflow_drops
            .fetch_add(1, Ordering::Relaxed);
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
            // Custom-request path: 1-arg callback. `pvmonitor_with_request`
            // does not surface the channel's FieldDesc per event, so we
            // pre-fetch it once via `pvinfo` and reuse the Arc across
            // every event in this subscription generation. A failed
            // pvinfo aborts this subscribe attempt and retries — the
            // archiver invariant is that every V4 sample on disk
            // carries the channel-INIT descriptor, so degrading to
            // value-recovery is not allowed.
            let canonical = match pva_client.pvinfo(&pv_name).await {
                Ok(d) => Arc::new(d),
                Err(e) => {
                    counters
                        .transient_error_count
                        .fetch_add(1, Ordering::Relaxed);
                    warn!(
                        pv = pv_name,
                        "PVA pvinfo failed: {e}; cannot capture canonical descriptor — retrying subscribe"
                    );
                    tokio::select! {
                        _ = cancel_token.cancelled() => return,
                        _ = tokio::time::sleep(CA_RETRY_DELAY) => continue,
                    }
                }
            };
            let pv_name_cb = pv_name.clone();
            let tx_cb = tx.clone();
            let conn_info_cb = conn_info.clone();
            let counters_cb = counters.clone();
            let extras_cb = extras.clone();
            let extras_paths_cb = extras_paths.clone();
            let canonical_cb = canonical.clone();
            let cb = move |field: &PvField| {
                pva_handle_event(
                    field,
                    &canonical_cb,
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
            // until our explicit drop. The per-event FieldDesc the
            // reactor hands us IS the channel-INIT descriptor (pvxs
            // refreshes only on type-change, which forces reconnect
            // upstream), so it's wire-faithful for Union / Variant
            // shapes that `PvField::descriptor()` would degrade.
            let pv_name_cb = pv_name.clone();
            let tx_cb = tx.clone();
            let conn_info_cb = conn_info.clone();
            let counters_cb = counters.clone();
            let extras_cb = extras.clone();
            let extras_paths_cb = extras_paths.clone();
            let cb = move |desc: &epics_rs::pva::pvdata::FieldDesc, field: &PvField| {
                pva_handle_event(
                    field,
                    desc,
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

        // pvget_full carries the channel's canonical FieldDesc alongside
        // the value, so V4GenericBytes encoding can preserve Union /
        // UnionArray / Variant schemas that `PvField::descriptor()`
        // would otherwise degrade.
        let res = tokio::time::timeout(pvget_timeout, pva_client.pvget_full(&pv_name)).await;
        let (field, canonical) = match res {
            Ok(Ok(r)) => (r.value, r.introspection),
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

        let Some(value) = pv_field_scalar_to_archiver(&field, &canonical) else {
            counters.type_change_drops.fetch_add(1, Ordering::Relaxed);
            debug!(
                pv = pv_name,
                "PVA scan value has no archiver mapping; dropping"
            );
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
        refresh_nt_enum_extras(&field, &extras);
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
        if let Err(rejected) = send_with_backpressure(&tx, pv_sample).await {
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
        let prec_arg = if prec_changed {
            new_prec.as_deref()
        } else {
            None
        };
        let egu_arg = if egu_changed {
            new_egu.as_deref()
        } else {
            None
        };
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
        SampleMode::Scan { period_secs } => Duration::from_secs_f64((period_secs * 3.0).max(60.0)),
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
                                    snapshot.timestamp.into(),
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
                            let mut sample =
                                ArchiverSample::new(snapshot.timestamp.into(), archiver_val);
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
                            if let Err(pv_sample) = send_with_backpressure(&tx, pv_sample).await {
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

/// Send a sample, applying backpressure when the bounded channel is
/// full. Tries non-blocking first; on a full channel it falls back to
/// a blocking `send().await`, so the producer stalls until the writer
/// drains space — the sample is delivered, NOT dropped.
///
/// The saturation event is surfaced as
/// `archiver_write_channel_backpressure_stalls_total` so operators can
/// see the writer falling behind. It MUST NOT touch
/// `buffer_overflow_drops`: that counter means *dropped* samples, and
/// counting a delivered-after-stall sample as a drop was a dual
/// meaning that inflated the dropped-events report with samples that
/// were never lost.
async fn send_with_backpressure(
    tx: &mpsc::Sender<PvSample>,
    pv_sample: PvSample,
) -> Result<(), PvSample> {
    match tx.try_send(pv_sample) {
        Ok(()) => Ok(()),
        Err(tokio::sync::mpsc::error::TrySendError::Full(pv_sample)) => {
            metrics::counter!("archiver_write_channel_backpressure_stalls_total").increment(1);
            // Backpressure: await until the writer drains space. The
            // sample is delivered, not dropped — so no drop counter.
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
                if send_with_backpressure(&tx, pv_sample).await.is_err() {
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
        EpicsValue::String(s) => s.to_string(),
        EpicsValue::Short(v) => v.to_string(),
        EpicsValue::Float(v) => v.to_string(),
        EpicsValue::Enum(v) => v.to_string(),
        // Transient NTEnum carrier: render the index, same as `Enum`.
        EpicsValue::EnumWithChoices { index, .. } => index.to_string(),
        EpicsValue::Char(v) => v.to_string(),
        EpicsValue::Long(v) => v.to_string(),
        EpicsValue::Int64(v) => v.to_string(),
        EpicsValue::UInt64(v) => v.to_string(),
        EpicsValue::UShort(v) => v.to_string(),
        EpicsValue::ULong(v) => v.to_string(),
        EpicsValue::Double(v) => v.to_string(),
        EpicsValue::ShortArray(v) => format!("{v:?}"),
        EpicsValue::FloatArray(v) => format!("{v:?}"),
        EpicsValue::EnumArray(v) => format!("{v:?}"),
        EpicsValue::DoubleArray(v) => format!("{v:?}"),
        EpicsValue::LongArray(v) => format!("{v:?}"),
        EpicsValue::Int64Array(v) => format!("{v:?}"),
        EpicsValue::UInt64Array(v) => format!("{v:?}"),
        EpicsValue::UShortArray(v) => format!("{v:?}"),
        EpicsValue::ULongArray(v) => format!("{v:?}"),
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
        ScalarValue::String(s) => ArchiverValue::ScalarString(s.to_string()),
    }
}

/// Extract an `ArchiverValue` from a top-level PVA value. NTScalar /
/// NTScalarArray are unwrapped via `value`; NTEnum collapses to
/// `ScalarEnum(index)` (Java archiver parity — choices land in the
/// monitor callback's extras cache); bare Structure roots (NTTable,
/// NTNDArray, user-defined NTs) wire-encode the whole root as opaque
/// [`ArchiverValue::V4GenericBytes`] so retrieval can reconstruct the
/// full structure.
///
/// `canonical_desc` is the channel-INIT descriptor that producers
/// (monitor/scan/live_value callsites) plumb through so
/// [`encode_pv_field_self_describing`] preserves Union / UnionArray
/// / Variant shapes that [`PvField::descriptor()`] would degrade.
/// Required at every callsite — there is no value-recovery fallback.
fn pv_field_scalar_to_archiver(
    field: &PvField,
    canonical_desc: &epics_rs::pva::pvdata::FieldDesc,
) -> Option<ArchiverValue> {
    // NTEnum special-case: take the `value.index` int as the scalar
    // sample. Must run before `pv_value_field` peeling, because
    // peeling on an NTEnum returns the inner `enum_t` Structure,
    // which would fall through to the V4GenericBytes path.
    if let Some((index, _)) = nt_enum_parts(field) {
        return Some(ArchiverValue::ScalarEnum(index));
    }
    match pv_value_field(field) {
        PvField::Scalar(s) => Some(scalar_value_to_archiver(s)),
        PvField::ScalarArrayTyped(arr) => Some(typed_scalar_array_to_archiver(arr)),
        PvField::ScalarArray(items) => {
            // Legacy untyped scalar-array path (rare with pvxs 0.14+,
            // which decodes into `ScalarArrayTyped`). Promote to the
            // typed form so a single converter covers both.
            let st = items.first()?.scalar_type();
            let typed = TypedScalarArray::from_scalar_values(items, st)?;
            Some(typed_scalar_array_to_archiver(&typed))
        }
        // Any other shape — NTTable (`value` is itself a Structure of
        // column arrays), NTNDArray, user-defined NTs — archive the
        // root PvField (NOT the unwrapped value, which would lose
        // labels / alarm / timeStamp) as opaque V4GenericBytes.
        _ => Some(ArchiverValue::V4GenericBytes(
            encode_pv_field_self_describing(field, canonical_desc),
        )),
    }
}

/// Pick the `(ArchDbType, element_count)` to record at archive_pv time
/// from a PVA introspection-pvget result.
///
/// - NTScalar / NTScalarArray: typed by the unwrapped `value` field.
/// - Bare scalar / scalar-array root: typed directly.
/// - Anything else (NTTable, NTNDArray, user-defined NT structures):
///   archived as [`ArchDbType::V4GenericBytes`] with element_count = 1
///   (the structure as a whole is one sample; inner cardinality is
///   carried inside the wire-encoded bytes).
fn pv_field_to_arch_db_type(field: &PvField) -> Option<(ArchDbType, i32)> {
    // NTEnum: register as ScalarEnum (the index is the archived
    // value); choices live in extras_cache rather than the dbr_type.
    if nt_enum_parts(field).is_some() {
        return Some((ArchDbType::ScalarEnum, 1));
    }
    let value = pv_value_field(field);
    Some(match value {
        PvField::Scalar(s) => (scalar_value_to_arch_db_type(s), 1),
        PvField::ScalarArray(arr) => {
            let elem = arr.first()?;
            (
                scalar_type_to_waveform(elem.scalar_type()),
                arr.len() as i32,
            )
        }
        PvField::ScalarArrayTyped(arr) => {
            (scalar_type_to_waveform(arr.scalar_type()), arr.len() as i32)
        }
        _ => (ArchDbType::V4GenericBytes, 1),
    })
}

/// Scalar-form ArchDbType for a [`ScalarValue`].
fn scalar_value_to_arch_db_type(s: &ScalarValue) -> ArchDbType {
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
    }
}

/// Waveform-form ArchDbType for a [`ScalarType`].
fn scalar_type_to_waveform(st: ScalarType) -> ArchDbType {
    match st {
        ScalarType::Boolean => ArchDbType::WaveformEnum,
        ScalarType::Byte | ScalarType::UByte => ArchDbType::WaveformByte,
        ScalarType::Short | ScalarType::UShort => ArchDbType::WaveformShort,
        ScalarType::Int | ScalarType::UInt | ScalarType::Long | ScalarType::ULong => {
            ArchDbType::WaveformInt
        }
        ScalarType::Float => ArchDbType::WaveformFloat,
        ScalarType::Double => ArchDbType::WaveformDouble,
        ScalarType::String => ArchDbType::WaveformString,
    }
}

/// Convert a [`TypedScalarArray`] into the matching [`ArchiverValue`]
/// vector variant. Widening (i8/i16/i64 → i32) mirrors the scalar path
/// in [`scalar_value_to_archiver`], so a waveform of `Long` stores as
/// `VectorInt` (out-of-range values truncate, same caveat as the CA
/// `DbFieldType::Int64` mapping).
fn typed_scalar_array_to_archiver(arr: &TypedScalarArray) -> ArchiverValue {
    match arr {
        TypedScalarArray::Boolean(a) => {
            ArchiverValue::VectorEnum(a.iter().map(|&b| b as i32).collect())
        }
        TypedScalarArray::Byte(a) => {
            ArchiverValue::VectorChar(a.iter().map(|&v| v as u8).collect())
        }
        TypedScalarArray::UByte(a) => ArchiverValue::VectorChar(a.to_vec()),
        TypedScalarArray::Short(a) => {
            ArchiverValue::VectorShort(a.iter().map(|&v| v as i32).collect())
        }
        TypedScalarArray::UShort(a) => {
            ArchiverValue::VectorShort(a.iter().map(|&v| v as i32).collect())
        }
        TypedScalarArray::Int(a) => ArchiverValue::VectorInt(a.to_vec()),
        TypedScalarArray::UInt(a) => {
            ArchiverValue::VectorInt(a.iter().map(|&v| v as i32).collect())
        }
        TypedScalarArray::Long(a) => {
            ArchiverValue::VectorInt(a.iter().map(|&v| v as i32).collect())
        }
        TypedScalarArray::ULong(a) => {
            ArchiverValue::VectorInt(a.iter().map(|&v| v as i32).collect())
        }
        TypedScalarArray::Float(a) => ArchiverValue::VectorFloat(a.to_vec()),
        TypedScalarArray::Double(a) => ArchiverValue::VectorDouble(a.to_vec()),
        TypedScalarArray::String(a) => {
            ArchiverValue::VectorString(a.iter().map(|s| s.to_string()).collect())
        }
    }
}

/// NTEnum shape detection. Returns `Some((index, choices))` when
/// `field` is a Structure whose `struct_id` starts with
/// `"epics:nt/NTEnum"` AND carries a `value` substructure of the
/// `enum_t` shape (`index: integer`, `choices: string[]`). Java
/// archiver follows the same dual gate — explicit normative ID +
/// shape match — to avoid mis-classifying user structs that happen
/// to expose an `index` field.
///
/// `choices` is `None` when the substructure omits the field; the
/// monitor callback then leaves the prior cached value in place.
fn nt_enum_parts(field: &PvField) -> Option<(i32, Option<Vec<String>>)> {
    let PvField::Structure(root) = field else {
        return None;
    };
    if !root.struct_id.starts_with("epics:nt/NTEnum") {
        return None;
    }
    let PvField::Structure(inner) = root.get_field("value")? else {
        return None;
    };
    let index = match inner.get_field("index")? {
        PvField::Scalar(ScalarValue::Int(v)) => *v,
        PvField::Scalar(ScalarValue::Long(v)) => *v as i32,
        PvField::Scalar(ScalarValue::Short(v)) => *v as i32,
        PvField::Scalar(ScalarValue::UInt(v)) => *v as i32,
        PvField::Scalar(ScalarValue::ULong(v)) => *v as i32,
        PvField::Scalar(ScalarValue::UShort(v)) => *v as i32,
        _ => return None,
    };
    let choices = match inner.get_field("choices") {
        Some(PvField::ScalarArrayTyped(TypedScalarArray::String(arr))) => {
            Some(arr.iter().map(|s| s.to_string()).collect())
        }
        Some(PvField::ScalarArray(items)) => {
            let mut out = Vec::with_capacity(items.len());
            for s in items {
                if let ScalarValue::String(c) = s {
                    out.push(c.to_string());
                } else {
                    return None; // mixed-type choices array — reject
                }
            }
            Some(out)
        }
        Some(_) => return None,
        None => None,
    };
    Some((index, choices))
}

/// Encode NTEnum `choices` (UTF-8 strings) into the extras-cache
/// payload that lands in `field_values` per sample. JSON-array form
/// so retrieval clients can `JSON.parse(meta.enum_strs)` directly —
/// pipe/comma delimiters would break on choice names that contain
/// those characters. Hand-rolled to avoid pulling `serde_json` into
/// archiver-engine for a single string.
fn nt_enum_choices_to_extras(choices: &[String]) -> String {
    let mut out = String::with_capacity(2 + choices.iter().map(|s| s.len() + 3).sum::<usize>());
    out.push('[');
    for (i, c) in choices.iter().enumerate() {
        if i > 0 {
            out.push(',');
        }
        out.push('"');
        for ch in c.chars() {
            match ch {
                '"' => out.push_str("\\\""),
                '\\' => out.push_str("\\\\"),
                '\n' => out.push_str("\\n"),
                '\r' => out.push_str("\\r"),
                '\t' => out.push_str("\\t"),
                c if (c as u32) < 0x20 => {
                    use std::fmt::Write;
                    let _ = write!(out, "\\u{:04x}", c as u32);
                }
                c => out.push(c),
            }
        }
        out.push('"');
    }
    out.push(']');
    out
}

/// Refresh the extras cache for an NTEnum-shaped monitor event so
/// downstream retrieval can surface `enum_strs` alongside the
/// archived index. No-op when `field` is not an NTEnum or its
/// `choices` substructure is absent (cached value from the last
/// successful event stays put).
fn refresh_nt_enum_extras(field: &PvField, extras: &ExtraFieldsCache) {
    if let Some((_, Some(choices))) = nt_enum_parts(field) {
        extras.insert("enum_strs".to_string(), nt_enum_choices_to_extras(&choices));
    }
}

/// Serialize a top-level [`PvField`] (NTTable, NTNDArray, user-defined
/// structures) as the self-describing payload stored under
/// [`ArchDbType::V4GenericBytes`]. Layout is `type_desc || value` in
/// PVA wire format (BigEndian), so each sample on disk is decodable
/// in isolation — no shared introspection cache needed across the
/// archive day-roll boundary.
///
/// `canonical_desc` is the channel's introspection descriptor captured
/// at INIT (via `pvinfo` / `pvget_full` / per-event `pvmonitor_handle`
/// callback). It is **required**: `PvField::descriptor()` recovery
/// is lossy for `Union` (drops sibling variants), `UnionArray`
/// (empties variants list), and `Variant` (degrades to bare
/// `Variant`) — see epics-pva-rs `structure.rs:descriptor` doc. The
/// archiver invariant is that every V4 sample on disk preserves the
/// channel-level descriptor, so callers must plumb it through rather
/// than relying on value-recovery.
fn encode_pv_field_self_describing(
    field: &PvField,
    canonical_desc: &epics_rs::pva::pvdata::FieldDesc,
) -> Vec<u8> {
    let mut out = Vec::with_capacity(256);
    encode_type_desc(canonical_desc, ByteOrder::Big, &mut out);
    encode_pv_field(field, canonical_desc, ByteOrder::Big, &mut out);
    out
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
        ScalarValue::String(s) => s.to_string(),
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
            // Byte-preserving `PvString` → lossy text for the EGU slot.
            let lossy = u.as_str_lossy();
            let t = lossy.trim();
            if t.is_empty() {
                None
            } else {
                Some(t.to_string())
            }
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
        // are truncated by the i32 cast in epics_value_to_archiver. The
        // unsigned 64/32-bit EPICS-internal types fold into the same i32
        // slot the way their `EpicsValue` samples do (UInt64/ULong → ScalarInt),
        // keeping the registered ArchDbType in lockstep with the per-sample
        // ArchiverValue produced by `epics_value_to_archiver`.
        DbFieldType::Int64 | DbFieldType::UInt64 | DbFieldType::ULong => ArchDbType::ScalarInt,
        // DBF_USHORT mirrors the PVA `ScalarValue::UShort` → ScalarShort path.
        DbFieldType::UShort => ArchDbType::ScalarShort,
        DbFieldType::Double => ArchDbType::ScalarDouble,
    }
}

/// Convert epics-base-rs EpicsValue to archiver ArchiverValue.
fn epics_value_to_archiver(val: &EpicsValue) -> ArchiverValue {
    match val {
        EpicsValue::String(s) => ArchiverValue::ScalarString(s.to_string()),
        EpicsValue::Short(v) => ArchiverValue::ScalarShort(*v as i32),
        EpicsValue::Float(v) => ArchiverValue::ScalarFloat(*v),
        EpicsValue::Enum(v) => ArchiverValue::ScalarEnum(*v as i32),
        // Transient NTEnum carrier: archive the index, same as `Enum`.
        EpicsValue::EnumWithChoices { index, .. } => ArchiverValue::ScalarEnum(*index as i32),
        EpicsValue::Char(v) => ArchiverValue::ScalarByte(vec![*v]),
        EpicsValue::Long(v) => ArchiverValue::ScalarInt(*v),
        // i64/u64/u32 fold into the i32 ScalarInt slot (out-of-range values
        // truncate) — matches the `DbFieldType` registration mapping above.
        EpicsValue::Int64(v) => ArchiverValue::ScalarInt(*v as i32),
        EpicsValue::UInt64(v) => ArchiverValue::ScalarInt(*v as i32),
        EpicsValue::ULong(v) => ArchiverValue::ScalarInt(*v as i32),
        EpicsValue::UShort(v) => ArchiverValue::ScalarShort(*v as i32),
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
        EpicsValue::UInt64Array(v) => {
            ArchiverValue::VectorInt(v.iter().map(|x| *x as i32).collect())
        }
        EpicsValue::ULongArray(v) => {
            ArchiverValue::VectorInt(v.iter().map(|x| *x as i32).collect())
        }
        EpicsValue::UShortArray(v) => {
            ArchiverValue::VectorShort(v.iter().map(|x| *x as i32).collect())
        }
        EpicsValue::CharArray(v) => ArchiverValue::VectorChar(v.clone()),
        EpicsValue::StringArray(v) => {
            ArchiverValue::VectorString(v.iter().map(|s| s.to_string()).collect())
        }
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
    /// `try_send`s into this; when full, the OFFENDING SHARD's
    /// drop is recorded under
    /// `archiver_dispatcher_shard_overflow_drops_total{shard=N}`
    /// and the sample's per-PV `buffer_overflow_drops` counter is
    /// bumped. Other shards keep flowing — that's the per-shard
    /// isolation guarantee.
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

/// Hash a PV name to a shard index in `0..n` — the dispatcher's
/// consistent-hash router. Pinning each PV to one shard keeps its
/// samples ordered and its per-PV writer slot touched by a single
/// shard worker. The flush owner is global and keyed by PV name
/// (not by shard), so this hash has no storage-side counterpart to
/// stay in lock-step with; it lives next to its only caller, the
/// dispatcher.
///
/// `DefaultHasher` is fine for in-process partitioning; hash quality
/// across restarts is irrelevant because the on-disk layout is keyed
/// by PV name, not shard.
fn shard_for_pv(pv: &str, n: usize) -> usize {
    use std::collections::hash_map::DefaultHasher;
    use std::hash::{Hash, Hasher};
    debug_assert!(n > 0);
    if n <= 1 {
        return 0;
    }
    let mut h = DefaultHasher::new();
    pv.hash(&mut h);
    (h.finish() % n as u64) as usize
}

/// Run an N-shard write pool: 1 dispatcher (hashes pv_name → shard)
/// plus N parallel `write_loop_with_config` workers. Each shard has
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

    // Coalescing per-PV pending-timestamp map shared between the
    // flush owner, every shard worker, and every shard worker's
    // spawn_blocking late-success closure. Replaces the previous
    // mpsc report channel — see [`PendingReports`] for why.
    let pending = Arc::new(PendingReports::new());

    // Spawn the global flush owner first so it's ready to flush
    // as soon as shards start appending.
    let flush_owner_handle = tokio::spawn(flush_owner_loop(
        storage.clone(),
        registry.clone(),
        pending.clone(),
        shutdown.clone(),
        cfg.write_loop.clone(),
    ));

    if n == 1 {
        // Fast path: single shard, no dispatcher hop. We become
        // the shard worker ourselves and read from `rx` directly.
        shard_append_loop(
            0,
            storage.clone(),
            rx,
            pending.clone(),
            shutdown.clone(),
            cfg.write_loop.clone(),
        )
        .await;
    } else {
        // Multi-shard path: dispatcher + N shard workers.
        let mut shard_txs = Vec::with_capacity(n);
        let mut shard_handles = Vec::with_capacity(n);
        for shard_idx in 0..n {
            let (s_tx, s_rx) = mpsc::channel::<PvSample>(cfg.per_shard_buffer);
            shard_txs.push(s_tx);
            let storage = storage.clone();
            let pending = pending.clone();
            let shard_shutdown = shutdown.clone();
            let shard_cfg = cfg.write_loop.clone();
            shard_handles.push(tokio::spawn(shard_append_loop(
                shard_idx,
                storage,
                s_rx,
                pending,
                shard_shutdown,
                shard_cfg,
            )));
        }

        dispatch_loop(rx, shard_txs, shutdown.clone()).await;

        // Dispatcher exited → shard sample receivers see EOS or
        // the shutdown signal directly via their watch::Receiver.
        // Wait for shards to finish their drain branches.
        for h in shard_handles {
            let _ = h.await;
        }
    }

    // Flush owner uses its own shutdown-watch + drain_total_budget
    // grace timer, not an EOS signal — see flush_owner_loop. Wait
    // for it to finish.
    let _ = flush_owner_handle.await;
}

/// Reads the upstream main mpsc, hashes each sample's `pv_name`,
/// and forwards to the appropriate shard channel via
/// `Sender::try_send` for **per-shard isolation**.
///
/// `try_send` (not `send().await`) is the load-balancing
/// invariant: if one slow shard's channel is full, the dispatcher
/// drops THAT shard's overflow into `buffer_overflow_drops` and
/// keeps routing for the other shards. Using `send().await` would
/// block the dispatcher on the slow shard, back-pressuring the
/// upstream main channel and starving every other shard's PVs —
/// the exact failure mode the sharded layout is meant to prevent.
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
                    // Drain remaining samples (try_send for the
                    // same reason as the steady-state branch).
                    // Mirror the steady-state overflow accounting
                    // so a saturated shard's drain-time drops are
                    // visible to operators — silent loss here
                    // would shadow real samples lost during
                    // shutdown.
                    while let Ok(sample) = rx.try_recv() {
                        let idx = shard_for_pv(&sample.pv_name, n);
                        match shard_txs[idx].try_send(sample) {
                            Ok(()) => {}
                            Err(mpsc::error::TrySendError::Full(s)) => {
                                if let Some(c) = s.counters.as_ref() {
                                    c.buffer_overflow_drops
                                        .fetch_add(1, Ordering::Relaxed);
                                }
                                metrics::counter!(
                                    "archiver_dispatcher_shard_overflow_drops_total",
                                    "shard" => idx.to_string(),
                                    "phase" => "shutdown_drain",
                                )
                                .increment(1);
                                debug!(
                                    shard = idx,
                                    pv = s.pv_name,
                                    "Shutdown-drain shard channel full; sample dropped"
                                );
                            }
                            Err(mpsc::error::TrySendError::Closed(s)) => {
                                warn!(
                                    shard = idx,
                                    pv = s.pv_name,
                                    "Shutdown-drain shard channel closed; sample dropped"
                                );
                            }
                        }
                    }
                    break;
                }
            }
            maybe = rx.recv() => {
                match maybe {
                    Some(sample) => {
                        let idx = shard_for_pv(&sample.pv_name, n);
                        match shard_txs[idx].try_send(sample) {
                            Ok(()) => {}
                            Err(mpsc::error::TrySendError::Full(s)) => {
                                // Shard saturated. Surface the
                                // drop on the SAMPLE's per-PV
                                // counter so operators can pin
                                // the loss to a specific PV+shard.
                                if let Some(c) = s.counters.as_ref() {
                                    c.buffer_overflow_drops
                                        .fetch_add(1, Ordering::Relaxed);
                                }
                                metrics::counter!(
                                    "archiver_dispatcher_shard_overflow_drops_total",
                                    "shard" => idx.to_string(),
                                )
                                .increment(1);
                                debug!(
                                    shard = idx,
                                    pv = s.pv_name,
                                    "Shard channel full; sample dropped \
                                     (per-shard isolation)"
                                );
                            }
                            Err(mpsc::error::TrySendError::Closed(s)) => {
                                // Shard worker died (panic or
                                // shutdown race). We'll lose this
                                // sample but keep the dispatcher
                                // alive for OTHER shards.
                                warn!(
                                    shard = idx,
                                    pv = s.pv_name,
                                    "Shard channel closed; sample dropped"
                                );
                            }
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

/// RAII guard that clears the `flush_in_flight` flag when dropped,
/// even if the surrounding spawn_blocking closure panics. Without
/// this, an unwind inside `block_on(flush_ingest_writes)` would
/// skip the manual `store(false)` and leave the flag stuck `true`
/// forever — every subsequent ticker would see "still in flight"
/// and silently skip flushing, freezing the registry's `last_event`.
struct FlushInFlightGuard {
    flag: Arc<std::sync::atomic::AtomicBool>,
}

impl Drop for FlushInFlightGuard {
    fn drop(&mut self) {
        self.flag.store(false, Ordering::Release);
    }
}

/// Run one flush + commit cycle.
///
/// Spawns `flush_ingest_writes` on the blocking pool with a bounded
/// timeout, then commits the pending registry timestamps for every
/// PV the flush returned cleanly. Maintains an `in_flight` flag set
/// by the spawn_blocking task so a caller (the ticker branch) can
/// avoid stacking concurrent flushes when the previous one is still
/// running on a wedged FS.
///
/// Returns `true` if a flush was actually launched, `false` if the
/// helper was a no-op (in_flight already set, ts_updates empty).
///
/// Mutates `ts_updates`:
///   * Clean flush: drops `failed` PVs (bytes lost), commits all
///     other entries, drops committed entries on success.
///   * Deferred PVs: STAY in `ts_updates` for the next cycle.
///     Their bytes are still buffered, not lost.
///   * Flush error / panic: leaves every entry intact for retry.
///   * Flush timeout: CLEARS every entry. We don't know which PVs
///     reached disk; the in-flight task may still be running and
///     may even fail late, removing a writer from the cache. The
///     next flush would then see a "clean" state for that PV and
///     wrongly commit. Conservative drop is the only safe move
///     (Java archiver has the same trade-off — under-commit on
///     timeout, never over-commit). Future samples will repopulate
///     `ts_updates` and the next clean flush catches the registry up.
async fn run_flush_and_commit(
    storage: &Arc<dyn StoragePlugin>,
    registry: &Arc<PvRegistry>,
    pending: &Arc<PendingReports>,
    flush_timeout: Duration,
    in_flight: &Arc<std::sync::atomic::AtomicBool>,
) -> bool {
    if in_flight.load(Ordering::Acquire) {
        // Previous flush hasn't finished. Don't queue another —
        // we'd just stack spawn_blocking tasks on the wedged FS
        // and saturate the blocking pool. Wait for the existing
        // one to drain.
        return false;
    }
    // Snapshot the current pending map BEFORE setting in_flight
    // so concurrent shards reporting after this point survive
    // into the next cycle (their entries will be in
    // `pending` but not in `snapshot`, so we won't remove them
    // when we clean up). The snapshot is the authoritative view
    // of "what we're trying to commit this cycle".
    let snapshot = pending.snapshot();
    if snapshot.is_empty() {
        return false;
    }
    in_flight.store(true, Ordering::Release);

    let storage_for_flush = storage.clone();
    let in_flight_for_task = in_flight.clone();
    let flush_join = tokio::task::spawn_blocking(move || {
        // RAII guard: clears `in_flight` on every exit path
        // (return, panic, future-cancellation). The guard's
        // existence is the contract — manually storing false
        // BEFORE returning would skip cleanup on a panic inside
        // block_on / inside flush_ingest_writes, leaving the flag
        // stuck `true` and silently freezing all future flushes.
        let _guard = FlushInFlightGuard {
            flag: in_flight_for_task,
        };
        let rt = tokio::runtime::Handle::current();
        rt.block_on(storage_for_flush.flush_ingest_writes())
    });
    match tokio::time::timeout(flush_timeout, flush_join).await {
        Ok(Ok(Ok(IngestFlushResult { failed, deferred }))) => {
            // Observed success: NOW — and only now — drain the
            // storage loss queue, in the owner's own context,
            // atomically with acting on it below. A timed-out or
            // panicked flush never reaches this branch, so it can
            // never consume a loss marker the owner won't apply.
            // This closes the over-commit hole where an abandoned
            // spawn_blocking flush task drained the queue late and
            // its result (the lost-PV list) was discarded — the
            // next clean flush would then commit a stale
            // `last_event` for bytes that never reached disk.
            //
            // The drained markers carry losses recorded outside this
            // pass (LRU dirty-eviction, partition rollover,
            // ghost-file, ETL evict-by-path, read-side flush
            // failures). Merge them with this pass's own `failed`
            // and dedupe.
            let mut failed = failed;
            failed.extend(storage.take_loss_markers());
            failed.sort();
            failed.dedup();
            // Failed PVs: bytes lost. Drop every failed PV's
            // pending entry UNCONDITIONALLY — regardless of
            // whether the snapshot's value still matches.
            //
            // The previous "remove only if matches snapshot"
            // policy assumed a concurrent shard's post-snapshot
            // report meant "newer bytes ARE on disk". That
            // assumption fails for the eviction/loss-queue path:
            // the loss queue records only PV names, so a loss
            // event recorded between snapshot and flush-return
            // has no way to communicate which writer generation
            // (or timestamp horizon) it applies to. A
            // post-snapshot pending entry could refer to bytes
            // that ARE on disk (fresh writer after eviction) OR
            // bytes that were also lost — we can't tell.
            //
            // Conservative drop is the safe choice: never
            // commit a `last_event` for a PV the storage just
            // told us had lost bytes, even at the cost of an
            // occasional under-commit that the next sample
            // resolves.
            if !failed.is_empty() {
                pending.remove_failed(&failed);
                error!(
                    "STS flush dropped {} PV(s) from timestamp commit \
                     (bytes never reached disk): {:?}",
                    failed.len(),
                    failed
                );
            }
            // Deferred PVs: writer slot busy, bytes still buffered.
            // We do NOT remove them from `pending` (next cycle
            // will catch them) and we EXCLUDE them from this
            // cycle's commit batch.
            if !deferred.is_empty() {
                debug!(
                    "STS flush deferred {} PV(s); keeping in pending \
                     for next cycle: {:?}",
                    deferred.len(),
                    deferred
                );
            }
            // Build commit batch from the snapshot, excluding
            // failed (bytes lost) and deferred (bytes not on disk
            // yet).
            let failed_set: std::collections::HashSet<&str> =
                failed.iter().map(|s| s.as_str()).collect();
            let deferred_set: std::collections::HashSet<&str> =
                deferred.iter().map(|s| s.as_str()).collect();
            let to_commit: Vec<(&str, SystemTime)> = snapshot
                .iter()
                .filter(|(pv, _)| {
                    !failed_set.contains(pv.as_str()) && !deferred_set.contains(pv.as_str())
                })
                .map(|(pv, ts)| (pv.as_str(), *ts))
                .collect();
            if to_commit.is_empty() {
                return true;
            }
            match registry.batch_update_timestamps(&to_commit) {
                Ok(()) => {
                    // Remove committed entries from the shared
                    // map — but only if their current value still
                    // matches the snapshot's. This preserves
                    // concurrent updates that arrived between
                    // snapshot and remove.
                    let committed_map: std::collections::HashMap<String, SystemTime> = to_commit
                        .iter()
                        .map(|(pv, ts)| ((*pv).to_string(), *ts))
                        .collect();
                    pending.remove_committed(&committed_map);
                }
                Err(e) => {
                    // Don't remove — retry on next cycle.
                    // Otherwise a transient SQLite error orphans
                    // timestamps the disk has but the registry
                    // doesn't know about, with no retry path.
                    error!(
                        "Registry timestamp commit failed; \
                         keeping {} pending for retry: {e}",
                        snapshot.len()
                    );
                }
            }
        }
        Ok(Ok(Err(e))) => {
            error!(
                "STS ingest flush errored; deferring all {} \
                 pending timestamp commits: {e}",
                snapshot.len()
            );
        }
        Ok(Err(join_err)) => {
            error!(
                "STS ingest flush task panicked; deferring all {} \
                 pending timestamp commits: {join_err}",
                snapshot.len()
            );
        }
        Err(_) => {
            // Flush wedged. Conservative drop: the spawn_blocking
            // task may still be running and may even fail late
            // (which would remove a PV's writer from the cache,
            // making the NEXT flush return "clean" and tricking
            // the owner into committing a stale value for a PV
            // whose old bytes are gone). Drop the snapshot's
            // entries from `pending` (only if unchanged), so the
            // owner doesn't trust them; future samples rebuild
            // pending and the next clean flush catches the
            // registry up.
            //
            // The `in_flight` flag stays TRUE until the
            // spawn_blocking task naturally finishes — this
            // throttles us from queueing yet another flush onto
            // the wedged FS until it clears.
            metrics::counter!("archiver_storage_flush_timeouts_total").increment(1);
            let dropped = snapshot.len();
            pending.remove_committed(&snapshot);
            error!(
                "STS ingest flush timed out after {flush_timeout:?}; \
                 conservatively dropped {dropped} pending timestamp \
                 commit(s) (task remains on blocking pool; \
                 timestamps will be rebuilt from subsequent samples)"
            );
        }
    }
    true
}

/// Coalescing per-PV pending-timestamp map shared between every
/// shard worker (incl. their spawn_blocking late-success closures)
/// and the single global flush owner.
///
/// **Why a shared map instead of an mpsc channel?** With a channel,
/// a slow flush owner causes the buffer to fill and `try_send`
/// drops happen — for a PV that goes silent right after a dropped
/// report, the registry's `last_event` is permanently
/// under-committed. With a coalescing map keyed by PV, every
/// shard call is an `entry().and_modify().or_insert()` that
/// always succeeds; concurrent updates from different shards
/// never lose data because the map is bounded by **PV count**
/// (typical: thousands), not by sample rate (typical: 100k/s).
///
/// **Coalescing semantics.** A successful append for `(pv, ts)`
/// upserts the entry to `max(current, ts)`. A stale late-success
/// report (e.g. from a spawn_blocking task that finished after
/// shard already wrote a newer sample) does NOT clobber a newer
/// committed value.
#[derive(Default)]
pub struct PendingReports {
    inner: dashmap::DashMap<String, SystemTime>,
}

impl PendingReports {
    pub fn new() -> Self {
        Self::default()
    }

    /// Coalescing report. If `pv` already has a newer timestamp,
    /// no-op. Always succeeds — this is the channel-replacement
    /// invariant that makes silent-PV under-commit impossible.
    pub fn report(&self, pv: &str, ts: SystemTime) {
        // DashMap's entry() locks one shard internally. Fast-path
        // updates use `and_modify`; brand-new PVs go through
        // `or_insert`. No external lock contention across shards.
        self.inner
            .entry(pv.to_string())
            .and_modify(|cur| {
                if *cur < ts {
                    *cur = ts;
                }
            })
            .or_insert(ts);
    }

    /// Snapshot the entire map for a flush cycle. Returns a
    /// owned HashMap; the shared map stays intact so concurrent
    /// shards can keep reporting during the flush.
    pub fn snapshot(&self) -> std::collections::HashMap<String, SystemTime> {
        self.inner
            .iter()
            .map(|kv| (kv.key().clone(), *kv.value()))
            .collect()
    }

    /// Remove entries whose CURRENT value still equals what we
    /// committed. If a concurrent shard advanced an entry to a
    /// newer ts after the snapshot, leave it alone — the next
    /// flush will pick it up.
    pub fn remove_committed(&self, committed: &std::collections::HashMap<String, SystemTime>) {
        for (pv, &committed_ts) in committed {
            // remove_if applies the predicate atomically inside
            // the DashMap shard lock.
            let _ = self.inner.remove_if(pv, |_, v| *v == committed_ts);
        }
    }

    /// Unconditionally remove every failed PV's entry from the
    /// pending map, regardless of its current timestamp.
    ///
    /// **Why unconditional.** The storage's loss queue carries
    /// only PV names — it cannot tell us whether a concurrent
    /// shard's post-snapshot report was for bytes that ARE on
    /// disk (e.g., a fresh writer opened after eviction) versus
    /// bytes that are also gone. The matching `remove_committed`
    /// path can leave a post-snapshot entry behind, and the next
    /// clean flush would commit it — turning a "PV's bytes were
    /// lost" report into a `last_event` lie.
    ///
    /// The trade-off: a brand-new sample whose bytes truly DID
    /// reach disk after the loss event gets dropped from this
    /// commit cycle. Future samples re-populate `pending` and the
    /// registry catches up. Under-commit is the safe direction;
    /// over-commit is never acceptable.
    pub fn remove_failed(&self, failed: &[String]) {
        for pv in failed {
            let _ = self.inner.remove(pv);
        }
    }

    pub fn is_empty(&self) -> bool {
        self.inner.is_empty()
    }

    pub fn len(&self) -> usize {
        self.inner.len()
    }
}

/// Same as [`write_loop`] but accepts the full [`WriteLoopConfig`].
///
/// Thin wrapper that runs a 1-shard sharded pool — kept on the
/// public surface for tests and any direct callers. The actual
/// work happens in [`run_sharded_write_pool`].
pub async fn write_loop_with_config(
    storage: Arc<dyn StoragePlugin>,
    registry: Arc<PvRegistry>,
    rx: mpsc::Receiver<PvSample>,
    shutdown: tokio::sync::watch::Receiver<bool>,
    cfg: WriteLoopConfig,
) {
    let pool_cfg = ShardedWritePoolConfig {
        shards: 1,
        // unused on the 1-shard fast path (rx is forwarded directly)
        per_shard_buffer: 4096,
        write_loop: cfg,
    };
    run_sharded_write_pool(storage, registry, rx, shutdown, pool_cfg).await;
}

/// Per-shard append worker. Drains samples from `sample_rx`,
/// drops out-of-order or type-changed samples, then runs each
/// `append_event_with_meta` on the blocking pool with a bounded
/// timeout. On every success — including the late-success path
/// where write_loop's outer timeout already abandoned the
/// JoinHandle — coalesces `(pv, ts)` into the shared
/// [`PendingReports`] map so the global flush owner sees it on
/// the next flush cycle.
///
/// The shard worker holds NO ts_updates / flush state of its own.
/// It tracks `last_ts` and `last_dbr_type` only for the
/// per-PV ordering / type-change drop checks; those are local
/// to the shard because the dispatcher's consistent hash pins each
/// PV to one shard.
async fn shard_append_loop(
    shard_idx: usize,
    storage: Arc<dyn StoragePlugin>,
    mut sample_rx: mpsc::Receiver<PvSample>,
    pending: Arc<PendingReports>,
    mut shutdown: tokio::sync::watch::Receiver<bool>,
    cfg: WriteLoopConfig,
) {
    let append_timeout = cfg.append_timeout;
    info!(shard = shard_idx, "Shard append loop started");
    // Per-PV state for the in-shard sanity drops. The dispatcher's
    // consistent hash keeps each PV in one shard, so these maps
    // never see cross-shard PVs.
    let mut last_ts: std::collections::HashMap<String, SystemTime> =
        std::collections::HashMap::new();
    let mut last_dbr_type: std::collections::HashMap<String, ArchDbType> =
        std::collections::HashMap::new();

    loop {
        tokio::select! {
            Some(pv_sample) = sample_rx.recv() => {
                shard_handle_sample(
                    shard_idx,
                    &storage,
                    &pending,
                    pv_sample,
                    &mut last_ts,
                    &mut last_dbr_type,
                    append_timeout,
                )
                .await;
            }
            _ = shutdown.changed() => {
                shard_drain_on_shutdown(
                    shard_idx,
                    &storage,
                    &pending,
                    &mut sample_rx,
                    &mut last_ts,
                    &mut last_dbr_type,
                    &cfg,
                )
                .await;
                break;
            }
        }
    }
    info!(shard = shard_idx, "Shard append loop exited");
}

/// Process one sample on the shard's hot path. Extracted so the
/// steady-state and shutdown-drain branches can share the same
/// "drop checks → spawn_blocking append → report on success"
/// recipe.
async fn shard_handle_sample(
    shard_idx: usize,
    storage: &Arc<dyn StoragePlugin>,
    pending: &Arc<PendingReports>,
    pv_sample: PvSample,
    last_ts: &mut std::collections::HashMap<String, SystemTime>,
    last_dbr_type: &mut std::collections::HashMap<String, ArchDbType>,
    append_timeout: Duration,
) {
    let ts = pv_sample.sample.timestamp;

    // Out-of-order timestamp drop. Storage requires monotonic
    // appends per PV; an older timestamp would produce a corrupt
    // partition. Tracked via shard-local `last_ts` so the check
    // survives flush cycles (the legacy ts_updates-based check
    // only caught within-cycle reorderings).
    if let Some(prev_ts) = last_ts.get(&pv_sample.pv_name)
        && ts < *prev_ts
    {
        if let Some(ref c) = pv_sample.counters {
            c.timestamp_drops.fetch_add(1, Ordering::Relaxed);
        }
        debug!(
            shard = shard_idx,
            pv = pv_sample.pv_name,
            ?ts,
            ?prev_ts,
            "Dropping out-of-order sample"
        );
        return;
    }

    // Type-change drop. The first sample defines the PV's wire
    // type; later samples with a different DBR get dropped
    // (operator must changeTypeForPV first).
    let prev_type = last_dbr_type.insert(pv_sample.pv_name.clone(), pv_sample.dbr_type);
    if let Some(prev) = prev_type
        && prev != pv_sample.dbr_type
    {
        if let Some(ref c) = pv_sample.counters {
            c.type_change_drops.fetch_add(1, Ordering::Relaxed);
            // Java parity (9f2234f): record the latest observed
            // DBR so the dropped-events report can show what the
            // IOC is now sending vs the archived type.
            c.latest_observed_dbr
                .store(pv_sample.dbr_type as i32, Ordering::Relaxed);
        }
        debug!(
            shard = shard_idx,
            pv = pv_sample.pv_name,
            ?prev,
            new = ?pv_sample.dbr_type,
            "Dropping type-changed sample"
        );
        // Restore prev_type so a single mismatched sample doesn't
        // permanently flip our recorded type.
        last_dbr_type.insert(pv_sample.pv_name.clone(), prev);
        return;
    }

    let meta = AppendMeta {
        element_count: pv_sample.element_count,
        ..Default::default()
    };
    let pv_name_for_post = pv_sample.pv_name.clone();
    let counters_for_post = pv_sample.counters.clone();
    let counters_in_task = pv_sample.counters.clone();
    let storage_for_task = storage.clone();
    let pending_for_task = pending.clone();

    // Bound each append + isolate sync syscall hangs.
    // `spawn_blocking` moves the actual I/O to the blocking
    // thread pool, so a stuck NFS write parks a blocking-pool
    // thread instead of a runtime worker. The
    // `tokio::time::timeout` then bounds how long the shard waits
    // for that join — a stuck task is abandoned and the shard
    // continues.
    //
    // Late-success path: a timed-out task may still complete its
    // write later. The closure ALWAYS reports into the shared
    // `PendingReports` map on Ok (whether or not the shard
    // already moved on), so the global flush owner picks up late
    // successes too. Per-PV serialization inside PlainPB keeps
    // any late completion ordered behind subsequent same-PV
    // samples. The map's coalescing semantics ensure a stale late
    // success does NOT clobber a newer hot-path commit.
    let join = tokio::task::spawn_blocking(move || {
        let rt = tokio::runtime::Handle::current();
        let res = rt.block_on(storage_for_task.append_event_with_meta(
            &pv_sample.pv_name,
            pv_sample.dbr_type,
            &pv_sample.sample,
            &meta,
        ));
        if res.is_ok() {
            metrics::counter!("archiver_events_stored_total").increment(1);
            if let Some(c) = counters_in_task.as_ref() {
                c.events_stored.fetch_add(1, Ordering::Relaxed);
            }
            // Coalesce into the shared map. Always succeeds —
            // unlike the previous mpsc try_send, a saturated
            // flush owner cannot cause this report to drop, so
            // a PV that goes silent right after a successful
            // append still gets its `last_event` committed once
            // the owner gets to its next flush cycle.
            pending_for_task.report(&pv_sample.pv_name, ts);
        }
        res
    });
    let res = tokio::time::timeout(append_timeout, join).await;
    // Conservative ordering high-water (principle 5). Bump
    // `last_ts` on EVERY storage-layer return path — success,
    // error, panic, timeout — not just on Ok and timeout.
    //
    // Why all four:
    //   * Ok       — sample on disk; obvious bump.
    //   * Timeout  — sample MAY land on disk via late success;
    //                bump to keep on-disk order monotonic.
    //   * Error    — current storage contract is binary, but a
    //                future partial-success-with-error variant
    //                could leave bytes on disk. Bumping now
    //                hedges against that.
    //   * Panic    — closure aborted mid-write; storage state
    //                is undefined. Bumping is the safe choice.
    //
    // The pre-check earlier in this function already drops
    // out-of-order and type-changed samples BEFORE we get here,
    // so this bump only ever sets the high-water to a ts the
    // shard explicitly accepted into the storage layer.
    last_ts.insert(pv_name_for_post.clone(), ts);
    match res {
        Ok(Ok(Ok(()))) => {
            // Report already went out from inside the closure.
        }
        Ok(Ok(Err(e))) => {
            error!(shard = shard_idx, pv = pv_name_for_post, "Write error: {e}");
        }
        Ok(Err(join_err)) => {
            error!(
                shard = shard_idx,
                pv = pv_name_for_post,
                "Storage write task panicked: {join_err}"
            );
        }
        Err(_) => {
            // Sample MAY still land on disk later (per-PV
            // serialization keeps order). The closure will report
            // into the shared pending map whenever it eventually
            // completes, so registry's `last_event` catches up
            // via the late path even though we move on now.
            error!(
                shard = shard_idx,
                pv = pv_name_for_post,
                "Storage append timed out after {append_timeout:?}; \
                 shard abandoning (task remains on blocking pool, \
                 may still succeed late)"
            );
            if let Some(ref c) = counters_for_post {
                c.storage_append_timeouts.fetch_add(1, Ordering::Relaxed);
            }
        }
    }
}

/// Drain the shard's remaining buffered samples on shutdown,
/// applying the same bounded-timeout discipline as the hot path.
async fn shard_drain_on_shutdown(
    shard_idx: usize,
    storage: &Arc<dyn StoragePlugin>,
    pending: &Arc<PendingReports>,
    sample_rx: &mut mpsc::Receiver<PvSample>,
    last_ts: &mut std::collections::HashMap<String, SystemTime>,
    last_dbr_type: &mut std::collections::HashMap<String, ArchDbType>,
    cfg: &WriteLoopConfig,
) {
    let drain_per_sample_timeout = cfg.drain_per_sample_timeout;
    let drain_total_budget = cfg.drain_total_budget;
    let drain_start = std::time::Instant::now();
    let mut drained_skipped = 0usize;
    while let Ok(pv_sample) = sample_rx.try_recv() {
        if drain_start.elapsed() > drain_total_budget {
            drained_skipped += 1;
            continue;
        }
        // Reuse the hot-path handler with a tighter timeout.
        // Late-success reports still flow into `pending` because
        // the closure clones the Arc independently of this
        // shard's lifecycle.
        shard_handle_sample(
            shard_idx,
            storage,
            pending,
            pv_sample,
            last_ts,
            last_dbr_type,
            drain_per_sample_timeout,
        )
        .await;
    }
    if drained_skipped > 0 {
        warn!(
            shard = shard_idx,
            "Shutdown drain budget ({drain_total_budget:?}) exhausted; \
             {drained_skipped} buffered sample(s) abandoned without \
             attempting append"
        );
    }
}

/// Single global flush owner. Reads from the shared
/// [`PendingReports`] map (which all shards coalesce into),
/// drives the periodic `flush_ingest_writes`, and commits to the
/// registry. Because ALL shards report into this one map and the
/// owner is the only consumer, the flush result and the
/// pending-commit data live in the same task — no shard 0 vs
/// shard 1 attribution skew.
async fn flush_owner_loop(
    storage: Arc<dyn StoragePlugin>,
    registry: Arc<PvRegistry>,
    pending: Arc<PendingReports>,
    mut shutdown: tokio::sync::watch::Receiver<bool>,
    cfg: WriteLoopConfig,
) {
    let flush_period = cfg.flush_period;
    let flush_timeout = cfg.flush_timeout;
    let shutdown_flush_timeout = cfg.shutdown_flush_timeout;
    let drain_total_budget = cfg.drain_total_budget;
    // tokio::time::interval(Duration::ZERO) panics. Config-side
    // validation rejects 0 at load time, but defending in depth
    // here keeps tests and direct callers (which build configs by
    // hand) from crashing the engine on a misconfigured value.
    let flush_period = if flush_period.is_zero() {
        warn!(
            "flush_owner: flush_period was Duration::ZERO; \
             clamping to 1s to avoid tokio::time::interval panic"
        );
        Duration::from_secs(1)
    } else {
        flush_period
    };
    info!("Flush owner started");

    let mut flush_ticker = tokio::time::interval(flush_period);
    flush_ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    let _ = flush_ticker.tick().await;

    let flush_in_flight = Arc::new(std::sync::atomic::AtomicBool::new(false));

    // Phase 1: steady state. Ticker-driven flush only. Reports
    // accumulate in the shared `PendingReports` map continuously
    // (no recv loop — shards write directly), so we don't need
    // to multiplex report consumption against the flush.
    loop {
        tokio::select! {
            biased;
            _ = shutdown.changed() => {
                if *shutdown.borrow() {
                    break;
                }
            }
            _ = flush_ticker.tick() => {
                if !pending.is_empty() {
                    run_flush_and_commit(
                        &storage,
                        &registry,
                        &pending,
                        flush_timeout,
                        &flush_in_flight,
                    )
                    .await;
                }
            }
        }
    }

    // Phase 2: shutdown grace. Two windows in one budget:
    //
    //   1. A brief minimum sleep so any in-flight spawn_blocking
    //      late-success closures can coalesce their reports into
    //      `pending` before the final flush snapshots it.
    //
    //   2. Wait for any still-running ticker flush to drain
    //      (`flush_in_flight` cleared by the spawn_blocking task's
    //      RAII guard). Without this, the final
    //      `run_flush_and_commit` would see `in_flight == true`,
    //      short-circuit, and skip every entry in `pending` —
    //      data on disk but no registry commit.
    //
    // Both windows fit inside the operator-configured
    // `drain_total_budget`. If a flush is genuinely wedged past
    // the budget, we proceed to final flush; that call will see
    // `in_flight` still set and short-circuit, but at least we
    // didn't pin the process forever. Future samples on restart
    // will re-populate `pending` and the registry catches up.
    let phase2_deadline = std::time::Instant::now() + drain_total_budget;
    let min_grace = std::cmp::min(drain_total_budget, Duration::from_millis(200));
    if !min_grace.is_zero() {
        tokio::time::sleep(min_grace).await;
    }
    while flush_in_flight.load(Ordering::Acquire) && std::time::Instant::now() < phase2_deadline {
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    if flush_in_flight.load(Ordering::Acquire) {
        warn!(
            "Shutdown grace exhausted while a flush is still in flight; \
             final flush will be skipped (pending entries left for next \
             process restart to rebuild from re-archived samples)"
        );
    }

    // Final flush + commit. Use shutdown_flush_timeout (typically
    // shorter than the steady-state flush_timeout) so a wedged FS
    // doesn't block shutdown indefinitely.
    info!(
        pending_at_shutdown = pending.len(),
        "Flush owner running final flush + commit"
    );
    if !pending.is_empty() {
        run_flush_and_commit(
            &storage,
            &registry,
            &pending,
            shutdown_flush_timeout,
            &flush_in_flight,
        )
        .await;
    }
    info!("Flush owner exiting");
}

#[cfg(test)]
mod pva_mapping_tests {
    use super::*;
    use epics_rs::pva::pvdata::PvStructure;
    use epics_rs::pva::pvdata::encode::{decode_pv_field, decode_type_desc};
    use std::io::Cursor;

    /// Build a synthetic NTTable with two double columns. Mirrors what
    /// a live IOC publishes for a typical waveform-table channel.
    fn make_nttable() -> PvField {
        let mut table = PvStructure::new("epics:nt/NTTable:1.0");
        let labels = TypedScalarArray::String(["x", "y"].iter().map(|s| (*s).into()).collect());
        table
            .fields
            .push(("labels".into(), PvField::ScalarArrayTyped(labels)));
        let mut value_inner = PvStructure::new("");
        value_inner.fields.push((
            "x".into(),
            PvField::ScalarArrayTyped(TypedScalarArray::Double(vec![1.0, 2.0, 3.0].into())),
        ));
        value_inner.fields.push((
            "y".into(),
            PvField::ScalarArrayTyped(TypedScalarArray::Double(vec![4.0, 5.0, 6.0].into())),
        ));
        table
            .fields
            .push(("value".into(), PvField::Structure(value_inner)));
        PvField::Structure(table)
    }

    #[test]
    fn nttable_classifies_as_v4_generic_bytes() {
        let pv = make_nttable();
        let (db, ec) = pv_field_to_arch_db_type(&pv).expect("classified");
        assert_eq!(db, ArchDbType::V4GenericBytes);
        assert_eq!(ec, 1);
    }

    #[test]
    fn nttable_round_trips_through_self_describing_bytes() {
        let pv = make_nttable();
        let av = pv_field_scalar_to_archiver(&pv, &pv.descriptor()).expect("converted");
        let bytes = match av {
            ArchiverValue::V4GenericBytes(b) => b,
            other => panic!("expected V4GenericBytes, got {:?}", other.db_type()),
        };
        let mut cur = Cursor::new(bytes.as_slice());
        let desc = decode_type_desc(&mut cur, ByteOrder::Big).expect("desc decode");
        let decoded = decode_pv_field(&desc, &mut cur, ByteOrder::Big).expect("value decode");
        let PvField::Structure(s) = decoded else {
            panic!("expected Structure after decode");
        };
        assert_eq!(s.struct_id, "epics:nt/NTTable:1.0");
        // labels column survives + value substructure has 2 columns.
        let labels = s
            .fields
            .iter()
            .find_map(|(n, f)| if n == "labels" { Some(f) } else { None })
            .expect("labels");
        let Some(PvField::ScalarArrayTyped(TypedScalarArray::String(arr))) = Some(labels) else {
            panic!("labels not a string array");
        };
        assert_eq!(arr.len(), 2);
        let value = s
            .fields
            .iter()
            .find_map(|(n, f)| {
                if n == "value" {
                    if let PvField::Structure(v) = f {
                        Some(v)
                    } else {
                        None
                    }
                } else {
                    None
                }
            })
            .expect("value substruct");
        assert_eq!(value.fields.len(), 2);
    }

    /// NTScalarArray of doubles should classify as WaveformDouble and
    /// convert to VectorDouble — this is the pre-existing PVA waveform
    /// path that was silently broken by the missing ScalarArrayTyped
    /// branch.
    #[test]
    fn nt_scalar_array_double_classifies_as_waveform() {
        let mut wrapper = PvStructure::new("epics:nt/NTScalarArray:1.0");
        wrapper.fields.push((
            "value".into(),
            PvField::ScalarArrayTyped(TypedScalarArray::Double(vec![1.5, 2.5, 3.5].into())),
        ));
        let pv = PvField::Structure(wrapper);

        let (db, ec) = pv_field_to_arch_db_type(&pv).expect("classified");
        assert_eq!(db, ArchDbType::WaveformDouble);
        assert_eq!(ec, 3);
        let av = pv_field_scalar_to_archiver(&pv, &pv.descriptor()).expect("converted");
        match av {
            ArchiverValue::VectorDouble(v) => assert_eq!(v, vec![1.5, 2.5, 3.5]),
            other => panic!("expected VectorDouble, got {:?}", other.db_type()),
        }
    }

    /// NTEnum: classification short-circuits to ScalarEnum (Java
    /// archiver parity — store the index, not the whole enum_t
    /// structure as opaque V4 bytes).
    #[test]
    fn nt_enum_classifies_as_scalar_enum() {
        let pv = make_nt_enum(2, &["Zero", "One", "Two"]);
        let (db, ec) = pv_field_to_arch_db_type(&pv).expect("classified");
        assert_eq!(db, ArchDbType::ScalarEnum);
        assert_eq!(ec, 1);
    }

    /// NTEnum: value extraction yields ScalarEnum(index).
    #[test]
    fn nt_enum_value_is_index() {
        let pv = make_nt_enum(2, &["Off", "On"]);
        let av = pv_field_scalar_to_archiver(&pv, &pv.descriptor()).expect("converted");
        match av {
            ArchiverValue::ScalarEnum(i) => assert_eq!(i, 2),
            other => panic!("expected ScalarEnum, got {:?}", other.db_type()),
        }
    }

    /// NTEnum: monitor callback feeds choices into the extras cache
    /// under the `enum_strs` key as a JSON-encoded string array.
    /// Downstream `attach_extras` then mirrors it into the sample's
    /// `field_values`, which the retrieval JSON surfaces under `meta`.
    #[test]
    fn nt_enum_extras_carries_json_choices() {
        let pv = make_nt_enum(1, &["Off", "On", "Trip"]);
        let extras: ExtraFieldsCache = DashMap::new();
        refresh_nt_enum_extras(&pv, &extras);
        let stored = extras
            .get("enum_strs")
            .map(|s| s.value().clone())
            .expect("enum_strs cached");
        assert_eq!(stored, r#"["Off","On","Trip"]"#);
    }

    /// Top-level `UnionArray` PV: with the canonical channel
    /// descriptor plumbed through (per epics-rs `descriptor()` doc
    /// guidance), wire encoding preserves the variants list and the
    /// values round-trip. The archiver invariant requires the
    /// canonical descriptor at every callsite — there is no
    /// value-recovery fallback path to contrast against any more.
    #[test]
    fn union_array_roundtrips_with_canonical_descriptor() {
        use epics_rs::pva::pvdata::encode::{decode_pv_field, decode_type_desc};
        use epics_rs::pva::pvdata::{FieldDesc, UnionItem};
        use std::io::Cursor;

        let variants_desc = vec![
            ("intVal".to_string(), FieldDesc::Scalar(ScalarType::Int)),
            ("dblVal".to_string(), FieldDesc::Scalar(ScalarType::Double)),
        ];
        let canonical = FieldDesc::UnionArray {
            struct_id: String::new(),
            variants: variants_desc.clone(),
        };
        let items = vec![
            Some(UnionItem {
                selector: 0,
                variant_name: "intVal".into(),
                value: PvField::Scalar(ScalarValue::Int(42)),
            }),
            Some(UnionItem {
                selector: 1,
                variant_name: "dblVal".into(),
                value: PvField::Scalar(ScalarValue::Double(1.5)),
            }),
        ];
        let field = PvField::UnionArray(items);

        let wire = encode_pv_field_self_describing(&field, &canonical);
        let mut cur = Cursor::new(wire.as_slice());
        let desc_back = decode_type_desc(&mut cur, ByteOrder::Big).expect("desc");
        let val_back = decode_pv_field(&desc_back, &mut cur, ByteOrder::Big).expect("value");
        let PvField::UnionArray(items_back) = val_back else {
            panic!("expected UnionArray, got {val_back:?}");
        };
        assert_eq!(items_back.len(), 2);
        let item0 = items_back[0].as_ref().expect("union element 0 present");
        assert_eq!(item0.selector, 0);
        assert!(matches!(item0.value, PvField::Scalar(ScalarValue::Int(42))));
        let item1 = items_back[1].as_ref().expect("union element 1 present");
        assert_eq!(item1.selector, 1);
        match &item1.value {
            PvField::Scalar(ScalarValue::Double(d)) => assert!((*d - 1.5).abs() < 1e-9),
            other => panic!("variant 1 not Double, got {other:?}"),
        }

        // Descriptor's variants list survives the wire round-trip
        // (this is what was lost in the pre-plumb-through behaviour).
        match desc_back {
            FieldDesc::UnionArray { variants, .. } => {
                assert_eq!(variants.len(), 2);
                assert_eq!(variants[0].0, "intVal");
                assert!(matches!(variants[0].1, FieldDesc::Scalar(ScalarType::Int)));
                assert_eq!(variants[1].0, "dblVal");
                assert!(matches!(
                    variants[1].1,
                    FieldDesc::Scalar(ScalarType::Double)
                ));
            }
            other => panic!("expected UnionArray descriptor, got {other:?}"),
        }
    }

    /// NTNDArray-style root: `value` is a `Union` over ten POD scalar
    /// array variants (one for each NT-spec POD type). Live PVA only
    /// ever ships ONE variant per sample, but the channel-INIT
    /// descriptor advertises all ten — and the archiver invariant
    /// requires every V4GenericBytes sample to carry the channel-INIT
    /// descriptor verbatim. This test pushes a `PvField` shaped like
    /// a live monitor event through `pv_field_scalar_to_archiver`
    /// using a multi-variant canonical descriptor, then decodes the
    /// resulting wire bytes and confirms ALL ten variant slots
    /// survive on disk. Previously (descriptor-recovery fallback)
    /// only the active variant would round-trip — regression guard
    /// for that bug.
    #[test]
    fn nt_nd_array_union_preserves_all_variants_via_canonical_desc() {
        use epics_rs::pva::pvdata::FieldDesc;
        use epics_rs::pva::pvdata::encode::{decode_pv_field, decode_type_desc};
        use std::io::Cursor;

        // The ten NT-spec POD variants for NTNDArray::value.
        let variant_specs: &[(&str, ScalarType)] = &[
            ("booleanValue", ScalarType::Boolean),
            ("byteValue", ScalarType::Byte),
            ("shortValue", ScalarType::Short),
            ("intValue", ScalarType::Int),
            ("longValue", ScalarType::Long),
            ("ubyteValue", ScalarType::UByte),
            ("ushortValue", ScalarType::UShort),
            ("uintValue", ScalarType::UInt),
            ("ulongValue", ScalarType::ULong),
            ("floatValue", ScalarType::Float),
            ("doubleValue", ScalarType::Double),
        ];
        // Build the inner union descriptor with all eleven variants.
        let union_variants: Vec<(String, FieldDesc)> = variant_specs
            .iter()
            .map(|(n, st)| (n.to_string(), FieldDesc::ScalarArray(*st)))
            .collect();
        let canonical = FieldDesc::Structure {
            struct_id: "epics:nt/NTNDArray:1.0".into(),
            fields: vec![(
                "value".into(),
                FieldDesc::Union {
                    struct_id: String::new(),
                    variants: union_variants.clone(),
                },
            )],
        };

        // Build the runtime value: union currently holds the
        // `doubleValue` variant (selector index 10) — a typical live
        // sample shape.
        let mut root = PvStructure::new("epics:nt/NTNDArray:1.0");
        root.fields.push((
            "value".into(),
            PvField::Union {
                selector: 10,
                variant_name: "doubleValue".into(),
                value: Box::new(PvField::ScalarArrayTyped(TypedScalarArray::Double(
                    vec![1.0, 2.0, 3.0].into(),
                ))),
            },
        ));
        let pv = PvField::Structure(root);

        // Push through the V4 archiver pipeline.
        let av = pv_field_scalar_to_archiver(&pv, &canonical).expect("converted");
        let bytes = match av {
            ArchiverValue::V4GenericBytes(b) => b,
            other => panic!("expected V4GenericBytes, got {:?}", other.db_type()),
        };

        // Decode and confirm:
        //   (a) the active variant carries the live value;
        //   (b) the descriptor advertises all eleven variants (the
        //       sibling variants that descriptor-recovery would lose).
        let mut cur = Cursor::new(bytes.as_slice());
        let desc_back = decode_type_desc(&mut cur, ByteOrder::Big).expect("desc");
        let decoded = decode_pv_field(&desc_back, &mut cur, ByteOrder::Big).expect("value");
        let PvField::Structure(s) = decoded else {
            panic!("expected Structure root");
        };
        let value = s
            .fields
            .iter()
            .find_map(|(n, f)| if n == "value" { Some(f) } else { None })
            .expect("value");
        let PvField::Union {
            selector,
            variant_name,
            value: inner,
        } = value
        else {
            panic!("expected Union, got {value:?}");
        };
        assert_eq!(*selector, 10);
        assert_eq!(variant_name, "doubleValue");
        let PvField::ScalarArrayTyped(TypedScalarArray::Double(arr)) = inner.as_ref() else {
            panic!("expected Double[]");
        };
        assert_eq!(arr.as_ref(), &[1.0, 2.0, 3.0]);

        // The decoded descriptor's union variants list survived.
        let value_field_desc = match &desc_back {
            FieldDesc::Structure { fields, .. } => fields
                .iter()
                .find_map(|(n, f)| if n == "value" { Some(f) } else { None })
                .expect("value field in desc"),
            other => panic!("expected Structure descriptor, got {other:?}"),
        };
        let FieldDesc::Union {
            variants: v_back, ..
        } = value_field_desc
        else {
            panic!("expected Union descriptor for value");
        };
        assert_eq!(
            v_back.len(),
            variant_specs.len(),
            "all sibling Union variants must survive the wire round-trip — \
             this is what value-recovery descriptor would have dropped"
        );
        for (i, (expected_name, expected_st)) in variant_specs.iter().enumerate() {
            assert_eq!(&v_back[i].0, expected_name);
            assert!(matches!(v_back[i].1, FieldDesc::ScalarArray(st) if st == *expected_st));
        }
    }

    /// User-defined Structure that LOOKS like NTEnum but lacks the
    /// `epics:nt/NTEnum` struct_id is NOT treated as one — falls
    /// through to V4GenericBytes. Guards against false positives on
    /// custom structs that happen to use `index`/`choices` field
    /// names.
    #[test]
    fn structure_without_nt_enum_id_is_v4_bytes() {
        let mut inner = PvStructure::new("");
        inner
            .fields
            .push(("index".into(), PvField::Scalar(ScalarValue::Int(1))));
        inner.fields.push((
            "choices".into(),
            PvField::ScalarArrayTyped(TypedScalarArray::String(
                ["a", "b"].iter().map(|s| (*s).into()).collect(),
            )),
        ));
        let mut root = PvStructure::new("my:custom/Thing:1.0");
        root.fields
            .push(("value".into(), PvField::Structure(inner)));
        let pv = PvField::Structure(root);
        let (db, _) = pv_field_to_arch_db_type(&pv).expect("classified");
        assert_eq!(db, ArchDbType::V4GenericBytes);
    }

    fn make_nt_enum(index: i32, choices: &[&str]) -> PvField {
        let mut inner = PvStructure::new("enum_t");
        inner
            .fields
            .push(("index".into(), PvField::Scalar(ScalarValue::Int(index))));
        let arr = TypedScalarArray::String(choices.iter().map(|s| (*s).into()).collect());
        inner
            .fields
            .push(("choices".into(), PvField::ScalarArrayTyped(arr)));
        let mut root = PvStructure::new("epics:nt/NTEnum:1.0");
        root.fields
            .push(("value".into(), PvField::Structure(inner)));
        PvField::Structure(root)
    }

    /// Bare NTScalar of doubles still classifies as ScalarDouble and
    /// converts to ScalarDouble — regression guard for the pre-existing
    /// path that earlier callers depended on.
    #[test]
    fn nt_scalar_double_still_scalar() {
        let mut wrapper = PvStructure::new("epics:nt/NTScalar:1.0");
        wrapper
            .fields
            .push(("value".into(), PvField::Scalar(ScalarValue::Double(42.5))));
        let pv = PvField::Structure(wrapper);
        let (db, ec) = pv_field_to_arch_db_type(&pv).expect("classified");
        assert_eq!(db, ArchDbType::ScalarDouble);
        assert_eq!(ec, 1);
        match pv_field_scalar_to_archiver(&pv, &pv.descriptor()).expect("converted") {
            ArchiverValue::ScalarDouble(v) => assert!((v - 42.5).abs() < 1e-9),
            other => panic!("expected ScalarDouble, got {:?}", other.db_type()),
        }
    }
}
