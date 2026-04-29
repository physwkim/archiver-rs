use std::sync::{Arc, Mutex};
use std::time::{Duration, SystemTime};

use dashmap::DashMap;
use epics_rs::ca::client::{CaChannel, CaClient, ConnectionEvent};
use epics_rs::base::types::{DbFieldType, EpicsValue};
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

/// Connection state tracked per PV.
#[derive(Debug, Clone, Default)]
pub struct ConnectionInfo {
    pub connected_since: Option<SystemTime>,
    pub last_event_time: Option<SystemTime>,
    pub is_connected: bool,
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
    /// Storage backend.
    #[allow(dead_code)]
    storage: Arc<dyn StoragePlugin>,
    /// PV metadata registry.
    registry: Arc<PvRegistry>,
    /// Sample sender for the write thread.
    sample_tx: mpsc::Sender<PvSample>,
    /// Optional policy configuration.
    policy: Option<PolicyConfig>,
}

/// A sample ready to be written to storage.
pub struct PvSample {
    pub pv_name: String,
    pub dbr_type: ArchDbType,
    pub sample: ArchiverSample,
    pub element_count: Option<i32>,
}

impl ChannelManager {
    pub async fn new(
        storage: Arc<dyn StoragePlugin>,
        registry: Arc<PvRegistry>,
        policy: Option<PolicyConfig>,
    ) -> anyhow::Result<(Self, mpsc::Receiver<PvSample>)> {
        let ca_client = CaClient::new().await.map_err(|e| anyhow::anyhow!("{e}"))?;
        let (tx, rx) = mpsc::channel(SAMPLE_CHANNEL_CAPACITY);

        let mgr = Self {
            ca_client,
            channels: DashMap::new(),
            pending_archives: DashMap::new(),
            storage,
            registry,
            sample_tx: tx,
            policy,
        };

        Ok((mgr, rx))
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
            warn!(restored, failed = total - restored, "Some PVs failed to restore");
        }

        Ok(restored)
    }

    /// Start archiving a new PV.
    pub async fn archive_pv(
        &self,
        pv_name: &str,
        sample_mode: &SampleMode,
    ) -> anyhow::Result<()> {
        if self.channels.contains_key(pv_name) {
            anyhow::bail!("PV {pv_name} is already being archived");
        }

        // Atomically claim the PV to prevent concurrent archive_pv races.
        // The guard ensures cleanup even if this future is cancelled.
        if self.pending_archives.insert(pv_name.to_string(), ()).is_some() {
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
        let effective_mode = if let Some(ref policy) = self.policy {
            if let Some(p) = policy.find_policy(pv_name) {
                p.to_sample_mode()
            } else {
                sample_mode.clone()
            }
        } else {
            sample_mode.clone()
        };

        // Connect to discover the native type.
        let channel = self.ca_client.create_channel(pv_name);
        channel
            .wait_connected(CA_CONNECT_TIMEOUT)
            .await
            .map_err(|e| anyhow::anyhow!("Failed to connect to {pv_name}: {e}"))?;

        let info = self.ca_client.cainfo(pv_name).await
            .map_err(|e| anyhow::anyhow!("Failed to get info for {pv_name}: {e}"))?;

        let dbr_type = dbr_field_to_arch_type(info.native_type);
        let element_count = info.element_count as i32;

        // Register in SQLite.
        self.registry
            .register_pv(pv_name, dbr_type, &effective_mode, element_count)?;

        let record = self.registry.get_pv(pv_name)?
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

        self.channels.insert(
            pv_name.clone(),
            PvHandle {
                channel: channel.clone(),
                cancel_token: cancel_token.clone(),
                dbr_type,
                conn_info: conn_info.clone(),
                extras: extras.clone(),
                field_tokens: field_tokens.clone(),
                update_lock: Arc::new(tokio::sync::Mutex::new(())),
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
            );
        }

        let tx = self.sample_tx.clone();
        let token = cancel_token.clone();
        let ci = conn_info.clone();
        let extras_for_loop = extras.clone();

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
        let (parent_token, extras, field_tokens, update_lock) = {
            let Some(handle) = self.channels.get(pv_name) else {
                return Ok(());
            };
            (
                handle.cancel_token.clone(),
                handle.extras.clone(),
                handle.field_tokens.clone(),
                handle.update_lock.clone(),
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
        for key in to_remove {
            if let Some((_, token)) = field_tokens.remove(&key) {
                token.cancel();
            }
            extras.remove(&key);
        }

        // Spawn tasks for fields newly added. Existing fields keep their
        // task and their last cached value.
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
                );
            }
        }
        Ok(())
    }

    /// Pause archiving for a PV.
    pub fn pause_pv(&self, pv_name: &str) -> anyhow::Result<()> {
        if let Some((_key, handle)) = self.channels.remove(pv_name) {
            handle.cancel_token.cancel();
            metrics::gauge!("archiver_pvs_active").decrement(1.0);
        }
        self.registry.set_status(pv_name, PvStatus::Paused)?;
        info!(pv = pv_name, "Paused archiving");
        Ok(())
    }

    /// Resume a paused PV. Only paused or error PVs can be resumed;
    /// calling resume on an already-active PV is a no-op (returns Ok).
    pub async fn resume_pv(&self, pv_name: &str) -> anyhow::Result<()> {
        let record = self
            .registry
            .get_pv(pv_name)?
            .ok_or_else(|| anyhow::anyhow!("PV {pv_name} not found in registry"))?;

        // Guard: if already active with a live task, nothing to do.
        if record.status == PvStatus::Active && self.channels.contains_key(pv_name) {
            info!(pv = pv_name, "PV is already actively archived, skipping resume");
            return Ok(());
        }

        // Cancel any orphaned task to prevent duplicate subscriptions.
        if let Some((_key, handle)) = self.channels.remove(pv_name) {
            handle.cancel_token.cancel();
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
    pub fn stop_pv(&self, pv_name: &str) -> anyhow::Result<()> {
        if let Some((_key, handle)) = self.channels.remove(pv_name) {
            handle.cancel_token.cancel();
            metrics::gauge!("archiver_pvs_active").decrement(1.0);
        }
        self.registry.set_status(pv_name, PvStatus::Inactive)?;
        info!(pv = pv_name, "Stopped archiving (inactive)");
        Ok(())
    }

    /// Remove a PV from archiving entirely.
    pub fn destroy_pv(&self, pv_name: &str) -> anyhow::Result<()> {
        if let Some((_key, handle)) = self.channels.remove(pv_name) {
            handle.cancel_token.cancel();
            metrics::gauge!("archiver_pvs_active").decrement(1.0);
        }
        self.registry.remove_pv(pv_name)?;
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
        self.channels
            .get(pv)
            .map(|h| h.conn_info.lock().unwrap_or_else(|e| e.into_inner()).clone())
    }

    /// Get PV names that have never received any event (connected_since == None).
    pub fn get_never_connected_pvs(&self) -> Vec<String> {
        self.channels
            .iter()
            .filter(|entry| {
                let ci = entry.value().conn_info.lock().unwrap_or_else(|e| e.into_inner());
                ci.connected_since.is_none()
            })
            .map(|entry| entry.key().clone())
            .collect()
    }

    /// Get PV names that are currently disconnected (is_connected == false).
    pub fn get_currently_disconnected_pvs(&self) -> Vec<String> {
        self.channels
            .iter()
            .filter(|entry| {
                let ci = entry.value().conn_info.lock().unwrap_or_else(|e| e.into_inner());
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
) {
    loop {
        // Wait for connection, respecting cancellation.
        tokio::select! {
            _ = cancel_token.cancelled() => return,
            result = channel.wait_connected(CA_RECONNECT_TIMEOUT) => {
                if result.is_err() {
                    {
                        let mut ci = conn_info.lock().unwrap_or_else(|e| e.into_inner());
                        ci.is_connected = false;
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
                            {
                                let mut ci = conn_info.lock().unwrap_or_else(|e| e.into_inner());
                                if ci.connected_since.is_none() {
                                    ci.connected_since = Some(now);
                                }
                                ci.is_connected = true;
                                ci.last_event_time = Some(now);
                            }
                            let archiver_val = epics_value_to_archiver(&snapshot.value);
                            let mut sample = ArchiverSample::new(now, archiver_val);
                            attach_extras(&extras, &mut sample);
                            let pv_sample = PvSample {
                                pv_name: pv_name.clone(),
                                dbr_type,
                                sample,
                                element_count: Some(element_count),
                            };
                            if tx.send(pv_sample).await.is_err() {
                                return; // Write loop shut down
                            }
                        }
                        Some(Err(e)) => {
                            warn!(pv = pv_name, "Monitor error: {e}");
                        }
                        None => break, // Monitor ended, reconnect
                    }
                }
            }
        }

        // Monitor ended (disconnect) — loop back to reconnect.
        {
            let mut ci = conn_info.lock().unwrap_or_else(|e| e.into_inner());
            ci.is_connected = false;
        }
        debug!(pv = pv_name, "Monitor ended, waiting for reconnection");
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
) {
    let period = Duration::from_secs_f64(period_secs);
    let mut interval = tokio::time::interval(period);

    loop {
        tokio::select! {
            _ = cancel_token.cancelled() => return,
            _ = interval.tick() => {}
        }

        if channel.wait_connected(CA_RETRY_DELAY).await.is_err() {
            let mut ci = conn_info.lock().unwrap_or_else(|e| e.into_inner());
            ci.is_connected = false;
            continue;
        }

        match channel.get().await {
            Ok((_dbr_type, epics_val)) => {
                let now = SystemTime::now();
                {
                    let mut ci = conn_info.lock().unwrap_or_else(|e| e.into_inner());
                    if ci.connected_since.is_none() {
                        ci.connected_since = Some(now);
                    }
                    ci.is_connected = true;
                    ci.last_event_time = Some(now);
                }
                let archiver_val = epics_value_to_archiver(&epics_val);
                let mut sample = ArchiverSample::new(now, archiver_val);
                attach_extras(&extras, &mut sample);
                let pv_sample = PvSample {
                    pv_name: pv_name.clone(),
                    dbr_type,
                    sample,
                    element_count: Some(element_count),
                };
                if tx.send(pv_sample).await.is_err() {
                    return;
                }
            }
            Err(e) => {
                debug!(pv = pv_name, "Scan read error: {e}");
            }
        }
    }
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
) {
    let full_name = format!("{pv_name}.{field}");
    let channel = ca_client.create_channel(&full_name);
    let field_owned = field.to_string();
    let pv_owned = pv_name.to_string();

    tokio::spawn(async move {
        // Initial connect attempt — failure here is non-fatal (the field may
        // not exist on every IOC; we just leave the cache empty).
        if channel.wait_connected(CA_CONNECT_TIMEOUT).await.is_err() {
            debug!(
                pv = pv_owned,
                field = field_owned,
                "Extra-field channel did not connect within timeout (will keep retrying via subscribe)"
            );
        }

        loop {
            // Cancel-aware subscribe attempt.
            tokio::select! {
                _ = parent_token.cancelled() => return,
                sub = channel.subscribe() => {
                    let mut monitor = match sub {
                        Ok(m) => m,
                        Err(e) => {
                            debug!(
                                pv = pv_owned,
                                field = field_owned,
                                "Extra-field subscribe failed: {e}; retrying"
                            );
                            tokio::select! {
                                _ = parent_token.cancelled() => return,
                                _ = tokio::time::sleep(CA_RETRY_DELAY) => continue,
                            }
                        }
                    };
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
    });
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
    // when the same PV receives many samples between flushes.
    let mut ts_updates: std::collections::HashMap<String, SystemTime> =
        std::collections::HashMap::new();
    let mut last_flush = std::time::Instant::now();

    loop {
        tokio::select! {
            Some(pv_sample) = rx.recv() => {
                let ts = pv_sample.sample.timestamp;
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
