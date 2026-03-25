use std::sync::{Arc, Mutex};
use std::time::{Duration, SystemTime};

use dashmap::DashMap;
use epics_base_rs::client::{CaChannel, CaClient, ConnectionEvent};
use epics_base_rs::types::{DbFieldType, EpicsValue};
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;
use tracing::{debug, error, info, warn};

use archiver_core::registry::{PvRecord, PvRegistry, PvStatus, SampleMode};
use archiver_core::storage::traits::{AppendMeta, StoragePlugin};
use archiver_core::types::{ArchDbType, ArchiverSample, ArchiverValue};

use crate::policy::PolicyConfig;

/// Connection state tracked per PV.
#[derive(Debug, Clone)]
pub struct ConnectionInfo {
    pub connected_since: Option<SystemTime>,
    pub last_event_time: Option<SystemTime>,
    pub is_connected: bool,
}

impl Default for ConnectionInfo {
    fn default() -> Self {
        Self {
            connected_since: None,
            last_event_time: None,
            is_connected: false,
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
}

/// Manages EPICS Channel Access connections and dispatches archived samples to storage.
pub struct ChannelManager {
    /// The CA client context.
    ca_client: CaClient,
    /// Active channels: PV name → handle with cancellation.
    channels: DashMap<String, PvHandle>,
    /// Storage backend.
    #[allow(dead_code)]
    storage: Arc<dyn StoragePlugin>,
    /// PV metadata registry.
    registry: Arc<PvRegistry>,
    /// Sample sender for the write thread.
    sample_tx: mpsc::UnboundedSender<PvSample>,
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
    ) -> anyhow::Result<(Self, mpsc::UnboundedReceiver<PvSample>)> {
        let ca_client = CaClient::new().await.map_err(|e| anyhow::anyhow!("{e}"))?;
        let (tx, rx) = mpsc::unbounded_channel();

        let mgr = Self {
            ca_client,
            channels: DashMap::new(),
            storage,
            registry,
            sample_tx: tx,
            policy,
        };

        Ok((mgr, rx))
    }

    /// Restore all active PVs from the registry (called on startup).
    pub async fn restore_from_registry(&self) -> anyhow::Result<u64> {
        let active_pvs = self.registry.pvs_by_status(PvStatus::Active)?;
        let count = active_pvs.len() as u64;
        info!(count, "Restoring PVs from registry");

        for record in active_pvs {
            if let Err(e) = self.start_archiving_internal(&record).await {
                warn!(pv = record.pv_name, "Failed to restore PV: {e}");
                self.registry.set_status(&record.pv_name, PvStatus::Error)?;
            }
        }

        Ok(count)
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
            .wait_connected(Duration::from_secs(10))
            .await
            .map_err(|e| anyhow::anyhow!("Failed to connect to {pv_name}: {e}"))?;

        let info = self.ca_client.cainfo(pv_name).await
            .map_err(|e| anyhow::anyhow!("Failed to get info for {pv_name}: {e}"))?;

        let dbr_type = dbr_field_to_arch_type(info.native_type);
        let element_count = info.element_count as i32;

        // Register in SQLite.
        self.registry
            .register_pv(pv_name, dbr_type, &effective_mode, element_count)?;

        let record = self.registry.get_pv(pv_name)?.unwrap();
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

        self.channels.insert(
            pv_name.clone(),
            PvHandle {
                channel: channel.clone(),
                cancel_token: cancel_token.clone(),
                dbr_type,
                conn_info: conn_info.clone(),
            },
        );

        let tx = self.sample_tx.clone();
        let token = cancel_token.clone();
        let ci = conn_info.clone();

        match &record.sample_mode {
            SampleMode::Monitor => {
                tokio::spawn(async move {
                    monitor_loop(pv_name, dbr_type, element_count, channel, tx, token, ci).await;
                });
            }
            SampleMode::Scan { period_secs } => {
                let period = *period_secs;
                tokio::spawn(async move {
                    scan_loop(pv_name, dbr_type, element_count, channel, tx, token, period, ci)
                        .await;
                });
            }
        }

        Ok(())
    }

    /// Pause archiving for a PV.
    pub fn pause_pv(&self, pv_name: &str) -> anyhow::Result<()> {
        if let Some((_key, handle)) = self.channels.remove(pv_name) {
            handle.cancel_token.cancel();
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
        info!(pv = pv_name, "Resumed archiving");
        Ok(())
    }

    /// Stop archiving a PV without removing it from the registry.
    /// Sets the PV status to Inactive (data retained, monitoring stopped).
    pub fn stop_pv(&self, pv_name: &str) -> anyhow::Result<()> {
        if let Some((_key, handle)) = self.channels.remove(pv_name) {
            handle.cancel_token.cancel();
        }
        self.registry.set_status(pv_name, PvStatus::Inactive)?;
        metrics::gauge!("archiver_pvs_active").decrement(1.0);
        info!(pv = pv_name, "Stopped archiving (inactive)");
        Ok(())
    }

    /// Remove a PV from archiving entirely.
    pub fn destroy_pv(&self, pv_name: &str) -> anyhow::Result<()> {
        if let Some((_key, handle)) = self.channels.remove(pv_name) {
            handle.cancel_token.cancel();
        }
        self.registry.remove_pv(pv_name)?;
        metrics::gauge!("archiver_pvs_active").decrement(1.0);
        info!(pv = pv_name, "Destroyed archiving channel");
        Ok(())
    }

    /// List all currently archived PV names (from registry).
    pub fn list_pvs(&self) -> Vec<String> {
        self.registry.all_pv_names().unwrap_or_default()
    }

    /// Match PVs by glob pattern (from registry).
    pub fn matching_pvs(&self, pattern: &str) -> Vec<String> {
        self.registry.matching_pvs(pattern).unwrap_or_default()
    }

    /// Get the registry reference.
    pub fn registry(&self) -> &Arc<PvRegistry> {
        &self.registry
    }

    /// Get connection info for a PV.
    pub fn get_connection_info(&self, pv: &str) -> Option<ConnectionInfo> {
        self.channels
            .get(pv)
            .map(|h| h.conn_info.lock().unwrap().clone())
    }

    /// Get PV names that have never received any event (connected_since == None).
    pub fn get_never_connected_pvs(&self) -> Vec<String> {
        self.channels
            .iter()
            .filter(|entry| {
                let ci = entry.value().conn_info.lock().unwrap();
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
                let ci = entry.value().conn_info.lock().unwrap();
                !ci.is_connected
            })
            .map(|entry| entry.key().clone())
            .collect()
    }
}

/// Monitor loop: subscribe to a channel and forward values.
async fn monitor_loop(
    pv_name: String,
    dbr_type: ArchDbType,
    element_count: i32,
    channel: CaChannel,
    tx: mpsc::UnboundedSender<PvSample>,
    cancel_token: CancellationToken,
    conn_info: Arc<Mutex<ConnectionInfo>>,
) {
    loop {
        // Wait for connection, respecting cancellation.
        tokio::select! {
            _ = cancel_token.cancelled() => return,
            result = channel.wait_connected(Duration::from_secs(30)) => {
                if result.is_err() {
                    {
                        let mut ci = conn_info.lock().unwrap();
                        ci.is_connected = false;
                    }
                    // Wait for reconnection via connection events.
                    let mut conn_rx = channel.connection_events();
                    loop {
                        tokio::select! {
                            _ = cancel_token.cancelled() => return,
                            event = conn_rx.recv() => {
                                match event {
                                    Ok(ConnectionEvent::Connected) => break,
                                    Ok(ConnectionEvent::Disconnected) => continue,
                                    Err(_) => return,
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
                    _ = tokio::time::sleep(Duration::from_secs(5)) => continue,
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
                        Some(Ok(epics_val)) => {
                            let now = SystemTime::now();
                            {
                                let mut ci = conn_info.lock().unwrap();
                                if ci.connected_since.is_none() {
                                    ci.connected_since = Some(now);
                                }
                                ci.is_connected = true;
                                ci.last_event_time = Some(now);
                            }
                            let archiver_val = epics_value_to_archiver(&epics_val, dbr_type);
                            let sample = ArchiverSample::new(now, archiver_val);
                            let pv_sample = PvSample {
                                pv_name: pv_name.clone(),
                                dbr_type,
                                sample,
                                element_count: Some(element_count),
                            };
                            if tx.send(pv_sample).is_err() {
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
            let mut ci = conn_info.lock().unwrap();
            ci.is_connected = false;
        }
        debug!(pv = pv_name, "Monitor ended, waiting for reconnection");
    }
}

/// Scan loop: periodically read a channel value.
async fn scan_loop(
    pv_name: String,
    dbr_type: ArchDbType,
    element_count: i32,
    channel: CaChannel,
    tx: mpsc::UnboundedSender<PvSample>,
    cancel_token: CancellationToken,
    period_secs: f64,
    conn_info: Arc<Mutex<ConnectionInfo>>,
) {
    let period = Duration::from_secs_f64(period_secs);
    let mut interval = tokio::time::interval(period);

    loop {
        tokio::select! {
            _ = cancel_token.cancelled() => return,
            _ = interval.tick() => {}
        }

        if channel.wait_connected(Duration::from_secs(5)).await.is_err() {
            let mut ci = conn_info.lock().unwrap();
            ci.is_connected = false;
            continue;
        }

        match channel.get().await {
            Ok((_dbr_type, epics_val)) => {
                let now = SystemTime::now();
                {
                    let mut ci = conn_info.lock().unwrap();
                    if ci.connected_since.is_none() {
                        ci.connected_since = Some(now);
                    }
                    ci.is_connected = true;
                    ci.last_event_time = Some(now);
                }
                let archiver_val = epics_value_to_archiver(&epics_val, dbr_type);
                let sample = ArchiverSample::new(now, archiver_val);
                let pv_sample = PvSample {
                    pv_name: pv_name.clone(),
                    dbr_type,
                    sample,
                    element_count: Some(element_count),
                };
                if tx.send(pv_sample).is_err() {
                    return;
                }
            }
            Err(e) => {
                debug!(pv = pv_name, "Scan read error: {e}");
            }
        }
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
fn epics_value_to_archiver(val: &EpicsValue, _expected_type: ArchDbType) -> ArchiverValue {
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
    mut rx: mpsc::UnboundedReceiver<PvSample>,
    mut shutdown: tokio::sync::watch::Receiver<bool>,
    flush_period: Duration,
) {
    info!("Write loop started");
    let mut ts_updates: Vec<(String, SystemTime)> = Vec::new();
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
                    ts_updates.push((pv_sample.pv_name, ts));
                }

                // Batch flush timestamps to SQLite.
                if last_flush.elapsed() > flush_period && !ts_updates.is_empty() {
                    let refs: Vec<(&str, SystemTime)> = ts_updates
                        .iter()
                        .map(|(name, ts)| (name.as_str(), *ts))
                        .collect();
                    if let Err(e) = registry.batch_update_timestamps(&refs) {
                        error!("Failed to flush timestamps: {e}");
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
                    let _ = storage
                        .append_event_with_meta(
                            &pv_sample.pv_name,
                            pv_sample.dbr_type,
                            &pv_sample.sample,
                            &meta,
                        )
                        .await;
                }
                // Final timestamp flush.
                if !ts_updates.is_empty() {
                    let refs: Vec<(&str, SystemTime)> = ts_updates
                        .iter()
                        .map(|(name, ts)| (name.as_str(), *ts))
                        .collect();
                    let _ = registry.batch_update_timestamps(&refs);
                }
                info!("Write loop shutting down");
                break;
            }
        }
    }
}
