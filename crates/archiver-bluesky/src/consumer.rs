use std::collections::HashSet;
use std::sync::Arc;

use rdkafka::config::ClientConfig;
use rdkafka::consumer::{Consumer, StreamConsumer};
use rdkafka::message::Message;
use tokio_stream::StreamExt;
use tracing::{debug, error, info, warn};

use archiver_core::config::BlueskyConfig;
use archiver_core::registry::{PvRegistry, SampleMode};
use archiver_core::storage::traits::{AppendMeta, StoragePlugin};
use archiver_core::types::{ArchDbType, ArchiverValue};

use crate::documents::{BlueskyDocument, TypedDocument};
use crate::pv_mapper::PvMapper;

/// Kafka consumer that reads Bluesky documents and converts them to PV samples.
pub struct BlueskyConsumer {
    config: BlueskyConfig,
    storage: Arc<dyn StoragePlugin>,
    registry: Arc<PvRegistry>,
}

impl BlueskyConsumer {
    pub fn new(
        config: BlueskyConfig,
        storage: Arc<dyn StoragePlugin>,
        registry: Arc<PvRegistry>,
    ) -> Self {
        Self {
            config,
            storage,
            registry,
        }
    }

    /// Run the consumer loop. Call as a spawned task.
    pub async fn run(&self, mut shutdown: tokio::sync::watch::Receiver<bool>) -> anyhow::Result<()> {
        let consumer: StreamConsumer = ClientConfig::new()
            .set("bootstrap.servers", &self.config.bootstrap_servers)
            .set("group.id", &self.config.group_id)
            .set("auto.offset.reset", "latest")
            .set("enable.auto.commit", "true")
            .create()?;

        consumer.subscribe(&[&self.config.topic])?;
        info!(
            topic = self.config.topic,
            beamline = self.config.beamline,
            "Bluesky Kafka consumer started"
        );

        let mut mapper = PvMapper::new(&self.config.beamline);
        let mut stream = consumer.stream();
        // Cache of already-registered PV names to avoid repeated SQLite lookups.
        let mut registered_pvs: HashSet<String> = HashSet::new();
        // Batch timestamp updates.
        let mut ts_updates: Vec<(String, std::time::SystemTime)> = Vec::new();
        let mut last_flush = std::time::Instant::now();

        loop {
            tokio::select! {
                msg = stream.next() => {
                    match msg {
                        Some(Ok(borrowed_msg)) => {
                            if let Some(payload) = borrowed_msg.payload() {
                                self.process_message(
                                    payload,
                                    &mut mapper,
                                    &mut registered_pvs,
                                    &mut ts_updates,
                                ).await;
                            }

                            // Flush timestamps to SQLite every 30 seconds.
                            if last_flush.elapsed() > std::time::Duration::from_secs(30)
                                && !ts_updates.is_empty()
                            {
                                let refs: Vec<(&str, std::time::SystemTime)> = ts_updates
                                    .iter()
                                    .map(|(name, ts)| (name.as_str(), *ts))
                                    .collect();
                                if let Err(e) = self.registry.batch_update_timestamps(&refs) {
                                    error!("Failed to flush Bluesky timestamps: {e}");
                                }
                                ts_updates.clear();
                                last_flush = std::time::Instant::now();
                            }
                        }
                        Some(Err(e)) => {
                            error!("Kafka error: {e}");
                        }
                        None => {
                            warn!("Kafka stream ended");
                            break;
                        }
                    }
                }
                _ = shutdown.changed() => {
                    info!("Bluesky consumer shutting down");
                    break;
                }
            }
        }

        // Final timestamp flush.
        if !ts_updates.is_empty() {
            let refs: Vec<(&str, std::time::SystemTime)> = ts_updates
                .iter()
                .map(|(name, ts)| (name.as_str(), *ts))
                .collect();
            let _ = self.registry.batch_update_timestamps(&refs);
        }

        Ok(())
    }

    async fn process_message(
        &self,
        payload: &[u8],
        mapper: &mut PvMapper,
        registered_pvs: &mut HashSet<String>,
        ts_updates: &mut Vec<(String, std::time::SystemTime)>,
    ) {
        let doc: BlueskyDocument = match serde_json::from_slice(payload) {
            Ok(doc) => doc,
            Err(e) => {
                debug!("Failed to parse Bluesky document: {e}");
                return;
            }
        };

        // Extract descriptor UID for event metadata lookup.
        let event_descriptor_uid = match doc.parse() {
            Some(TypedDocument::Event(ref e)) => Some(e.descriptor.clone()),
            Some(TypedDocument::EventPage(ref p)) => Some(p.descriptor.clone()),
            _ => None,
        };

        let samples = mapper.map_document(&doc);

        for (pv_name, dbr_type, sample) in samples {
            // Register PV in SQLite if not already done.
            self.ensure_registered(&pv_name, dbr_type, registered_pvs);

            let element_count = element_count_for_value(&sample.value);

            // Build headers from DataKey metadata if available.
            let mut headers = Vec::new();
            if let Some(ref desc_uid) = event_descriptor_uid {
                // Extract the data key name from the PV name suffix.
                // PV names are like EXP:BL:motor:name:readback or EXP:BL:det:name:value
                let key_name = pv_name
                    .rsplit(':')
                    .nth(1)
                    .unwrap_or("");
                if let Some(meta) = mapper.get_data_key_meta(desc_uid, key_name) {
                    if let Some(ref units) = meta.units {
                        headers.push(("EGU".to_string(), units.clone()));
                        // Also update registry metadata (once per PV).
                        if registered_pvs.contains(&pv_name) {
                            let _ = self.registry.update_metadata(
                                &pv_name,
                                meta.precision.as_ref().map(|p| p.to_string()).as_deref(),
                                Some(units.as_str()),
                            );
                        }
                    }
                    if let Some(prec) = meta.precision {
                        headers.push(("PREC".to_string(), prec.to_string()));
                    }
                }
            }

            let meta = AppendMeta {
                element_count: Some(element_count),
                headers,
            };
            let ts = sample.timestamp;
            if let Err(e) = self
                .storage
                .append_event_with_meta(&pv_name, dbr_type, &sample, &meta)
                .await
            {
                error!(pv = pv_name, "Failed to store Bluesky PV sample: {e}");
            } else {
                ts_updates.push((pv_name, ts));
            }
        }
    }

    fn ensure_registered(
        &self,
        pv_name: &str,
        dbr_type: ArchDbType,
        registered_pvs: &mut HashSet<String>,
    ) {
        if registered_pvs.contains(pv_name) {
            return;
        }
        // Check SQLite in case it was registered in a previous run.
        if let Ok(Some(_)) = self.registry.get_pv(pv_name) {
            registered_pvs.insert(pv_name.to_string());
            return;
        }
        // Register as Bluesky-ingested PV.
        let sample_mode = SampleMode::Monitor;
        if let Err(e) = self
            .registry
            .register_pv(pv_name, dbr_type, &sample_mode, 1)
        {
            error!(pv = pv_name, "Failed to register Bluesky PV: {e}");
        } else {
            debug!(pv = pv_name, "Registered Bluesky PV in registry");
            registered_pvs.insert(pv_name.to_string());
        }
    }
}

/// Determine element count from value (1 for scalars, vec.len() for waveforms).
fn element_count_for_value(value: &ArchiverValue) -> i32 {
    match value {
        ArchiverValue::VectorString(v) => v.len() as i32,
        ArchiverValue::VectorChar(v) => v.len() as i32,
        ArchiverValue::VectorShort(v) => v.len() as i32,
        ArchiverValue::VectorInt(v) => v.len() as i32,
        ArchiverValue::VectorEnum(v) => v.len() as i32,
        ArchiverValue::VectorFloat(v) => v.len() as i32,
        ArchiverValue::VectorDouble(v) => v.len() as i32,
        _ => 1,
    }
}
