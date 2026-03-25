use std::time::SystemTime;

use async_trait::async_trait;

use crate::storage::partition::PartitionGranularity;
use crate::types::{ArchDbType, ArchiverSample, EventStreamDesc};

/// A stream of archived events (read side).
pub trait EventStream: Send {
    fn description(&self) -> &EventStreamDesc;
    fn next_event(&mut self) -> anyhow::Result<Option<ArchiverSample>>;
}

/// Optional metadata to include in PlainPB PayloadInfo headers.
#[derive(Debug, Clone, Default)]
pub struct AppendMeta {
    pub element_count: Option<i32>,
    pub headers: Vec<(String, String)>,
}

/// Storage plugin trait — the primary interface for reading/writing archived data.
#[async_trait]
pub trait StoragePlugin: Send + Sync {
    fn name(&self) -> &str;
    fn partition_granularity(&self) -> PartitionGranularity;

    /// Append a single sample to storage.
    async fn append_event(
        &self,
        pv: &str,
        dbr_type: ArchDbType,
        sample: &ArchiverSample,
    ) -> anyhow::Result<()>;

    /// Append a single sample with optional metadata for PlainPB headers.
    async fn append_event_with_meta(
        &self,
        pv: &str,
        dbr_type: ArchDbType,
        sample: &ArchiverSample,
        _meta: &AppendMeta,
    ) -> anyhow::Result<()> {
        // Default implementation ignores metadata.
        self.append_event(pv, dbr_type, sample).await
    }

    /// Read data for a PV within a time range. Returns multiple streams
    /// (one per partition file).
    async fn get_data(
        &self,
        pv: &str,
        start: SystemTime,
        end: SystemTime,
    ) -> anyhow::Result<Vec<Box<dyn EventStream>>>;

    /// Get the most recent known event for a PV.
    async fn get_last_known_event(
        &self,
        pv: &str,
    ) -> anyhow::Result<Option<ArchiverSample>>;

    /// Delete all stored data for a PV. Returns the number of files deleted.
    /// Default implementation returns 0 (no-op for backward compatibility).
    async fn delete_pv_data(&self, _pv: &str) -> anyhow::Result<u64> {
        Ok(0)
    }
}

/// Post-processor trait for data reduction (mean, max, min, etc.).
pub trait PostProcessor: Send {
    fn name(&self) -> &str;
    fn interval_secs(&self) -> u64;
    fn process(&self, input: Box<dyn EventStream>) -> Box<dyn EventStream>;
}
