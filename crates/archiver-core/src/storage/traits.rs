use std::path::PathBuf;
use std::time::SystemTime;

use async_trait::async_trait;

use crate::storage::partition::PartitionGranularity;
use crate::types::{ArchDbType, ArchiverSample, EventStreamDesc};

/// Per-tier description of a storage stage. Surfaced through the
/// `getStoresForPV` and `getApplianceMetrics` BPL endpoints so operators
/// can see tier layout and per-PV file counts without poking the disk.
#[derive(Debug, Clone)]
pub struct StoreSummary {
    pub name: String,
    pub root_folder: PathBuf,
    pub granularity: PartitionGranularity,
    /// Number of `.pb` partition files this tier holds for the given PV.
    /// `None` when the summary was requested without a PV scope.
    pub pv_file_count: Option<u64>,
    /// Sum of `.pb` file sizes (bytes) for the given PV in this tier.
    /// `None` when the summary was requested without a PV scope.
    pub pv_size_bytes: Option<u64>,
    /// Total size on disk of all `.pb` files in this tier (bytes), summed across PVs.
    /// `None` when the summary is PV-scoped.
    pub total_size_bytes: Option<u64>,
    /// Total number of `.pb` files in this tier across all PVs.
    /// `None` when the summary is PV-scoped.
    pub total_files: Option<u64>,
}

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

/// Outcome of a single `flush_ingest_writes` pass.
///
/// `failed` and `deferred` carry **different** semantics for callers
/// like the engine's write_loop:
///
/// * **failed** — the flush errored for this PV **on this pass**; its
///   buffered bytes have been discarded (writer evicted via
///   `into_parts`). The caller MUST drop these from any pending
///   timestamp commit AND from its `ts_updates` map so the registry
///   doesn't claim `last_event` for samples that never reached disk.
///   Losses recorded *outside* this pass (LRU eviction, partition
///   rollover, ghost-file, ETL evict-by-path, read-side flush
///   failures) are NOT in `failed` — they accumulate in the plugin's
///   loss queue and are drained by the owner via
///   [`StoragePlugin::take_loss_markers`].
///
/// * **deferred** — the writer's per-PV slot was already locked by
///   an in-flight append. The bytes are still buffered and will be
///   flushed on the next cycle. The caller MUST skip these from
///   THIS cycle's commit but MUST keep them in `ts_updates` so the
///   timestamp commits on a later cycle. Treating deferred as a
///   permanent failure permanently loses the registry timestamp
///   for any PV that gets busy and then goes silent.
#[derive(Debug, Clone, Default)]
pub struct IngestFlushResult {
    pub failed: Vec<String>,
    pub deferred: Vec<String>,
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
    async fn get_last_known_event(&self, pv: &str) -> anyhow::Result<Option<ArchiverSample>>;

    /// Get the last sample whose timestamp is strictly before `target`.
    /// Used by retrieval to prepend a continuity sample when the user's
    /// query window starts in a gap between samples (Java's
    /// `getLastEventOfPreviousPartitionBeforeTimeAsStream`). Returns None
    /// if no such sample exists.
    ///
    /// Default implementation: walks `get_last_known_event` and returns
    /// it iff its timestamp is < target. Plugins with cheaper backward
    /// scans should override.
    async fn get_last_event_before(
        &self,
        pv: &str,
        target: SystemTime,
    ) -> anyhow::Result<Option<ArchiverSample>> {
        match self.get_last_known_event(pv).await? {
            Some(sample) if sample.timestamp < target => Ok(Some(sample)),
            _ => Ok(None),
        }
    }

    /// Delete all stored data for a PV. Returns the number of files deleted.
    /// Default implementation returns 0 (no-op for backward compatibility).
    async fn delete_pv_data(&self, _pv: &str) -> anyhow::Result<u64> {
        Ok(0)
    }

    /// Flush any buffered writes to disk. Default is no-op.
    async fn flush_writes(&self) -> anyhow::Result<()> {
        Ok(())
    }

    /// Flush only the cached writers used by the *ingest* path (the
    /// engine's monitor/scan write_loop). See [`IngestFlushResult`]
    /// for the failed/deferred distinction — `failed` PVs lost
    /// their buffered bytes, `deferred` PVs are still buffered and
    /// must be retried on the next cycle.
    ///
    /// Multi-tier implementations should limit scope to the ingest
    /// tier (e.g. STS only) so a slow MTS/LTS mount can't stall the
    /// live archive pipeline. ETL drives MTS/LTS flushing separately.
    async fn flush_ingest_writes(&self) -> anyhow::Result<IngestFlushResult> {
        // Default: best-effort fall back to flush_writes. Implementations
        // that can identify per-PV failures/deferrals should override.
        self.flush_writes()
            .await
            .map(|_| IngestFlushResult::default())
    }

    /// Drain and return the accumulated dirty-write loss markers — PV
    /// names whose buffered bytes were classified as lost outside a
    /// single [`flush_ingest_writes`] pass (LRU dirty-eviction,
    /// partition rollover, ghost-file, ETL evict-by-path, read-side
    /// flush failures).
    ///
    /// **Single-owner invariant.** Only the engine's flush owner may
    /// call this, and only on the *observed-success* path of a flush
    /// cycle — so marker consumption is atomic with the owner applying
    /// the drop (dropping the PV from its pending-timestamp commit).
    /// The owner runs `flush_ingest_writes` inside a `spawn_blocking`
    /// task it abandons on timeout; draining must therefore live HERE,
    /// in a call the owner makes from its own context after observing
    /// success, not inside the abandon-able flush body. A timed-out or
    /// panicked flush consumes nothing, so its markers re-surface next
    /// cycle instead of being silently swallowed (which would let the
    /// next clean flush commit a `last_event` for lost bytes).
    ///
    /// The default returns empty for plugins without a loss queue.
    fn take_loss_markers(&self) -> Vec<String> {
        Vec::new()
    }

    /// Per-tier summary scoped to a single PV: name, root folder, granularity,
    /// and how many `.pb` files this tier holds for that PV. Total size /
    /// total files are left None.
    fn stores_for_pv(&self, pv: &str) -> anyhow::Result<Vec<StoreSummary>>;

    /// Per-tier summary aggregated across all PVs: total size on disk and
    /// total file count. `pv_file_count` is left None.
    fn appliance_metrics(&self) -> anyhow::Result<Vec<StoreSummary>>;

    /// Rename `from` → `to` in this storage backend. Implementations may copy
    /// or rename underlying files; the contract is that after a successful
    /// return, reads for `to` see all data previously stored under `from` and
    /// reads for `from` see none. Defaults to error so missing implementations
    /// surface explicitly.
    async fn rename_pv(&self, _from: &str, _to: &str) -> anyhow::Result<u64> {
        anyhow::bail!("rename_pv not implemented for this storage plugin")
    }
}

/// Post-processor trait for data reduction (mean, max, min, etc.).
pub trait PostProcessor: Send {
    fn name(&self) -> &str;
    fn interval_secs(&self) -> u64;
    fn process(&self, input: Box<dyn EventStream>) -> Box<dyn EventStream>;
}
