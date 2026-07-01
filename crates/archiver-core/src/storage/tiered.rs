use std::sync::Arc;
use std::time::SystemTime;

use async_trait::async_trait;

use crate::config::{StorageConfig, TierConfig};
use crate::storage::partition::PartitionGranularity;
use crate::storage::plainpb::{FdBudget, PlainPbStoragePlugin};
use crate::storage::traits::{AppendMeta, EventStream, StoragePlugin, StoreSummary};
use crate::types::{ArchDbType, ArchiverSample};

/// Per-tier defaults for the open-writer cap when neither
/// [`StorageConfig::max_open_writers_total`] nor
/// [`TierConfig::max_open_writers`] is set. STS gets the bulk
/// because it's the live ingest path (every active PV gets a
/// writer); MTS/LTS are ETL-side and only need a small concurrent
/// working set.
const STS_DEFAULT_FD_CAP: usize = 512;
const MTS_DEFAULT_FD_CAP: usize = 64;
const LTS_DEFAULT_FD_CAP: usize = 64;

fn tier_fd_cap(tier: &TierConfig, default_cap: usize) -> usize {
    match tier.max_open_writers {
        Some(0) => usize::MAX,
        Some(n) => n,
        None => default_cap,
    }
}

/// 3-tier storage manager: STS (short-term) → MTS (medium-term) → LTS (long-term).
///
/// Writes always go to STS. Reads merge across all tiers.
/// ETL moves data from STS → MTS → LTS over time.
pub struct TieredStorage {
    pub sts: Arc<PlainPbStoragePlugin>,
    pub mts: Arc<PlainPbStoragePlugin>,
    pub lts: Arc<PlainPbStoragePlugin>,
}

impl TieredStorage {
    pub fn from_config(config: &StorageConfig) -> Self {
        // If the operator opted into a process-wide cap, build ONE
        // shared FdBudget and clone it into every tier — STS, MTS,
        // and LTS then draw permits from the same pool, so the
        // total open-writer count across the whole process stays
        // at-or-below the cap. Otherwise each tier gets its own
        // budget at the per-tier configured (or default) cap.
        let (sts_budget, mts_budget, lts_budget) = match config.max_open_writers_total {
            Some(0) => (
                FdBudget::unbounded(),
                FdBudget::unbounded(),
                FdBudget::unbounded(),
            ),
            Some(total) => {
                let shared = FdBudget::new(total);
                (shared.clone(), shared.clone(), shared)
            }
            None => (
                FdBudget::new(tier_fd_cap(&config.sts, STS_DEFAULT_FD_CAP)),
                FdBudget::new(tier_fd_cap(&config.mts, MTS_DEFAULT_FD_CAP)),
                FdBudget::new(tier_fd_cap(&config.lts, LTS_DEFAULT_FD_CAP)),
            ),
        };
        // One durability mode for the whole appliance: when
        // fsync_on_flush is set, every tier's flush syncs to disk.
        // STS is the live ingest path the operator opts in for; MTS/
        // LTS get the same treatment for free (their ETL flushes are
        // infrequent) so the guarantee is uniform across tiers.
        let fsync = config.fsync_on_flush;
        Self {
            sts: Arc::new(
                PlainPbStoragePlugin::with_fd_budget(
                    "STS",
                    config.sts.root_folder.clone(),
                    config.sts.partition_granularity,
                    sts_budget,
                )
                .with_fsync_on_flush(fsync),
            ),
            mts: Arc::new(
                PlainPbStoragePlugin::with_fd_budget(
                    "MTS",
                    config.mts.root_folder.clone(),
                    config.mts.partition_granularity,
                    mts_budget,
                )
                .with_fsync_on_flush(fsync),
            ),
            lts: Arc::new(
                PlainPbStoragePlugin::with_fd_budget(
                    "LTS",
                    config.lts.root_folder.clone(),
                    config.lts.partition_granularity,
                    lts_budget,
                )
                .with_fsync_on_flush(fsync),
            ),
        }
    }

    /// Get all tiers in order (LTS first for reading — oldest data first).
    pub fn read_order(&self) -> Vec<Arc<PlainPbStoragePlugin>> {
        vec![self.lts.clone(), self.mts.clone(), self.sts.clone()]
    }
}

#[async_trait]
impl StoragePlugin for TieredStorage {
    fn name(&self) -> &str {
        "TieredStorage"
    }

    fn partition_granularity(&self) -> PartitionGranularity {
        // STS granularity is the finest.
        self.sts.partition_granularity()
    }

    async fn append_event(
        &self,
        pv: &str,
        dbr_type: ArchDbType,
        sample: &ArchiverSample,
    ) -> anyhow::Result<()> {
        // Always write to STS.
        self.sts.append_event(pv, dbr_type, sample).await
    }

    async fn append_event_with_meta(
        &self,
        pv: &str,
        dbr_type: ArchDbType,
        sample: &ArchiverSample,
        meta: &AppendMeta,
    ) -> anyhow::Result<()> {
        self.sts
            .append_event_with_meta(pv, dbr_type, sample, meta)
            .await
    }

    async fn get_data(
        &self,
        pv: &str,
        start: SystemTime,
        end: SystemTime,
    ) -> anyhow::Result<Vec<Box<dyn EventStream>>> {
        let mut all_streams = Vec::new();
        // Read from LTS (oldest) → MTS → STS (newest). Skip any tier whose
        // SKIP_<NAME>_FOR_RETRIEVAL flag is set so an operator can route
        // around a wedged NFS-backed LTS without restarting the appliance
        // (Java's namedFlags retrieval gate, 9f636c14).
        for tier in self.read_order() {
            if crate::flags::skip_tier_for_retrieval(tier.name()) {
                tracing::debug!(tier = tier.name(), pv, "skipping tier for retrieval");
                continue;
            }
            let mut streams = tier.get_data(pv, start, end).await?;
            all_streams.append(&mut streams);
        }
        Ok(all_streams)
    }

    async fn get_last_known_event(&self, pv: &str) -> anyhow::Result<Option<ArchiverSample>> {
        // Try STS first (most recent), then MTS, then LTS. Honor the
        // SKIP_<NAME>_FOR_RETRIEVAL flags here too so a flag flipped for
        // outage routing affects every read path, not just `get_data`.
        if !crate::flags::skip_tier_for_retrieval(self.sts.name())
            && let Some(sample) = self.sts.get_last_known_event(pv).await?
        {
            return Ok(Some(sample));
        }
        if !crate::flags::skip_tier_for_retrieval(self.mts.name())
            && let Some(sample) = self.mts.get_last_known_event(pv).await?
        {
            return Ok(Some(sample));
        }
        if crate::flags::skip_tier_for_retrieval(self.lts.name()) {
            return Ok(None);
        }
        self.lts.get_last_known_event(pv).await
    }

    async fn get_last_event_before(
        &self,
        pv: &str,
        target: SystemTime,
    ) -> anyhow::Result<Option<ArchiverSample>> {
        // Try STS → MTS → LTS, returning the first hit. Each tier already
        // walks its own partition list newest-first, so this preserves the
        // overall newest-first contract. Skipped tiers are silently
        // bypassed.
        if !crate::flags::skip_tier_for_retrieval(self.sts.name())
            && let Some(sample) = self.sts.get_last_event_before(pv, target).await?
        {
            return Ok(Some(sample));
        }
        if !crate::flags::skip_tier_for_retrieval(self.mts.name())
            && let Some(sample) = self.mts.get_last_event_before(pv, target).await?
        {
            return Ok(Some(sample));
        }
        if crate::flags::skip_tier_for_retrieval(self.lts.name()) {
            return Ok(None);
        }
        self.lts.get_last_event_before(pv, target).await
    }

    async fn delete_pv_data(&self, pv: &str) -> anyhow::Result<u64> {
        let sts_count = self.sts.delete_pv_data(pv).await?;
        let mts_count = self.mts.delete_pv_data(pv).await?;
        let lts_count = self.lts.delete_pv_data(pv).await?;
        Ok(sts_count + mts_count + lts_count)
    }

    async fn flush_writes(&self) -> anyhow::Result<()> {
        self.sts.flush_writes().await?;
        self.mts.flush_writes().await?;
        self.lts.flush_writes().await?;
        Ok(())
    }

    async fn flush_ingest_writes(
        &self,
    ) -> anyhow::Result<crate::storage::traits::IngestFlushResult> {
        // The engine's write_loop only writes to STS (see
        // `append_event_with_meta`); MTS/LTS writers exist only for
        // ETL. Limiting the ingest-path flush to STS prevents an
        // NFS-stalled long-term store from blocking live archiving.
        // Per-PV failure/deferred lists bubble up so write_loop can
        // drop failures from the pending-timestamp commit and keep
        // deferreds for the next cycle.
        self.sts.flush_ingest_writes().await
    }

    fn take_loss_markers(&self) -> Vec<String> {
        // Ingest writes only ever touch STS (see
        // `append_event_with_meta`), so only STS's loss markers drive the
        // ingest flush owner's pending-timestamp drop; those are returned
        // upward. Returning MTS/LTS markers here would be WRONG — those
        // PVs are never in the ingest pending set, so surfacing them would
        // drop a healthy ingest commit.
        //
        // MTS/LTS still accumulate loss markers when an ETL-side flush
        // genuinely fails, but `move_file` aborts that move on the same
        // failure (`flush_writes` bails) and keeps the source for a later
        // retry, so those markers are redundant for correctness. Drain
        // them here anyway (discarding the result) so a sustained
        // dest-tier I/O fault can't grow the queues without bound.
        // TieredStorage owns the drain of every tier's queue; only STS's
        // is surfaced.
        let sts = self.sts.take_loss_markers();
        let _ = self.mts.take_loss_markers();
        let _ = self.lts.take_loss_markers();
        sts
    }

    fn stores_for_pv(&self, pv: &str) -> anyhow::Result<Vec<StoreSummary>> {
        let mut all = Vec::with_capacity(3);
        all.extend(self.sts.stores_for_pv(pv)?);
        all.extend(self.mts.stores_for_pv(pv)?);
        all.extend(self.lts.stores_for_pv(pv)?);
        Ok(all)
    }

    fn appliance_metrics(&self) -> anyhow::Result<Vec<StoreSummary>> {
        let mut all = Vec::with_capacity(3);
        all.extend(self.sts.appliance_metrics()?);
        all.extend(self.mts.appliance_metrics()?);
        all.extend(self.lts.appliance_metrics()?);
        Ok(all)
    }

    async fn rename_pv(&self, from: &str, to: &str) -> anyhow::Result<u64> {
        let s = self.sts.rename_pv(from, to).await?;
        let m = self.mts.rename_pv(from, to).await?;
        let l = self.lts.rename_pv(from, to).await?;
        Ok(s + m + l)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::PathBuf;

    fn plugin(name: &str) -> Arc<PlainPbStoragePlugin> {
        // No disk access: constructor stores fields, and the loss-queue
        // operations under test touch only an in-memory Vec.
        Arc::new(PlainPbStoragePlugin::new(
            name,
            PathBuf::from(format!("/nonexistent-{name}")),
            PartitionGranularity::Hour,
        ))
    }

    #[test]
    fn take_loss_markers_drains_all_tiers_but_returns_only_sts() {
        let tiered = TieredStorage {
            sts: plugin("STS"),
            mts: plugin("MTS"),
            lts: plugin("LTS"),
        };
        tiered.sts.push_loss_marker_for_test("PV:STS");
        tiered.mts.push_loss_marker_for_test("PV:MTS");
        tiered.lts.push_loss_marker_for_test("PV:LTS");

        // Only STS markers reach the ingest flush owner; MTS/LTS markers
        // would spuriously drop healthy ingest commits.
        assert_eq!(tiered.take_loss_markers(), vec!["PV:STS".to_string()]);

        // All three queues were drained — a second drain of any tier
        // yields nothing, proving MTS/LTS can't grow unbounded.
        assert!(tiered.take_loss_markers().is_empty());
        assert!(tiered.mts.take_loss_markers().is_empty());
        assert!(tiered.lts.take_loss_markers().is_empty());
    }
}
