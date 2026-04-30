use std::sync::Arc;
use std::time::SystemTime;

use async_trait::async_trait;

use crate::config::StorageConfig;
use crate::storage::partition::PartitionGranularity;
use crate::storage::plainpb::PlainPbStoragePlugin;
use crate::storage::traits::{AppendMeta, EventStream, StoragePlugin, StoreSummary};
use crate::types::{ArchDbType, ArchiverSample};

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
        Self {
            sts: Arc::new(PlainPbStoragePlugin::new(
                "STS",
                config.sts.root_folder.clone(),
                config.sts.partition_granularity,
            )),
            mts: Arc::new(PlainPbStoragePlugin::new(
                "MTS",
                config.mts.root_folder.clone(),
                config.mts.partition_granularity,
            )),
            lts: Arc::new(PlainPbStoragePlugin::new(
                "LTS",
                config.lts.root_folder.clone(),
                config.lts.partition_granularity,
            )),
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
