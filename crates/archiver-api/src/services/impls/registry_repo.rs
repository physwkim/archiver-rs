use std::sync::Arc;
use std::time::{Duration, SystemTime};

use archiver_core::registry::{PvRecord, PvRegistry, PvStatus, SampleMode};
use archiver_core::types::ArchDbType;

use crate::services::traits::{PvCommandRepository, PvQueryRepository};

pub struct RegistryRepository {
    inner: Arc<PvRegistry>,
}

impl RegistryRepository {
    pub fn new(registry: Arc<PvRegistry>) -> Self {
        Self { inner: registry }
    }
}

impl PvQueryRepository for RegistryRepository {
    fn get_pv(&self, pv: &str) -> anyhow::Result<Option<PvRecord>> {
        self.inner.get_pv(pv)
    }

    fn all_pv_names(&self) -> anyhow::Result<Vec<String>> {
        self.inner.all_pv_names()
    }

    fn matching_pvs(&self, pattern: &str) -> anyhow::Result<Vec<String>> {
        self.inner.matching_pvs(pattern)
    }

    fn count(&self, status: Option<PvStatus>) -> anyhow::Result<u64> {
        self.inner.count(status)
    }

    fn all_records(&self) -> anyhow::Result<Vec<PvRecord>> {
        self.inner.all_records()
    }

    fn pvs_by_status(&self, status: PvStatus) -> anyhow::Result<Vec<PvRecord>> {
        self.inner.pvs_by_status(status)
    }

    fn recently_added_pvs(&self, since: SystemTime) -> anyhow::Result<Vec<PvRecord>> {
        self.inner.recently_added_pvs(since)
    }

    fn recently_modified_pvs(&self, since: SystemTime) -> anyhow::Result<Vec<PvRecord>> {
        self.inner.recently_modified_pvs(since)
    }

    fn silent_pvs(&self, threshold: Duration) -> anyhow::Result<Vec<PvRecord>> {
        self.inner.silent_pvs(threshold)
    }
}

impl PvCommandRepository for RegistryRepository {
    fn register_pv(&self, pv: &str, dbr_type: ArchDbType, mode: &SampleMode, element_count: i32) -> anyhow::Result<()> {
        self.inner.register_pv(pv, dbr_type, mode, element_count)
    }

    fn remove_pv(&self, pv: &str) -> anyhow::Result<bool> {
        self.inner.remove_pv(pv)
    }

    fn set_status(&self, pv: &str, status: PvStatus) -> anyhow::Result<bool> {
        self.inner.set_status(pv, status)
    }

    fn update_sample_mode(&self, pv: &str, mode: &SampleMode) -> anyhow::Result<bool> {
        self.inner.update_sample_mode(pv, mode)
    }

    fn update_metadata(&self, pv: &str, prec: Option<&str>, egu: Option<&str>) -> anyhow::Result<bool> {
        self.inner.update_metadata(pv, prec, egu)
    }

    fn import_pv(
        &self,
        pv: &str,
        dbr_type: ArchDbType,
        mode: &SampleMode,
        element_count: i32,
        status: PvStatus,
        created_at: Option<&str>,
        prec: Option<&str>,
        egu: Option<&str>,
    ) -> anyhow::Result<()> {
        self.inner.import_pv(pv, dbr_type, mode, element_count, status, created_at, prec, egu)
    }
}
