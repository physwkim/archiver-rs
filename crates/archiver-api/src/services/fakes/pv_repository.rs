use std::collections::HashMap;
use std::sync::Mutex;
use std::time::{Duration, SystemTime};

use archiver_core::registry::{PvRecord, PvStatus, SampleMode};
use archiver_core::types::ArchDbType;

use crate::services::traits::{PvCommandRepository, PvQueryRepository};

pub struct InMemoryPvRepository {
    pvs: Mutex<HashMap<String, PvRecord>>,
}

impl InMemoryPvRepository {
    pub fn new() -> Self {
        Self {
            pvs: Mutex::new(HashMap::new()),
        }
    }
}

impl Default for InMemoryPvRepository {
    fn default() -> Self {
        Self::new()
    }
}

impl PvQueryRepository for InMemoryPvRepository {
    fn get_pv(&self, pv: &str) -> anyhow::Result<Option<PvRecord>> {
        Ok(self.pvs.lock().unwrap().get(pv).cloned())
    }

    fn all_pv_names(&self) -> anyhow::Result<Vec<String>> {
        let mut names: Vec<String> = self.pvs.lock().unwrap().keys().cloned().collect();
        names.sort();
        Ok(names)
    }

    fn matching_pvs(&self, pattern: &str) -> anyhow::Result<Vec<String>> {
        let lock = self.pvs.lock().unwrap();
        let glob = pattern.replace('*', "");
        let mut names: Vec<String> = lock
            .keys()
            .filter(|k| {
                if pattern.ends_with('*') {
                    k.starts_with(&glob)
                } else if pattern.starts_with('*') {
                    k.ends_with(&glob)
                } else {
                    k.contains(&glob)
                }
            })
            .cloned()
            .collect();
        names.sort();
        Ok(names)
    }

    fn count(&self, status: Option<PvStatus>) -> anyhow::Result<u64> {
        let lock = self.pvs.lock().unwrap();
        let count = match status {
            Some(s) => lock.values().filter(|r| r.status == s).count(),
            None => lock.len(),
        };
        Ok(count as u64)
    }

    fn all_records(&self) -> anyhow::Result<Vec<PvRecord>> {
        let mut records: Vec<PvRecord> = self.pvs.lock().unwrap().values().cloned().collect();
        records.sort_by(|a, b| a.pv_name.cmp(&b.pv_name));
        Ok(records)
    }

    fn pvs_by_status(&self, status: PvStatus) -> anyhow::Result<Vec<PvRecord>> {
        let lock = self.pvs.lock().unwrap();
        let records: Vec<PvRecord> = lock
            .values()
            .filter(|r| r.status == status)
            .cloned()
            .collect();
        Ok(records)
    }

    fn recently_added_pvs(&self, since: SystemTime) -> anyhow::Result<Vec<PvRecord>> {
        let lock = self.pvs.lock().unwrap();
        let records: Vec<PvRecord> = lock
            .values()
            .filter(|r| r.created_at >= chrono::DateTime::<chrono::Utc>::from(since))
            .cloned()
            .collect();
        Ok(records)
    }

    fn recently_modified_pvs(&self, since: SystemTime) -> anyhow::Result<Vec<PvRecord>> {
        let lock = self.pvs.lock().unwrap();
        let records: Vec<PvRecord> = lock
            .values()
            .filter(|r| {
                r.last_timestamp
                    .map(|ts| ts >= since)
                    .unwrap_or(false)
            })
            .cloned()
            .collect();
        Ok(records)
    }

    fn silent_pvs(&self, threshold: Duration) -> anyhow::Result<Vec<PvRecord>> {
        let cutoff = SystemTime::now() - threshold;
        let lock = self.pvs.lock().unwrap();
        let records: Vec<PvRecord> = lock
            .values()
            .filter(|r| {
                r.last_timestamp
                    .map(|ts| ts < cutoff)
                    .unwrap_or(false)
            })
            .cloned()
            .collect();
        Ok(records)
    }
}

impl PvCommandRepository for InMemoryPvRepository {
    fn register_pv(
        &self,
        pv: &str,
        dbr_type: ArchDbType,
        mode: &SampleMode,
        element_count: i32,
    ) -> anyhow::Result<()> {
        let now = chrono::Utc::now();
        let record = PvRecord {
            pv_name: pv.to_string(),
            dbr_type,
            sample_mode: mode.clone(),
            element_count,
            status: PvStatus::Active,
            created_at: now,
            updated_at: now,
            last_timestamp: None,
            prec: None,
            egu: None,
        };
        self.pvs.lock().unwrap().insert(pv.to_string(), record);
        Ok(())
    }

    fn remove_pv(&self, pv: &str) -> anyhow::Result<bool> {
        Ok(self.pvs.lock().unwrap().remove(pv).is_some())
    }

    fn set_status(&self, pv: &str, status: PvStatus) -> anyhow::Result<bool> {
        if let Some(record) = self.pvs.lock().unwrap().get_mut(pv) {
            record.status = status;
            Ok(true)
        } else {
            Ok(false)
        }
    }

    fn update_sample_mode(&self, pv: &str, mode: &SampleMode) -> anyhow::Result<bool> {
        if let Some(record) = self.pvs.lock().unwrap().get_mut(pv) {
            record.sample_mode = mode.clone();
            Ok(true)
        } else {
            Ok(false)
        }
    }

    fn update_metadata(
        &self,
        pv: &str,
        prec: Option<&str>,
        egu: Option<&str>,
    ) -> anyhow::Result<bool> {
        if let Some(record) = self.pvs.lock().unwrap().get_mut(pv) {
            if let Some(p) = prec {
                record.prec = Some(p.to_string());
            }
            if let Some(e) = egu {
                record.egu = Some(e.to_string());
            }
            Ok(true)
        } else {
            Ok(false)
        }
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
        let created = created_at
            .and_then(|s| chrono::DateTime::parse_from_rfc3339(s).ok())
            .map(|dt| dt.with_timezone(&chrono::Utc))
            .unwrap_or_else(chrono::Utc::now);

        let now = chrono::Utc::now();
        let record = PvRecord {
            pv_name: pv.to_string(),
            dbr_type,
            sample_mode: mode.clone(),
            element_count,
            status,
            created_at: created,
            updated_at: now,
            last_timestamp: None,
            prec: prec.map(|s| s.to_string()),
            egu: egu.map(|s| s.to_string()),
        };
        self.pvs.lock().unwrap().insert(pv.to_string(), record);
        Ok(())
    }
}
