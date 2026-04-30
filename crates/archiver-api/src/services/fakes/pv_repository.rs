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
        // Mirror real registry's `WHERE alias_for IS NULL`: this returns
        // *real* PVs only, not aliases. expanded_pv_names is the
        // alias-inclusive variant.
        let lock = self.pvs.lock().unwrap();
        let mut names: Vec<String> = lock
            .values()
            .filter(|r| r.alias_for.is_none())
            .map(|r| r.pv_name.clone())
            .collect();
        names.sort();
        Ok(names)
    }

    fn matching_pvs(&self, pattern: &str) -> anyhow::Result<Vec<String>> {
        // Real registry filters aliases out of glob matches.
        let lock = self.pvs.lock().unwrap();
        let glob = pattern.replace('*', "");
        let mut names: Vec<String> = lock
            .values()
            .filter(|r| r.alias_for.is_none())
            .map(|r| r.pv_name.clone())
            .filter(|k| {
                if pattern.ends_with('*') {
                    k.starts_with(&glob)
                } else if pattern.starts_with('*') {
                    k.ends_with(&glob)
                } else {
                    k.contains(&glob)
                }
            })
            .collect();
        names.sort();
        Ok(names)
    }

    fn count(&self, status: Option<PvStatus>) -> anyhow::Result<u64> {
        // Real registry's count() excludes aliases. Aliases have
        // `status='alias'` and would otherwise inflate Active counts when
        // a caller queries with status=None.
        let lock = self.pvs.lock().unwrap();
        let count = match status {
            Some(s) => lock
                .values()
                .filter(|r| r.alias_for.is_none() && r.status == s)
                .count(),
            None => lock.values().filter(|r| r.alias_for.is_none()).count(),
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
        // Real registry filters aliases out (`WHERE created_at >= ? AND alias_for IS NULL`).
        let since_dt = chrono::DateTime::<chrono::Utc>::from(since);
        let lock = self.pvs.lock().unwrap();
        let records: Vec<PvRecord> = lock
            .values()
            .filter(|r| r.alias_for.is_none() && r.created_at >= since_dt)
            .cloned()
            .collect();
        Ok(records)
    }

    fn recently_modified_pvs(&self, since: SystemTime) -> anyhow::Result<Vec<PvRecord>> {
        // Mirror the real registry: filter on updated_at (any schema-affecting
        // change), not last_timestamp (sample arrival). Aliases excluded for
        // parity with `WHERE alias_for IS NULL`.
        let since_dt = chrono::DateTime::<chrono::Utc>::from(since);
        let lock = self.pvs.lock().unwrap();
        let records: Vec<PvRecord> = lock
            .values()
            .filter(|r| r.alias_for.is_none() && r.updated_at >= since_dt)
            .cloned()
            .collect();
        Ok(records)
    }

    fn silent_pvs(&self, threshold: Duration) -> anyhow::Result<Vec<PvRecord>> {
        // Real registry filters aliases out (aliases never carry samples,
        // so they'd otherwise pollute the silent-PV report).
        let cutoff = SystemTime::now() - threshold;
        let lock = self.pvs.lock().unwrap();
        let records: Vec<PvRecord> = lock
            .values()
            .filter(|r| {
                r.alias_for.is_none() && r.last_timestamp.map(|ts| ts < cutoff).unwrap_or(false)
            })
            .cloned()
            .collect();
        Ok(records)
    }

    fn canonical_name(&self, name: &str) -> anyhow::Result<String> {
        let lock = self.pvs.lock().unwrap();
        Ok(lock
            .get(name)
            .and_then(|r| r.alias_for.clone())
            .unwrap_or_else(|| name.to_string()))
    }

    fn aliases_for(&self, target: &str) -> anyhow::Result<Vec<String>> {
        let lock = self.pvs.lock().unwrap();
        let mut names: Vec<String> = lock
            .values()
            .filter(|r| r.alias_for.as_deref() == Some(target))
            .map(|r| r.pv_name.clone())
            .collect();
        names.sort();
        Ok(names)
    }

    fn all_aliases(&self) -> anyhow::Result<Vec<(String, String)>> {
        let lock = self.pvs.lock().unwrap();
        let mut pairs: Vec<(String, String)> = lock
            .values()
            .filter_map(|r| r.alias_for.clone().map(|t| (r.pv_name.clone(), t)))
            .collect();
        pairs.sort_by(|a, b| a.0.cmp(&b.0));
        Ok(pairs)
    }

    fn expanded_pv_names(&self) -> anyhow::Result<Vec<String>> {
        // Real registry returns ALL names including aliases (no filter on
        // alias_for). Don't delegate to all_pv_names — that one filters
        // aliases out.
        let mut names: Vec<String> = self.pvs.lock().unwrap().keys().cloned().collect();
        names.sort();
        Ok(names)
    }

    fn matching_pvs_expanded(&self, pattern: &str) -> anyhow::Result<Vec<String>> {
        // Glob matches across both real PVs and aliases (Java parity).
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
            alias_for: None,
            archive_fields: Vec::new(),
            policy_name: None,
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
            record.updated_at = chrono::Utc::now();
            Ok(true)
        } else {
            Ok(false)
        }
    }

    fn update_sample_mode(&self, pv: &str, mode: &SampleMode) -> anyhow::Result<bool> {
        if let Some(record) = self.pvs.lock().unwrap().get_mut(pv) {
            record.sample_mode = mode.clone();
            record.updated_at = chrono::Utc::now();
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
            record.updated_at = chrono::Utc::now();
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
        alias_for: Option<&str>,
        archive_fields: &[String],
        policy_name: Option<&str>,
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
            alias_for: alias_for.map(|s| s.to_string()),
            archive_fields: archive_fields.to_vec(),
            policy_name: policy_name.map(|s| s.to_string()),
        };
        self.pvs.lock().unwrap().insert(pv.to_string(), record);
        Ok(())
    }

    fn update_archive_fields(&self, pv: &str, fields: &[String]) -> anyhow::Result<bool> {
        if let Some(record) = self.pvs.lock().unwrap().get_mut(pv) {
            record.archive_fields = fields.to_vec();
            record.updated_at = chrono::Utc::now();
            Ok(true)
        } else {
            Ok(false)
        }
    }

    fn update_policy_name(&self, pv: &str, policy_name: Option<&str>) -> anyhow::Result<bool> {
        if let Some(record) = self.pvs.lock().unwrap().get_mut(pv) {
            record.policy_name = policy_name.map(|s| s.to_string());
            record.updated_at = chrono::Utc::now();
            Ok(true)
        } else {
            Ok(false)
        }
    }

    fn add_alias(&self, alias: &str, target: &str) -> anyhow::Result<()> {
        if alias == target {
            anyhow::bail!("alias and target must differ");
        }
        let mut lock = self.pvs.lock().unwrap();
        let target_record = lock
            .get(target)
            .ok_or_else(|| anyhow::anyhow!("target PV '{target}' not found"))?
            .clone();
        if target_record.alias_for.is_some() {
            anyhow::bail!("target '{target}' is itself an alias");
        }
        if let Some(existing) = lock.get(alias) {
            if existing.alias_for.as_deref() == Some(target) {
                return Ok(());
            }
            anyhow::bail!("'{alias}' already exists in registry");
        }
        let now = chrono::Utc::now();
        let row = PvRecord {
            pv_name: alias.to_string(),
            dbr_type: target_record.dbr_type,
            sample_mode: target_record.sample_mode.clone(),
            element_count: target_record.element_count,
            status: PvStatus::Alias,
            created_at: now,
            updated_at: now,
            last_timestamp: None,
            prec: None,
            egu: None,
            alias_for: Some(target.to_string()),
            archive_fields: Vec::new(),
            policy_name: None,
        };
        lock.insert(alias.to_string(), row);
        Ok(())
    }

    fn remove_alias(&self, alias: &str) -> anyhow::Result<bool> {
        let mut lock = self.pvs.lock().unwrap();
        let is_alias = lock
            .get(alias)
            .map(|r| r.alias_for.is_some())
            .unwrap_or(false);
        if is_alias {
            lock.remove(alias);
            Ok(true)
        } else {
            Ok(false)
        }
    }
}
