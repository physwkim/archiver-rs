//! PV metadata registry backed by SQLite.
//!
//! Persists PV archiving configuration and state across restarts.
//! This is metadata only — time-series data lives in PlainPB files.

use std::path::Path;
use std::sync::Mutex;
use std::time::{Duration, SystemTime};

use chrono::{DateTime, Utc};
use rusqlite::{params, Connection, OptionalExtension};
use tracing::info;

use crate::types::ArchDbType;

/// PV archiving status.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PvStatus {
    Active,
    Paused,
    Error,
    Inactive,
}

impl PvStatus {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Active => "active",
            Self::Paused => "paused",
            Self::Error => "error",
            Self::Inactive => "inactive",
        }
    }

    pub fn from_str(s: &str) -> Self {
        match s {
            "active" => Self::Active,
            "paused" => Self::Paused,
            "error" => Self::Error,
            "inactive" => Self::Inactive,
            _ => Self::Active,
        }
    }
}

/// Sampling mode stored in the registry.
#[derive(Debug, Clone, PartialEq)]
pub enum SampleMode {
    Monitor,
    Scan { period_secs: f64 },
}

impl SampleMode {
    fn to_db(&self) -> (&str, f64) {
        match self {
            Self::Monitor => ("monitor", 0.0),
            Self::Scan { period_secs } => ("scan", *period_secs),
        }
    }

    fn from_db(mode: &str, period: f64) -> Self {
        match mode {
            "scan" => Self::Scan { period_secs: period },
            _ => Self::Monitor,
        }
    }
}

/// A PV record in the registry.
#[derive(Debug, Clone)]
pub struct PvRecord {
    pub pv_name: String,
    pub dbr_type: ArchDbType,
    pub sample_mode: SampleMode,
    pub status: PvStatus,
    pub element_count: i32,
    pub last_timestamp: Option<SystemTime>,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
    pub prec: Option<String>,
    pub egu: Option<String>,
}

/// SQLite-backed PV metadata registry.
pub struct PvRegistry {
    conn: Mutex<Connection>,
}

impl PvRegistry {
    /// Open (or create) the registry database at the given path.
    pub fn open(path: &Path) -> anyhow::Result<Self> {
        let conn = Connection::open(path)?;
        let registry = Self {
            conn: Mutex::new(conn),
        };
        registry.init_schema()?;
        Ok(registry)
    }

    /// Create an in-memory registry (for testing).
    pub fn in_memory() -> anyhow::Result<Self> {
        let conn = Connection::open_in_memory()?;
        let registry = Self {
            conn: Mutex::new(conn),
        };
        registry.init_schema()?;
        Ok(registry)
    }

    fn init_schema(&self) -> anyhow::Result<()> {
        let conn = self.conn.lock().unwrap();
        conn.execute_batch(
            "
            CREATE TABLE IF NOT EXISTS pv_info (
                pv_name         TEXT PRIMARY KEY NOT NULL,
                dbr_type        INTEGER NOT NULL,
                sample_mode     TEXT NOT NULL DEFAULT 'monitor',
                sample_period   REAL NOT NULL DEFAULT 0.0,
                status          TEXT NOT NULL DEFAULT 'active',
                element_count   INTEGER NOT NULL DEFAULT 1,
                last_timestamp  TEXT,
                created_at      TEXT NOT NULL,
                updated_at      TEXT NOT NULL,
                prec            TEXT,
                egu             TEXT
            );

            CREATE INDEX IF NOT EXISTS idx_pv_status ON pv_info(status);
            CREATE INDEX IF NOT EXISTS idx_pv_prefix ON pv_info(pv_name COLLATE NOCASE);
            ",
        )?;
        // Migration: add prec/egu columns if table was created with old schema.
        let _ = conn.execute_batch(
            "ALTER TABLE pv_info ADD COLUMN prec TEXT;
             ALTER TABLE pv_info ADD COLUMN egu TEXT;",
        );
        info!("PV registry schema initialized");
        Ok(())
    }

    /// Register a new PV for archiving.
    pub fn register_pv(
        &self,
        pv_name: &str,
        dbr_type: ArchDbType,
        sample_mode: &SampleMode,
        element_count: i32,
    ) -> anyhow::Result<()> {
        let conn = self.conn.lock().unwrap();
        let now = Utc::now().to_rfc3339();
        let (mode_str, period) = sample_mode.to_db();

        conn.execute(
            "INSERT OR REPLACE INTO pv_info
             (pv_name, dbr_type, sample_mode, sample_period, status, element_count, created_at, updated_at)
             VALUES (?1, ?2, ?3, ?4, 'active', ?5, COALESCE((SELECT created_at FROM pv_info WHERE pv_name = ?1), ?6), ?6)",
            params![pv_name, dbr_type as i32, mode_str, period, element_count, now],
        )?;
        Ok(())
    }

    /// Update PV status (active, paused, error).
    pub fn set_status(&self, pv_name: &str, status: PvStatus) -> anyhow::Result<bool> {
        let conn = self.conn.lock().unwrap();
        let now = Utc::now().to_rfc3339();
        let rows = conn.execute(
            "UPDATE pv_info SET status = ?1, updated_at = ?2 WHERE pv_name = ?3",
            params![status.as_str(), now, pv_name],
        )?;
        Ok(rows > 0)
    }

    /// Update the last known timestamp for a PV.
    pub fn update_last_timestamp(
        &self,
        pv_name: &str,
        timestamp: SystemTime,
    ) -> anyhow::Result<()> {
        let conn = self.conn.lock().unwrap();
        let dt = DateTime::<Utc>::from(timestamp).to_rfc3339();
        let now = Utc::now().to_rfc3339();
        conn.execute(
            "UPDATE pv_info SET last_timestamp = ?1, updated_at = ?2 WHERE pv_name = ?3",
            params![dt, now, pv_name],
        )?;
        Ok(())
    }

    /// Remove a PV from the registry entirely.
    pub fn remove_pv(&self, pv_name: &str) -> anyhow::Result<bool> {
        let conn = self.conn.lock().unwrap();
        let rows = conn.execute("DELETE FROM pv_info WHERE pv_name = ?1", params![pv_name])?;
        Ok(rows > 0)
    }

    /// Get a single PV record.
    pub fn get_pv(&self, pv_name: &str) -> anyhow::Result<Option<PvRecord>> {
        let conn = self.conn.lock().unwrap();
        conn.query_row(
            "SELECT pv_name, dbr_type, sample_mode, sample_period, status, element_count,
                    last_timestamp, created_at, updated_at, prec, egu
             FROM pv_info WHERE pv_name = ?1",
            params![pv_name],
            |row| Ok(row_to_record(row)),
        )
        .optional()
        .map_err(Into::into)
    }

    /// List all PV names.
    pub fn all_pv_names(&self) -> anyhow::Result<Vec<String>> {
        let conn = self.conn.lock().unwrap();
        let mut stmt = conn.prepare("SELECT pv_name FROM pv_info ORDER BY pv_name")?;
        let names = stmt
            .query_map([], |row| row.get(0))?
            .collect::<Result<Vec<String>, _>>()?;
        Ok(names)
    }

    /// List all PVs with a given status.
    pub fn pvs_by_status(&self, status: PvStatus) -> anyhow::Result<Vec<PvRecord>> {
        let conn = self.conn.lock().unwrap();
        let mut stmt = conn.prepare(
            "SELECT pv_name, dbr_type, sample_mode, sample_period, status, element_count,
                    last_timestamp, created_at, updated_at, prec, egu
             FROM pv_info WHERE status = ?1 ORDER BY pv_name",
        )?;
        let records = stmt
            .query_map(params![status.as_str()], |row| Ok(row_to_record(row)))?
            .collect::<Result<Vec<_>, _>>()?;
        Ok(records)
    }

    /// Match PV names by glob pattern (SQL GLOB).
    /// Supports `*` and `?` wildcards.
    pub fn matching_pvs(&self, pattern: &str) -> anyhow::Result<Vec<String>> {
        let conn = self.conn.lock().unwrap();
        let mut stmt =
            conn.prepare("SELECT pv_name FROM pv_info WHERE pv_name GLOB ?1 ORDER BY pv_name")?;
        let names = stmt
            .query_map(params![pattern], |row| row.get(0))?
            .collect::<Result<Vec<String>, _>>()?;
        Ok(names)
    }

    /// Count total PVs, optionally filtered by status.
    pub fn count(&self, status: Option<PvStatus>) -> anyhow::Result<u64> {
        let conn = self.conn.lock().unwrap();
        let count: u64 = match status {
            Some(s) => conn.query_row(
                "SELECT COUNT(*) FROM pv_info WHERE status = ?1",
                params![s.as_str()],
                |row| row.get(0),
            )?,
            None => conn.query_row("SELECT COUNT(*) FROM pv_info", [], |row| row.get(0))?,
        };
        Ok(count)
    }

    /// Batch update last timestamps (for periodic flush).
    pub fn batch_update_timestamps(
        &self,
        updates: &[(&str, SystemTime)],
    ) -> anyhow::Result<()> {
        let mut conn = self.conn.lock().unwrap();
        let tx = conn.transaction()?;
        let now = Utc::now().to_rfc3339();
        {
            let mut stmt = tx.prepare(
                "UPDATE pv_info SET last_timestamp = ?1, updated_at = ?2 WHERE pv_name = ?3",
            )?;
            for (pv_name, ts) in updates {
                let dt = DateTime::<Utc>::from(*ts).to_rfc3339();
                stmt.execute(params![dt, now, pv_name])?;
            }
        }
        tx.commit()?;
        Ok(())
    }

    /// Get PVs added since a given time.
    pub fn recently_added_pvs(&self, since: SystemTime) -> anyhow::Result<Vec<PvRecord>> {
        let conn = self.conn.lock().unwrap();
        let since_str = DateTime::<Utc>::from(since).to_rfc3339();
        let mut stmt = conn.prepare(
            "SELECT pv_name, dbr_type, sample_mode, sample_period, status, element_count,
                    last_timestamp, created_at, updated_at, prec, egu
             FROM pv_info WHERE created_at >= ?1 ORDER BY created_at DESC",
        )?;
        let records = stmt
            .query_map(params![since_str], |row| Ok(row_to_record(row)))?
            .collect::<Result<Vec<_>, _>>()?;
        Ok(records)
    }

    /// Get PVs modified since a given time.
    pub fn recently_modified_pvs(&self, since: SystemTime) -> anyhow::Result<Vec<PvRecord>> {
        let conn = self.conn.lock().unwrap();
        let since_str = DateTime::<Utc>::from(since).to_rfc3339();
        let mut stmt = conn.prepare(
            "SELECT pv_name, dbr_type, sample_mode, sample_period, status, element_count,
                    last_timestamp, created_at, updated_at, prec, egu
             FROM pv_info WHERE updated_at >= ?1 ORDER BY updated_at DESC",
        )?;
        let records = stmt
            .query_map(params![since_str], |row| Ok(row_to_record(row)))?
            .collect::<Result<Vec<_>, _>>()?;
        Ok(records)
    }

    /// Update the sample mode and period for a PV.
    pub fn update_sample_mode(&self, pv_name: &str, mode: &SampleMode) -> anyhow::Result<bool> {
        let conn = self.conn.lock().unwrap();
        let now = Utc::now().to_rfc3339();
        let (mode_str, period) = mode.to_db();
        let rows = conn.execute(
            "UPDATE pv_info SET sample_mode = ?1, sample_period = ?2, updated_at = ?3 WHERE pv_name = ?4",
            params![mode_str, period, now, pv_name],
        )?;
        Ok(rows > 0)
    }

    /// Update PREC and EGU metadata for a PV.
    pub fn update_metadata(
        &self,
        pv_name: &str,
        prec: Option<&str>,
        egu: Option<&str>,
    ) -> anyhow::Result<bool> {
        let conn = self.conn.lock().unwrap();
        let now = Utc::now().to_rfc3339();
        let rows = conn.execute(
            "UPDATE pv_info SET prec = COALESCE(?1, prec), egu = COALESCE(?2, egu), updated_at = ?3 WHERE pv_name = ?4",
            params![prec, egu, now, pv_name],
        )?;
        Ok(rows > 0)
    }

    /// Import a PV with all fields in a single SQL operation.
    /// Used during config import to atomically set status, created_at, and metadata.
    pub fn import_pv(
        &self,
        pv_name: &str,
        dbr_type: ArchDbType,
        sample_mode: &SampleMode,
        element_count: i32,
        status: PvStatus,
        created_at: Option<&str>,
        prec: Option<&str>,
        egu: Option<&str>,
    ) -> anyhow::Result<()> {
        let conn = self.conn.lock().unwrap();
        let now = Utc::now().to_rfc3339();
        let (mode_str, period) = sample_mode.to_db();
        let created = created_at.unwrap_or(&now);

        conn.execute(
            "INSERT OR REPLACE INTO pv_info
             (pv_name, dbr_type, sample_mode, sample_period, status, element_count,
              created_at, updated_at, prec, egu)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10)",
            params![
                pv_name,
                dbr_type as i32,
                mode_str,
                period,
                status.as_str(),
                element_count,
                created,
                now,
                prec,
                egu,
            ],
        )?;
        Ok(())
    }

    /// Get all PV records (for export).
    pub fn all_records(&self) -> anyhow::Result<Vec<PvRecord>> {
        let conn = self.conn.lock().unwrap();
        let mut stmt = conn.prepare(
            "SELECT pv_name, dbr_type, sample_mode, sample_period, status, element_count,
                    last_timestamp, created_at, updated_at, prec, egu
             FROM pv_info ORDER BY pv_name",
        )?;
        let records = stmt
            .query_map([], |row| Ok(row_to_record(row)))?
            .collect::<Result<Vec<_>, _>>()?;
        Ok(records)
    }

    /// Get PVs that have not received events for longer than the threshold duration.
    /// Only returns PVs that have a last_timestamp (have received at least one event).
    pub fn silent_pvs(&self, threshold: Duration) -> anyhow::Result<Vec<PvRecord>> {
        let conn = self.conn.lock().unwrap();
        let cutoff = SystemTime::now()
            .checked_sub(threshold)
            .unwrap_or(SystemTime::UNIX_EPOCH);
        let cutoff_str = DateTime::<Utc>::from(cutoff).to_rfc3339();
        let mut stmt = conn.prepare(
            "SELECT pv_name, dbr_type, sample_mode, sample_period, status, element_count,
                    last_timestamp, created_at, updated_at, prec, egu
             FROM pv_info WHERE last_timestamp IS NOT NULL AND last_timestamp < ?1
             ORDER BY last_timestamp ASC",
        )?;
        let records = stmt
            .query_map(params![cutoff_str], |row| Ok(row_to_record(row)))?
            .collect::<Result<Vec<_>, _>>()?;
        Ok(records)
    }
}

fn row_to_record(row: &rusqlite::Row) -> PvRecord {
    let pv_name: String = row.get(0).unwrap();
    let dbr_type_i: i32 = row.get(1).unwrap();
    let sample_mode_str: String = row.get(2).unwrap();
    let sample_period: f64 = row.get(3).unwrap();
    let status_str: String = row.get(4).unwrap();
    let element_count: i32 = row.get(5).unwrap();
    let last_ts_str: Option<String> = row.get(6).unwrap();
    let created_str: String = row.get(7).unwrap();
    let updated_str: String = row.get(8).unwrap();
    let prec: Option<String> = row.get(9).unwrap_or(None);
    let egu: Option<String> = row.get(10).unwrap_or(None);

    let last_timestamp = last_ts_str.and_then(|s| {
        DateTime::parse_from_rfc3339(&s)
            .ok()
            .map(|dt| dt.with_timezone(&Utc).into())
    });

    PvRecord {
        pv_name,
        dbr_type: ArchDbType::from_i32(dbr_type_i).unwrap_or(ArchDbType::ScalarDouble),
        sample_mode: SampleMode::from_db(&sample_mode_str, sample_period),
        status: PvStatus::from_str(&status_str),
        element_count,
        last_timestamp,
        created_at: DateTime::parse_from_rfc3339(&created_str)
            .map(|dt| dt.with_timezone(&Utc))
            .unwrap_or_else(|_| Utc::now()),
        updated_at: DateTime::parse_from_rfc3339(&updated_str)
            .map(|dt| dt.with_timezone(&Utc))
            .unwrap_or_else(|_| Utc::now()),
        prec,
        egu,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_register_and_get() {
        let reg = PvRegistry::in_memory().unwrap();
        reg.register_pv("SIM:Sine", ArchDbType::ScalarDouble, &SampleMode::Monitor, 1)
            .unwrap();

        let record = reg.get_pv("SIM:Sine").unwrap().unwrap();
        assert_eq!(record.pv_name, "SIM:Sine");
        assert_eq!(record.dbr_type, ArchDbType::ScalarDouble);
        assert_eq!(record.status, PvStatus::Active);
    }

    #[test]
    fn test_status_transitions() {
        let reg = PvRegistry::in_memory().unwrap();
        reg.register_pv("SIM:Test", ArchDbType::ScalarDouble, &SampleMode::Monitor, 1)
            .unwrap();

        reg.set_status("SIM:Test", PvStatus::Paused).unwrap();
        let r = reg.get_pv("SIM:Test").unwrap().unwrap();
        assert_eq!(r.status, PvStatus::Paused);

        reg.set_status("SIM:Test", PvStatus::Active).unwrap();
        let r = reg.get_pv("SIM:Test").unwrap().unwrap();
        assert_eq!(r.status, PvStatus::Active);
    }

    #[test]
    fn test_pattern_matching() {
        let reg = PvRegistry::in_memory().unwrap();
        reg.register_pv("SIM:Sine", ArchDbType::ScalarDouble, &SampleMode::Monitor, 1).unwrap();
        reg.register_pv("SIM:Cosine", ArchDbType::ScalarDouble, &SampleMode::Monitor, 1).unwrap();
        reg.register_pv("EXP:BL1:run:active", ArchDbType::ScalarEnum, &SampleMode::Monitor, 1).unwrap();
        reg.register_pv("EXP:BL1:motor:th:readback", ArchDbType::ScalarDouble, &SampleMode::Monitor, 1).unwrap();

        let sim = reg.matching_pvs("SIM:*").unwrap();
        assert_eq!(sim.len(), 2);

        let exp = reg.matching_pvs("EXP:BL1:*").unwrap();
        assert_eq!(exp.len(), 2);

        let motor = reg.matching_pvs("EXP:*:motor:*").unwrap();
        assert_eq!(motor.len(), 1);
    }

    #[test]
    fn test_count_and_list() {
        let reg = PvRegistry::in_memory().unwrap();
        for i in 0..100 {
            reg.register_pv(
                &format!("PV:Test:{i:04}"),
                ArchDbType::ScalarDouble,
                &SampleMode::Monitor,
                1,
            ).unwrap();
        }

        assert_eq!(reg.count(None).unwrap(), 100);
        assert_eq!(reg.count(Some(PvStatus::Active)).unwrap(), 100);

        let names = reg.all_pv_names().unwrap();
        assert_eq!(names.len(), 100);
    }

    #[test]
    fn test_remove_pv() {
        let reg = PvRegistry::in_memory().unwrap();
        reg.register_pv("SIM:Gone", ArchDbType::ScalarDouble, &SampleMode::Monitor, 1).unwrap();
        assert!(reg.get_pv("SIM:Gone").unwrap().is_some());

        reg.remove_pv("SIM:Gone").unwrap();
        assert!(reg.get_pv("SIM:Gone").unwrap().is_none());
    }

    #[test]
    fn test_batch_update_timestamps() {
        let reg = PvRegistry::in_memory().unwrap();
        reg.register_pv("PV:A", ArchDbType::ScalarDouble, &SampleMode::Monitor, 1).unwrap();
        reg.register_pv("PV:B", ArchDbType::ScalarDouble, &SampleMode::Monitor, 1).unwrap();

        let now = SystemTime::now();
        reg.batch_update_timestamps(&[("PV:A", now), ("PV:B", now)]).unwrap();

        let a = reg.get_pv("PV:A").unwrap().unwrap();
        assert!(a.last_timestamp.is_some());
    }

    #[test]
    fn test_recently_added_pvs() {
        let reg = PvRegistry::in_memory().unwrap();
        let before = SystemTime::now() - Duration::from_secs(1);
        reg.register_pv("PV:New", ArchDbType::ScalarDouble, &SampleMode::Monitor, 1).unwrap();

        let recent = reg.recently_added_pvs(before).unwrap();
        assert_eq!(recent.len(), 1);
        assert_eq!(recent[0].pv_name, "PV:New");

        let future = SystemTime::now() + Duration::from_secs(3600);
        let none = reg.recently_added_pvs(future).unwrap();
        assert!(none.is_empty());
    }

    #[test]
    fn test_recently_modified_pvs() {
        let reg = PvRegistry::in_memory().unwrap();
        reg.register_pv("PV:Mod", ArchDbType::ScalarDouble, &SampleMode::Monitor, 1).unwrap();
        let before = SystemTime::now() - Duration::from_secs(1);

        // Modify status to update updated_at.
        reg.set_status("PV:Mod", PvStatus::Paused).unwrap();

        let recent = reg.recently_modified_pvs(before).unwrap();
        assert!(recent.iter().any(|r| r.pv_name == "PV:Mod"));
    }

    #[test]
    fn test_update_sample_mode() {
        let reg = PvRegistry::in_memory().unwrap();
        reg.register_pv("PV:Mode", ArchDbType::ScalarDouble, &SampleMode::Monitor, 1).unwrap();

        let new_mode = SampleMode::Scan { period_secs: 5.0 };
        assert!(reg.update_sample_mode("PV:Mode", &new_mode).unwrap());

        let r = reg.get_pv("PV:Mode").unwrap().unwrap();
        assert_eq!(r.sample_mode, SampleMode::Scan { period_secs: 5.0 });
    }

    #[test]
    fn test_silent_pvs() {
        let reg = PvRegistry::in_memory().unwrap();
        reg.register_pv("PV:Silent", ArchDbType::ScalarDouble, &SampleMode::Monitor, 1).unwrap();
        reg.register_pv("PV:NoData", ArchDbType::ScalarDouble, &SampleMode::Monitor, 1).unwrap();

        // Set PV:Silent's last_timestamp to 2 hours ago.
        let old_time = SystemTime::now() - Duration::from_secs(7200);
        reg.update_last_timestamp("PV:Silent", old_time).unwrap();

        // PV:NoData has no last_timestamp — should not appear.
        let silent = reg.silent_pvs(Duration::from_secs(3600)).unwrap();
        assert_eq!(silent.len(), 1);
        assert_eq!(silent[0].pv_name, "PV:Silent");
    }
}
