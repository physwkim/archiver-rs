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
///
/// `Alias` is a sentinel applied to alias rows so they don't appear in
/// status-filtered queries (`pvs_by_status(Active)`, getPVCount, restore
/// loops). Aliases are routing entries, not archive subjects.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PvStatus {
    Active,
    Paused,
    Error,
    Inactive,
    Alias,
}

impl PvStatus {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Active => "active",
            Self::Paused => "paused",
            Self::Error => "error",
            Self::Inactive => "inactive",
            Self::Alias => "alias",
        }
    }
}

impl std::str::FromStr for PvStatus {
    type Err = std::convert::Infallible;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Ok(match s {
            "active" => Self::Active,
            "paused" => Self::Paused,
            "error" => Self::Error,
            "inactive" => Self::Inactive,
            "alias" => Self::Alias,
            _ => Self::Active,
        })
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
    /// When set, this row is an alias pointing at another PV name.
    /// Lookups should resolve to that target before reading/writing data.
    pub alias_for: Option<String>,
    /// Names of EPICS metadata fields (e.g. ["HIHI","LOLO","EGU"]) that the
    /// engine should sample alongside the main value and attach to events.
    pub archive_fields: Vec<String>,
    /// Name of the policy that selected this PV's sampling configuration.
    pub policy_name: Option<String>,
}

/// SQLite-backed PV metadata registry.
pub struct PvRegistry {
    conn: Mutex<Connection>,
}

impl PvRegistry {
    fn lock_conn(&self) -> anyhow::Result<std::sync::MutexGuard<'_, Connection>> {
        self.conn
            .lock()
            .map_err(|e| anyhow::anyhow!("PV registry lock poisoned: {e}"))
    }

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
        let conn = self.lock_conn()?;
        // Step 1: create the table (idempotent) and indexes that don't depend
        // on columns added by later migrations.
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
                egu             TEXT,
                alias_for       TEXT,
                archive_fields  TEXT,
                policy_name     TEXT
            );

            CREATE INDEX IF NOT EXISTS idx_pv_status ON pv_info(status);
            CREATE INDEX IF NOT EXISTS idx_pv_prefix ON pv_info(pv_name COLLATE NOCASE);
            ",
        )?;
        // Step 2: migrations for tables created with older schemas. Each statement
        // runs independently because SQLite stops on the first duplicate-column
        // error when batched.
        for stmt in [
            "ALTER TABLE pv_info ADD COLUMN prec TEXT",
            "ALTER TABLE pv_info ADD COLUMN egu TEXT",
            "ALTER TABLE pv_info ADD COLUMN alias_for TEXT",
            "ALTER TABLE pv_info ADD COLUMN archive_fields TEXT",
            "ALTER TABLE pv_info ADD COLUMN policy_name TEXT",
        ] {
            let _ = conn.execute(stmt, []);
        }
        // Step 3: indexes that reference newly-added columns. Done after ALTER
        // so an upgraded database has the columns to index.
        conn.execute_batch(
            "CREATE INDEX IF NOT EXISTS idx_pv_alias \
             ON pv_info(alias_for) WHERE alias_for IS NOT NULL;",
        )?;
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
        let conn = self.lock_conn()?;
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
        let conn = self.lock_conn()?;
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
        let conn = self.lock_conn()?;
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
        let conn = self.lock_conn()?;
        let rows = conn.execute("DELETE FROM pv_info WHERE pv_name = ?1", params![pv_name])?;
        Ok(rows > 0)
    }

    /// Get a single PV record.
    pub fn get_pv(&self, pv_name: &str) -> anyhow::Result<Option<PvRecord>> {
        let conn = self.lock_conn()?;
        conn.query_row(
            "SELECT pv_name, dbr_type, sample_mode, sample_period, status, element_count,
                    last_timestamp, created_at, updated_at, prec, egu,
                    alias_for, archive_fields, policy_name
             FROM pv_info WHERE pv_name = ?1",
            params![pv_name],
            row_to_record,
        )
        .optional()
        .map_err(Into::into)
    }

    /// List all real PV names (alias rows excluded). Use
    /// [`Self::expanded_pv_names`] to include aliases.
    pub fn all_pv_names(&self) -> anyhow::Result<Vec<String>> {
        let conn = self.lock_conn()?;
        let mut stmt = conn.prepare(
            "SELECT pv_name FROM pv_info WHERE alias_for IS NULL ORDER BY pv_name",
        )?;
        let names = stmt
            .query_map([], |row| row.get(0))?
            .collect::<Result<Vec<String>, _>>()?;
        Ok(names)
    }

    /// List all real PVs with a given status. Alias rows have
    /// `status='alias'` and are excluded from every other-status query.
    pub fn pvs_by_status(&self, status: PvStatus) -> anyhow::Result<Vec<PvRecord>> {
        let conn = self.lock_conn()?;
        let mut stmt = conn.prepare(
            "SELECT pv_name, dbr_type, sample_mode, sample_period, status, element_count,
                    last_timestamp, created_at, updated_at, prec, egu,
                    alias_for, archive_fields, policy_name
             FROM pv_info WHERE status = ?1 ORDER BY pv_name",
        )?;
        let records = stmt
            .query_map(params![status.as_str()], row_to_record)?
            .collect::<Result<Vec<_>, _>>()?;
        Ok(records)
    }

    /// Match real PV names by glob pattern (SQL GLOB). Excludes aliases.
    pub fn matching_pvs(&self, pattern: &str) -> anyhow::Result<Vec<String>> {
        let conn = self.lock_conn()?;
        let mut stmt = conn.prepare(
            "SELECT pv_name FROM pv_info
             WHERE pv_name GLOB ?1 AND alias_for IS NULL
             ORDER BY pv_name",
        )?;
        let names = stmt
            .query_map(params![pattern], |row| row.get(0))?
            .collect::<Result<Vec<String>, _>>()?;
        Ok(names)
    }

    /// Count real PVs, optionally filtered by status. Excludes aliases.
    pub fn count(&self, status: Option<PvStatus>) -> anyhow::Result<u64> {
        let conn = self.lock_conn()?;
        let count: u64 = match status {
            Some(s) => conn.query_row(
                "SELECT COUNT(*) FROM pv_info
                 WHERE status = ?1 AND alias_for IS NULL",
                params![s.as_str()],
                |row| row.get(0),
            )?,
            None => conn.query_row(
                "SELECT COUNT(*) FROM pv_info WHERE alias_for IS NULL",
                [],
                |row| row.get(0),
            )?,
        };
        Ok(count)
    }

    /// Batch update last timestamps (for periodic flush).
    pub fn batch_update_timestamps(
        &self,
        updates: &[(&str, SystemTime)],
    ) -> anyhow::Result<()> {
        let mut conn = self.lock_conn()?;
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

    /// Get real PVs added since a given time (aliases excluded).
    pub fn recently_added_pvs(&self, since: SystemTime) -> anyhow::Result<Vec<PvRecord>> {
        let conn = self.lock_conn()?;
        let since_str = DateTime::<Utc>::from(since).to_rfc3339();
        let mut stmt = conn.prepare(
            "SELECT pv_name, dbr_type, sample_mode, sample_period, status, element_count,
                    last_timestamp, created_at, updated_at, prec, egu,
                    alias_for, archive_fields, policy_name
             FROM pv_info WHERE created_at >= ?1 AND alias_for IS NULL
             ORDER BY created_at DESC",
        )?;
        let records = stmt
            .query_map(params![since_str], row_to_record)?
            .collect::<Result<Vec<_>, _>>()?;
        Ok(records)
    }

    /// Get real PVs modified since a given time (aliases excluded).
    pub fn recently_modified_pvs(&self, since: SystemTime) -> anyhow::Result<Vec<PvRecord>> {
        let conn = self.lock_conn()?;
        let since_str = DateTime::<Utc>::from(since).to_rfc3339();
        let mut stmt = conn.prepare(
            "SELECT pv_name, dbr_type, sample_mode, sample_period, status, element_count,
                    last_timestamp, created_at, updated_at, prec, egu,
                    alias_for, archive_fields, policy_name
             FROM pv_info WHERE updated_at >= ?1 AND alias_for IS NULL
             ORDER BY updated_at DESC",
        )?;
        let records = stmt
            .query_map(params![since_str], row_to_record)?
            .collect::<Result<Vec<_>, _>>()?;
        Ok(records)
    }

    /// Update the sample mode and period for a PV.
    pub fn update_sample_mode(&self, pv_name: &str, mode: &SampleMode) -> anyhow::Result<bool> {
        let conn = self.lock_conn()?;
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
        let conn = self.lock_conn()?;
        let now = Utc::now().to_rfc3339();
        let rows = conn.execute(
            "UPDATE pv_info SET prec = COALESCE(?1, prec), egu = COALESCE(?2, egu), updated_at = ?3 WHERE pv_name = ?4",
            params![prec, egu, now, pv_name],
        )?;
        Ok(rows > 0)
    }

    /// Import a PV with all fields in a single SQL operation.
    /// Used during config import to atomically set status, created_at, and metadata.
    #[allow(clippy::too_many_arguments)]
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
        alias_for: Option<&str>,
        archive_fields: &[String],
        policy_name: Option<&str>,
    ) -> anyhow::Result<()> {
        let conn = self.lock_conn()?;
        let now = Utc::now().to_rfc3339();
        let (mode_str, period) = sample_mode.to_db();
        let created = created_at.unwrap_or(&now);
        let archive_fields_json = if archive_fields.is_empty() {
            None
        } else {
            Some(serde_json::to_string(archive_fields)?)
        };

        conn.execute(
            "INSERT OR REPLACE INTO pv_info
             (pv_name, dbr_type, sample_mode, sample_period, status, element_count,
              created_at, updated_at, prec, egu, alias_for, archive_fields, policy_name)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13)",
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
                alias_for,
                archive_fields_json,
                policy_name,
            ],
        )?;
        Ok(())
    }

    /// Set or clear the archive_fields list for a PV.
    pub fn update_archive_fields(
        &self,
        pv_name: &str,
        fields: &[String],
    ) -> anyhow::Result<bool> {
        let conn = self.lock_conn()?;
        let now = Utc::now().to_rfc3339();
        let json = if fields.is_empty() {
            None
        } else {
            Some(serde_json::to_string(fields)?)
        };
        let rows = conn.execute(
            "UPDATE pv_info SET archive_fields = ?1, updated_at = ?2 WHERE pv_name = ?3",
            params![json, now, pv_name],
        )?;
        Ok(rows > 0)
    }

    /// Set or clear the policy_name on a PV.
    pub fn update_policy_name(
        &self,
        pv_name: &str,
        policy_name: Option<&str>,
    ) -> anyhow::Result<bool> {
        let conn = self.lock_conn()?;
        let now = Utc::now().to_rfc3339();
        let rows = conn.execute(
            "UPDATE pv_info SET policy_name = ?1, updated_at = ?2 WHERE pv_name = ?3",
            params![policy_name, now, pv_name],
        )?;
        Ok(rows > 0)
    }

    /// Add an alias `alias` that points at the existing PV `target`.
    /// Fails if target does not exist or if `alias` already maps elsewhere.
    /// The alias row mirrors target's dbr_type/sample_mode/element_count for
    /// display, but `alias_for` distinguishes it from real PVs.
    pub fn add_alias(&self, alias: &str, target: &str) -> anyhow::Result<()> {
        if alias == target {
            anyhow::bail!("alias and target must differ");
        }
        let conn = self.lock_conn()?;
        // Resolve target (must be a real PV, not itself an alias).
        let row: Option<(i32, String, f64, i32, Option<String>)> = conn
            .query_row(
                "SELECT dbr_type, sample_mode, sample_period, element_count, alias_for
                 FROM pv_info WHERE pv_name = ?1",
                params![target],
                |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?, r.get(3)?, r.get(4)?)),
            )
            .optional()?;
        let (dbr_type, mode, period, ec, target_alias) = row
            .ok_or_else(|| anyhow::anyhow!("target PV '{target}' not found"))?;
        if target_alias.is_some() {
            anyhow::bail!("target PV '{target}' is itself an alias; aliases of aliases are not allowed");
        }
        // Reject if `alias` already exists either as a real PV or different alias.
        let existing: Option<Option<String>> = conn
            .query_row(
                "SELECT alias_for FROM pv_info WHERE pv_name = ?1",
                params![alias],
                |r| r.get(0),
            )
            .optional()?;
        if let Some(existing_alias) = existing {
            if existing_alias.as_deref() == Some(target) {
                return Ok(()); // idempotent
            }
            anyhow::bail!("'{alias}' already exists in registry");
        }
        let now = Utc::now().to_rfc3339();
        conn.execute(
            "INSERT INTO pv_info
             (pv_name, dbr_type, sample_mode, sample_period, status, element_count,
              created_at, updated_at, alias_for)
             VALUES (?1, ?2, ?3, ?4, 'alias', ?5, ?6, ?6, ?7)",
            params![alias, dbr_type, mode, period, ec, now, target],
        )?;
        Ok(())
    }

    /// Remove an alias row. Returns true if removed, false if the row was not
    /// an alias or did not exist. Real PVs are not removed by this method.
    pub fn remove_alias(&self, alias: &str) -> anyhow::Result<bool> {
        let conn = self.lock_conn()?;
        let rows = conn.execute(
            "DELETE FROM pv_info WHERE pv_name = ?1 AND alias_for IS NOT NULL",
            params![alias],
        )?;
        Ok(rows > 0)
    }

    /// If `name` is an alias, return its target. Returns None if `name` is
    /// already a real PV or does not exist.
    pub fn resolve_alias(&self, name: &str) -> anyhow::Result<Option<String>> {
        let conn = self.lock_conn()?;
        let row: Option<Option<String>> = conn
            .query_row(
                "SELECT alias_for FROM pv_info WHERE pv_name = ?1",
                params![name],
                |r| r.get(0),
            )
            .optional()?;
        Ok(row.flatten())
    }

    /// Return the canonical PV name. If `name` is an alias, returns the target;
    /// otherwise returns the input unchanged. Used by lookup/retrieval paths.
    pub fn canonical_name(&self, name: &str) -> anyhow::Result<String> {
        Ok(self.resolve_alias(name)?.unwrap_or_else(|| name.to_string()))
    }

    /// List all alias names pointing at a given target PV.
    pub fn aliases_for(&self, target: &str) -> anyhow::Result<Vec<String>> {
        let conn = self.lock_conn()?;
        let mut stmt = conn.prepare(
            "SELECT pv_name FROM pv_info WHERE alias_for = ?1 ORDER BY pv_name",
        )?;
        let names = stmt
            .query_map(params![target], |row| row.get(0))?
            .collect::<Result<Vec<String>, _>>()?;
        Ok(names)
    }

    /// All `(alias, target)` pairs in the registry.
    pub fn all_aliases(&self) -> anyhow::Result<Vec<(String, String)>> {
        let conn = self.lock_conn()?;
        let mut stmt = conn.prepare(
            "SELECT pv_name, alias_for FROM pv_info
             WHERE alias_for IS NOT NULL ORDER BY pv_name",
        )?;
        let rows = stmt
            .query_map([], |row| Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?)))?
            .collect::<Result<Vec<_>, _>>()?;
        Ok(rows)
    }

    /// All PV names including aliases (`getAllExpandedPVNames`).
    pub fn expanded_pv_names(&self) -> anyhow::Result<Vec<String>> {
        let conn = self.lock_conn()?;
        let mut stmt = conn.prepare("SELECT pv_name FROM pv_info ORDER BY pv_name")?;
        let names = stmt
            .query_map([], |row| row.get(0))?
            .collect::<Result<Vec<String>, _>>()?;
        Ok(names)
    }

    /// Get all PV records (for export).
    pub fn all_records(&self) -> anyhow::Result<Vec<PvRecord>> {
        let conn = self.lock_conn()?;
        let mut stmt = conn.prepare(
            "SELECT pv_name, dbr_type, sample_mode, sample_period, status, element_count,
                    last_timestamp, created_at, updated_at, prec, egu,
                    alias_for, archive_fields, policy_name
             FROM pv_info ORDER BY pv_name",
        )?;
        let records = stmt
            .query_map([], row_to_record)?
            .collect::<Result<Vec<_>, _>>()?;
        Ok(records)
    }

    /// Get real PVs that have not received events for longer than the
    /// threshold duration. Only returns PVs that have a last_timestamp.
    /// Aliases are excluded — they don't carry event timestamps.
    pub fn silent_pvs(&self, threshold: Duration) -> anyhow::Result<Vec<PvRecord>> {
        let conn = self.lock_conn()?;
        let cutoff = SystemTime::now()
            .checked_sub(threshold)
            .unwrap_or(SystemTime::UNIX_EPOCH);
        let cutoff_str = DateTime::<Utc>::from(cutoff).to_rfc3339();
        let mut stmt = conn.prepare(
            "SELECT pv_name, dbr_type, sample_mode, sample_period, status, element_count,
                    last_timestamp, created_at, updated_at, prec, egu,
                    alias_for, archive_fields, policy_name
             FROM pv_info WHERE last_timestamp IS NOT NULL AND last_timestamp < ?1
                            AND alias_for IS NULL
             ORDER BY last_timestamp ASC",
        )?;
        let records = stmt
            .query_map(params![cutoff_str], row_to_record)?
            .collect::<Result<Vec<_>, _>>()?;
        Ok(records)
    }
}

fn row_to_record(row: &rusqlite::Row) -> rusqlite::Result<PvRecord> {
    let pv_name: String = row.get(0)?;
    let dbr_type_i: i32 = row.get(1)?;
    let sample_mode_str: String = row.get(2)?;
    let sample_period: f64 = row.get(3)?;
    let status_str: String = row.get(4)?;
    let element_count: i32 = row.get(5)?;
    let last_ts_str: Option<String> = row.get(6)?;
    let created_str: String = row.get(7)?;
    let updated_str: String = row.get(8)?;
    let prec: Option<String> = row.get(9).unwrap_or(None);
    let egu: Option<String> = row.get(10).unwrap_or(None);
    let alias_for: Option<String> = row.get(11).unwrap_or(None);
    let archive_fields_json: Option<String> = row.get(12).unwrap_or(None);
    let policy_name: Option<String> = row.get(13).unwrap_or(None);

    let last_timestamp = last_ts_str.and_then(|s| {
        DateTime::parse_from_rfc3339(&s)
            .ok()
            .map(|dt| dt.with_timezone(&Utc).into())
    });

    let archive_fields = archive_fields_json
        .as_deref()
        .and_then(|s| serde_json::from_str::<Vec<String>>(s).ok())
        .unwrap_or_default();

    Ok(PvRecord {
        pv_name,
        dbr_type: ArchDbType::from_i32(dbr_type_i).unwrap_or(ArchDbType::ScalarDouble),
        sample_mode: SampleMode::from_db(&sample_mode_str, sample_period),
        status: status_str.parse().unwrap_or(PvStatus::Active),
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
        alias_for,
        archive_fields,
        policy_name,
    })
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
    fn test_archive_fields_roundtrip() {
        let reg = PvRegistry::in_memory().unwrap();
        reg.register_pv("PV:Fields", ArchDbType::ScalarDouble, &SampleMode::Monitor, 1)
            .unwrap();

        // Default is empty.
        let r = reg.get_pv("PV:Fields").unwrap().unwrap();
        assert!(r.archive_fields.is_empty());

        // Set and read back.
        let fields = vec!["HIHI".to_string(), "LOLO".to_string(), "EGU".to_string()];
        assert!(reg.update_archive_fields("PV:Fields", &fields).unwrap());
        let r = reg.get_pv("PV:Fields").unwrap().unwrap();
        assert_eq!(r.archive_fields, fields);

        // Clearing with [] removes the JSON entry.
        assert!(reg.update_archive_fields("PV:Fields", &[]).unwrap());
        let r = reg.get_pv("PV:Fields").unwrap().unwrap();
        assert!(r.archive_fields.is_empty());
    }

    #[test]
    fn test_policy_name_roundtrip() {
        let reg = PvRegistry::in_memory().unwrap();
        reg.register_pv("PV:Pol", ArchDbType::ScalarDouble, &SampleMode::Monitor, 1)
            .unwrap();

        let r = reg.get_pv("PV:Pol").unwrap().unwrap();
        assert!(r.policy_name.is_none());

        assert!(reg.update_policy_name("PV:Pol", Some("fast")).unwrap());
        let r = reg.get_pv("PV:Pol").unwrap().unwrap();
        assert_eq!(r.policy_name.as_deref(), Some("fast"));

        assert!(reg.update_policy_name("PV:Pol", None).unwrap());
        let r = reg.get_pv("PV:Pol").unwrap().unwrap();
        assert!(r.policy_name.is_none());
    }

    #[test]
    fn test_import_pv_with_alias_and_fields() {
        let reg = PvRegistry::in_memory().unwrap();
        let fields = vec!["HIHI".to_string(), "LOLO".to_string()];
        reg.import_pv(
            "PV:Aliased",
            ArchDbType::ScalarDouble,
            &SampleMode::Monitor,
            1,
            PvStatus::Active,
            None,
            Some("3"),
            Some("mA"),
            Some("PV:Real"),
            &fields,
            Some("ring"),
        )
        .unwrap();

        let r = reg.get_pv("PV:Aliased").unwrap().unwrap();
        assert_eq!(r.alias_for.as_deref(), Some("PV:Real"));
        assert_eq!(r.archive_fields, fields);
        assert_eq!(r.policy_name.as_deref(), Some("ring"));
        assert_eq!(r.prec.as_deref(), Some("3"));
        assert_eq!(r.egu.as_deref(), Some("mA"));
    }

    #[test]
    fn test_migration_from_old_schema() {
        // Build a connection with the v0.1.4 schema (no alias/archive_fields/policy)
        // to verify ALTER TABLE migrations succeed and old rows decode cleanly.
        let conn = Connection::open_in_memory().unwrap();
        conn.execute_batch(
            "CREATE TABLE pv_info (
                pv_name        TEXT PRIMARY KEY NOT NULL,
                dbr_type       INTEGER NOT NULL,
                sample_mode    TEXT NOT NULL DEFAULT 'monitor',
                sample_period  REAL NOT NULL DEFAULT 0.0,
                status         TEXT NOT NULL DEFAULT 'active',
                element_count  INTEGER NOT NULL DEFAULT 1,
                last_timestamp TEXT,
                created_at     TEXT NOT NULL,
                updated_at     TEXT NOT NULL,
                prec           TEXT,
                egu            TEXT
            );",
        )
        .unwrap();
        let now = Utc::now().to_rfc3339();
        conn.execute(
            "INSERT INTO pv_info
             (pv_name, dbr_type, sample_mode, sample_period, status, element_count,
              created_at, updated_at, prec, egu)
             VALUES (?1, ?2, 'monitor', 0.0, 'active', 1, ?3, ?3, NULL, NULL)",
            params!["PV:Legacy", ArchDbType::ScalarDouble as i32, now],
        )
        .unwrap();

        let reg = PvRegistry {
            conn: Mutex::new(conn),
        };
        // Re-running init_schema should add the new columns without dropping data.
        reg.init_schema().unwrap();

        let r = reg.get_pv("PV:Legacy").unwrap().unwrap();
        assert_eq!(r.pv_name, "PV:Legacy");
        assert!(r.alias_for.is_none());
        assert!(r.archive_fields.is_empty());
        assert!(r.policy_name.is_none());

        // New writes work too.
        assert!(reg
            .update_archive_fields("PV:Legacy", &["HIHI".to_string()])
            .unwrap());
        let r = reg.get_pv("PV:Legacy").unwrap().unwrap();
        assert_eq!(r.archive_fields, vec!["HIHI".to_string()]);
    }

    #[test]
    fn test_aliases_basic() {
        let reg = PvRegistry::in_memory().unwrap();
        reg.register_pv("RING:Current", ArchDbType::ScalarDouble, &SampleMode::Monitor, 1)
            .unwrap();

        // Add an alias and verify resolution.
        reg.add_alias("DEV:Current", "RING:Current").unwrap();
        assert_eq!(
            reg.resolve_alias("DEV:Current").unwrap().as_deref(),
            Some("RING:Current"),
        );
        assert!(reg.resolve_alias("RING:Current").unwrap().is_none()); // real PV
        assert!(reg.resolve_alias("Nonexistent").unwrap().is_none());

        assert_eq!(reg.canonical_name("DEV:Current").unwrap(), "RING:Current");
        assert_eq!(reg.canonical_name("RING:Current").unwrap(), "RING:Current");

        // Aliases for / all aliases.
        assert_eq!(
            reg.aliases_for("RING:Current").unwrap(),
            vec!["DEV:Current".to_string()],
        );
        assert_eq!(
            reg.all_aliases().unwrap(),
            vec![("DEV:Current".to_string(), "RING:Current".to_string())],
        );

        // Expanded names contains both real and alias.
        let expanded = reg.expanded_pv_names().unwrap();
        assert!(expanded.contains(&"RING:Current".to_string()));
        assert!(expanded.contains(&"DEV:Current".to_string()));

        // Idempotent re-add.
        reg.add_alias("DEV:Current", "RING:Current").unwrap();
        assert_eq!(reg.aliases_for("RING:Current").unwrap().len(), 1);

        // Remove alias.
        assert!(reg.remove_alias("DEV:Current").unwrap());
        assert!(reg.resolve_alias("DEV:Current").unwrap().is_none());
        assert!(!reg.remove_alias("DEV:Current").unwrap()); // already gone
    }

    #[test]
    fn test_alias_conflicts() {
        let reg = PvRegistry::in_memory().unwrap();
        reg.register_pv("PV:A", ArchDbType::ScalarDouble, &SampleMode::Monitor, 1).unwrap();
        reg.register_pv("PV:B", ArchDbType::ScalarDouble, &SampleMode::Monitor, 1).unwrap();

        // Cannot alias to nonexistent target.
        assert!(reg.add_alias("Alias:X", "Nonexistent").is_err());

        // Cannot self-alias.
        assert!(reg.add_alias("PV:A", "PV:A").is_err());

        // Alias name conflicts with existing real PV.
        assert!(reg.add_alias("PV:B", "PV:A").is_err());

        // Alias of alias not allowed.
        reg.add_alias("Alias:A", "PV:A").unwrap();
        assert!(reg.add_alias("Alias:Two", "Alias:A").is_err());

        // remove_alias does NOT delete real PVs.
        assert!(!reg.remove_alias("PV:A").unwrap());
        assert!(reg.get_pv("PV:A").unwrap().is_some());
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
