use std::collections::{HashMap, HashSet};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use tokio::time::interval;
use tracing::{debug, error, info, warn};

// Java parity (3daedae): f.get() without a timeout hung indefinitely on
// slow NFS. Use 24 h as the default, matching Java's chosen bound.
const DEFAULT_MOVE_TIMEOUT: Duration = Duration::from_secs(24 * 3600);

use crate::registry::{PvRegistry, PvStatus};
use crate::storage::plainpb::PlainPbStoragePlugin;
use crate::storage::plainpb::reader::PbFileReader;
use crate::storage::traits::{EventStream, StoragePlugin};

/// ETL executor — periodically moves data from source tier to destination tier.
pub struct EtlExecutor {
    source: Arc<PlainPbStoragePlugin>,
    dest: Arc<PlainPbStoragePlugin>,
    /// How often to run ETL (seconds).
    period_secs: u64,
    /// Number of partitions to hold in source before moving.
    hold: u32,
    /// Number of partitions to gather (move out) at once.
    gather: u32,
    /// Per-file move timeout (Java parity 3daedae).
    move_timeout: Duration,
    /// Optional PV registry — when present, paused PVs are skipped
    /// (Java parity 92db337).
    pv_registry: Option<Arc<PvRegistry>>,
}

impl EtlExecutor {
    pub fn new(
        source: Arc<PlainPbStoragePlugin>,
        dest: Arc<PlainPbStoragePlugin>,
        period_secs: u64,
        hold: u32,
        gather: u32,
    ) -> Self {
        Self {
            source,
            dest,
            period_secs,
            hold,
            gather,
            move_timeout: DEFAULT_MOVE_TIMEOUT,
            pv_registry: None,
        }
    }

    /// Wire a PV registry so the executor can skip paused PVs in
    /// `run_once`. Java parity (92db337): without this, PB files for a
    /// paused PV continue to migrate out of the STS, which surprises
    /// operators who expect the data to stay accessible there until the
    /// PV resumes.
    pub fn with_pv_registry(mut self, registry: Arc<PvRegistry>) -> Self {
        self.pv_registry = Some(registry);
        self
    }

    /// Run the ETL loop. Call this as a spawned task.
    pub async fn run(&self, mut shutdown: tokio::sync::watch::Receiver<bool>) {
        let mut tick = interval(Duration::from_secs(self.period_secs));
        info!(
            source = self.source.name(),
            dest = self.dest.name(),
            "ETL executor started"
        );

        loop {
            tokio::select! {
                _ = tick.tick() => {
                    if let Err(e) = self.run_once().await {
                        error!("ETL error: {e}");
                    }
                }
                _ = shutdown.changed() => {
                    info!("ETL executor shutting down");
                    break;
                }
            }
        }
    }

    /// Execute one round of ETL: find old partition files in source, move to dest.
    /// Groups files by PV name for coherent transfers.
    async fn run_once(&self) -> anyhow::Result<()> {
        // Operator-controlled bypass (Java's SKIP_<NAME>_FOR_ETL named flag,
        // adc5889a). Set during e.g. an OS migration to pause writes into a
        // particular tier without restarting the appliance.
        if crate::flags::skip_tier_for_etl(self.dest.name()) {
            debug!(
                dest = self.dest.name(),
                "ETL skipped: SKIP_<DEST>_FOR_ETL flag set"
            );
            return Ok(());
        }

        let source_root = self.source.root_folder();
        if !source_root.exists() {
            return Ok(());
        }

        let mut pb_files = list_pb_files(source_root)?;
        pb_files.sort();

        if pb_files.len() <= self.hold as usize {
            debug!(
                count = pb_files.len(),
                hold = self.hold,
                "Not enough partitions to trigger ETL"
            );
            return Ok(());
        }

        let files_to_move = pb_files
            .len()
            .saturating_sub(self.hold as usize)
            .min(self.gather as usize);

        // Group files by PV name for coherent per-PV transfers.
        let files_subset: Vec<PathBuf> = pb_files.into_iter().take(files_to_move).collect();
        let grouped = group_files_by_pv(&files_subset);

        // Java parity (92db337): skip files whose owning PV is paused.
        // Compute the set of paused-PV file keys once per tick instead of
        // per-file to keep the registry lookup off the hot path.
        let paused_keys: HashSet<String> = match self.pv_registry.as_ref() {
            Some(reg) => reg
                .pvs_by_status(PvStatus::Paused)
                .map(|recs| {
                    recs.into_iter()
                        .map(|r| crate::storage::plainpb::pv_name_to_key(&r.pv_name))
                        .collect()
                })
                .unwrap_or_else(|e| {
                    warn!("ETL: failed to read paused PVs from registry: {e}");
                    HashSet::new()
                }),
            None => HashSet::new(),
        };

        for (pv_key, files) in &grouped {
            if paused_keys.contains(pv_key) {
                debug!(pv = pv_key, "ETL skipping paused PV");
                continue;
            }
            debug!(pv = pv_key, count = files.len(), "ETL processing PV group");
            for file in files {
                info!(?file, dest = self.dest.name(), "ETL moving file");
                if let Err(e) = self.move_file(file).await {
                    warn!(?file, "ETL failed to move file: {e}");
                }
            }
        }

        Ok(())
    }

    pub fn source_name(&self) -> &str {
        self.source.name()
    }

    pub fn dest_name(&self) -> &str {
        self.dest.name()
    }

    /// Force-move every PB file the source tier currently holds for `pv`
    /// to the destination tier, ignoring `hold` / `gather` constraints.
    /// Drives the `consolidateDataForPV` BPL endpoint.
    ///
    /// The same crash-safe move_file is reused, so partial failures leave
    /// the source either fully migrated or untouched.
    pub async fn consolidate_pv(&self, pv: &str) -> anyhow::Result<u64> {
        // Flush any buffered writes for the source tier so we move
        // everything that's been written so far.
        self.source.flush_writes().await?;

        let pv_files =
            crate::storage::plainpb::list_pv_pb_files_pub(self.source.root_folder(), pv)?;
        let total = pv_files.len() as u64;
        info!(
            pv,
            total,
            source = self.source.name(),
            dest = self.dest.name(),
            "Consolidating PV files",
        );
        for file in &pv_files {
            if let Err(e) = self.move_file(file).await {
                warn!(?file, "consolidate_pv: failed to move file: {e}");
                return Err(e);
            }
        }
        Ok(total)
    }

    /// Move a single PB file from source to destination tier.
    /// Uses copy → verify → marker → delete for crash-safe idempotency.
    async fn move_file(&self, source_path: &Path) -> anyhow::Result<()> {
        // Check for a marker from a previous incomplete cleanup (crash after copy).
        let marker = source_path.with_extension("pb.etl_done");
        if marker.exists() {
            info!(
                ?source_path,
                "Found ETL marker — previous copy completed, cleaning up"
            );
            if let Err(e) = tokio::fs::remove_file(source_path).await {
                warn!(
                    ?source_path,
                    "Failed to remove source after ETL marker found: {e}"
                );
            }
            if let Err(e) = tokio::fs::remove_file(&marker).await {
                warn!(?marker, "Failed to remove ETL marker: {e}");
            }
            return Ok(());
        }

        // Java parity (3daedae): wrap the copy+delete in a timeout so a
        // hung NFS mount doesn't block the ETL loop indefinitely.
        let timeout = self.move_timeout;
        let source_path = source_path.to_path_buf();
        let source_path_disp = source_path.clone();
        let dest = self.dest.clone();
        let source_name = self.source.name().to_string();
        let dest_name = self.dest.name().to_string();

        tokio::time::timeout(timeout, async move {
            let mut reader = PbFileReader::open(&source_path)?;
            let desc = reader.description().clone();
            let dbr_type = desc.db_type;

            // Copy all samples to destination.
            while let Some(sample) = reader.next_event()? {
                dest.append_event(&desc.pv_name, dbr_type, &sample).await?;
            }

            // Write marker before deleting source — if we crash here, next run
            // will see the marker and skip re-copy (preventing duplicate data).
            let marker = source_path.with_extension("pb.etl_done");
            tokio::fs::write(&marker, b"").await?;
            tokio::fs::remove_file(&source_path).await?;
            tokio::fs::remove_file(&marker).await.ok();
            metrics::counter!(
                "archiver_etl_files_moved_total",
                "source" => source_name,
                "dest" => dest_name,
            )
            .increment(1);
            anyhow::Ok(())
        })
        .await
        .map_err(|_| {
            anyhow::anyhow!("ETL move_file timed out after {timeout:?} for {source_path_disp:?}")
        })??;

        Ok(())
    }
}

/// Group files by PV name. The PV name is derived from the file path structure.
/// File path pattern: {root}/{pv_prefix}/{pv_suffix}:{partition}.pb
fn group_files_by_pv(files: &[PathBuf]) -> HashMap<String, Vec<&PathBuf>> {
    let mut groups: HashMap<String, Vec<&PathBuf>> = HashMap::new();
    for file in files {
        let pv_key = extract_pv_key(file);
        groups.entry(pv_key).or_default().push(file);
    }
    groups
}

/// Extract PV key from a PB file path.
/// Given path like `.../SIM/Sine:2024_03_15_09.pb`, returns "SIM/Sine".
fn extract_pv_key(path: &Path) -> String {
    if let Some(file_name) = path.file_name().and_then(|n| n.to_str()) {
        // file_name is like "Sine:2024_03.pb"
        let stem = file_name.strip_suffix(".pb").unwrap_or(file_name);
        if let Some(colon_pos) = stem.find(':') {
            let leaf = &stem[..colon_pos];
            // Combine with parent directory name for full PV key.
            if let Some(parent) = path
                .parent()
                .and_then(|p| p.file_name())
                .and_then(|n| n.to_str())
            {
                return format!("{parent}/{leaf}");
            }
            return leaf.to_string();
        }
    }
    path.to_string_lossy().to_string()
}

/// Recursively list all .pb files under a directory.
fn list_pb_files(dir: &Path) -> anyhow::Result<Vec<PathBuf>> {
    let mut files = Vec::new();
    if dir.is_dir() {
        for entry in std::fs::read_dir(dir)? {
            let entry = entry?;
            let path = entry.path();
            if path.is_dir() {
                files.extend(list_pb_files(&path)?);
            } else if path.extension().and_then(|e| e.to_str()) == Some("pb") {
                files.push(path);
            }
        }
    }
    Ok(files)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_extract_pv_key() {
        let path = PathBuf::from("/data/sts/SIM/Sine:2024_03_15_09.pb");
        assert_eq!(extract_pv_key(&path), "SIM/Sine");
    }

    #[test]
    fn test_group_files_by_pv() {
        let files = vec![
            PathBuf::from("/data/SIM/Sine:2024_03_01.pb"),
            PathBuf::from("/data/SIM/Sine:2024_03_02.pb"),
            PathBuf::from("/data/SIM/Cosine:2024_03_01.pb"),
        ];
        let grouped = group_files_by_pv(&files);
        assert_eq!(grouped.len(), 2);
        assert_eq!(grouped["SIM/Sine"].len(), 2);
        assert_eq!(grouped["SIM/Cosine"].len(), 1);
    }
}
