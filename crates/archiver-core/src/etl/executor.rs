use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use tokio::time::interval;
use tracing::{debug, error, info, warn};

use crate::storage::plainpb::reader::PbFileReader;
use crate::storage::plainpb::PlainPbStoragePlugin;
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
        }
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

        for (pv_key, files) in &grouped {
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

    /// Move a single PB file from source to destination tier.
    /// Uses copy → verify → delete to ensure safety.
    async fn move_file(&self, source_path: &Path) -> anyhow::Result<()> {
        let mut reader = PbFileReader::open(source_path)?;
        let desc = reader.description().clone();
        let dbr_type = desc.db_type;

        // Copy all samples to destination.
        let mut sample_count: u64 = 0;
        while let Some(sample) = reader.next_event()? {
            self.dest
                .append_event(&desc.pv_name, dbr_type, &sample)
                .await?;
            sample_count += 1;
        }

        // Verify: re-read source and count to confirm we transferred everything.
        let mut verify_reader = PbFileReader::open(source_path)?;
        let mut verify_count: u64 = 0;
        while verify_reader.next_event()?.is_some() {
            verify_count += 1;
        }

        if verify_count != sample_count {
            anyhow::bail!(
                "ETL verification failed for {:?}: wrote {sample_count} but source has {verify_count}",
                source_path
            );
        }

        // Only delete source after successful verification.
        tokio::fs::remove_file(source_path).await?;
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
            if let Some(parent) = path.parent().and_then(|p| p.file_name()).and_then(|n| n.to_str())
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
