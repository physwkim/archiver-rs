pub mod codec;
pub mod reader;
pub mod search;
pub mod writer;

use std::path::{Path, PathBuf};
use std::time::SystemTime;

use async_trait::async_trait;
use tracing::debug;

use crate::storage::partition::PartitionGranularity;
use crate::storage::traits::{AppendMeta, EventStream, StoragePlugin};
use crate::types::{ArchDbType, ArchiverSample};

use self::reader::PbFileReader;
use self::writer::PbFileWriter;

/// PlainPB storage plugin — binary-compatible with Java EPICS Archiver Appliance.
pub struct PlainPbStoragePlugin {
    plugin_name: String,
    root_folder: PathBuf,
    granularity: PartitionGranularity,
}

impl PlainPbStoragePlugin {
    pub fn new(name: &str, root_folder: PathBuf, granularity: PartitionGranularity) -> Self {
        Self {
            plugin_name: name.to_string(),
            root_folder,
            granularity,
        }
    }

    /// Build the file path for a PV at a given timestamp.
    /// Format: {root}/{pv_key}:{partition_name}.pb
    /// where pv_key replaces `:` with `/` in the PV name.
    pub fn file_path_for(&self, pv: &str, ts: SystemTime) -> PathBuf {
        let pv_key = pv_name_to_key(pv);
        let partition_name =
            crate::storage::partition::partition_name(ts, self.granularity);
        let filename = format!("{pv_key}:{partition_name}.pb");
        self.root_folder.join(filename)
    }

    /// List all PB files for a PV in a time range.
    fn list_files_for_range(
        &self,
        pv: &str,
        start: SystemTime,
        end: SystemTime,
    ) -> Vec<PathBuf> {
        let partitions =
            crate::storage::partition::partitions_in_range(start, end, self.granularity);
        let pv_key = pv_name_to_key(pv);
        partitions
            .into_iter()
            .map(|pname| {
                let filename = format!("{pv_key}:{pname}.pb");
                self.root_folder.join(filename)
            })
            .filter(|p| p.exists())
            .collect()
    }

    pub fn root_folder(&self) -> &Path {
        &self.root_folder
    }
}

/// Convert PV name to file path key.
/// `SIM:Sine` → `SIM/Sine`
fn pv_name_to_key(pv: &str) -> String {
    pv.replace(':', "/")
}

#[async_trait]
impl StoragePlugin for PlainPbStoragePlugin {
    fn name(&self) -> &str {
        &self.plugin_name
    }

    fn partition_granularity(&self) -> PartitionGranularity {
        self.granularity
    }

    async fn append_event(
        &self,
        pv: &str,
        dbr_type: ArchDbType,
        sample: &ArchiverSample,
    ) -> anyhow::Result<()> {
        let path = self.file_path_for(pv, sample.timestamp);
        debug!(?path, pv, "appending event");

        // Ensure parent directory exists.
        if let Some(parent) = path.parent() {
            tokio::fs::create_dir_all(parent).await?;
        }

        let writer = PbFileWriter::new(&path);
        writer.append_sample(pv, dbr_type, sample).await
    }

    async fn append_event_with_meta(
        &self,
        pv: &str,
        dbr_type: ArchDbType,
        sample: &ArchiverSample,
        meta: &AppendMeta,
    ) -> anyhow::Result<()> {
        let path = self.file_path_for(pv, sample.timestamp);
        debug!(?path, pv, "appending event with metadata");

        if let Some(parent) = path.parent() {
            tokio::fs::create_dir_all(parent).await?;
        }

        let writer = PbFileWriter::new(&path);
        writer
            .append_sample_with_meta(pv, dbr_type, sample, meta.element_count, &meta.headers)
            .await
    }

    async fn get_data(
        &self,
        pv: &str,
        start: SystemTime,
        end: SystemTime,
    ) -> anyhow::Result<Vec<Box<dyn EventStream>>> {
        let files = self.list_files_for_range(pv, start, end);
        let mut streams: Vec<Box<dyn EventStream>> = Vec::new();
        for file in files {
            let reader = PbFileReader::open(&file)?;
            streams.push(Box::new(reader));
        }
        Ok(streams)
    }

    async fn get_last_known_event(
        &self,
        pv: &str,
    ) -> anyhow::Result<Option<ArchiverSample>> {
        // Find all existing PB files for this PV and read the last sample
        // from the most recent file.
        let pv_key = pv_name_to_key(pv);
        let pv_dir = self.root_folder.join(
            pv_key.rsplit_once('/').map(|(dir, _)| dir).unwrap_or(""),
        );

        if !pv_dir.exists() {
            return Ok(None);
        }

        let pv_file_prefix = pv_key
            .rsplit_once('/')
            .map(|(_, name)| name)
            .unwrap_or(&pv_key);

        let mut pb_files: Vec<std::path::PathBuf> = std::fs::read_dir(&pv_dir)?
            .filter_map(|e| e.ok())
            .map(|e| e.path())
            .filter(|p| {
                p.extension().and_then(|e| e.to_str()) == Some("pb")
                    && p.file_name()
                        .and_then(|n| n.to_str())
                        .map(|n| {
                            n.starts_with(pv_file_prefix)
                                && n[pv_file_prefix.len()..].starts_with(':')
                        })
                        .unwrap_or(false)
            })
            .collect();

        pb_files.sort();

        // Read from the last (most recent) file.
        for path in pb_files.into_iter().rev() {
            let mut reader = PbFileReader::open(&path)?;
            let mut last = None;
            while let Some(sample) = reader.next_event()? {
                last = Some(sample);
            }
            if last.is_some() {
                return Ok(last);
            }
        }
        Ok(None)
    }

    async fn delete_pv_data(&self, pv: &str) -> anyhow::Result<u64> {
        let pv_key = pv_name_to_key(pv);
        let pv_dir = self.root_folder.join(
            pv_key.rsplit_once('/').map(|(dir, _)| dir).unwrap_or(""),
        );

        if !pv_dir.exists() {
            return Ok(0);
        }

        let pv_file_prefix = pv_key
            .rsplit_once('/')
            .map(|(_, name)| name)
            .unwrap_or(&pv_key);

        let mut deleted = 0u64;
        let entries: Vec<_> = std::fs::read_dir(&pv_dir)?
            .filter_map(|e| e.ok())
            .map(|e| e.path())
            .filter(|p| {
                p.extension().and_then(|e| e.to_str()) == Some("pb")
                    && p.file_name()
                        .and_then(|n| n.to_str())
                        .map(|n| {
                            n.starts_with(pv_file_prefix)
                                && n[pv_file_prefix.len()..].starts_with(':')
                        })
                        .unwrap_or(false)
            })
            .collect();

        for path in entries {
            tokio::fs::remove_file(&path).await?;
            deleted += 1;
        }

        // Clean up empty directory.
        if pv_dir.exists() {
            let is_empty = std::fs::read_dir(&pv_dir)?.next().is_none();
            if is_empty {
                let _ = tokio::fs::remove_dir(&pv_dir).await;
            }
        }

        debug!(pv, deleted, "Deleted PV data files");
        Ok(deleted)
    }
}
