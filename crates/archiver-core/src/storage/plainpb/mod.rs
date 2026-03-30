pub mod codec;
pub mod reader;
pub mod search;
pub mod writer;

use std::collections::{HashMap, HashSet};
use std::io::{BufRead, BufWriter, Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::sync::Mutex;
use std::time::SystemTime;

use async_trait::async_trait;
use prost::Message;
use tracing::debug;

use crate::storage::partition::PartitionGranularity;
use crate::storage::traits::{AppendMeta, EventStream, StoragePlugin};
use crate::types::{ArchDbType, ArchiverSample};

use self::reader::PbFileReader;

/// Cached file handle for writing.
struct CachedWriter {
    writer: BufWriter<std::fs::File>,
}

/// PlainPB storage plugin — binary-compatible with Java EPICS Archiver Appliance.
pub struct PlainPbStoragePlugin {
    plugin_name: String,
    root_folder: PathBuf,
    granularity: PartitionGranularity,
    /// Cached BufWriter handles, keyed by file path.
    /// Avoids open/flush/close per sample.
    write_cache: Mutex<HashMap<PathBuf, CachedWriter>>,
    /// Directories known to exist. Avoids redundant create_dir_all syscalls.
    known_dirs: Mutex<HashSet<PathBuf>>,
}

impl PlainPbStoragePlugin {
    pub fn new(name: &str, root_folder: PathBuf, granularity: PartitionGranularity) -> Self {
        Self {
            plugin_name: name.to_string(),
            root_folder,
            granularity,
            write_cache: Mutex::new(HashMap::new()),
            known_dirs: Mutex::new(HashSet::new()),
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

    /// Ensure a parent directory exists, using a cached set to skip repeated syscalls.
    fn ensure_parent_dir(&self, path: &Path) -> anyhow::Result<()> {
        if let Some(parent) = path.parent() {
            let needs_create = {
                let dirs = self.known_dirs.lock()
                    .map_err(|e| anyhow::anyhow!("dir cache poisoned: {e}"))?;
                !dirs.contains(parent)
            };
            if needs_create {
                std::fs::create_dir_all(parent)?;
                let mut dirs = self.known_dirs.lock()
                    .map_err(|e| anyhow::anyhow!("dir cache poisoned: {e}"))?;
                dirs.insert(parent.to_path_buf());
            }
        }
        Ok(())
    }

    /// Write a sample using the cached BufWriter, creating the file + header if needed.
    fn write_cached(
        &self,
        path: &Path,
        pv: &str,
        dbr_type: ArchDbType,
        sample: &ArchiverSample,
        meta: &AppendMeta,
    ) -> anyhow::Result<()> {
        let sample_bytes = writer::encode_sample(dbr_type, sample)?;
        let escaped_sample = codec::escape(&sample_bytes);

        let mut cache = self.write_cache.lock()
            .map_err(|e| anyhow::anyhow!("write cache poisoned: {e}"))?;

        let path_buf = path.to_path_buf();
        if !cache.contains_key(&path_buf) {
            let file_exists = path.exists();
            let file = std::fs::OpenOptions::new()
                .create(true)
                .append(true)
                .open(path)?;
            let mut bw = BufWriter::with_capacity(64 * 1024, file);

            if !file_exists {
                let (year, _, _) = sample.decompose_timestamp();
                let header = writer::build_payload_info(
                    pv, dbr_type, year, meta.element_count, &meta.headers,
                );
                let header_bytes = header.encode_to_vec();
                let escaped_header = codec::escape(&header_bytes);
                bw.write_all(&escaped_header)?;
                bw.write_all(&[codec::NEWLINE])?;
            }

            cache.insert(path_buf.clone(), CachedWriter { writer: bw });
        }

        let cached = cache.get_mut(&path_buf).expect("just inserted");
        cached.writer.write_all(&escaped_sample)?;
        cached.writer.write_all(&[codec::NEWLINE])?;
        Ok(())
    }
}

/// Convert PV name to file path key.
/// `SIM:Sine` → `SIM/Sine`
fn pv_name_to_key(pv: &str) -> String {
    pv.replace(':', "/")
}

/// Read the last sample from a PB file by seeking near the end.
/// Falls back to full sequential read for edge cases (e.g., very large single sample).
fn read_last_sample_from_file(path: &Path) -> anyhow::Result<Option<ArchiverSample>> {
    let file = std::fs::File::open(path)?;
    let file_len = file.metadata()?.len();
    if file_len == 0 {
        return Ok(None);
    }

    let mut rdr = std::io::BufReader::new(file);

    // Read header to get year and dbr_type.
    let mut header_line = Vec::new();
    rdr.read_until(codec::NEWLINE, &mut header_line)?;
    if header_line.last() == Some(&codec::NEWLINE) {
        header_line.pop();
    }
    let header_bytes = codec::unescape(&header_line);
    let payload_info = archiver_proto::epics_event::PayloadInfo::decode(header_bytes.as_slice())?;
    let year = payload_info.year;
    let dbr_type = ArchDbType::from_i32(payload_info.r#type)
        .unwrap_or(ArchDbType::ScalarDouble);

    let header_end = rdr.stream_position()?;
    if header_end >= file_len {
        return Ok(None);
    }

    // Read the last 64KB (or less) to find the final sample line.
    let data_len = file_len - header_end;
    let chunk_size = (64 * 1024u64).min(data_len);
    let seek_pos = file_len - chunk_size;
    rdr.seek(SeekFrom::Start(seek_pos))?;

    let mut tail = Vec::with_capacity(chunk_size as usize);
    rdr.read_to_end(&mut tail)?;

    // Trim trailing newline.
    if tail.last() == Some(&codec::NEWLINE) {
        tail.pop();
    }

    if tail.is_empty() {
        return Ok(None);
    }

    // Find the last complete line (after the last newline byte in the chunk).
    let last_line_data = if let Some(pos) = tail.iter().rposition(|&b| b == codec::NEWLINE) {
        &tail[pos + 1..]
    } else if seek_pos <= header_end {
        // Entire data section is in the chunk — this IS the (only) line.
        &tail
    } else {
        // Very large single line that exceeds 64KB — fall back to sequential read.
        let mut reader = PbFileReader::open(path)?;
        let mut last = None;
        while let Some(sample) = reader.next_event()? {
            last = Some(sample);
        }
        return Ok(last);
    };

    if last_line_data.is_empty() {
        return Ok(None);
    }

    let raw = codec::unescape(last_line_data);
    Ok(Some(reader::decode_sample(dbr_type, year, &raw)?))
}

/// Build PV file prefix info for matching files in a directory.
fn pv_file_parts(pv: &str) -> (PathBuf, String) {
    let pv_key = pv_name_to_key(pv);
    let dir_part = pv_key.rsplit_once('/').map(|(dir, _)| dir).unwrap_or("");
    let file_prefix = pv_key
        .rsplit_once('/')
        .map(|(_, name)| name)
        .unwrap_or(&pv_key)
        .to_string();
    (PathBuf::from(dir_part), file_prefix)
}

/// List PB files for a PV in a directory, matching the PV file prefix.
fn list_pv_pb_files(root: &Path, pv: &str) -> anyhow::Result<Vec<PathBuf>> {
    let (dir_part, file_prefix) = pv_file_parts(pv);
    let pv_dir = root.join(&dir_part);

    if !pv_dir.exists() {
        return Ok(Vec::new());
    }

    let mut files: Vec<PathBuf> = std::fs::read_dir(&pv_dir)?
        .filter_map(|e| e.ok())
        .map(|e| e.path())
        .filter(|p| {
            p.extension().and_then(|e| e.to_str()) == Some("pb")
                && p.file_name()
                    .and_then(|n| n.to_str())
                    .is_some_and(|n| {
                        n.starts_with(&file_prefix)
                            && n[file_prefix.len()..].starts_with(':')
                    })
        })
        .collect();

    files.sort();
    Ok(files)
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
        let meta = AppendMeta::default();
        self.append_event_with_meta(pv, dbr_type, sample, &meta).await
    }

    async fn append_event_with_meta(
        &self,
        pv: &str,
        dbr_type: ArchDbType,
        sample: &ArchiverSample,
        meta: &AppendMeta,
    ) -> anyhow::Result<()> {
        let path = self.file_path_for(pv, sample.timestamp);
        debug!(?path, pv, "appending event");

        self.ensure_parent_dir(&path)?;
        self.write_cached(&path, pv, dbr_type, sample, meta)
    }

    async fn get_data(
        &self,
        pv: &str,
        start: SystemTime,
        end: SystemTime,
    ) -> anyhow::Result<Vec<Box<dyn EventStream>>> {
        // Flush cached writes so readers see the latest data.
        self.flush_writes().await?;

        let files = self.list_files_for_range(pv, start, end);
        let mut streams: Vec<Box<dyn EventStream>> = Vec::new();
        for file in files {
            let reader = PbFileReader::open_seeked(&file, start)?;
            streams.push(Box::new(reader));
        }
        Ok(streams)
    }

    async fn get_last_known_event(
        &self,
        pv: &str,
    ) -> anyhow::Result<Option<ArchiverSample>> {
        // Flush cached writes so readers can see the latest data.
        self.flush_writes().await?;

        let pb_files = list_pv_pb_files(&self.root_folder, pv)?;

        // Read from the last (most recent) file, using optimized tail read.
        for path in pb_files.into_iter().rev() {
            if let Some(sample) = read_last_sample_from_file(&path)? {
                return Ok(Some(sample));
            }
        }
        Ok(None)
    }

    async fn delete_pv_data(&self, pv: &str) -> anyhow::Result<u64> {
        // Evict any cached writers for this PV before deleting files.
        {
            let pv_key = pv_name_to_key(pv);
            let mut cache = self.write_cache.lock()
                .map_err(|e| anyhow::anyhow!("write cache poisoned: {e}"))?;
            cache.retain(|path, cached| {
                let dominated = path.to_string_lossy().contains(&pv_key);
                if dominated {
                    let _ = cached.writer.flush();
                }
                !dominated
            });
        }

        let entries = list_pv_pb_files(&self.root_folder, pv)?;
        let mut deleted = 0u64;
        for path in entries {
            tokio::fs::remove_file(&path).await?;
            deleted += 1;
        }

        // Clean up empty directory.
        let (dir_part, _) = pv_file_parts(pv);
        let pv_dir = self.root_folder.join(&dir_part);
        if pv_dir.exists() {
            let is_empty = std::fs::read_dir(&pv_dir)?.next().is_none();
            if is_empty {
                let _ = tokio::fs::remove_dir(&pv_dir).await;
            }
        }

        debug!(pv, deleted, "Deleted PV data files");
        Ok(deleted)
    }

    async fn flush_writes(&self) -> anyhow::Result<()> {
        let mut cache = self.write_cache.lock()
            .map_err(|e| anyhow::anyhow!("write cache poisoned: {e}"))?;
        let mut to_remove = Vec::new();
        for (path, cached) in cache.iter_mut() {
            if let Err(e) = cached.writer.flush() {
                tracing::warn!(?path, "Failed to flush cached writer: {e}");
                to_remove.push(path.clone());
            }
        }
        for path in to_remove {
            cache.remove(&path);
        }
        Ok(())
    }
}
