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
use crate::storage::traits::{AppendMeta, EventStream, StoragePlugin, StoreSummary};
use crate::types::{ArchDbType, ArchiverSample};

use self::reader::PbFileReader;

/// Cached file handle for writing the current partition of a PV.
struct CachedWriter {
    path: PathBuf,
    writer: BufWriter<std::fs::File>,
}

/// PlainPB storage plugin — binary-compatible with Java EPICS Archiver Appliance.
pub struct PlainPbStoragePlugin {
    plugin_name: String,
    root_folder: PathBuf,
    granularity: PartitionGranularity,
    /// One `BufWriter` per PV, pointing at that PV's current partition file.
    /// When the partition rolls over (hour/day/year boundary), the old writer
    /// is flushed and dropped so we never accumulate stale file handles.
    write_cache: Mutex<HashMap<String, CachedWriter>>,
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

        // If the cached writer points at a different path, the partition has
        // rolled over — flush and drop the old writer before opening the new
        // one so we don't leak file handles.
        if let Some(existing) = cache.get_mut(pv)
            && existing.path != path_buf
        {
            if let Err(e) = existing.writer.flush() {
                tracing::warn!(
                    pv,
                    old_path = ?existing.path,
                    "Failed to flush writer on partition rollover: {e}"
                );
            }
            cache.remove(pv);
        }

        if !cache.contains_key(pv) {
            // Java parity (651c3a6b): treat a zero-byte file the same as
            // a missing one. A crash mid-create would otherwise leave us
            // appending samples to a file with no PayloadInfo header,
            // producing a permanently undecodable file.
            let needs_header = !path.exists()
                || std::fs::metadata(path).map(|m| m.len() == 0).unwrap_or(false);
            let file = std::fs::OpenOptions::new()
                .create(true)
                .append(true)
                .open(path)?;
            let mut bw = BufWriter::with_capacity(64 * 1024, file);

            if needs_header {
                let (year, _, _) = sample.decompose_timestamp();
                let header = writer::build_payload_info(
                    pv, dbr_type, year, meta.element_count, &meta.headers,
                );
                let header_bytes = header.encode_to_vec();
                let escaped_header = codec::escape(&header_bytes);
                bw.write_all(&escaped_header)?;
                bw.write_all(&[codec::NEWLINE])?;
            }

            cache.insert(
                pv.to_string(),
                CachedWriter { path: path_buf, writer: bw },
            );
        }

        let cached = cache.get_mut(pv).expect("just inserted");
        cached.writer.write_all(&escaped_sample)?;
        cached.writer.write_all(&[codec::NEWLINE])?;
        Ok(())
    }
}

/// Convert PV name to file path key.
/// `SIM:Sine` → `SIM/Sine`
///
/// Defensive: an attacker-supplied PV name like `../../etc/passwd` would
/// otherwise pass straight through and let `Path::join` escape the
/// storage root. Registry-side validation already rejects these at
/// register_pv / import_pv / add_alias time, but we re-validate here so
/// any code path that bypasses the registry (e.g. retrieval of a PV
/// name read directly from a PB file's PayloadInfo) still fails closed.
/// Returns a sanitized fallback rather than panicking so retrieval
/// errors stay diagnosable.
fn pv_name_to_key(pv: &str) -> String {
    if !crate::registry::is_valid_pv_name(pv) {
        // Strip every disallowed character so path joins stay anchored
        // at the storage root. Use a marker prefix so an audit can spot
        // these: we never write to such paths in normal operation.
        let mut sanitized = String::with_capacity(pv.len() + 16);
        sanitized.push_str("__invalid__/");
        for c in pv.chars() {
            if c.is_ascii_alphanumeric() || c == '_' || c == '-' {
                sanitized.push(c);
            } else {
                sanitized.push('_');
            }
        }
        tracing::warn!(pv, "PV name rejected by validator; sanitized to {sanitized}");
        return sanitized;
    }
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
    if let Ok(sample) = reader::decode_sample(dbr_type, year, &raw) {
        return Ok(Some(sample));
    }

    // Java parity (20ec1a02): a crash mid-write leaves a torn last line.
    // Walk forward from the start tracking the last good sample so the
    // tail-corruption case still surfaces a usable answer instead of an
    // I/O-style error to the caller. Bounded by the file size (we'll
    // stop at end-of-stream); the cost only matters for the rare
    // corrupt-tail case.
    tracing::warn!(
        ?path,
        "PB tail decode failed; falling back to forward scan for last good sample"
    );
    let mut reader = PbFileReader::open(path)?;
    let mut last = None;
    while let Ok(Some(sample)) = reader.next_event() {
        last = Some(sample);
    }
    Ok(last)
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
/// Crate-public re-export of [`list_pv_pb_files`] — used by the ETL
/// executor to consolidate one PV's files without duplicating the
/// directory-walking logic.
pub fn list_pv_pb_files_pub(root: &Path, pv: &str) -> anyhow::Result<Vec<PathBuf>> {
    list_pv_pb_files(root, pv)
}

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

    async fn get_last_event_before(
        &self,
        pv: &str,
        target: SystemTime,
    ) -> anyhow::Result<Option<ArchiverSample>> {
        self.flush_writes().await?;

        let pb_files = list_pv_pb_files(&self.root_folder, pv)?;

        // Walk newest-to-oldest; first file whose final sample is before
        // `target` provides the answer (its last sample IS the answer).
        // For files whose final sample is at-or-after target, scan from
        // the start to find the last sample with ts < target.
        for path in pb_files.into_iter().rev() {
            let Some(last) = read_last_sample_from_file(&path)? else {
                continue;
            };
            if last.timestamp < target {
                return Ok(Some(last));
            }
            // Final sample is past target — scan the file forward and
            // track the latest sample with ts < target.
            let mut reader = PbFileReader::open(&path)?;
            let mut last_before: Option<ArchiverSample> = None;
            while let Some(sample) = reader.next_event()? {
                if sample.timestamp >= target {
                    break;
                }
                last_before = Some(sample);
            }
            if last_before.is_some() {
                return Ok(last_before);
            }
            // Every sample in this file is at-or-after target; the answer,
            // if any, lives in an older file.
        }
        Ok(None)
    }

    async fn delete_pv_data(&self, pv: &str) -> anyhow::Result<u64> {
        // Evict the cached writer for this PV before deleting files.
        {
            let mut cache = self.write_cache.lock()
                .map_err(|e| anyhow::anyhow!("write cache poisoned: {e}"))?;
            if let Some(mut cached) = cache.remove(pv) {
                let _ = cached.writer.flush();
            }
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
        for (pv, cached) in cache.iter_mut() {
            if let Err(e) = cached.writer.flush() {
                tracing::warn!(pv, path = ?cached.path, "Failed to flush cached writer: {e}");
                to_remove.push(pv.clone());
            }
        }
        for pv in to_remove {
            cache.remove(&pv);
        }
        Ok(())
    }

    fn stores_for_pv(&self, pv: &str) -> anyhow::Result<Vec<StoreSummary>> {
        let files = list_pv_pb_files(&self.root_folder, pv).unwrap_or_default();
        let count = files.len() as u64;
        let bytes: u64 = files
            .iter()
            .filter_map(|p| std::fs::metadata(p).ok())
            .map(|m| m.len())
            .sum();
        Ok(vec![StoreSummary {
            name: self.plugin_name.clone(),
            root_folder: self.root_folder.clone(),
            granularity: self.granularity,
            pv_file_count: Some(count),
            pv_size_bytes: Some(bytes),
            total_size_bytes: None,
            total_files: None,
        }])
    }

    fn appliance_metrics(&self) -> anyhow::Result<Vec<StoreSummary>> {
        let (total_files, total_size) = total_pb_stats(&self.root_folder);
        Ok(vec![StoreSummary {
            name: self.plugin_name.clone(),
            root_folder: self.root_folder.clone(),
            granularity: self.granularity,
            pv_file_count: None,
            pv_size_bytes: None,
            total_size_bytes: Some(total_size),
            total_files: Some(total_files),
        }])
    }

    async fn rename_pv(&self, from: &str, to: &str) -> anyhow::Result<u64> {
        // Evict any cached writers for the source PV so they don't keep an
        // open handle on a soon-to-be-renamed file.
        {
            let mut cache = self.write_cache.lock()
                .map_err(|e| anyhow::anyhow!("write cache poisoned: {e}"))?;
            if let Some(mut cached) = cache.remove(from) {
                let _ = cached.writer.flush();
            }
            // Also evict the destination cache entry if any (defensive).
            if let Some(mut cached) = cache.remove(to) {
                let _ = cached.writer.flush();
            }
        }

        let from_files = list_pv_pb_files(&self.root_folder, from)?;
        if from_files.is_empty() {
            return Ok(0);
        }
        let from_key = pv_name_to_key(from);
        let from_leaf = from_key.rsplit('/').next().unwrap_or(&from_key).to_string();
        let to_key = pv_name_to_key(to);
        let to_leaf = to_key.rsplit('/').next().unwrap_or(&to_key).to_string();

        // Ensure destination parent directory exists so std::fs::rename can
        // place files across PV-name prefixes (e.g. SIM:Sine -> RING:Current
        // changes the parent dir from SIM/ to RING/).
        let (to_dir_part, _) = pv_file_parts(to);
        let to_dir = self.root_folder.join(&to_dir_part);
        if !to_dir.as_os_str().is_empty() && !to_dir.exists() {
            std::fs::create_dir_all(&to_dir)?;
        }

        let mut moved = 0u64;
        for src in &from_files {
            let file_name = src
                .file_name()
                .and_then(|n| n.to_str())
                .ok_or_else(|| anyhow::anyhow!("non-utf8 filename: {src:?}"))?;
            // file is "{from_leaf}:{partition}.pb" — replace the leaf prefix.
            let suffix = file_name
                .strip_prefix(&from_leaf)
                .and_then(|s| s.strip_prefix(':'))
                .ok_or_else(|| anyhow::anyhow!("filename {file_name} did not match expected PV leaf"))?;
            let new_name = format!("{to_leaf}:{suffix}");
            let dst = to_dir.join(new_name);
            std::fs::rename(src, &dst)?;
            moved += 1;
        }

        // Clean up empty source directory.
        let (from_dir_part, _) = pv_file_parts(from);
        let from_dir = self.root_folder.join(&from_dir_part);
        if !from_dir_part.as_os_str().is_empty()
            && from_dir.exists()
            && std::fs::read_dir(&from_dir)?.next().is_none()
        {
            let _ = std::fs::remove_dir(&from_dir);
        }

        Ok(moved)
    }
}

/// Sum sizes and counts of `.pb` files under `root` recursively. Errors are
/// logged and ignored so a single unreadable file doesn't poison the metric.
fn total_pb_stats(root: &Path) -> (u64, u64) {
    fn walk(p: &Path, files: &mut u64, bytes: &mut u64) {
        let entries = match std::fs::read_dir(p) {
            Ok(e) => e,
            Err(_) => return,
        };
        for entry in entries.flatten() {
            let path = entry.path();
            if path.is_dir() {
                walk(&path, files, bytes);
            } else if path.extension().and_then(|e| e.to_str()) == Some("pb") {
                *files += 1;
                if let Ok(meta) = entry.metadata() {
                    *bytes += meta.len();
                }
            }
        }
    }
    let mut files = 0u64;
    let mut bytes = 0u64;
    if root.exists() {
        walk(root, &mut files, &mut bytes);
    }
    (files, bytes)
}
