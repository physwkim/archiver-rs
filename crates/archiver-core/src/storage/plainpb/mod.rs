pub mod codec;
pub mod reader;
pub mod search;
pub mod writer;

use std::collections::{HashMap, HashSet};
use std::io::{BufRead, BufWriter, Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::SystemTime;

/// Default cap on simultaneously-open `BufWriter` file handles across
/// all PVs in one PlainPB tier. Sized to stay well clear of the
/// typical Linux process fd ulimit (1024) so the storage plugin can
/// run on an out-of-the-box host without `ulimit -n` tuning. Sites
/// with raised ulimits and tens of thousands of active PVs should
/// override via [`PlainPbStoragePlugin::with_max_open_writers`].
pub const DEFAULT_MAX_OPEN_WRITERS: usize = 512;

use async_trait::async_trait;
use prost::Message;
use tracing::debug;

use crate::storage::partition::PartitionGranularity;
use crate::storage::traits::{AppendMeta, EventStream, StoragePlugin, StoreSummary};
use crate::types::{ArchDbType, ArchiverSample};

use self::reader::PbFileReader;

/// RAII handle that decrements the plugin's `open_writers` counter
/// when the owning [`CachedWriter`] is dropped. Tying the decrement
/// to Drop guarantees the count stays in sync with reality even if
/// a future code path takes the writer without going through
/// `flush_dirty_writers` / `evict_lru_writer` — every drop site
/// already exercises this guard.
struct WriterFdGuard {
    counter: Arc<AtomicUsize>,
}

impl Drop for WriterFdGuard {
    fn drop(&mut self) {
        self.counter.fetch_sub(1, Ordering::Relaxed);
    }
}

/// Cached file handle for writing the current partition of a PV.
struct CachedWriter {
    path: PathBuf,
    writer: BufWriter<std::fs::File>,
    /// `true` between writes and the next successful flush. Lets
    /// `flush_writes` skip writers that have nothing pending so a
    /// reader-side `get_data` doesn't pay an O(N) syscall storm
    /// across every cached writer (Java parity is similar — the
    /// `dirty` bit short-circuits the iteration).
    dirty: bool,
    /// Last access timestamp — the LRU key used by the always-on
    /// fd-cap eviction path: when [`PlainPbStoragePlugin::open_writers`]
    /// reaches `max_open_writers`, the writer with the smallest
    /// `last_used` is evicted to make room for the next open.
    last_used: SystemTime,
    /// Decrements `open_writers` on drop. Field name starts with
    /// `_` because it's never read directly — it exists purely for
    /// its Drop side-effect.
    _fd_guard: WriterFdGuard,
}

/// Per-PV serialization slot. Holds the cached writer (if any) and
/// a tombstone bit so a concurrent `append` doesn't resurrect a PV
/// whose `delete_pv_data`/`rename_pv` is already in flight.
///
/// Each slot is wrapped in its own `Mutex` so I/O for one PV cannot
/// stall I/O for any other. The outer `write_cache` map is locked
/// only while inserting/looking up/removing slots — never while
/// holding a filesystem syscall.
struct PvWriterSlot {
    writer: Option<CachedWriter>,
    /// Set under the slot lock by `delete_pv_data` / `rename_pv`.
    /// Once true, every subsequent `append` for this PV bails with
    /// an error so the deleted PV doesn't reappear from a racing
    /// late writer. The slot stays in `write_cache` only until the
    /// caller that set the flag clears the entry from the map; new
    /// `append`s that look up the PV after the cache eviction get a
    /// fresh slot.
    dead: bool,
}

/// Aggregated outcome of one `flush_dirty_writers` pass — used to
/// give the read-side and the write-side flush surfaces different
/// semantics over the same underlying iteration.
struct FlushOutcome {
    /// PVs whose `flush()` syscall errored. Their cached writers
    /// have been evicted (buffered bytes discarded via `into_parts`)
    /// and their entries removed from the map. Surfaced to the
    /// write_loop so it drops their `last_event` from the registry
    /// commit batch.
    failed: Vec<String>,
    /// PVs whose slot was already locked (an `append` or another
    /// flush is in flight). Their dirty bytes remain buffered and
    /// will be picked up on the next flush cycle. Surfaced to the
    /// write_loop alongside `failed` so the registry doesn't claim
    /// `last_event` for samples whose bytes are still in BufWriter
    /// memory — under-commit, never over-commit.
    deferred: Vec<String>,
}

/// Wraps a `PbFileReader` and clamps emitted samples to `[start, end]`.
///
/// Java parity (e3b4471 + 88c7601): `binary_search_pb_file` returns
/// `None` when every sample in the file is older than `start`, leaving
/// the reader at the data section start. Without a lower-bound filter
/// the wrapper would leak the entire file's stale contents into the
/// retrieval merge. The upper bound covers files included by partition
/// name whose actual sample timestamps spill past `end`.
struct BoundedReader {
    inner: PbFileReader,
    start: SystemTime,
    end: SystemTime,
    done: bool,
}

impl BoundedReader {
    fn new(inner: PbFileReader, start: SystemTime, end: SystemTime) -> Self {
        Self {
            inner,
            start,
            end,
            done: false,
        }
    }
}

impl crate::storage::traits::EventStream for BoundedReader {
    fn description(&self) -> &crate::types::EventStreamDesc {
        self.inner.description()
    }

    fn next_event(&mut self) -> anyhow::Result<Option<crate::types::ArchiverSample>> {
        if self.done {
            return Ok(None);
        }
        loop {
            match self.inner.next_event()? {
                None => {
                    self.done = true;
                    return Ok(None);
                }
                // Below `start` or above `end`: drop and continue. PB
                // partition files are append-ordered but timestamps
                // within one file aren't strictly monotonic — clock
                // backsteps and late backfills exist — so a single
                // out-of-window sample must NOT terminate the stream
                // (Java's reader keeps consuming until EOF).
                Some(s) if s.timestamp < self.start => continue,
                Some(s) if s.timestamp > self.end => continue,
                Some(s) => return Ok(Some(s)),
            }
        }
    }
}

use crate::retrieval::query::SingleSampleStream;

/// PlainPB storage plugin — binary-compatible with Java EPICS Archiver Appliance.
pub struct PlainPbStoragePlugin {
    plugin_name: String,
    root_folder: PathBuf,
    granularity: PartitionGranularity,
    /// One per-PV slot, each holding (at most) one `BufWriter` pointed
    /// at that PV's current partition file. The outer `Mutex<HashMap>`
    /// is held only briefly to look up / insert / remove a slot; all
    /// I/O happens under the per-slot mutex so a stuck syscall on one
    /// PV does NOT block any other PV's appends or flushes.
    write_cache: Mutex<HashMap<String, Arc<Mutex<PvWriterSlot>>>>,
    /// Directories known to exist. Avoids redundant create_dir_all syscalls.
    known_dirs: Mutex<HashSet<PathBuf>>,
    /// Always-on cap on simultaneously-open `BufWriter` file
    /// handles across every PV. When `open_writers` reaches this
    /// cap, `write_cached` proactively evicts the LRU non-busy
    /// writer before opening a new one, so a site with 50k active
    /// PVs doesn't blow past the process fd ulimit. `usize::MAX`
    /// disables the cap entirely.
    max_open_writers: usize,
    /// Live count of [`CachedWriter`]s held in any slot. Maintained
    /// by [`WriterFdGuard`]: incremented on every fresh
    /// `CachedWriter` construction, decremented on drop. Compared
    /// against `max_open_writers` in `write_cached` to drive the
    /// always-on LRU eviction.
    open_writers: Arc<AtomicUsize>,
}

impl PlainPbStoragePlugin {
    pub fn new(name: &str, root_folder: PathBuf, granularity: PartitionGranularity) -> Self {
        Self::with_max_open_writers(name, root_folder, granularity, DEFAULT_MAX_OPEN_WRITERS)
    }

    /// Construct with an explicit cap on simultaneously-open writers.
    /// Pass `0` (or `usize::MAX`) to disable the cap; otherwise the
    /// LRU eviction kicks in once `open_writers` reaches the cap.
    /// Sites that have raised `ulimit -n` for tens of thousands of
    /// active PVs should pass that fd budget here so the writer
    /// cache doesn't thrash on EMFILE.
    pub fn with_max_open_writers(
        name: &str,
        root_folder: PathBuf,
        granularity: PartitionGranularity,
        max_open_writers: usize,
    ) -> Self {
        let cap = if max_open_writers == 0 {
            usize::MAX
        } else {
            max_open_writers
        };
        Self {
            plugin_name: name.to_string(),
            root_folder,
            granularity,
            write_cache: Mutex::new(HashMap::new()),
            known_dirs: Mutex::new(HashSet::new()),
            max_open_writers: cap,
            open_writers: Arc::new(AtomicUsize::new(0)),
        }
    }

    /// Snapshot of the current open-writer count. Exposed for
    /// observability in tests and for sites that want to surface a
    /// metric — production callers should NOT branch on this value
    /// (it's instantaneous and lock-free, so it can drift between
    /// the read and any subsequent action).
    pub fn open_writer_count(&self) -> usize {
        self.open_writers.load(Ordering::Relaxed)
    }

    /// Build the file path for a PV at a given timestamp.
    /// Format: {root}/{pv_key}:{partition_name}.pb
    /// where pv_key replaces `:` with `/` in the PV name.
    pub fn file_path_for(&self, pv: &str, ts: SystemTime) -> PathBuf {
        let pv_key = pv_name_to_key(pv);
        let partition_name = crate::storage::partition::partition_name(ts, self.granularity);
        let filename = format!("{pv_key}:{partition_name}.pb");
        self.root_folder.join(filename)
    }

    /// List all PB files for a PV in a time range.
    fn list_files_for_range(&self, pv: &str, start: SystemTime, end: SystemTime) -> Vec<PathBuf> {
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

    /// Flush every dirty cached writer that we can lock without
    /// blocking. Per-slot `try_lock` keeps a stuck PV (e.g. an NFS
    /// hang in an in-flight `append`) from blocking the flush of
    /// every other PV — that one PV is reported in `deferred` and
    /// retried on the next flush cycle.
    ///
    /// Errored flushes evict the cached writer (buffered bytes are
    /// dropped via `into_parts` so `BufWriter::drop` cannot re-issue
    /// the failing syscall) and are reported in `failed`.
    fn flush_dirty_writers(&self) -> FlushOutcome {
        // Snapshot slot Arcs under a brief outer lock so we never
        // hold the outer cache mutex while attempting an inner-slot
        // lock — that would deadlock with an `append` that holds
        // its slot lock and is waiting on the outer lock to record
        // a partition rollover.
        let snapshot: Vec<(String, Arc<Mutex<PvWriterSlot>>)> = {
            let cache = self.write_cache.lock().unwrap_or_else(|e| e.into_inner());
            cache.iter().map(|(k, v)| (k.clone(), v.clone())).collect()
        };

        let mut failed = Vec::new();
        let mut deferred = Vec::new();
        let mut to_remove = Vec::new();

        for (pv, slot_arc) in snapshot {
            let mut slot_guard = match slot_arc.try_lock() {
                Ok(g) => g,
                Err(std::sync::TryLockError::WouldBlock) => {
                    // Someone is mid-append (or another flush). Their
                    // bytes (if any) stay buffered; we'll retry next
                    // cycle. Caller must drop this PV from any pending
                    // ts_updates commit so the registry doesn't claim
                    // bytes that haven't reached disk yet.
                    deferred.push(pv);
                    continue;
                }
                Err(std::sync::TryLockError::Poisoned(p)) => p.into_inner(),
            };
            let Some(cached) = slot_guard.writer.as_mut() else {
                continue;
            };
            // Skip writers with nothing pending — every `get_data`
            // call lands here, and at scale (1000+ PVs) the avoided
            // syscall storm matters: only PVs that actually buffered
            // bytes since the last flush get touched.
            if !cached.dirty {
                continue;
            }
            match cached.writer.flush() {
                Ok(()) => cached.dirty = false,
                Err(e) => {
                    tracing::warn!(pv, path = ?cached.path, "Failed to flush cached writer: {e}");
                    metrics::counter!(
                        "archiver_pb_flush_failures_total",
                        "tier" => self.plugin_name.clone(),
                    )
                    .increment(1);
                    // `into_parts` drops buffered bytes WITHOUT letting
                    // BufWriter::drop attempt a final flush — that flush
                    // would re-issue the failing syscall, and the buffer
                    // is already suspect (the reason we got here).
                    if let Some(removed) = slot_guard.writer.take() {
                        let (_file, _buffered) = removed.writer.into_parts();
                    }
                    failed.push(pv.clone());
                    to_remove.push(pv);
                }
            }
        }

        if !to_remove.is_empty() {
            let mut cache = self.write_cache.lock().unwrap_or_else(|e| e.into_inner());
            for pv in &to_remove {
                cache.remove(pv);
            }
        }

        FlushOutcome { failed, deferred }
    }

    /// Drop any cached BufWriter whose target path matches `path`.
    /// Call this after `remove_file` on a `.pb` file the engine may have
    /// open — without it, subsequent `append_event` writes go to the
    /// deleted-but-still-open inode (a "ghost" file invisible to readers
    /// because `list_files_for_range` walks the directory). Safe no-op
    /// when nothing matches. Returns true if a writer was evicted.
    pub fn evict_writer_for_path(&self, path: &Path) -> bool {
        // Recover from poison rather than skip the eviction — the
        // contract is "after this returns, no future write goes
        // through the cached writer for `path`", and a poisoned
        // mutex shouldn't be a way to break that invariant.
        let candidates: Vec<(String, Arc<Mutex<PvWriterSlot>>)> = {
            let cache = self.write_cache.lock().unwrap_or_else(|e| {
                tracing::warn!(?path, "write cache poisoned at evict_writer_for_path: {e}");
                e.into_inner()
            });
            cache
                .iter()
                .filter(|(_, slot)| {
                    slot.try_lock()
                        .ok()
                        .and_then(|g| g.writer.as_ref().map(|cw| cw.path == path))
                        .unwrap_or(false)
                })
                .map(|(pv, slot)| (pv.clone(), slot.clone()))
                .collect()
        };
        let mut removed = false;
        for (pv, slot_arc) in candidates {
            let mut slot_guard = slot_arc.lock().unwrap_or_else(|e| e.into_inner());
            if let Some(mut cached) = slot_guard.writer.take() {
                let _ = cached.writer.flush();
                removed = true;
            }
            // Drop the slot from the outer map so a later append
            // creates a fresh slot and doesn't observe a stale
            // (now-empty) writer entry.
            drop(slot_guard);
            let mut cache = self.write_cache.lock().unwrap_or_else(|e| e.into_inner());
            cache.remove(&pv);
        }
        removed
    }

    /// Ensure a parent directory exists, using a cached set to skip repeated syscalls.
    fn ensure_parent_dir(&self, path: &Path) -> anyhow::Result<()> {
        if let Some(parent) = path.parent() {
            let needs_create = {
                // Recover from poison: known_dirs mutations are
                // single-statement HashSet inserts, so a panicking
                // thread can't leave half-modified state. Better to
                // proceed than fail every future write_cached call.
                let dirs = self
                    .known_dirs
                    .lock()
                    .unwrap_or_else(|e| e.into_inner());
                !dirs.contains(parent)
            };
            if needs_create {
                std::fs::create_dir_all(parent)?;
                let mut dirs = self
                    .known_dirs
                    .lock()
                    .unwrap_or_else(|e| e.into_inner());
                dirs.insert(parent.to_path_buf());
            }
        }
        Ok(())
    }

    /// Look up the slot Arc for `pv`, creating an empty one under a
    /// brief outer-cache lock if absent. The returned Arc lets the
    /// caller serialise on the per-PV mutex without holding the
    /// outer cache lock during file I/O.
    fn slot_for(&self, pv: &str) -> Arc<Mutex<PvWriterSlot>> {
        let mut cache = self.write_cache.lock().unwrap_or_else(|e| e.into_inner());
        cache
            .entry(pv.to_string())
            .or_insert_with(|| {
                Arc::new(Mutex::new(PvWriterSlot {
                    writer: None,
                    dead: false,
                }))
            })
            .clone()
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

        let path_buf = path.to_path_buf();
        let slot_arc = self.slot_for(pv);
        // Recover from poison: per-PV slot mutations are confined to
        // this method and `flush_dirty_writers` / `delete_pv_data` /
        // `rename_pv`, all of which leave internally-consistent state
        // on early return. Better to proceed than fail every future
        // append for a PV whose writer once panicked.
        let mut slot = slot_arc.lock().unwrap_or_else(|e| e.into_inner());

        // Tombstone check: a concurrent `delete_pv_data` /
        // `rename_pv` may have grabbed this Arc and set `dead`.
        // Bail rather than recreate the file we just deleted.
        if slot.dead {
            anyhow::bail!(
                "PV `{pv}` was deleted/renamed concurrently; refusing to recreate file"
            );
        }

        // If the cached writer points at a different path, the partition has
        // rolled over — flush and drop the old writer before opening the new
        // one so we don't leak file handles.
        if let Some(existing) = slot.writer.as_mut()
            && existing.path != path_buf
        {
            if let Err(e) = existing.writer.flush() {
                tracing::warn!(
                    pv,
                    old_path = ?existing.path,
                    "Failed to flush writer on partition rollover: {e}"
                );
            }
            slot.writer = None;
        }

        // Defense-in-depth for ghost-file writes: if the cached writer's
        // target path no longer exists on disk (deleted by ETL while we
        // missed the eviction, by manual `rm`, by a flaky NFS mount, …)
        // its bytes are going into an orphaned inode invisible to readers.
        // Drop the writer so the next branch reopens a fresh file.
        if let Some(existing) = slot.writer.as_ref()
            && !existing.path.exists()
        {
            tracing::warn!(
                pv,
                path = ?existing.path,
                "Cached writer's file disappeared from filesystem; reopening"
            );
            if let Some(mut cached) = slot.writer.take() {
                let _ = cached.writer.flush();
            }
        }

        if slot.writer.is_none() {
            let needs_header = file_needs_header(path);

            // Always-on fd cap: if we're already at the configured
            // ceiling, evict the LRU non-busy writer BEFORE asking
            // the kernel for another fd. Without this proactive
            // step, every active-PV count above the cap would either
            // (a) blow past the OS ulimit if the cap is below
            // ulimit, or (b) silently grow without bound. Loop so
            // bursty appends across many PVs converge to the cap
            // even when several callers race to evict.
            while self.open_writers.load(Ordering::Relaxed) >= self.max_open_writers {
                if !self.evict_lru_writer(pv) {
                    // No evictable candidate — every other slot is
                    // busy. Fall through and let the open syscall
                    // either succeed (we're below ulimit anyway) or
                    // fail with EMFILE for the recovery branch below.
                    break;
                }
            }

            // Open with EMFILE/ENFILE recovery: if the OS file-handle
            // table is exhausted (cap was set above ulimit, or other
            // processes consumed fds), evict our LRU cached writer
            // to free a descriptor and retry once. Without this,
            // every subsequent write fails until a writer is
            // naturally evicted (e.g. by partition rollover hours
            // later).
            let file = match std::fs::OpenOptions::new()
                .create(true)
                .append(true)
                .open(path)
            {
                Ok(f) => f,
                Err(e) if is_too_many_open_files(&e) && self.evict_lru_writer(pv) => {
                    tracing::warn!(
                        ?path,
                        "Hit OS file-handle limit; evicted LRU writer and \
                         retrying open"
                    );
                    std::fs::OpenOptions::new()
                        .create(true)
                        .append(true)
                        .open(path)?
                }
                Err(e) => return Err(e.into()),
            };
            let mut bw = BufWriter::with_capacity(64 * 1024, file);

            if needs_header {
                let (year, _, _) = sample.decompose_timestamp();
                let header = writer::build_payload_info(
                    pv,
                    dbr_type,
                    year,
                    meta.element_count,
                    &meta.headers,
                );
                let header_bytes = header.encode_to_vec();
                let escaped_header = codec::escape(&header_bytes);
                // Single write_all so the header+newline never split
                // across BufWriter flushes — same atomicity rationale
                // as the sample frame below.
                let mut header_frame =
                    Vec::with_capacity(escaped_header.len() + 1);
                header_frame.extend_from_slice(&escaped_header);
                header_frame.push(codec::NEWLINE);
                bw.write_all(&header_frame)?;
            }

            // Increment BEFORE constructing the CachedWriter so
            // the WriterFdGuard on the new writer balances against
            // this fetch_add via its Drop impl. Doing it here (not
            // inside a `CachedWriter::new`) keeps the count tied to
            // the single construction path.
            self.open_writers.fetch_add(1, Ordering::Relaxed);
            slot.writer = Some(CachedWriter {
                path: path_buf,
                writer: bw,
                // Header bytes (if any) are buffered but not yet
                // flushed. Mark dirty so the periodic flush picks
                // them up — without this, a PV that gets created
                // and then receives no further samples within a
                // flush_period would never persist its header.
                dirty: true,
                last_used: SystemTime::now(),
                _fd_guard: WriterFdGuard {
                    counter: self.open_writers.clone(),
                },
            });
        }

        let cached = slot.writer.as_mut().expect("just inserted");
        cached.last_used = SystemTime::now();
        // Atomic-at-buffer-layer sample frame: a single `write_all`
        // means the BufWriter never splits the sample/newline pair
        // across two internal flushes. OS-level write atomicity is
        // still bounded by the kernel page boundary, but this removes
        // our contribution to the partial-record risk.
        let mut frame = Vec::with_capacity(escaped_sample.len() + 1);
        frame.extend_from_slice(&escaped_sample);
        frame.push(codec::NEWLINE);
        if let Err(e) = cached.writer.write_all(&frame) {
            // After a partial write (ENOSPC, NFS hiccup, …) the
            // BufWriter's internal state is suspect — reusing it
            // would compound the corruption (tail garbage, repeated
            // failures). Evict so the next call goes through the
            // create+append+header path which validates the file
            // and reopens fresh. Tail-trim runs in `file_needs_header`
            // on that next open and removes any partial record.
            //
            // Use `into_parts` to discard buffered bytes WITHOUT
            // letting BufWriter::drop attempt a final flush; that
            // flush would re-issue the same failing syscall (worst
            // case: another partial write) and the buffered bytes
            // are already suspect.
            tracing::warn!(
                pv,
                path = ?cached.path,
                "Write failed; evicting cached writer to force \
                 reopen on next sample: {e}"
            );
            if let Some(removed) = slot.writer.take() {
                let (_file, _buffered) = removed.writer.into_parts();
                // _file dropped → fd closed without flush.
            }
            // Drop the slot from the outer map so the next append
            // gets a clean lookup (no stale empty slot lingering).
            drop(slot);
            let mut cache = self.write_cache.lock().unwrap_or_else(|e| e.into_inner());
            cache.remove(pv);
            return Err(e.into());
        }
        cached.dirty = true;
        Ok(())
    }

    /// Find the slot with the oldest `last_used` (excluding
    /// `current_pv`, which holds its own slot lock at the call site)
    /// and evict its writer. Slots whose mutex is currently held by
    /// another thread are skipped — `try_lock` returning `WouldBlock`
    /// means an in-flight append is already using that fd, so taking
    /// it would not free anything anyway.
    ///
    /// Returns `true` when something was evicted (caller should
    /// retry the failed open), `false` when no candidate could be
    /// freed (caller should fail through with the original ENFILE).
    fn evict_lru_writer(&self, current_pv: &str) -> bool {
        // Snapshot Arcs under the outer lock, then probe each with
        // try_lock so a stuck slot doesn't make EMFILE recovery
        // itself hang.
        let candidates: Vec<(String, Arc<Mutex<PvWriterSlot>>)> = {
            let cache = self.write_cache.lock().unwrap_or_else(|e| e.into_inner());
            cache
                .iter()
                .filter(|(pv, _)| pv.as_str() != current_pv)
                .map(|(pv, slot)| (pv.clone(), slot.clone()))
                .collect()
        };

        let mut oldest: Option<(String, Arc<Mutex<PvWriterSlot>>, SystemTime)> = None;
        for (pv, slot_arc) in candidates {
            let Ok(guard) = slot_arc.try_lock() else {
                continue;
            };
            let Some(cw) = guard.writer.as_ref() else {
                drop(guard);
                continue;
            };
            let last_used = cw.last_used;
            drop(guard);
            match &oldest {
                Some((_, _, ts)) if *ts <= last_used => {}
                _ => oldest = Some((pv, slot_arc, last_used)),
            }
        }

        let Some((pv, slot_arc, _)) = oldest else {
            return false;
        };
        let Ok(mut guard) = slot_arc.try_lock() else {
            // Lost the race — another thread grabbed the slot
            // between our scan and the eviction.
            return false;
        };
        let Some(mut cached) = guard.writer.take() else {
            return false;
        };
        let _ = cached.writer.flush();
        tracing::debug!(
            pv,
            path = ?cached.path,
            "LRU evicted to recover file descriptor"
        );
        drop(guard);
        // Remove the now-empty slot from the outer map.
        let mut cache = self.write_cache.lock().unwrap_or_else(|e| e.into_inner());
        cache.remove(&pv);
        true
    }
}

/// True iff `e` corresponds to a POSIX EMFILE (per-process fd limit)
/// or ENFILE (system-wide fd limit). Used by write_cached's open
/// retry to distinguish recoverable resource exhaustion from real
/// filesystem errors. Codes are POSIX-standard (Linux + macOS:
/// EMFILE=24, ENFILE=23).
fn is_too_many_open_files(e: &std::io::Error) -> bool {
    matches!(e.raw_os_error(), Some(23) | Some(24))
}


/// Decide whether a file at `path` needs a fresh PayloadInfo header
/// written before sample data is appended. Returns `true` when:
/// 1. the file doesn't exist,
/// 2. the file exists but is 0 bytes (Java parity 651c3a6b: a crash
///    mid-create would otherwise leave a header-less file), OR
/// 3. the file exists with bytes but `PbFileReader::open` cannot
///    parse the header — in that case we **truncate** the file so
///    the caller's `create+append` opens cleanly. Without this
///    third branch, a partial-header crash makes the file forever
///    unreadable AND every subsequent append silently piles garbage
///    onto a corrupt prefix.
fn file_needs_header(path: &Path) -> bool {
    if !path.exists() {
        return true;
    }
    let size = std::fs::metadata(path).map(|m| m.len()).unwrap_or(0);
    if size == 0 {
        return true;
    }
    if PbFileReader::open(path).is_err() {
        tracing::warn!(
            ?path,
            "PB file has unreadable header; truncating so a fresh \
             header gets written"
        );
        if let Err(e) = std::fs::OpenOptions::new()
            .write(true)
            .truncate(true)
            .open(path)
        {
            tracing::warn!(?path, "Failed to truncate corrupt PB file: {e}");
        }
        return true;
    }
    // Header is valid; defend against a tail with a partial sample
    // (writer killed mid-flush). Truncating to the last NEWLINE
    // boundary loses at most one record but keeps the file readable
    // — without this, a reader hits the partial record and stops
    // returning every sample after that point.
    if let Err(e) = trim_to_last_newline(path) {
        tracing::warn!(?path, "Failed to trim partial trailing record: {e}");
    }
    false
}

/// Truncate `path` to end at its last NEWLINE byte (inclusive). Used
/// to drop a partial sample frame at file tail after a crash.
fn trim_to_last_newline(path: &Path) -> std::io::Result<()> {
    use std::io::{Read, Seek, SeekFrom};
    let mut file = std::fs::OpenOptions::new()
        .read(true)
        .write(true)
        .open(path)?;
    let len = file.metadata()?.len();
    if len == 0 {
        return Ok(());
    }
    // If the very last byte is already a NEWLINE, nothing to trim.
    file.seek(SeekFrom::End(-1))?;
    let mut tail = [0u8; 1];
    file.read_exact(&mut tail)?;
    if tail[0] == codec::NEWLINE {
        return Ok(());
    }
    // Scan backwards in chunks for the last NEWLINE.
    const CHUNK: usize = 4096;
    let mut buf = vec![0u8; CHUNK];
    let mut window_end = len;
    while window_end > 0 {
        let read_len = (window_end as usize).min(CHUNK);
        let read_start = window_end - read_len as u64;
        file.seek(SeekFrom::Start(read_start))?;
        file.read_exact(&mut buf[..read_len])?;
        if let Some(idx) = buf[..read_len].iter().rposition(|&b| b == codec::NEWLINE) {
            let new_len = read_start + idx as u64 + 1;
            tracing::warn!(
                ?path,
                old_len = len,
                new_len,
                "Trimming partial trailing PB record"
            );
            file.set_len(new_len)?;
            return Ok(());
        }
        window_end = read_start;
    }
    // No NEWLINE anywhere — file is just one giant un-terminated
    // record (or single header line that didn't get its newline).
    // Leave as-is; truncation here would be more destructive than
    // the corruption we're trying to fix.
    Ok(())
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
pub(crate) fn pv_name_to_key(pv: &str) -> String {
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
        tracing::warn!(
            pv,
            "PV name rejected by validator; sanitized to {sanitized}"
        );
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
    let dbr_type = ArchDbType::from_i32(payload_info.r#type).unwrap_or(ArchDbType::ScalarDouble);

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
                && p.file_name().and_then(|n| n.to_str()).is_some_and(|n| {
                    n.starts_with(&file_prefix) && n[file_prefix.len()..].starts_with(':')
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
        self.append_event_with_meta(pv, dbr_type, sample, &meta)
            .await
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

        // Java parity (88c7601): single-file short-circuit. When the only
        // matching file's last sample is older than `start`, return that
        // single event in a tiny stream rather than opening a full reader
        // that the lower-bound filter would just discard. Equivalent to
        // Java's `CallableEventStream.makeOneEventCallable(...)` branch.
        // Java parity (88c7601): Java compares `lastEventEpochSeconds <= startTime`,
        // so a file whose final sample lands exactly on `start` still
        // short-circuits. `<` would force a full reader open at the
        // boundary and emit the same single sample after a wasted seek.
        if files.len() == 1
            && let Some(last) = read_last_sample_from_file(&files[0])?
            && last.timestamp <= start
        {
            let reader = PbFileReader::open(&files[0])?;
            let desc = reader.description().clone();
            return Ok(vec![Box::new(SingleSampleStream {
                desc,
                sample: Some(last),
            })]);
        }

        let mut streams: Vec<Box<dyn EventStream>> = Vec::new();
        for file in files {
            let reader = PbFileReader::open_seeked(&file, start)?;
            // Java parity (e3b4471 + 88c7601): clamp output at both
            // ends. Without the upper bound, files whose partition name
            // overlaps the query but whose late-arriving samples spill
            // past `end` leak stale tail data. Without the lower bound,
            // a binary-search miss leaves the reader at data-start and
            // emits every pre-`start` sample in the file.
            streams.push(Box::new(BoundedReader::new(reader, start, end)));
        }
        Ok(streams)
    }

    async fn get_last_known_event(&self, pv: &str) -> anyhow::Result<Option<ArchiverSample>> {
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
        // Tombstone the slot under its own lock so a concurrent
        // `append` (holding the same Arc but blocked on the slot
        // lock) bails when it wakes up — without the tombstone, it
        // would happily recreate the file we're about to delete.
        let slot_arc = {
            let mut cache = self
                .write_cache
                .lock()
                .map_err(|e| anyhow::anyhow!("write cache poisoned: {e}"))?;
            cache.remove(pv)
        };
        if let Some(arc) = slot_arc {
            let mut slot = arc.lock().unwrap_or_else(|e| e.into_inner());
            slot.dead = true;
            if let Some(mut cached) = slot.writer.take() {
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
        // Read-side callers (`get_data` etc.) only care about real
        // I/O errors. Deferred writers (an `append` is in flight)
        // are not failures — those bytes will reach disk on the
        // next cycle, and the reader can still see everything
        // already flushed. Surfacing deferred as Err would make
        // every concurrent read+write race look like storage death.
        let outcome = self.flush_dirty_writers();
        if !outcome.failed.is_empty() {
            anyhow::bail!(
                "{} writer flush(es) failed (first pv={})",
                outcome.failed.len(),
                outcome.failed[0],
            );
        }
        Ok(())
    }

    async fn flush_ingest_writes(&self) -> anyhow::Result<Vec<String>> {
        // Write_loop wants both:
        //   - failed: bytes lost — drop ts_updates for these
        //   - deferred: bytes still buffered — drop ts_updates so
        //     the registry doesn't claim more than what's on disk;
        //     they'll be picked up next flush cycle.
        // Both classes must NOT advance the registry's `last_event`,
        // so the union goes back to the caller as one list.
        let outcome = self.flush_dirty_writers();
        let mut all = outcome.failed;
        all.extend(outcome.deferred);
        Ok(all)
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
        // open handle on a soon-to-be-renamed file. Tombstone source so a
        // racing append cannot resurrect it after we've moved its files.
        let from_slot = {
            let mut cache = self
                .write_cache
                .lock()
                .map_err(|e| anyhow::anyhow!("write cache poisoned: {e}"))?;
            // Also evict the destination cache entry if any (defensive).
            // Destination is NOT tombstoned — `to` is the live PV after
            // this returns, and future appends to it are legal.
            if let Some(arc) = cache.remove(to) {
                let mut slot = arc.lock().unwrap_or_else(|e| e.into_inner());
                if let Some(mut cached) = slot.writer.take() {
                    let _ = cached.writer.flush();
                }
            }
            cache.remove(from)
        };
        if let Some(arc) = from_slot {
            let mut slot = arc.lock().unwrap_or_else(|e| e.into_inner());
            slot.dead = true;
            if let Some(mut cached) = slot.writer.take() {
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
                .ok_or_else(|| {
                    anyhow::anyhow!("filename {file_name} did not match expected PV leaf")
                })?;
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
