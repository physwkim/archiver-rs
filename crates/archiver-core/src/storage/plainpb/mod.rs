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

/// Shared fd permit pool used by [`PlainPbStoragePlugin`]. Cloning
/// the budget hands the SAME counter to multiple plugins, so a
/// process-wide cap can be enforced across STS/MTS/LTS instead of
/// each tier privately keeping its own 512-fd ceiling and silently
/// summing past the process ulimit.
///
/// Internally a `Arc<AtomicUsize>` plus a `max`. CAS-based
/// reservation in `try_reserve` makes the cap a hard ceiling under
/// concurrency (no check-then-fetch_add race).
#[derive(Clone)]
pub struct FdBudget {
    counter: Arc<AtomicUsize>,
    max: usize,
}

impl FdBudget {
    /// New budget with the given cap. `0` is sentinel for
    /// "unbounded" (lifts the cap to `usize::MAX`); any other value
    /// is the hard ceiling.
    pub fn new(max: usize) -> Self {
        Self {
            counter: Arc::new(AtomicUsize::new(0)),
            max: if max == 0 { usize::MAX } else { max },
        }
    }

    /// Convenience constructor for sites that want to disable the
    /// internal cap entirely (e.g. when the OS ulimit alone is the
    /// only ceiling that matters).
    pub fn unbounded() -> Self {
        Self::new(usize::MAX)
    }

    /// Snapshot of the current open-writer count (across every
    /// plugin sharing this budget). Lock-free; can drift between
    /// the read and any subsequent action.
    pub fn count(&self) -> usize {
        self.counter.load(Ordering::Relaxed)
    }

    /// Configured cap.
    pub fn max(&self) -> usize {
        self.max
    }

    /// Atomically reserve one permit. Returns `Some(WriterFdGuard)`
    /// on success; the guard's Drop releases the permit. CAS loop
    /// closes the check-then-act race on contended paths.
    fn try_reserve(&self) -> Option<WriterFdGuard> {
        loop {
            let cur = self.counter.load(Ordering::Acquire);
            if cur >= self.max {
                return None;
            }
            match self.counter.compare_exchange_weak(
                cur,
                cur + 1,
                Ordering::AcqRel,
                Ordering::Acquire,
            ) {
                Ok(_) => {
                    return Some(WriterFdGuard {
                        counter: self.counter.clone(),
                    });
                }
                Err(_) => continue,
            }
        }
    }
}

use async_trait::async_trait;
use prost::Message;
use tracing::debug;

use crate::storage::partition::PartitionGranularity;
use crate::storage::traits::{
    AppendMeta, EventStream, IngestFlushResult, StoragePlugin, StoreSummary,
};
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

/// RAII cleanup for the tombstoned-slot pattern used by
/// `delete_pv_data` and `rename_pv`. Ensures `cache.remove(pv)`
/// runs on every return path — including `?` short-circuits and
/// panics — so a PV cannot be left permanently undead because an
/// intermediate `tokio::fs::remove_file` errored. Without this
/// guard the slot stays in `write_cache` with `dead == true` and
/// every future append for this PV name bails forever.
struct TombstoneCleanupGuard<'a> {
    plugin: &'a PlainPbStoragePlugin,
    pv: String,
}

impl<'a> Drop for TombstoneCleanupGuard<'a> {
    fn drop(&mut self) {
        let mut cache = self
            .plugin
            .write_cache
            .lock()
            .unwrap_or_else(|e| e.into_inner());
        cache.remove(&self.pv);
    }
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
    /// Shared fd permit pool. Cloning the [`FdBudget`] across
    /// multiple plugins ties them to the SAME counter and ceiling,
    /// so STS+MTS+LTS can enforce a single process-wide cap rather
    /// than each tier silently keeping its own 512-fd ceiling.
    fd_budget: FdBudget,
    /// PVs whose dirty bytes were evicted by the LRU path AND
    /// whose flush failed, so the bytes are LOST. Drained by the
    /// next `flush_dirty_writers` and merged into `failed` so
    /// write_loop drops these PVs from `ts_updates` instead of
    /// silently committing a stale timestamp.
    evicted_with_loss: Mutex<Vec<String>>,
    /// When `true`, every dirty-writer flush also `sync_all`s the
    /// file to disk before reporting success, so a committed
    /// `last_event` can never outlive its bytes across a power loss.
    /// `false` (default) = page-cache durability only (Java parity).
    /// See [`crate::config::StorageConfig::fsync_on_flush`].
    fsync_on_flush: bool,
}

impl PlainPbStoragePlugin {
    pub fn new(name: &str, root_folder: PathBuf, granularity: PartitionGranularity) -> Self {
        Self::with_max_open_writers(name, root_folder, granularity, DEFAULT_MAX_OPEN_WRITERS)
    }

    /// Construct with an explicit cap on simultaneously-open writers.
    /// Equivalent to `with_fd_budget(name, root, granularity,
    /// FdBudget::new(max_open_writers))` — sites that want to
    /// SHARE the budget across multiple tiers should call
    /// `with_fd_budget` directly with a clone of the same `FdBudget`.
    pub fn with_max_open_writers(
        name: &str,
        root_folder: PathBuf,
        granularity: PartitionGranularity,
        max_open_writers: usize,
    ) -> Self {
        Self::with_fd_budget(
            name,
            root_folder,
            granularity,
            FdBudget::new(max_open_writers),
        )
    }

    /// Construct using a (possibly-shared) fd permit pool.
    ///
    /// Pass the SAME [`FdBudget`] (via `.clone()`) to multiple
    /// plugins to enforce a process-wide cap; pass a fresh
    /// `FdBudget::new(N)` per plugin for the legacy per-tier cap.
    pub fn with_fd_budget(
        name: &str,
        root_folder: PathBuf,
        granularity: PartitionGranularity,
        fd_budget: FdBudget,
    ) -> Self {
        Self {
            plugin_name: name.to_string(),
            root_folder,
            granularity,
            write_cache: Mutex::new(HashMap::new()),
            known_dirs: Mutex::new(HashSet::new()),
            fd_budget,
            evicted_with_loss: Mutex::new(Vec::new()),
            fsync_on_flush: false,
        }
    }

    /// Enable/disable `sync_all` after every dirty-writer flush. Off by
    /// default (page-cache durability, Java parity); on trades write
    /// throughput for power-loss durability. Builder-style so it
    /// composes with any constructor:
    /// `PlainPbStoragePlugin::with_fd_budget(..).with_fsync_on_flush(true)`.
    pub fn with_fsync_on_flush(mut self, on: bool) -> Self {
        self.fsync_on_flush = on;
        self
    }

    /// Flush a dirty cached writer, then `sync_all` it to disk when
    /// `fsync_on_flush` is set. A single fallible step so callers get
    /// one `io::Result` to classify: a flush OR fsync failure is
    /// treated identically (the bytes may not be durable). No-op fsync
    /// (and zero syscall) when the flag is off.
    fn flush_and_maybe_sync(&self, cached: &mut CachedWriter) -> std::io::Result<()> {
        cached.writer.flush()?;
        if self.fsync_on_flush {
            cached.writer.get_ref().sync_all()?;
        }
        Ok(())
    }

    /// Snapshot of the current open-writer count for the budget
    /// this plugin draws from. When the budget is shared across
    /// tiers, this reflects the GLOBAL count, not just this tier's
    /// share. Lock-free; instantaneous, can drift.
    pub fn open_writer_count(&self) -> usize {
        self.fd_budget.count()
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

    /// Flush every dirty cached writer (across all PVs) that we
    /// can lock without blocking. Per-slot `try_lock` keeps a
    /// stuck PV from blocking the flush of every other PV — those
    /// are reported in `deferred` and retried next cycle.
    ///
    /// Errored flushes evict the writer (buffered bytes dropped
    /// via `into_parts`) and are recorded BOTH in the returned
    /// `failed` list (for the immediate caller) AND in
    /// `evicted_with_loss` (so the next ingest-side flush_owner
    /// pass can pick them up even if the immediate caller was the
    /// read-side `flush_writes`). Without the second record, a
    /// retrieval-triggered flush could swallow a failure before
    /// the global flush owner ever sees it.
    ///
    /// **Does not drain `evicted_with_loss`.** That drain belongs
    /// exclusively to `take_loss_markers`, which the global flush
    /// owner calls on its success path — so the owner is the single
    /// consumer of loss markers. Read-side callers (retrieval, ETL)
    /// must NOT consume the queue or owner-side ts_updates would
    /// silently advance past PVs whose bytes never reached disk.
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
                    deferred.push(pv);
                    continue;
                }
                Err(std::sync::TryLockError::Poisoned(p)) => p.into_inner(),
            };
            let Some(cached) = slot_guard.writer.as_mut() else {
                continue;
            };
            if !cached.dirty {
                continue;
            }
            match self.flush_and_maybe_sync(cached) {
                Ok(()) => {
                    // Principle 4 (flush truth): flush succeeded
                    // at the syscall level, but if the underlying
                    // file is gone (ETL deleted it; an `rm -f` ran;
                    // an NFS race), the bytes went into the page
                    // cache for an unlinked inode — not reader-
                    // visible. Treat as loss to keep the registry's
                    // `last_event` honest.
                    if !cached.path.exists() {
                        tracing::warn!(
                            pv,
                            path = ?cached.path,
                            "Flush succeeded but file is gone; bytes are not \
                             reader-visible — surfacing PV to loss queue"
                        );
                        metrics::counter!(
                            "archiver_pb_flush_failures_total",
                            "tier" => self.plugin_name.clone(),
                        )
                        .increment(1);
                        if let Some(removed) = slot_guard.writer.take() {
                            let (_file, _buffered) = removed.writer.into_parts();
                        }
                        self.record_dirty_loss(&pv);
                        failed.push(pv.clone());
                        to_remove.push(pv);
                    } else {
                        cached.dirty = false;
                    }
                }
                Err(e) => {
                    tracing::warn!(pv, path = ?cached.path, "Failed to flush/fsync cached writer: {e}");
                    metrics::counter!(
                        "archiver_pb_flush_failures_total",
                        "tier" => self.plugin_name.clone(),
                    )
                    .increment(1);
                    if let Some(removed) = slot_guard.writer.take() {
                        let (_file, _buffered) = removed.writer.into_parts();
                    }
                    // Persist the loss so the next flush_ingest_writes
                    // surfaces it to the global owner — even if THIS
                    // call was triggered by retrieval/ETL, the owner
                    // must still learn that PV's bytes were lost so
                    // it doesn't commit a stale `last_event`.
                    self.record_dirty_loss(&pv);
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

    /// Push `pv` onto the persistent loss queue so the next
    /// `flush_ingest_writes` call surfaces it to the global flush
    /// owner. Idempotent at the caller site — duplicate entries
    /// are harmless because the owner's `ts_updates.remove` is
    /// idempotent.
    fn record_dirty_loss(&self, pv: &str) {
        let mut ev = self
            .evicted_with_loss
            .lock()
            .unwrap_or_else(|e| e.into_inner());
        ev.push(pv.to_string());
    }

    /// Test-only accessor to seed a loss marker without staging a real
    /// flush failure. Used by the `TieredStorage` drain test to verify
    /// every tier's queue is drained. Compiled out of release builds.
    #[cfg(test)]
    pub(crate) fn push_loss_marker_for_test(&self, pv: &str) {
        self.record_dirty_loss(pv);
    }

    /// Helper used by every code path that drops a dirty writer
    /// outside the regular flush iteration (partition rollover,
    /// ghost-file reopen, evict_writer_for_path, write_cached
    /// write-error path, LRU eviction). Tries to flush; on flush
    /// failure pushes the PV to `evicted_with_loss`. Returns
    /// `Ok(())` when the bytes are reader-visible on disk,
    /// `Err(io::Error)` when the bytes were lost.
    ///
    /// **Always uses `into_parts` to drop the BufWriter**, on
    /// every path. Without this, the loss branch's `Err` return
    /// would leave `cached.writer` to be dropped normally —
    /// `BufWriter::drop` would re-issue the failing flush syscall
    /// behind our back, possibly writing a partial frame to disk
    /// after we've already classified the bytes as lost. The
    /// remaining fields (path, last_used, _fd_guard) drop at
    /// scope exit; `_fd_guard` releases the fd permit.
    ///
    /// Also checks `path.exists()` after a successful flush —
    /// flushing to a deleted inode returns Ok at the syscall
    /// level but the bytes are not reader-visible (principle 4:
    /// flush success must mean reader-visible).
    fn drop_dirty_writer(&self, pv: &str, cached: CachedWriter) -> std::io::Result<()> {
        let CachedWriter {
            path,
            mut writer,
            dirty,
            last_used: _,
            _fd_guard,
        } = cached;
        let flush_res = if dirty {
            // Flush to page cache, then sync_all to disk when enabled.
            // A rolled-over / evicted partition's tail must be as
            // durable as the flush cycle's: its sample timestamps can
            // already be committed to the registry, so leaving the
            // bytes page-cache-only would let `last_event` outlive
            // them across a power loss.
            match writer.flush() {
                Ok(()) if self.fsync_on_flush => writer.get_ref().sync_all(),
                other => other,
            }
        } else {
            Ok(())
        };
        // Discard the BufWriter without invoking its Drop — keeps
        // a failed flush from re-firing as a drop-time auto-flush.
        let (_file, _buffered) = writer.into_parts();
        // _fd_guard drops at end of this function, releasing the
        // fd permit.

        match flush_res {
            Ok(()) => {
                if dirty && !path.exists() {
                    // Flushed to a deleted inode. Bytes went into
                    // the OS page cache for an unlinked file —
                    // not reader-visible. Treat as loss.
                    tracing::warn!(
                        pv,
                        ?path,
                        "Dirty-writer flush succeeded but file is gone; \
                         bytes are not reader-visible — surfacing PV to loss queue"
                    );
                    metrics::counter!(
                        "archiver_pb_dirty_drop_loss_total",
                        "tier" => self.plugin_name.clone(),
                    )
                    .increment(1);
                    self.record_dirty_loss(pv);
                    Err(std::io::Error::new(
                        std::io::ErrorKind::NotFound,
                        "flushed to deleted inode",
                    ))
                } else {
                    Ok(())
                }
            }
            Err(e) => {
                tracing::warn!(
                    pv,
                    ?path,
                    "Dirty-writer drop flush failed; dirty bytes lost — \
                     surfacing PV to loss queue: {e}"
                );
                metrics::counter!(
                    "archiver_pb_dirty_drop_loss_total",
                    "tier" => self.plugin_name.clone(),
                )
                .increment(1);
                self.record_dirty_loss(pv);
                Err(e)
            }
        }
    }

    /// Variant of [`drop_dirty_writer`] for paths where the
    /// underlying file is GONE (ghost-file disappearance,
    /// `evict_writer_for_path` after `remove_file`). Skips the
    /// flush attempt — flushing to a deleted inode either fails
    /// or vanishes silently — and unconditionally records loss
    /// for any dirty bytes.
    fn drop_writer_file_gone(&self, pv: &str, cached: CachedWriter) {
        if cached.dirty {
            tracing::warn!(
                pv,
                path = ?cached.path,
                "Dirty bytes lost — file disappeared while writer was \
                 still buffering; surfacing PV to loss queue"
            );
            metrics::counter!(
                "archiver_pb_dirty_drop_loss_total",
                "tier" => self.plugin_name.clone(),
            )
            .increment(1);
            self.record_dirty_loss(pv);
        }
        // Drop the CachedWriter explicitly: into_parts avoids
        // BufWriter's drop-time auto-flush so we don't try to
        // write to a deleted file.
        let (_file, _buffered) = cached.writer.into_parts();
    }

    /// Drop any cached BufWriter whose target path matches `path`.
    /// Call this after `remove_file` on a `.pb` file the engine may have
    /// open — without it, subsequent `append_event` writes go to the
    /// deleted-but-still-open inode (a "ghost" file invisible to readers
    /// because `list_files_for_range` walks the directory). Safe no-op
    /// when nothing matches. Returns true if a writer was evicted.
    ///
    /// **Definitive (principle 3 / eviction ownership):** the slot
    /// pointing at `path` (if any) is evicted before this returns.
    /// Fast path uses the file path's deterministic encoding to
    /// derive the PV name and look up exactly one slot, avoiding
    /// the previous full-scan blocking-lock cost (where one stuck
    /// unrelated slot could delay every ETL eviction). Falls back
    /// to a scan only if the path's filename can't be parsed.
    ///
    /// Sync method, blocking on the target slot's mutex — the
    /// ETL caller wraps it in `tokio::task::spawn_blocking` so
    /// the runtime worker isn't held during the wait.
    pub fn evict_writer_for_path(&self, path: &Path) -> bool {
        // Fast path: the file path encodes the PV name (see
        // `pv_name_to_key`), so we can derive it and look up
        // exactly one slot. Avoids the previous full-scan
        // behaviour where one stuck unrelated slot could delay
        // every ETL eviction.
        if let Some(pv) = self.pv_name_from_path(path) {
            return self.evict_writer_for_pv_at_path(&pv, path);
        }
        // Fallback: file path didn't decode (caller passed a
        // non-storage path, or the encoding scheme has drifted).
        // Run the legacy scan so we don't miss a writer just
        // because the filename was unusual.
        self.evict_writer_for_path_scan(path)
    }

    /// Reverse the deterministic `pv_name_to_key` → file-path
    /// encoding. Returns `Some(pv_name)` for any path under the
    /// storage root whose filename matches the
    /// `{pv_key}:{partition}.pb` layout.
    ///
    /// Public so the ETL executor can group a tier's partition files
    /// by their true PV name (the same key the writer slots use)
    /// rather than a coarser path heuristic — getting this exact for
    /// multi-segment PV names is what lets ETL guarantee it never
    /// moves a PV's live partition.
    pub fn pv_name_from_path(&self, path: &Path) -> Option<String> {
        let rel = path.strip_prefix(&self.root_folder).ok()?;
        let s = rel.to_str()?;
        // Strip the trailing partition+extension (everything from
        // the LAST `:` onward — the last colon is the separator
        // because `pv_name_to_key` has already replaced any colon
        // in the PV name with `/`).
        let colon = s.rfind(':')?;
        let pv_key = &s[..colon];
        if pv_key.is_empty() {
            return None;
        }
        Some(pv_key.replace('/', ":"))
    }

    /// Direct-lookup eviction: lock just one slot (the one keyed
    /// by `pv`) and evict if its writer's path matches. Returns
    /// `true` when a writer was evicted, `false` otherwise.
    fn evict_writer_for_pv_at_path(&self, pv: &str, path: &Path) -> bool {
        let slot_arc = {
            let cache = self.write_cache.lock().unwrap_or_else(|e| {
                tracing::warn!(?path, "write cache poisoned at evict_writer_for_path: {e}");
                e.into_inner()
            });
            cache.get(pv).cloned()
        };
        let Some(arc) = slot_arc else {
            return false;
        };
        let mut slot_guard = arc.lock().unwrap_or_else(|e| e.into_inner());
        let matches = slot_guard
            .writer
            .as_ref()
            .map(|cw| cw.path == path)
            .unwrap_or(false);
        if !matches {
            return false;
        }
        let Some(cached) = slot_guard.writer.take() else {
            return false;
        };
        self.drop_writer_file_gone(pv, cached);
        drop(slot_guard);
        let mut cache = self.write_cache.lock().unwrap_or_else(|e| e.into_inner());
        cache.remove(pv);
        true
    }

    /// Fallback full-scan eviction for paths that don't decode
    /// to a known PV name. Snapshots every slot Arc and blocking-
    /// locks each — same correctness as the fast path, but with
    /// the O(N) cost the fast path is designed to avoid.
    fn evict_writer_for_path_scan(&self, path: &Path) -> bool {
        let snapshot: Vec<(String, Arc<Mutex<PvWriterSlot>>)> = {
            let cache = self.write_cache.lock().unwrap_or_else(|e| {
                tracing::warn!(?path, "write cache poisoned at evict_writer_for_path: {e}");
                e.into_inner()
            });
            cache
                .iter()
                .map(|(pv, slot)| (pv.clone(), slot.clone()))
                .collect()
        };

        let mut removed = false;
        let mut to_remove = Vec::new();
        for (pv, slot_arc) in snapshot {
            let mut slot_guard = slot_arc.lock().unwrap_or_else(|e| e.into_inner());
            let matches = slot_guard
                .writer
                .as_ref()
                .map(|cw| cw.path == path)
                .unwrap_or(false);
            if !matches {
                continue;
            }
            if let Some(cached) = slot_guard.writer.take() {
                self.drop_writer_file_gone(&pv, cached);
                removed = true;
            }
            drop(slot_guard);
            to_remove.push(pv);
        }
        if !to_remove.is_empty() {
            let mut cache = self.write_cache.lock().unwrap_or_else(|e| e.into_inner());
            for pv in &to_remove {
                cache.remove(pv);
            }
        }
        removed
    }

    /// Unlink a `.pb` file, treating an already-absent file as success
    /// (another mover/recovery pass, or a manual `rm`, got there first).
    fn unlink_idempotent(path: &Path) -> std::io::Result<bool> {
        match std::fs::remove_file(path) {
            Ok(()) => Ok(true),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(true),
            Err(e) => Err(e),
        }
    }

    /// Delete a partition file that ETL has finished copying to another
    /// tier, **under the per-PV slot lock**. This is the only sanctioned
    /// way for ETL to unlink a source-tier `.pb` file; it replaces the
    /// old "remove_file then evict_writer_for_path" ordering whose gap
    /// let an append land in the just-deleted inode.
    ///
    /// Returns:
    /// - `Ok(true)`  — the file is gone (deleted now, or already
    ///   absent). If a CLEAN writer was still cached on this exact path
    ///   its bytes are already on disk, so it is dropped first (via
    ///   `into_parts`, no drop-time auto-flush) and the next append for
    ///   the PV opens a fresh file instead of writing into the unlinked
    ///   inode.
    /// - `Ok(false)` — a DIRTY writer is open on this path: it holds
    ///   buffered bytes that were NOT part of the copy. The file is left
    ///   intact so those bytes can never be lost. This means a still-live
    ///   partition reached the mover — periodic ETL selection must exclude
    ///   the live partition, and `consolidate_pv` must pause the PV first.
    /// - `Err(e)` — the unlink syscall itself failed.
    ///
    /// Holding the slot lock across the liveness check and the unlink is
    /// what closes the orphaned-inode race: a concurrent `append` for
    /// this PV serializes on the same mutex, so it cannot open a writer
    /// on `path` between the check and the delete. Once the file is gone
    /// and the lock released, the next append computes a newer partition
    /// path (timestamps move forward) and never targets the removed file.
    ///
    /// Sync (blocking slot lock + unlink). ETL wraps it in
    /// `spawn_blocking` so a runtime worker isn't held during the wait.
    pub fn remove_moved_partition(&self, path: &Path) -> std::io::Result<bool> {
        // A path that doesn't decode to a known PV layout cannot be held
        // by any slot — just unlink it.
        let Some(pv) = self.pv_name_from_path(path) else {
            return Self::unlink_idempotent(path);
        };

        let slot_arc = {
            let cache = self.write_cache.lock().unwrap_or_else(|e| e.into_inner());
            cache.get(&pv).cloned()
        };
        let Some(arc) = slot_arc else {
            // No slot for this PV → nothing is writing the path.
            return Self::unlink_idempotent(path);
        };

        let mut slot = arc.lock().unwrap_or_else(|e| e.into_inner());
        if let Some(cached) = slot.writer.as_ref()
            && cached.path == path
        {
            if cached.dirty {
                // Live partition with un-copied buffered bytes — never
                // delete it; the caller's selection should have excluded
                // it (or consolidate should have paused the PV).
                return Ok(false);
            }
            // Clean writer on this exact path: its bytes are already
            // durable on disk. Drop it before the unlink so the fd is
            // closed and BufWriter's drop-time auto-flush can't re-fire
            // into the soon-to-be-unlinked inode.
            if let Some(taken) = slot.writer.take() {
                let (_file, _buffered) = taken.writer.into_parts();
            }
        }

        // Unlink while STILL holding the slot lock: a concurrent append
        // for this PV is blocked on this mutex and cannot open a writer
        // on `path` between the liveness check above and this unlink.
        // Leaving an empty slot (writer == None) in the cache is
        // harmless — flush_dirty_writers skips it and the next append
        // reuses it.
        Self::unlink_idempotent(path)
    }

    /// Roll a destination partition file back to a known-good byte
    /// length, discarding any cached writer on it WITHOUT recording loss,
    /// then truncate and (under `fsync_on_flush`) make the new length
    /// durable.
    ///
    /// This is the ETL idempotency primitive: when a failed mid-copy
    /// append left a partial tail on `path`, the mover truncates back to
    /// the length recorded in its checkpoint before re-appending, so a
    /// retry yields exactly one copy of each sample. The discarded buffer
    /// is that failed append (or clean bytes already on disk), never
    /// archive loss — so, unlike [`evict_writer_for_path`], this must NOT
    /// record a dirty-loss marker or bump `archiver_pb_dirty_drop_loss_total`.
    ///
    /// The per-PV slot lock is held across the discard AND the `set_len`,
    /// so a concurrent `append_event` for this PV cannot open a writer on
    /// `path` in between. No parent-dir fsync: changing a file's length
    /// adds or removes no directory entry.
    ///
    /// Idempotent: an absent file with `len == 0` is already at the target.
    ///
    /// Sync (blocking slot lock + truncate/fsync); ETL wraps it in
    /// `spawn_blocking` so a runtime worker isn't held during the wait.
    pub fn truncate_partition(&self, path: &Path, len: u64) -> std::io::Result<()> {
        // Hold the per-PV slot lock (when the path decodes to a slotted
        // PV) across the discard + truncate so no concurrent append can
        // open a writer on `path` between the two.
        let slot_arc = self.pv_name_from_path(path).and_then(|pv| {
            let cache = self.write_cache.lock().unwrap_or_else(|e| e.into_inner());
            cache.get(&pv).cloned()
        });
        let mut slot_guard = slot_arc
            .as_ref()
            .map(|arc| arc.lock().unwrap_or_else(|e| e.into_inner()));
        if let Some(guard) = slot_guard.as_mut()
            && guard
                .writer
                .as_ref()
                .map(|cw| cw.path == path)
                .unwrap_or(false)
            && let Some(cached) = guard.writer.take()
        {
            // Discard the buffer WITHOUT recording loss: these bytes are a
            // failed ETL append being truncated away, not archive loss.
            // `into_parts` so `BufWriter::drop` can't re-issue a flush into
            // the file we're about to truncate.
            let (_file, _buffered) = cached.writer.into_parts();
        }

        match std::fs::OpenOptions::new().write(true).open(path) {
            Ok(file) => {
                file.set_len(len)?;
                if self.fsync_on_flush {
                    file.sync_all()?;
                }
            }
            // Absent file already at length 0 — nothing to truncate.
            Err(e) if e.kind() == std::io::ErrorKind::NotFound && len == 0 => {}
            Err(e) => return Err(e),
        }
        // `slot_guard` (if any) releases here, after the truncate.
        Ok(())
    }

    /// Durably create an ETL sidecar file (the `.etl_ckpt` checkpoint or
    /// the `.etl_done` marker) with `contents`. Uses `create_new` (O_EXCL)
    /// so an existing sidecar is never silently overwritten — the caller
    /// decides whether an `AlreadyExists` is a real error. Under
    /// `fsync_on_flush`, fsyncs the file then its parent directory so both
    /// the bytes AND the directory entry are durable before returning; the
    /// ETL commit ordering relies on a created sidecar being crash-visible.
    pub fn create_etl_sidecar(&self, path: &Path, contents: &[u8]) -> std::io::Result<()> {
        // The sidecar shares its directory with the partition file it
        // guards. For a brand-new dest partition that directory may not
        // exist yet (it is normally created by the first append), but the
        // checkpoint must be created BEFORE any append — so create the
        // directory durably here, or the O_EXCL open fails with NotFound.
        self.ensure_parent_dir(path)
            .map_err(|e| std::io::Error::other(e.to_string()))?;
        let mut f = std::fs::OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(path)?;
        f.write_all(contents)?;
        if self.fsync_on_flush {
            f.sync_all()?;
            if let Some(parent) = path.parent() {
                Self::sync_dir(parent)?;
            }
        }
        Ok(())
    }

    /// Durably remove an ETL sidecar file. An already-absent file is
    /// success (idempotent recovery). Under `fsync_on_flush`, fsyncs the
    /// parent directory so the removal is durable before returning.
    pub fn remove_etl_sidecar(&self, path: &Path) -> std::io::Result<()> {
        match std::fs::remove_file(path) {
            Ok(()) => {}
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
            Err(e) => return Err(e),
        }
        if self.fsync_on_flush
            && let Some(parent) = path.parent()
        {
            Self::sync_dir(parent)?;
        }
        Ok(())
    }

    /// Ensure a parent directory exists, using a cached set to skip repeated syscalls.
    fn ensure_parent_dir(&self, path: &Path) -> anyhow::Result<()> {
        if let Some(parent) = path.parent() {
            let needs_create = {
                // Recover from poison: known_dirs mutations are
                // single-statement HashSet inserts, so a panicking
                // thread can't leave half-modified state. Better to
                // proceed than fail every future write_cached call.
                let dirs = self.known_dirs.lock().unwrap_or_else(|e| e.into_inner());
                !dirs.contains(parent)
            };
            if needs_create {
                if self.fsync_on_flush {
                    // Durability: fsync every level of the new directory
                    // chain so the entries survive a power loss. Without
                    // this, a first-ever sample for a new PV prefix
                    // could be committed to the registry while the
                    // directory chain that makes its file reachable is
                    // still only in the page cache.
                    self.create_dir_all_synced(parent)?;
                } else {
                    std::fs::create_dir_all(parent)?;
                }
                let mut dirs = self.known_dirs.lock().unwrap_or_else(|e| e.into_inner());
                dirs.insert(parent.to_path_buf());
            }
        }
        Ok(())
    }

    /// fsync a directory so entries added to / removed from it become
    /// durable. Opening a directory read-only and calling `sync_all`
    /// is the POSIX way to flush directory metadata (Linux and macOS
    /// both allow fsync on a directory fd). The transient fd is opened
    /// and dropped immediately, outside the writer [`FdBudget`].
    fn sync_dir(dir: &Path) -> std::io::Result<()> {
        std::fs::File::open(dir)?.sync_all()
    }

    /// Like `create_dir_all`, but fsyncs each level of the chain so the
    /// new directory entries survive a power loss. Used only when
    /// `fsync_on_flush` is set.
    ///
    /// Durability is NOT inferred from `exists()`. This runs UNLOCKED on
    /// a plugin `Arc` shared across all shard write tasks, so a
    /// concurrent creator can make a directory visible (`exists()` ==
    /// true) *before* it has fsync'd that directory's entry into its
    /// parent. If a rider short-circuited on `exists()` it could return,
    /// append, and let the flush owner commit `last_event` while an
    /// ancestor entry is still page-cache-only — a power loss then makes
    /// the committed bytes unreachable (over-commit). A failed sync that
    /// is retried has the same trap: the level now `exists()`, so the
    /// re-sync would be skipped.
    ///
    /// So every level's parent is fsync'd UNCONDITIONALLY, whether or
    /// not the level pre-existed: each caller makes the whole chain
    /// durable itself, with no dependency on another thread's progress.
    /// `known_dirs` is populated by the caller only after this returns
    /// `Ok`, so membership there truly means "chain is durable" and the
    /// fast-path skip is safe. Bounded at `root_folder` (created at
    /// startup, assumed durable); levels above it are never touched.
    /// Concurrency-safe: an `AlreadyExists` from a racing creator is
    /// tolerated and that level's parent is still synced.
    fn create_dir_all_synced(&self, dir: &Path) -> std::io::Result<()> {
        let root = self.root_folder.as_path();
        // Collect the levels strictly under `root`, deepest first,
        // stopping AT `root` (never descend below/above it).
        let mut levels: Vec<&Path> = Vec::new();
        let mut cur = dir;
        while cur != root {
            levels.push(cur);
            match cur.parent() {
                Some(p) => cur = p,
                // `dir` isn't under `root` (never happens for paths from
                // `file_path_for`); stop rather than walk to fs root.
                None => break,
            }
        }
        // Shallowest-first: create each level if missing, then fsync its
        // parent unconditionally so the level's entry is durable.
        for level in levels.iter().rev() {
            match std::fs::create_dir(level) {
                Ok(()) => {}
                Err(e) if e.kind() == std::io::ErrorKind::AlreadyExists => {}
                Err(e) => return Err(e),
            }
            if let Some(parent) = level.parent() {
                Self::sync_dir(parent)?;
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
            anyhow::bail!("PV `{pv}` was deleted/renamed concurrently; refusing to recreate file");
        }

        // If the cached writer points at a different path, the partition has
        // rolled over — flush and drop the old writer before opening the new
        // one. Use `drop_dirty_writer` so a flush failure here records loss
        // (without it, partition rollover at the wrong moment silently lost
        // the old partition's last buffered samples).
        if let Some(existing) = slot.writer.as_ref()
            && existing.path != path_buf
            && let Some(cached) = slot.writer.take()
        {
            let _ = self.drop_dirty_writer(pv, cached);
        }

        // Defense-in-depth for ghost-file writes: if the cached writer's
        // target path no longer exists on disk (deleted by ETL while we
        // missed the eviction, by manual `rm`, by a flaky NFS mount, …)
        // its bytes are going into an orphaned inode invisible to readers.
        // Drop via `drop_writer_file_gone` so dirty bytes get a loss marker
        // (flushing to a deleted inode is meaningless; bytes are lost
        // regardless).
        if let Some(existing) = slot.writer.as_ref()
            && !existing.path.exists()
        {
            tracing::warn!(
                pv,
                path = ?existing.path,
                "Cached writer's file disappeared from filesystem; reopening"
            );
            if let Some(cached) = slot.writer.take() {
                self.drop_writer_file_gone(pv, cached);
            }
        }

        if slot.writer.is_none() {
            let needs_header = file_needs_header(path);
            // Whether the open below will CREATE the file (add a new
            // directory entry). Reliable under the per-PV slot lock: no
            // other writer targets this path. Gates the parent-dir
            // fsync so a plain reopen (post-eviction) of an
            // already-durable file skips the syscall.
            let is_new_file = self.fsync_on_flush && !path.exists();

            // Atomic fd-cap reservation: loop trying to reserve a
            // permit; each failed reservation triggers one LRU
            // eviction (which drops a CachedWriter, decrementing
            // `open_writers` via WriterFdGuard's Drop). Bail with a
            // clear error if no evictable candidate exists — better
            // than silently letting open() blow past the cap.
            //
            // CAS reservation closes the old check-then-fetch_add
            // race where N concurrent appends would all see "below
            // cap", all increment, and all open files past the cap.
            let fd_guard = loop {
                if let Some(guard) = self.fd_budget.try_reserve() {
                    break guard;
                }
                if !self.evict_lru_writer(pv) {
                    return Err(anyhow::anyhow!(
                        "PlainPB tier `{}` at fd cap ({}) and no evictable \
                         writer (all slots busy); refusing to open another \
                         to protect the process fd budget",
                        self.plugin_name,
                        self.fd_budget.max()
                    ));
                }
            };

            // Open with EMFILE/ENFILE recovery: even with our
            // internal reservation honoured, the OS-wide fd table
            // can still be exhausted (other processes, other tiers
            // sharing the same ulimit). Evict and retry once.
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

            // Durability: persist the file's directory entry so a
            // committed sample can't outlive its bytes across a power
            // loss. `sync_all` on the file (the flush path) makes the
            // DATA durable, but the name→inode link lives in the parent
            // directory and needs its own fsync. Only when we just
            // CREATED the file — a reopen of an existing (already
            // durable-entry) file needs no re-sync. If the sync fails,
            // remove the just-created empty file so the entry-sync is
            // retried on the next append: leaving the file behind would
            // flip `is_new_file` false next time and skip the sync,
            // yielding an un-synced entry a later commit could rely on.
            if is_new_file
                && let Some(parent) = path.parent()
                && let Err(e) = Self::sync_dir(parent)
            {
                drop(file);
                let _ = std::fs::remove_file(path);
                return Err(e.into());
            }

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
                let mut header_frame = Vec::with_capacity(escaped_header.len() + 1);
                header_frame.extend_from_slice(&escaped_header);
                header_frame.push(codec::NEWLINE);
                if let Err(e) = bw.write_all(&header_frame) {
                    // Header write failed. Discard the BufWriter
                    // via `into_parts` so its `Drop` can't re-issue
                    // the failing syscall — without this, the
                    // drop-time auto-flush could write a partial
                    // header to disk after we've already classified
                    // the bytes as lost. (Principle: failure-
                    // classified resource never goes through the
                    // normal destructor path.) The created file
                    // exists on disk; `file_needs_header` on the
                    // next attempt sees the unreadable header and
                    // truncates.
                    let (_file, _buffered) = bw.into_parts();
                    // fd_guard's Drop releases the fd permit at
                    // function exit.
                    return Err(e.into());
                }
            }

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
                // Move the reservation we obtained at the top of
                // this branch into the writer; its Drop will
                // release the fd permit when the writer is taken /
                // partition-rolled / evicted / dropped.
                _fd_guard: fd_guard,
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
            // Record loss IF the writer had previously-buffered
            // dirty bytes (other samples queued in BufWriter that
            // never reached disk). This sample's own ts won't be
            // reported because we return Err below — but past
            // samples whose ts WERE reported would otherwise be
            // false-committed by the global owner.
            let was_dirty = cached.dirty;
            if let Some(removed) = slot.writer.take() {
                let (_file, _buffered) = removed.writer.into_parts();
                // _file dropped → fd closed without flush.
                if was_dirty {
                    self.record_dirty_loss(pv);
                    metrics::counter!(
                        "archiver_pb_dirty_drop_loss_total",
                        "tier" => self.plugin_name.clone(),
                    )
                    .increment(1);
                }
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

    /// Evict one cached writer to free a file-descriptor permit.
    ///
    /// Two-pass policy:
    ///   1. **Clean writer LRU.** Pick the oldest slot whose
    ///      writer has `dirty == false`. Drop it; no flush needed,
    ///      no risk of data loss.
    ///   2. **Dirty writer LRU (last resort).** If no clean
    ///      candidate, pick the oldest dirty slot. Flush it. On
    ///      flush *failure*, push the PV onto `evicted_with_loss`
    ///      so the next `flush_dirty_writers` reports it in
    ///      `failed` — without this, write_loop would silently
    ///      commit a stale `last_event` for bytes that never
    ///      reached disk.
    ///
    /// Slots whose mutex is held by another thread are skipped —
    /// `try_lock` returning `WouldBlock` means an in-flight append
    /// is already using that fd, so taking it wouldn't free
    /// anything anyway.
    ///
    /// Returns `true` when something was evicted (caller can
    /// retry), `false` when no candidate could be freed.
    fn evict_lru_writer(&self, current_pv: &str) -> bool {
        let candidates: Vec<(String, Arc<Mutex<PvWriterSlot>>)> = {
            let cache = self.write_cache.lock().unwrap_or_else(|e| e.into_inner());
            cache
                .iter()
                .filter(|(pv, _)| pv.as_str() != current_pv)
                .map(|(pv, slot)| (pv.clone(), slot.clone()))
                .collect()
        };

        // Pass 1: prefer CLEAN writers. No flush, no risk of loss.
        if self.try_evict_with_filter(&candidates, /* want_dirty = */ false) {
            return true;
        }
        // Pass 2: forced fallback — dirty writers. Flush attempted;
        // failures surfaced via `evicted_with_loss`.
        self.try_evict_with_filter(&candidates, /* want_dirty = */ true)
    }

    /// Inner half of [`evict_lru_writer`] — find the oldest
    /// candidate matching `want_dirty` and evict it. Split out so
    /// the two passes share the snapshot + lock-retry logic.
    fn try_evict_with_filter(
        &self,
        candidates: &[(String, Arc<Mutex<PvWriterSlot>>)],
        want_dirty: bool,
    ) -> bool {
        let mut oldest: Option<(String, Arc<Mutex<PvWriterSlot>>, SystemTime)> = None;
        for (pv, slot_arc) in candidates {
            let Ok(guard) = slot_arc.try_lock() else {
                continue;
            };
            let Some(cw) = guard.writer.as_ref() else {
                drop(guard);
                continue;
            };
            if cw.dirty != want_dirty {
                drop(guard);
                continue;
            }
            let last_used = cw.last_used;
            drop(guard);
            match &oldest {
                Some((_, _, ts)) if *ts <= last_used => {}
                _ => oldest = Some((pv.clone(), slot_arc.clone(), last_used)),
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
        let Some(cached) = guard.writer.take() else {
            return false;
        };
        // Single dirty-drop helper does the flush, the
        // loss-marker bookkeeping, and the metric — kept
        // consistent with partition-rollover and write-error
        // paths so a future code path can't accidentally bypass
        // the loss surface.
        let _ = self.drop_dirty_writer(&pv, cached);
        // CachedWriter's WriterFdGuard already released the fd
        // permit when `drop_dirty_writer` consumed it.
        drop(guard);
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

/// Canonical PV key for comparing a registry PV name against the name
/// `pv_name_from_path` derives from a storage path.
///
/// The on-disk key encoding is lossy: `pv_name_to_key` maps `:` → `/`
/// while leaving any existing `/` untouched, so both `A:B` and `A/B`
/// share the key `A/B`, and `pv_name_from_path` maps that back to `A:B`.
/// A raw registry name that contains `/` (a valid PV-name separator) is
/// therefore NOT equal to its path-derived form — matching them by raw
/// string silently fails (e.g. a paused `RING/DCCT` would not match the
/// grouped key `RING:DCCT`). Routing both sides through this function —
/// which reproduces `pv_name_from_path`'s final `/`→`:` step over the
/// on-disk key — makes them comparable.
pub(crate) fn canonical_pv_key(pv: &str) -> String {
    pv_name_to_key(pv).replace('/', ":")
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
        // Three-phase delete to close the concurrent-append race:
        //
        //   Phase 1: KEEP the slot in the cache, set `dead = true`,
        //            drop the writer (file-gone helper). Any
        //            concurrent `append` that performs `slot_for`
        //            during this window — whether before or after
        //            we acquire the slot lock — gets the SAME
        //            tombstoned slot back. write_cached's
        //            dead-check then bails before opening any new
        //            file under this PV's name.
        //
        //   Phase 2: List + remove on-disk files (async).
        //
        //   Phase 3 (RAII): `_cleanup` drops at function exit and
        //            removes the slot from the cache. Runs on
        //            EVERY return path — Ok, ?, panic. Without
        //            this guard, an early-? from list_pv_pb_files
        //            or remove_file would leave the slot
        //            tombstoned forever with no operator-visible
        //            way to recover the PV name.
        let _cleanup = TombstoneCleanupGuard {
            plugin: self,
            pv: pv.to_string(),
        };
        let slot_arc = self.slot_for(pv);
        {
            let mut slot = slot_arc.lock().unwrap_or_else(|e| e.into_inner());
            slot.dead = true;
            if let Some(cached) = slot.writer.take() {
                // Files are about to be deleted unconditionally —
                // flushing buffered bytes is wasted I/O. Use the
                // file-gone helper so dirty bytes get a loss
                // marker, satisfying principle 1 (every dirty
                // drop classified). The next ingest flush will
                // surface the PV to the global owner; the owner's
                // commit is a no-op (registry row also torn down
                // by the management API), but the classification
                // stays uniform.
                self.drop_writer_file_gone(pv, cached);
            }
        }

        let entries = list_pv_pb_files(&self.root_folder, pv)?;
        let mut deleted = 0u64;
        for path in entries {
            tokio::fs::remove_file(&path).await?;
            deleted += 1;
        }

        // Clean up empty directory + invalidate the known_dirs
        // cache for it. Without the cache invalidation, a later
        // re-archive of the same PV would skip create_dir_all
        // (cache says "exists") and then fail to open with
        // ENOENT — the directory was deleted out from under the
        // cache.
        let (dir_part, _) = pv_file_parts(pv);
        let pv_dir = self.root_folder.join(&dir_part);
        if pv_dir.exists() {
            let is_empty = std::fs::read_dir(&pv_dir)?.next().is_none();
            if is_empty {
                let _ = tokio::fs::remove_dir(&pv_dir).await;
                let mut dirs = self.known_dirs.lock().unwrap_or_else(|e| e.into_inner());
                dirs.remove(&pv_dir);
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
        //
        // **Does NOT drain `evicted_with_loss`.** Loss markers are
        // reserved for the global flush owner (which drains them via
        // `take_loss_markers`) — read-side consumption would silently
        // swallow a failure before the owner can drop its `ts_updates`
        // for the lost PV. (Write failures during this read-side pass
        // are still recorded in `evicted_with_loss` by
        // `flush_dirty_writers` — they just aren't drained here.)
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

    async fn flush_ingest_writes(&self) -> anyhow::Result<IngestFlushResult> {
        // Report ONLY the failures THIS pass produced. The accumulated
        // loss queue (`evicted_with_loss`) is deliberately NOT drained
        // here — draining is owned exclusively by the flush owner via
        // `take_loss_markers`, which it calls from its own context only
        // after observing this call's success.
        //
        // Why the split (over-commit closure): the engine runs this
        // method inside a `spawn_blocking` task it abandons on timeout.
        // A `spawn_blocking` body keeps running to completion even after
        // its JoinHandle is dropped, so if the drain lived here an
        // abandoned-but-late flush would empty the queue and its result
        // would be discarded — the owner would never learn those PVs
        // lost bytes, and the next clean flush would commit a stale
        // `last_event` for them. Keeping the drain in the owner's
        // success path makes marker consumption atomic with acting on
        // it; a timed-out/panicked flush consumes nothing.
        //
        // `failed` still includes this-cycle flush failures, which
        // `flush_dirty_writers` ALSO recorded into the queue — so the
        // owner's `take_loss_markers` re-surfaces them and the engine
        // dedupes. (Keeping them in `failed` preserves the direct
        // contract that this pass's own flush errors are visible to the
        // immediate caller, e.g. read-side `flush_writes`.)
        let outcome = self.flush_dirty_writers();
        Ok(IngestFlushResult {
            failed: outcome.failed,
            deferred: outcome.deferred,
        })
    }

    /// Drain the dirty-write loss queue. See the trait method's
    /// single-owner invariant: only the engine's flush owner calls
    /// this, only on the observed-success path. `mem::take` hands the
    /// owner exactly the set present at drain time; any loss recorded
    /// after this returns stays queued for the next cycle.
    fn take_loss_markers(&self) -> Vec<String> {
        let mut ev = self
            .evicted_with_loss
            .lock()
            .unwrap_or_else(|e| e.into_inner());
        std::mem::take(&mut *ev)
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
        // Phase 1: tombstone the SOURCE slot in cache (don't
        // remove it yet — see delete_pv_data for the same race
        // rationale). Concurrent appends to `from` during the
        // file-rename window will find this same dead slot and
        // bail at the dead-check.
        //
        // Phase 3 (RAII): the cleanup guard removes the source
        // slot from the cache on every return path so a
        // partway-failed rename can't leave `from` permanently
        // tombstoned.
        let _cleanup = TombstoneCleanupGuard {
            plugin: self,
            pv: from.to_string(),
        };
        let from_slot = self.slot_for(from);
        {
            let mut slot = from_slot.lock().unwrap_or_else(|e| e.into_inner());
            slot.dead = true;
            if let Some(cached) = slot.writer.take() {
                // Try to flush so the post-rename dest file
                // inherits the source's last buffered samples.
                // On flush failure, record loss for `from`; the
                // global owner will drop its pending entry. The
                // source name's registry row is being removed by
                // the rename anyway, so the commit-side effect is
                // a no-op — but principle 1 (every dirty drop
                // classified) holds.
                let _ = self.drop_dirty_writer(from, cached);
            }
        }
        // Defensive: clear any stale destination writer before
        // the rename moves source files into the dest's path
        // namespace. Dest is NOT tombstoned — `to` is the live
        // PV after this returns, and future appends to it are
        // legal. (If the operator was actively appending to `to`
        // at the moment of rename, they made a mistake; we don't
        // optimise for that case.)
        //
        // **Outer lock briefly, then release**: take the slot
        // Arc out under the outer lock, drop the outer guard,
        // THEN lock the slot and run drop_dirty_writer (which
        // does sync flush I/O). Holding the outer lock across
        // the flush would block every concurrent slot_for() —
        // i.e., every shard's append — for the duration of the
        // dest writer's flush, which on slow storage could stall
        // the entire ingest path.
        let dest_slot_arc = {
            let mut cache = self
                .write_cache
                .lock()
                .map_err(|e| anyhow::anyhow!("write cache poisoned: {e}"))?;
            cache.remove(to)
        };
        if let Some(arc) = dest_slot_arc {
            let mut slot = arc.lock().unwrap_or_else(|e| e.into_inner());
            if let Some(cached) = slot.writer.take() {
                let _ = self.drop_dirty_writer(to, cached);
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
        if !to_dir.as_os_str().is_empty() {
            if self.fsync_on_flush {
                // Sync the whole chain (create_dir_all_synced is
                // idempotent and never infers durability from exists()):
                // a live append to `to` can be creating this dir
                // concurrently, so an exists()-gated skip could leave an
                // ancestor entry non-durable under a committed `to`.
                self.create_dir_all_synced(&to_dir)?;
            } else if !to_dir.exists() {
                std::fs::create_dir_all(&to_dir)?;
            }
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

        // Durability (destination): the renames added new directory
        // entries in to_dir. Persist them before returning success — the
        // caller commits `to`'s registry row on success, so a non-durable
        // dest entry under a committed `to` would over-commit across a
        // power loss (the FATAL class). No-op when fsync_on_flush is off.
        if self.fsync_on_flush && !to_dir.as_os_str().is_empty() {
            Self::sync_dir(&to_dir)?;
        }

        // Clean up empty source directory + invalidate
        // known_dirs cache for it (same rationale as
        // delete_pv_data — a stale entry would make the next
        // append to a re-archived `from` skip create_dir_all and
        // hit ENOENT).
        let (from_dir_part, _) = pv_file_parts(from);
        let from_dir = self.root_folder.join(&from_dir_part);
        // Durability (source): persist the entry removals the renames made
        // in from_dir. Best-effort — a stale source entry is orphan data,
        // not an over-commit (`from`'s registry row is removed by the
        // rename). Skip when from_dir == to_dir (already synced above).
        if self.fsync_on_flush
            && !from_dir_part.as_os_str().is_empty()
            && from_dir != to_dir
            && from_dir.exists()
        {
            let _ = Self::sync_dir(&from_dir);
        }
        if !from_dir_part.as_os_str().is_empty()
            && from_dir.exists()
            && std::fs::read_dir(&from_dir)?.next().is_none()
        {
            let _ = std::fs::remove_dir(&from_dir);
            // Persist the directory removal via its parent (best-effort).
            if self.fsync_on_flush
                && let Some(parent) = from_dir.parent()
            {
                let _ = Self::sync_dir(parent);
            }
            let mut dirs = self.known_dirs.lock().unwrap_or_else(|e| e.into_inner());
            dirs.remove(&from_dir);
        }

        // Phase 3 cleanup of the tombstoned source slot is done
        // by `_cleanup`'s Drop on function exit. Single source of
        // truth for the cleanup keeps it consistent across every
        // return path.

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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn canonical_pv_key_matches_path_derived_name() {
        let plugin =
            PlainPbStoragePlugin::new("t", PathBuf::from("/root"), PartitionGranularity::Hour);
        // For every PV name, the key that ETL's `paused` set must use
        // (canonical_pv_key) must equal the key `grouped` uses
        // (pv_name_from_path over the PV's storage path) — including
        // valid names that contain '/', where the raw registry name
        // differs from the path-derived form.
        for pv in ["SIM:Sine", "RING/DCCT", "A/B:C", "PLAIN"] {
            let key = pv_name_to_key(pv);
            let path = PathBuf::from(format!("/root/{key}:2024_01_01_00.pb"));
            let from_path = plugin.pv_name_from_path(&path).unwrap();
            assert_eq!(
                canonical_pv_key(pv),
                from_path,
                "canonical_pv_key must equal pv_name_from_path for {pv:?}"
            );
        }
        // The raw-name comparison ETL used before is exactly what breaks
        // for a '/'-containing name.
        assert_ne!("RING/DCCT", canonical_pv_key("RING/DCCT"));
    }

    #[tokio::test]
    async fn truncate_partition_discards_dirty_writer_without_loss() {
        use crate::types::ArchiverValue;
        use std::time::Duration;

        let dir = tempfile::tempdir().unwrap();
        let plugin =
            PlainPbStoragePlugin::new("STS", dir.path().to_path_buf(), PartitionGranularity::Hour);
        let ts = SystemTime::UNIX_EPOCH + Duration::from_secs(1_700_000_000);
        let sample = ArchiverSample::new(ts, ArchiverValue::ScalarDouble(1.0));
        let dbr = sample.value.db_type();

        // Two appends leave a DIRTY (unflushed) cached writer on D.
        plugin.append_event("PV:X", dbr, &sample).await.unwrap();
        plugin.append_event("PV:X", dbr, &sample).await.unwrap();
        let d = plugin.file_path_for("PV:X", ts);
        assert!(d.exists());

        // Truncate to 0 while the writer is still dirty.
        plugin.truncate_partition(&d, 0).unwrap();

        // The dirty buffer was DISCARDED, not surfaced as loss — an
        // intentional truncate is not archive loss.
        assert!(
            plugin.take_loss_markers().is_empty(),
            "intentional truncate must not record a dirty-loss marker"
        );
        assert_eq!(std::fs::metadata(&d).unwrap().len(), 0);

        // The slot no longer holds a writer on the truncated path: a fresh
        // append reopens the file rather than writing at a stale offset.
        plugin.append_event("PV:X", dbr, &sample).await.unwrap();
        plugin.flush_writes().await.unwrap();
        assert!(std::fs::metadata(&d).unwrap().len() > 0);
    }

    #[tokio::test]
    async fn truncate_partition_absent_file_len_zero_is_ok() {
        let dir = tempfile::tempdir().unwrap();
        let plugin =
            PlainPbStoragePlugin::new("MTS", dir.path().to_path_buf(), PartitionGranularity::Day);
        let missing = dir.path().join("PV:X:2024_01_01.pb");
        // No file yet, target length 0 — idempotent no-op.
        plugin.truncate_partition(&missing, 0).unwrap();
        assert!(!missing.exists());
    }

    #[test]
    fn etl_sidecar_create_makes_missing_dir_and_remove_is_idempotent() {
        let dir = tempfile::tempdir().unwrap();
        let plugin =
            PlainPbStoragePlugin::new("MTS", dir.path().to_path_buf(), PartitionGranularity::Day);
        // A brand-new dest partition's directory does not exist yet; the
        // sidecar must be creatable BEFORE any append, so create_etl_sidecar
        // creates the directory itself.
        let ckpt = dir.path().join("sub/PV:X:2024_01_01.pb.etl_ckpt");
        assert!(!ckpt.parent().unwrap().exists());
        plugin
            .create_etl_sidecar(&ckpt, b"42\nPV:X:2024_01_01_00.pb\n")
            .unwrap();
        assert_eq!(
            std::fs::read_to_string(&ckpt).unwrap(),
            "42\nPV:X:2024_01_01_00.pb\n"
        );

        // O_EXCL: a second create over an existing sidecar is an error.
        assert!(plugin.create_etl_sidecar(&ckpt, b"x").is_err());

        // Remove is durable and idempotent — a second remove of an
        // already-absent sidecar is success.
        plugin.remove_etl_sidecar(&ckpt).unwrap();
        assert!(!ckpt.exists());
        plugin.remove_etl_sidecar(&ckpt).unwrap();
    }
}
