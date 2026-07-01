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
use crate::storage::traits::{AppendMeta, EventStream, StoragePlugin};

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
    /// Serializes `move_file`'s critical section. Held across the whole
    /// move (locate → flush → checkpoint → append → commit → delete).
    ///
    /// Two distinct races require serialization:
    /// 1. WITHIN an executor: the periodic loop task and an operator's
    ///    `consolidate_pv` hold the SAME `Arc<EtlExecutor>` (`main.rs`
    ///    `etl_chain`), so two moves could interleave appends into the
    ///    same aggregating dest partition, or a truncate-on-retry could
    ///    clobber another mover's bytes.
    /// 2. ACROSS executors sharing a tier: a tier is one executor's DEST
    ///    and the next executor's SOURCE (MTS is `STS→MTS`'s dest and
    ///    `MTS→LTS`'s source, via the shared `tiered.mts` `Arc`). With a
    ///    per-executor gate, `STS→MTS` appending to an MTS day-partition
    ///    can interleave with `MTS→LTS` reading-then-deleting that same
    ///    partition: the reader snapshots D, the appender commits new
    ///    samples into D, the reader deletes D → the appended samples
    ///    never reached LTS and are lost (reachable via a backfilled STS
    ///    sample for a day being consolidated). The per-PV slot lock stops
    ///    torn file ops but NOT this read-copy-delete vs append ordering.
    ///
    /// So the gate is SHARED across the whole ETL chain: `main.rs` builds
    /// one `Arc<Mutex>` and hands it to every executor via
    /// `with_shared_move_gate`. A single chain-wide gate (vs a per-PV /
    /// per-partition lock map) is chosen because moves are already serial
    /// per caller and ETL is a cold background path: the only lost
    /// parallelism is one move waiting on another, and it avoids an
    /// unbounded lock map. `new` defaults to a private per-executor gate
    /// so a standalone executor (tests) is still self-serialized.
    move_gate: Arc<tokio::sync::Mutex<()>>,
}

impl EtlExecutor {
    pub fn new(
        source: Arc<PlainPbStoragePlugin>,
        dest: Arc<PlainPbStoragePlugin>,
        period_secs: u64,
        hold: u32,
        gather: u32,
    ) -> Self {
        // The idempotent copy relies on each source partition nesting in
        // exactly ONE dest partition — true only when the dest tier is
        // coarser-or-equal to the source (finer→coarser ETL). Guard it at
        // construction so a misconfiguration fails loudly instead of
        // silently splitting a copy across two dest partitions.
        // `PartitionGranularity` isn't `Ord`; compare via `approx_seconds`.
        assert!(
            dest.partition_granularity().approx_seconds()
                >= source.partition_granularity().approx_seconds(),
            "ETL dest tier granularity ({:?}) must be coarser-or-equal to source ({:?})",
            dest.partition_granularity(),
            source.partition_granularity(),
        );
        Self {
            source,
            dest,
            period_secs,
            hold,
            gather,
            move_timeout: DEFAULT_MOVE_TIMEOUT,
            pv_registry: None,
            move_gate: Arc::new(tokio::sync::Mutex::new(())),
        }
    }

    /// Share one move gate across every executor in the ETL chain so no
    /// two moves run concurrently anywhere in the chain. Required because
    /// adjacent executors share a tier (one's dest is the next's source):
    /// without a shared gate, `STS→MTS` appending to an MTS partition can
    /// interleave with `MTS→LTS` reading-then-deleting it and lose the
    /// appended samples (see the `move_gate` field). `main.rs` constructs
    /// one `Arc<Mutex>` and calls this on every executor.
    pub fn with_shared_move_gate(mut self, gate: Arc<tokio::sync::Mutex<()>>) -> Self {
        self.move_gate = gate;
        self
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

        let pb_files = list_pb_files(source_root)?;

        // Group every source partition by its TRUE PV name — the same
        // key the writer slots use (`pv_name_from_path`) — so per-PV
        // hold/gather and the live-partition exclusion are applied PER
        // PV. The old path globally sorted one flat file list and took
        // the lexicographically smallest, which let a PV whose name
        // sorts early surrender even its live (newest) partition to ETL.
        let mut grouped: HashMap<String, Vec<PathBuf>> = HashMap::new();
        for file in pb_files {
            match self.source.pv_name_from_path(&file) {
                Some(pv) => grouped.entry(pv).or_default().push(file),
                None => warn!(?file, "ETL: could not derive PV name from path; skipping"),
            }
        }

        // Java parity (92db337): skip files whose owning PV is paused.
        // `grouped`'s keys come from `pv_name_from_path`, whose on-disk
        // encoding is lossy (`:` and `/` both collapse to `/`), so the
        // registry name must be routed through the SAME canonicalization
        // (`canonical_pv_key`) or a paused PV whose name contains `/`
        // (e.g. `RING/DCCT` → grouped key `RING:DCCT`) would never match
        // and its files would migrate despite the pause. Computed once
        // per tick to keep the registry lookup off the per-file path.
        let paused: HashSet<String> = match self.pv_registry.as_ref() {
            Some(reg) => reg
                .pvs_by_status(PvStatus::Paused)
                .map(|recs| {
                    recs.into_iter()
                        .map(|r| crate::storage::plainpb::canonical_pv_key(&r.pv_name))
                        .collect()
                })
                .unwrap_or_else(|e| {
                    warn!("ETL: failed to read paused PVs from registry: {e}");
                    HashSet::new()
                }),
            None => HashSet::new(),
        };

        let hold = self.hold as usize;
        let gather = self.gather as usize;

        for (pv, mut files) in grouped {
            if paused.contains(&pv) {
                debug!(pv, "ETL skipping paused PV");
                continue;
            }
            // Chronological within the PV: `{prefix}:{YYYY_MM_DD_HH}.pb`
            // sorts lexicographically == by time.
            files.sort();
            let movable = select_movable(&files, hold, gather);
            if movable.is_empty() {
                continue;
            }
            debug!(pv, count = movable.len(), "ETL processing PV group");
            for file in movable {
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

    /// Whether the `.etl_done` marker may be cleared after a
    /// `remove_moved_partition` attempt. Only when the source is
    /// confirmed gone (`Ok(Ok(true))`): every other outcome
    /// (still-live/dirty, unlink error, evict-task panic) leaves the
    /// source on disk, so the marker must survive to drive a delete-only
    /// retry next cycle instead of a full re-copy.
    fn recovery_should_clear_marker(
        removed: &Result<std::io::Result<bool>, tokio::task::JoinError>,
    ) -> bool {
        matches!(removed, Ok(Ok(true)))
    }

    /// For a source partition still on disk, locate the single dest
    /// partition `D` its samples aggregate into and return
    /// `(D, source filename)` — the checkpoint's path stem and its owner
    /// id. `None` when the source can't be opened or holds no samples (no
    /// checkpoint could exist for it). Sync PB read, matching the copy
    /// path's own `PbFileReader::open`.
    fn locate_dest_and_owner(&self, source_path: &Path) -> Option<(PathBuf, String)> {
        let mut reader = PbFileReader::open(source_path).ok()?;
        let pv_name = reader.description().pv_name.clone();
        let first = reader.next_event().ok()??;
        let d_path = self.dest.file_path_for(&pv_name, first.timestamp);
        let s_filename = source_path.file_name()?.to_str()?.to_string();
        Some((d_path, s_filename))
    }

    /// Move a single PB file from source to destination tier.
    /// Uses copy → durable dest → marker → slot-locked delete for
    /// crash-safe idempotency.
    async fn move_file(&self, source_path: &Path) -> anyhow::Result<()> {
        // Serialize every move so no two run concurrently against the same
        // aggregating dest partition (see `move_gate`). Held across the
        // whole function — recovery included — so a move and a same-source
        // recovery can't race either.
        let _gate = self.move_gate.lock().await;

        // A partition with a pending INBOUND checkpoint is mid-aggregation by
        // a FINER tier (an in-flight append, or a crash-left partial). It is
        // not eligible for ANY outbound move: reading it would promote the
        // uncommitted partial tail to the coarser tier, and deleting it would
        // strand the finer tier's owner-retry on
        // `truncate_partition(<absent D>, anchor>0)` → `NotFound` every cycle
        // (permanent wedge). Defer — the finer tier commits, or its marker
        // recovery clears this checkpoint, then a later cycle moves the
        // partition out cleanly. No-op for the finest tier (nothing feeds it
        // via ETL, so it never carries an inbound checkpoint).
        let inbound_ckpt = source_path.with_extension("pb.etl_ckpt");
        if inbound_ckpt.exists() {
            debug!(
                ?source_path,
                ?inbound_ckpt,
                "ETL deferring: partition has a pending inbound checkpoint"
            );
            return Ok(());
        }

        // Marker from a previous incomplete cleanup (crash after the
        // copy was made durable). The marker is written ONLY after the
        // destination flush below, so its presence proves the dest tier
        // already held THIS partition's data — recovery deletes the source
        // without re-copying. The marker records the certified partition's
        // (len, mtime); before trusting it, confirm it still describes the
        // CURRENT source. A backfill re-creates the same-named partition with
        // new data (different len/mtime); a marker leaked by a crash — or a
        // swallowed remove — in the delete→remove-marker window would
        // otherwise delete that NEW data uncopied (silent loss). Only a
        // POSITIVE identity mismatch is stale, so a genuine marker never
        // triggers a re-copy (which would duplicate).
        let marker = source_path.with_extension("pb.etl_done");
        if marker.exists()
            && matches!(
                (read_marker_identity(&marker), source_identity(source_path)),
                (Some(recorded), Some(current)) if recorded != current
            )
        {
            warn!(
                ?source_path,
                ?marker,
                "ETL marker is stale (source re-created since it was certified); \
                 dropping it and re-copying"
            );
            // The marker no longer describes what is on disk — drop it and
            // fall through to a fresh idempotent copy of the re-created data.
            self.source.remove_etl_sidecar(&marker).ok();
        } else if marker.exists() {
            info!(
                ?source_path,
                "Found ETL marker — previous copy is durable in dest, cleaning up"
            );
            // Commit order is marker → remove ckpt → delete source. A crash
            // between the marker and the ckpt-removal leaves a stale
            // `<D>.pb.etl_ckpt` owned by this source: the copy is already
            // durable (marker present), so no in-flight append to D remains.
            // Clear our OWN checkpoint here — the committing owner is the
            // single party allowed to remove it (invariant: ckpt present ⟺
            // in-flight append to D exists). Owner-checked so another
            // source's live checkpoint on the same D is never touched;
            // best-effort so a source that can't be read to locate D just
            // leaves the reap to the orphan-removal path on the next move
            // into D. Runs before the delete below, while the source is
            // still readable.
            if let Some((d_path, s_filename)) = self.locate_dest_and_owner(source_path) {
                let ckpt = d_path.with_extension("pb.etl_ckpt");
                if read_ckpt(&ckpt).is_some_and(|(_, owner)| owner == s_filename)
                    && let Err(e) = self.dest.remove_etl_sidecar(&ckpt)
                {
                    warn!(
                        ?ckpt,
                        "ETL marker recovery: failed to clear owned dest checkpoint: {e}"
                    );
                }
            }
            // Delete the source under the per-PV slot lock so no append
            // can land in the just-deleted inode, and a still-live/dirty
            // partition is never destroyed.
            let source = self.source.clone();
            let path = source_path.to_path_buf();
            let removed =
                tokio::task::spawn_blocking(move || source.remove_moved_partition(&path)).await;
            match &removed {
                Ok(Ok(true)) => {} // source gone; marker cleared below
                Ok(Ok(false)) => warn!(
                    ?source_path,
                    "ETL marker recovery: source is still live/dirty; deferring delete"
                ),
                Ok(Err(e)) => warn!(
                    ?source_path,
                    "ETL marker recovery: failed to remove source (keeping marker \
                     for delete-only retry next cycle): {e}"
                ),
                Err(e) => warn!(
                    ?source_path,
                    "ETL marker recovery: evict task panicked (keeping marker for \
                     delete-only retry next cycle): {e}"
                ),
            }
            // Clear the marker ONLY when the source is confirmed gone. The
            // marker means "dest durably holds this partition", so it must
            // outlive every outcome that leaves the source on disk —
            // otherwise the next cycle re-selects the surviving source,
            // finds no marker, and re-runs the full copy, appending a
            // second copy of every sample (append_event has no dedup) once
            // per cycle until the unlink finally succeeds.
            if Self::recovery_should_clear_marker(&removed)
                && let Err(e) = tokio::fs::remove_file(&marker).await
            {
                warn!(?marker, "Failed to remove ETL marker: {e}");
            }
            return Ok(());
        }

        // Java parity (3daedae): the bulk COPY is wrapped in a timeout so a
        // hung NFS mount can't block the ETL loop indefinitely. But every
        // DESTRUCTIVE file mutation — the owner-retry `truncate_partition`
        // and the source unlink — is awaited to completion OUTSIDE that
        // timeout. `tokio::time::timeout` drops its inner future on elapse,
        // and dropping a future that is `.await`ing a `spawn_blocking`
        // JoinHandle DETACHES the blocking task (blocking tasks are never
        // cancelled — they run to completion). A detached `truncate_partition`
        // could then `set_len` D to a stale anchor AFTER `_gate` is released,
        // rolling a LATER move's just-committed bytes off D — silent,
        // permanent loss. So the timeout wraps only the cancellation-safe
        // copy loop: abandoning it mid-append leaves a partial tail past the
        // checkpoint anchor that the next retry's owner truncate rolls back.
        let timeout = self.move_timeout;
        let source_path = source_path.to_path_buf();
        let dest = self.dest.clone();
        let source = self.source.clone();
        let source_name = self.source.name().to_string();
        let dest_name = self.dest.name().to_string();

        let mut reader = PbFileReader::open(&source_path)?;
        let desc = reader.description().clone();
        let dbr_type = desc.db_type;

        // Peek the first sample to locate the single destination partition D.
        // ETL only moves finer→coarser and each sample routes by its own
        // timestamp, so every sample in this source partition nests in the D
        // derived from the first (the construction-time granularity guard
        // makes that hold; the per-sample check below catches a stray
        // out-of-range ts).
        let Some(first_sample) = reader.next_event()? else {
            // Empty source partition — no dest bytes to protect. Remove it
            // (slot-locked); awaited to completion so the unlink can't detach
            // past the gate. A crash just re-runs this idempotent delete.
            let source_for_evict = source.clone();
            let path_for_evict = source_path.clone();
            let removed = tokio::task::spawn_blocking(move || {
                source_for_evict.remove_moved_partition(&path_for_evict)
            })
            .await;
            return match removed {
                Ok(Ok(_)) => Ok(()),
                Ok(Err(e)) => Err(anyhow::anyhow!(
                    "ETL delete of empty partition {source_path:?} failed: {e}"
                )),
                Err(e) => Err(anyhow::anyhow!(
                    "ETL empty-partition delete task panicked for {source_path:?}: {e}"
                )),
            };
        };

        let d_path = dest.file_path_for(&desc.pv_name, first_sample.timestamp);
        let ckpt = d_path.with_extension("pb.etl_ckpt");
        let marker = source_path.with_extension("pb.etl_done");
        let s_filename = source_path
            .file_name()
            .and_then(|n| n.to_str())
            .ok_or_else(|| anyhow::anyhow!("ETL: source path has no filename: {source_path:?}"))?
            .to_string();

        // Flush the dest tier so D's on-disk length reflects every
        // previously-committed byte before we anchor to it. A failed flush
        // means an unknown dirty tail — abort (keep source) and never measure
        // a length we can't trust.
        dest.flush_writes().await?;
        // Only a genuinely ABSENT D anchors at 0. A non-NotFound stat error
        // (EIO/EACCES on a flaky mount) must NOT be coerced to 0: that false
        // anchor would be stamped into the checkpoint and a later owner-retry
        // would `truncate_partition(D, 0)`, destroying every prior source's
        // committed samples. Abort the move (keep source) instead.
        let len_before = match std::fs::metadata(&d_path) {
            Ok(m) => m.len(),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => 0,
            Err(e) => {
                return Err(anyhow::anyhow!(
                    "ETL: cannot stat dest partition {d_path:?} to anchor the copy \
                     (keeping source): {e}"
                ));
            }
        };

        // Establish the checkpoint anchor for this copy of S into D.
        // Invariant: `<D>.pb.etl_ckpt` present ⟺ an in-flight append to D
        // exists; its stored length is D's last known-good byte length; only
        // the owning source may truncate D to it.
        match read_ckpt(&ckpt) {
            Some((anchor, owner)) if owner == s_filename => {
                // Our own prior attempt failed mid-copy: roll D back to the
                // stored anchor (discarding the failed tail) and re-append
                // below. Keep the checkpoint — its anchor is the pre-append
                // length and must NOT be re-measured. Awaited to completion
                // (never under the copy timeout) so this truncate can't
                // detach and fire after the gate is released.
                let dest_for_trunc = dest.clone();
                let d_for_trunc = d_path.clone();
                tokio::task::spawn_blocking(move || {
                    dest_for_trunc.truncate_partition(&d_for_trunc, anchor)
                })
                .await??;
            }
            Some((_anchor, owner)) => {
                // A DIFFERENT source owns an in-flight append to D.
                let owner_src = source_path
                    .parent()
                    .map(|p| p.join(&owner))
                    .unwrap_or_else(|| PathBuf::from(&owner));
                if owner_src.exists() {
                    // The owner will finish or retry its own copy; appending
                    // now would interleave two sources into D. Defer — a later
                    // cycle retries this source once the owner clears its
                    // checkpoint.
                    debug!(
                        ?source_path,
                        ?ckpt,
                        owner,
                        "ETL deferring: another source owns the dest checkpoint"
                    );
                    return Ok(());
                }
                // Orphaned checkpoint: the owner committed and was deleted but
                // crashed before clearing it. Discard it and start a fresh
                // copy of this source.
                warn!(?ckpt, owner, "ETL removing orphaned dest checkpoint");
                dest.remove_etl_sidecar(&ckpt)?;
                dest.create_etl_sidecar(&ckpt, &ckpt_contents(len_before, &s_filename))?;
            }
            None => {
                // Torn (present-but-unparseable) or absent. A torn checkpoint
                // is fsync'd before any append byte, so it always predates an
                // append → D is at its true committed length; discard it and
                // re-anchor from len_before.
                if ckpt.exists() {
                    warn!(?ckpt, "ETL discarding torn dest checkpoint");
                    dest.remove_etl_sidecar(&ckpt)?;
                }
                dest.create_etl_sidecar(&ckpt, &ckpt_contents(len_before, &s_filename))?;
            }
        }

        // Carry the SOURCE partition's PayloadInfo metadata (element_count
        // for waveforms, custom field headers) into the copy. Plain
        // `append_event` uses `AppendMeta::default()`, which would stamp a
        // fresh D's header with `element_count: None` and no headers —
        // corrupting the type descriptor for waveform / field-carrying PVs.
        // (Ignored when D already has a header; only the first source into a
        // fresh D writes it.)
        let append_meta = AppendMeta {
            element_count: desc.element_count,
            headers: desc.headers.clone(),
        };

        // Bulk copy — the only genuinely NFS-hang-prone bulk work, so bound
        // it with the timeout. Abandoning it mid-append is safe: the peeked
        // first sample defines D; every subsequent sample must map to the
        // SAME D (a stray out-of-range ts would append to a second, unguarded
        // partition — refuse the move), and a partial tail past the anchor is
        // rolled back by the next retry's owner truncate.
        tokio::time::timeout(timeout, async {
            dest.append_event_with_meta(&desc.pv_name, dbr_type, &first_sample, &append_meta)
                .await?;
            while let Some(sample) = reader.next_event()? {
                if dest.file_path_for(&desc.pv_name, sample.timestamp) != d_path {
                    return Err(anyhow::anyhow!(
                        "ETL: a sample timestamp in {source_path:?} maps to a different dest \
                         partition than {d_path:?}; refusing to split the copy"
                    ));
                }
                dest.append_event_with_meta(&desc.pv_name, dbr_type, &sample, &append_meta)
                    .await?;
            }
            // Durability BEFORE the marker: flush + (under fsync_on_flush)
            // fsync D so the copy survives a crash before we commit.
            dest.flush_writes().await?;
            anyhow::Ok(())
        })
        .await
        .map_err(|_| {
            anyhow::anyhow!("ETL move_file copy timed out after {timeout:?} for {source_path:?}")
        })??;

        // Commit in strict order so any crash is recoverable without
        // duplicating or losing samples:
        //   marker → remove checkpoint → delete source → remove marker.
        // The marker means "dest durably holds S"; it MUST land before the
        // checkpoint is removed. Reversed, a crash in between would leave
        // neither marker nor checkpoint, and the next cycle would re-append S
        // from scratch (duplicate). The source delete is awaited to
        // completion (never under the copy timeout) so the unlink can't
        // detach and race a later move.
        // Certify the copy with the source's identity (byte length + mtime)
        // so a later cycle can tell THIS partition apart from a same-named one
        // a backfill re-creates, and never delete-without-copy the new data.
        // Abort (keep source) if the source can't be stat'd here: the
        // checkpoint is still present, so the next cycle's owner-retry
        // re-copies idempotently.
        let identity = source_identity(&source_path).ok_or_else(|| {
            anyhow::anyhow!(
                "ETL: cannot stat source {source_path:?} to certify the copy (keeping source)"
            )
        })?;
        source.create_etl_sidecar(&marker, &marker_contents(identity))?;
        dest.remove_etl_sidecar(&ckpt)?;

        // Slot-locked delete: remove the source under the per-PV slot lock
        // with a live-writer guard. No append can target the inode between
        // the liveness check and the unlink, and a still-dirty live partition
        // is never destroyed.
        let source_for_evict = source.clone();
        let path_for_evict = source_path.clone();
        let removed = tokio::task::spawn_blocking(move || {
            source_for_evict.remove_moved_partition(&path_for_evict)
        })
        .await;
        match removed {
            Ok(Ok(true)) => {
                source.remove_etl_sidecar(&marker).ok();
                metrics::counter!(
                    "archiver_etl_files_moved_total",
                    "source" => source_name,
                    "dest" => dest_name,
                )
                .increment(1);
                Ok(())
            }
            Ok(Ok(false)) => {
                // Still-live/dirty partition reached the mover (selection
                // should exclude it; consolidate must pause the PV first).
                // Leave BOTH source and marker: the dest copy is durable, and
                // a later cycle's marker-recovery deletes the source once it
                // rolls over to non-live. No re-copy, no loss.
                Err(anyhow::anyhow!(
                    "ETL refused to delete still-live/dirty partition {source_path:?}; \
                     dest copy is durable, source deferred to a later cycle"
                ))
            }
            Ok(Err(e)) => Err(anyhow::anyhow!(
                "ETL source delete failed for {source_path:?}: {e}"
            )),
            Err(e) => Err(anyhow::anyhow!(
                "ETL source-delete task panicked for {source_path:?}: {e}"
            )),
        }
    }
}

/// Serialize an ETL checkpoint sidecar: the dest partition's pre-append
/// byte length, then the owning source partition's filename, one per line.
fn ckpt_contents(len_before: u64, owner: &str) -> Vec<u8> {
    format!("{len_before}\n{owner}\n").into_bytes()
}

/// Identity of a source partition for the `.etl_done` certification:
/// `(byte length, mtime as nanoseconds since the epoch)`. A backfill that
/// re-creates or appends to the same-named partition changes at least one —
/// almost always both — so a marker recorded for the old partition no longer
/// matches. `None` if the path is absent or unstattable (the caller then
/// falls back to the conservative "genuine marker" behaviour).
fn source_identity(path: &Path) -> Option<(u64, u128)> {
    let m = std::fs::metadata(path).ok()?;
    let mtime = m
        .modified()
        .ok()?
        .duration_since(std::time::UNIX_EPOCH)
        .ok()?
        .as_nanos();
    Some((m.len(), mtime))
}

/// Serialize the `.etl_done` marker body: the certified source partition's
/// byte length, then its mtime nanoseconds, one per line.
fn marker_contents((len, mtime): (u64, u128)) -> Vec<u8> {
    format!("{len}\n{mtime}\n").into_bytes()
}

/// Parse an `.etl_done` marker body into the certified `(len, mtime_nanos)`.
/// `None` for a missing, empty (pre-identity), torn, or otherwise unparseable
/// marker — the caller then treats it as a genuine "copy is durable" marker
/// (delete-without-copy), matching the pre-identity behaviour.
fn read_marker_identity(path: &Path) -> Option<(u64, u128)> {
    let content = std::fs::read_to_string(path).ok()?;
    let mut lines = content.lines();
    let len: u64 = lines.next()?.trim().parse().ok()?;
    let mtime: u128 = lines.next()?.trim().parse().ok()?;
    Some((len, mtime))
}

/// Parse an ETL checkpoint sidecar into `(len_before, owner)`. Returns
/// `None` for a missing, torn, or otherwise unparseable file — the caller
/// treats that uniformly as "no valid checkpoint". Because the checkpoint
/// is fsync'd before any append byte, a torn file always predates an
/// append, so discarding it and re-anchoring from D's current length is
/// safe.
fn read_ckpt(path: &Path) -> Option<(u64, String)> {
    let content = std::fs::read_to_string(path).ok()?;
    let mut lines = content.lines();
    let len: u64 = lines.next()?.trim().parse().ok()?;
    let owner = lines.next()?.trim().to_string();
    if owner.is_empty() {
        return None;
    }
    Some((len, owner))
}

/// Per-PV ETL selection over one PV's chronologically-sorted partition
/// files. Keeps the newest `hold.max(1)` partitions in the source tier
/// and returns the oldest movable ones, at most `gather`.
///
/// The `.max(1)` is the structural live-partition guard: the newest
/// partition is the one the writer may still be appending to, so it is
/// NEVER offered for a move regardless of `hold` (including `hold == 0`).
fn select_movable(sorted: &[PathBuf], hold: usize, gather: usize) -> &[PathBuf] {
    let keep = hold.max(1);
    if sorted.len() <= keep {
        return &[];
    }
    let movable = &sorted[..sorted.len() - keep];
    &movable[..movable.len().min(gather)]
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

    fn p(part: &str) -> PathBuf {
        PathBuf::from(format!("/data/SIM/Sine:{part}.pb"))
    }

    #[test]
    fn recovery_clears_marker_only_when_source_confirmed_gone() {
        // Source gone → clear the marker.
        assert!(EtlExecutor::recovery_should_clear_marker(&Ok(Ok(true))));
        // Live/dirty → keep the marker (defer to a later cycle).
        assert!(!EtlExecutor::recovery_should_clear_marker(&Ok(Ok(false))));
        // Unlink failed → keep the marker so the next cycle retries
        // delete-only instead of re-copying (the bug this guards).
        assert!(!EtlExecutor::recovery_should_clear_marker(&Ok(Err(
            std::io::Error::other("unlink EIO")
        ))));
    }

    #[test]
    fn select_movable_keeps_newest_hold_and_excludes_live() {
        // 4 partitions, oldest→newest. hold=2 keeps the newest 2; the
        // oldest 2 are movable.
        let files = vec![
            p("2024_03_01"),
            p("2024_03_02"),
            p("2024_03_03"),
            p("2024_03_04"),
        ];
        let movable = select_movable(&files, 2, 10);
        assert_eq!(movable, &files[..2], "oldest two are movable");
        assert!(
            !movable.contains(&p("2024_03_04")),
            "newest/live partition must never be movable"
        );
    }

    #[test]
    fn select_movable_clamps_hold_zero_to_keep_live() {
        // hold=0 would move everything — but the live (newest)
        // partition must still be kept. keep == max(0,1) == 1.
        let files = vec![p("2024_03_01"), p("2024_03_02"), p("2024_03_03")];
        let movable = select_movable(&files, 0, 10);
        assert_eq!(movable, &files[..2], "all but the newest are movable");
        assert!(!movable.contains(&p("2024_03_03")));
    }

    #[test]
    fn select_movable_caps_at_gather() {
        let files = vec![
            p("2024_03_01"),
            p("2024_03_02"),
            p("2024_03_03"),
            p("2024_03_04"),
            p("2024_03_05"),
        ];
        // hold=1 leaves 4 movable, gather caps to the oldest 2.
        let movable = select_movable(&files, 1, 2);
        assert_eq!(movable, &files[..2]);
    }

    #[test]
    fn select_movable_empty_when_at_or_below_hold() {
        let files = vec![p("2024_03_01"), p("2024_03_02")];
        assert!(
            select_movable(&files, 2, 10).is_empty(),
            "<= hold partitions: nothing moves"
        );
        assert!(
            select_movable(&files, 5, 10).is_empty(),
            "fewer than hold: nothing moves"
        );
        assert!(
            select_movable(&[], 1, 10).is_empty(),
            "no partitions: nothing moves"
        );
    }

    // ---- crash-safety tests for the idempotent copy (finding #9) ----

    use crate::storage::partition::PartitionGranularity;
    use crate::types::{ArchiverSample, ArchiverValue};
    use std::time::SystemTime;

    // 2023-11-14 22:13:20 UTC — 800s into hour 22 of day 2023_11_14, so a
    // block of a few dozen seconds stays inside hour 22, while hour 22 and
    // hour 23 share the same day partition.
    const BASE_SECS: u64 = 1_700_000_000;

    fn sample_at(secs: u64, v: f64) -> ArchiverSample {
        ArchiverSample::new(
            SystemTime::UNIX_EPOCH + Duration::from_secs(secs),
            ArchiverValue::ScalarDouble(v),
        )
    }

    fn plugin(root: PathBuf, name: &str, g: PartitionGranularity) -> Arc<PlainPbStoragePlugin> {
        Arc::new(PlainPbStoragePlugin::new(name, root, g))
    }

    /// Append `samples` (all in one source partition) via the source tier
    /// and flush, returning the on-disk source partition path.
    async fn write_source_partition(
        src: &PlainPbStoragePlugin,
        pv: &str,
        samples: &[ArchiverSample],
    ) -> PathBuf {
        let dbr = samples[0].value.db_type();
        for s in samples {
            src.append_event(pv, dbr, s).await.unwrap();
        }
        src.flush_writes().await.unwrap();
        src.file_path_for(pv, samples[0].timestamp)
    }

    fn count_samples(path: &Path) -> usize {
        let mut r = PbFileReader::open(path).unwrap();
        let mut n = 0;
        while r.next_event().unwrap().is_some() {
            n += 1;
        }
        n
    }

    #[tokio::test]
    async fn move_file_fresh_copy_commits_and_cleans() {
        let tmp = tempfile::tempdir().unwrap();
        let src = plugin(tmp.path().join("sts"), "STS", PartitionGranularity::Hour);
        let dest = plugin(tmp.path().join("mts"), "MTS", PartitionGranularity::Day);
        let exec = EtlExecutor::new(src.clone(), dest.clone(), 3600, 0, 100);

        let pv = "SimpleSine";
        let samples: Vec<_> = (0..50)
            .map(|i| sample_at(BASE_SECS + i, i as f64))
            .collect();
        let s_path = write_source_partition(&src, pv, &samples).await;
        let d_path = dest.file_path_for(pv, samples[0].timestamp);
        let ckpt = d_path.with_extension("pb.etl_ckpt");
        let marker = s_path.with_extension("pb.etl_done");

        exec.move_file(&s_path).await.unwrap();

        assert_eq!(count_samples(&d_path), 50, "dest holds every sample once");
        assert!(!s_path.exists(), "source deleted after durable copy");
        assert!(!ckpt.exists(), "checkpoint cleared on commit");
        assert!(!marker.exists(), "done-marker cleared on commit");
    }

    #[tokio::test]
    async fn move_file_owner_retry_truncates_partial_not_duplicated() {
        // Simulates a crash after a prior attempt appended a PREFIX of S to
        // D and left an owner checkpoint (anchor = D's length before S = 0).
        // The retry must truncate the partial away and re-append exactly
        // once — never 10 + 50.
        let tmp = tempfile::tempdir().unwrap();
        let src = plugin(tmp.path().join("sts"), "STS", PartitionGranularity::Hour);
        let dest = plugin(tmp.path().join("mts"), "MTS", PartitionGranularity::Day);
        let exec = EtlExecutor::new(src.clone(), dest.clone(), 3600, 0, 100);

        let pv = "SimpleSine";
        let samples: Vec<_> = (0..50)
            .map(|i| sample_at(BASE_SECS + i, i as f64))
            .collect();
        let s_path = write_source_partition(&src, pv, &samples).await;
        let d_path = dest.file_path_for(pv, samples[0].timestamp);
        let ckpt = d_path.with_extension("pb.etl_ckpt");

        // Stage the failed-attempt state: a 10-sample partial in D and an
        // owner checkpoint whose anchor is D's pre-append length (0).
        let dbr = samples[0].value.db_type();
        for s in &samples[..10] {
            dest.append_event(pv, dbr, s).await.unwrap();
        }
        dest.flush_writes().await.unwrap();
        let s_filename = s_path.file_name().unwrap().to_str().unwrap();
        dest.create_etl_sidecar(&ckpt, format!("0\n{s_filename}\n").as_bytes())
            .unwrap();

        exec.move_file(&s_path).await.unwrap();

        assert_eq!(
            count_samples(&d_path),
            50,
            "partial prefix truncated, not duplicated"
        );
        assert!(!s_path.exists());
        assert!(!ckpt.exists());
    }

    #[tokio::test]
    async fn move_file_owner_retry_preserves_prior_source_truncates_only_partial() {
        // The delicate case: truncate rolls D back to a NON-ZERO anchor
        // (a prior source already committed into D). The retry must drop
        // only S's failed partial past the anchor and re-append S once,
        // leaving the prior source's samples byte-for-byte intact — never
        // truncate to 0, never duplicate.
        let tmp = tempfile::tempdir().unwrap();
        let src = plugin(tmp.path().join("sts"), "STS", PartitionGranularity::Hour);
        let dest = plugin(tmp.path().join("mts"), "MTS", PartitionGranularity::Day);
        let exec = EtlExecutor::new(src.clone(), dest.clone(), 3600, 0, 100);

        let pv = "SimpleSine";
        // A prior source (hour 22) already aggregated into the same day D;
        // S covers hour 23 and appends after it, so the anchor is > 0.
        let prior: Vec<_> = (0..25)
            .map(|i| sample_at(BASE_SECS + i, 100.0 + i as f64))
            .collect();
        let s: Vec<_> = (0..40)
            .map(|i| sample_at(BASE_SECS + 3600 + i, i as f64))
            .collect();
        let s_path = write_source_partition(&src, pv, &s).await;
        let d_path = dest.file_path_for(pv, s[0].timestamp);
        let ckpt = d_path.with_extension("pb.etl_ckpt");
        let dbr = s[0].value.db_type();

        // Commit the prior source into D, then record D's length: this is
        // the byte boundary S's checkpoint anchors to.
        for e in &prior {
            dest.append_event(pv, dbr, e).await.unwrap();
        }
        dest.flush_writes().await.unwrap();
        let anchor = std::fs::metadata(&d_path).unwrap().len();
        assert!(anchor > 0, "prior source yields a non-zero anchor");

        // Stage S's failed mid-copy: a 15-sample partial appended PAST the
        // anchor, plus S's owner checkpoint recorded at the anchor.
        for e in &s[..15] {
            dest.append_event(pv, dbr, e).await.unwrap();
        }
        dest.flush_writes().await.unwrap();
        let s_filename = s_path.file_name().unwrap().to_str().unwrap();
        dest.create_etl_sidecar(&ckpt, format!("{anchor}\n{s_filename}\n").as_bytes())
            .unwrap();

        exec.move_file(&s_path).await.unwrap();

        // 25 prior + 40 for S, each exactly once. A truncate-to-0 would
        // give 40; a non-truncated partial would give 25 + 15 + 40 = 80.
        assert_eq!(
            count_samples(&d_path),
            25 + 40,
            "partial truncated at anchor>0; prior source preserved, S once"
        );
        assert!(!s_path.exists());
        assert!(!ckpt.exists());
    }

    #[tokio::test]
    async fn move_file_defers_to_other_source_owning_checkpoint() {
        let tmp = tempfile::tempdir().unwrap();
        let src = plugin(tmp.path().join("sts"), "STS", PartitionGranularity::Hour);
        let dest = plugin(tmp.path().join("mts"), "MTS", PartitionGranularity::Day);
        let exec = EtlExecutor::new(src.clone(), dest.clone(), 3600, 0, 100);

        let pv = "SimpleSine";
        let samples: Vec<_> = (0..30)
            .map(|i| sample_at(BASE_SECS + 3600 + i, i as f64))
            .collect();
        let s_path = write_source_partition(&src, pv, &samples).await;
        let d_path = dest.file_path_for(pv, samples[0].timestamp);
        let ckpt = d_path.with_extension("pb.etl_ckpt");

        // A DIFFERENT source (still present on disk) owns the checkpoint.
        let owner_name = "SimpleSine:2023_11_14_22.pb";
        let owner_path = s_path.parent().unwrap().join(owner_name);
        std::fs::write(&owner_path, b"partial").unwrap();
        dest.create_etl_sidecar(&ckpt, format!("0\n{owner_name}\n").as_bytes())
            .unwrap();

        // Must defer: return Ok without touching D, source, or checkpoint.
        exec.move_file(&s_path).await.unwrap();

        assert!(s_path.exists(), "source kept when deferring");
        assert!(!d_path.exists(), "dest partition untouched when deferring");
        let (_, owner) = read_ckpt(&ckpt).expect("owner checkpoint intact");
        assert_eq!(owner, owner_name);
    }

    #[tokio::test]
    async fn move_file_discards_torn_checkpoint_and_copies_once() {
        let tmp = tempfile::tempdir().unwrap();
        let src = plugin(tmp.path().join("sts"), "STS", PartitionGranularity::Hour);
        let dest = plugin(tmp.path().join("mts"), "MTS", PartitionGranularity::Day);
        let exec = EtlExecutor::new(src.clone(), dest.clone(), 3600, 0, 100);

        let pv = "SimpleSine";
        let samples: Vec<_> = (0..40)
            .map(|i| sample_at(BASE_SECS + i, i as f64))
            .collect();
        let s_path = write_source_partition(&src, pv, &samples).await;
        let d_path = dest.file_path_for(pv, samples[0].timestamp);
        let ckpt = d_path.with_extension("pb.etl_ckpt");

        // Torn checkpoint: unparseable content, D not yet created. A torn
        // ckpt is fsync'd before any append, so no partial exists — the
        // mover discards it and copies fresh.
        dest.create_etl_sidecar(&ckpt, b"garbage-not-a-length")
            .unwrap();

        exec.move_file(&s_path).await.unwrap();

        assert_eq!(count_samples(&d_path), 40, "copied exactly once");
        assert!(!s_path.exists());
        assert!(!ckpt.exists());
    }

    #[tokio::test]
    async fn move_file_concurrent_same_dest_partition_no_corruption() {
        // Two sources (hour 22 + hour 23, same day → same dest partition D)
        // moved concurrently. The move gate serializes them so D ends with
        // both sources' samples, each exactly once, and no torn frames.
        let tmp = tempfile::tempdir().unwrap();
        let src = plugin(tmp.path().join("sts"), "STS", PartitionGranularity::Hour);
        let dest = plugin(tmp.path().join("mts"), "MTS", PartitionGranularity::Day);
        let exec = EtlExecutor::new(src.clone(), dest.clone(), 3600, 0, 100);

        let pv = "SimpleSine";
        let s1: Vec<_> = (0..20)
            .map(|i| sample_at(BASE_SECS + i, i as f64))
            .collect();
        let s2: Vec<_> = (0..30)
            .map(|i| sample_at(BASE_SECS + 3600 + i, i as f64))
            .collect();
        let s1_path = write_source_partition(&src, pv, &s1).await;
        let s2_path = write_source_partition(&src, pv, &s2).await;
        let d_path = dest.file_path_for(pv, s1[0].timestamp);
        assert_eq!(
            d_path,
            dest.file_path_for(pv, s2[0].timestamp),
            "both hours land in the same day partition"
        );

        let (r1, r2) = tokio::join!(exec.move_file(&s1_path), exec.move_file(&s2_path));
        r1.unwrap();
        r2.unwrap();

        assert_eq!(
            count_samples(&d_path),
            50,
            "both sources present, each once"
        );
        assert!(!s1_path.exists());
        assert!(!s2_path.exists());
        assert!(!d_path.with_extension("pb.etl_ckpt").exists());
    }

    #[tokio::test]
    async fn shared_gate_serializes_cross_executor_moves_without_loss() {
        // MTS is `STS→MTS`'s DEST and `MTS→LTS`'s SOURCE (shared plugin).
        // Without a shared move gate, STS→MTS appending to an MTS day
        // partition D can interleave with MTS→LTS reading-then-deleting D,
        // losing the appended samples. With the chain-wide gate the two
        // moves serialize, so every sample ends in exactly one of MTS-D or
        // the LTS partition — none lost, none duplicated — whichever move
        // wins the gate first.
        let tmp = tempfile::tempdir().unwrap();
        let sts = plugin(tmp.path().join("sts"), "STS", PartitionGranularity::Hour);
        let mts = plugin(tmp.path().join("mts"), "MTS", PartitionGranularity::Day);
        let lts = plugin(tmp.path().join("lts"), "LTS", PartitionGranularity::Year);

        let gate = Arc::new(tokio::sync::Mutex::new(()));
        let sts_mts = EtlExecutor::new(sts.clone(), mts.clone(), 3600, 0, 100)
            .with_shared_move_gate(gate.clone());
        let mts_lts = EtlExecutor::new(mts.clone(), lts.clone(), 3600, 0, 100)
            .with_shared_move_gate(gate.clone());

        let pv = "SimpleSine";
        // A prior day already sits in MTS as partition D (25 samples),
        // ready for MTS→LTS to move out.
        let prior: Vec<_> = (0..25)
            .map(|i| sample_at(BASE_SECS + i, 100.0 + i as f64))
            .collect();
        let dbr = prior[0].value.db_type();
        for e in &prior {
            mts.append_event(pv, dbr, e).await.unwrap();
        }
        mts.flush_writes().await.unwrap();
        let d_path = mts.file_path_for(pv, prior[0].timestamp);

        // A new STS partition (same day, hour 23) that STS→MTS appends into
        // that same MTS day partition D.
        let s_new: Vec<_> = (0..40)
            .map(|i| sample_at(BASE_SECS + 3600 + i, i as f64))
            .collect();
        let s_path = write_source_partition(&sts, pv, &s_new).await;
        assert_eq!(
            d_path,
            mts.file_path_for(pv, s_new[0].timestamp),
            "STS partition aggregates into the same MTS day D"
        );

        let (r1, r2) = tokio::join!(sts_mts.move_file(&s_path), mts_lts.move_file(&d_path));
        r1.unwrap();
        r2.unwrap();

        // Conservation: 25 prior + 40 new survive, each once, spread across
        // whatever MTS-D remains plus the LTS partition. Interleaved loss
        // would read < 65; a double-copy would read > 65.
        let l_path = lts.file_path_for(pv, prior[0].timestamp);
        let count = |p: &Path| if p.exists() { count_samples(p) } else { 0 };
        assert_eq!(
            count(&d_path) + count(&l_path),
            25 + 40,
            "no sample lost or duplicated across the cross-executor race"
        );
        assert!(!s_path.exists(), "STS source consumed");
    }

    #[tokio::test]
    async fn move_file_recovery_clears_owned_dest_checkpoint() {
        // Crash between commit steps marker(453) and remove-ckpt(454): the
        // dest durably holds S (D fully copied), the marker is present, and
        // S's owned checkpoint on D was NOT yet removed. Recovery must
        // delete the source, clear the marker, AND clear its own stale
        // checkpoint — without leaning on a later move into D to reap it.
        let tmp = tempfile::tempdir().unwrap();
        let src = plugin(tmp.path().join("sts"), "STS", PartitionGranularity::Hour);
        let dest = plugin(tmp.path().join("mts"), "MTS", PartitionGranularity::Day);
        let exec = EtlExecutor::new(src.clone(), dest.clone(), 3600, 0, 100);

        let pv = "SimpleSine";
        let samples: Vec<_> = (0..30)
            .map(|i| sample_at(BASE_SECS + i, i as f64))
            .collect();
        let s_path = write_source_partition(&src, pv, &samples).await;
        let d_path = dest.file_path_for(pv, samples[0].timestamp);
        let ckpt = d_path.with_extension("pb.etl_ckpt");
        let marker = s_path.with_extension("pb.etl_done");

        // Stage the crash state: D fully holds S, marker + owned ckpt present.
        let dbr = samples[0].value.db_type();
        for s in &samples {
            dest.append_event(pv, dbr, s).await.unwrap();
        }
        dest.flush_writes().await.unwrap();
        let s_filename = s_path.file_name().unwrap().to_str().unwrap();
        dest.create_etl_sidecar(&ckpt, format!("0\n{s_filename}\n").as_bytes())
            .unwrap();
        src.create_etl_sidecar(&marker, b"").unwrap();

        exec.move_file(&s_path).await.unwrap();

        assert!(!s_path.exists(), "recovery deletes the source");
        assert!(!marker.exists(), "recovery clears the marker");
        assert!(!ckpt.exists(), "recovery clears its own dest checkpoint");
        assert_eq!(
            count_samples(&d_path),
            30,
            "dest copy untouched by recovery"
        );
    }

    #[tokio::test]
    async fn move_file_recovery_leaves_other_sources_checkpoint() {
        // Recovery of S must NOT remove a checkpoint on D owned by a
        // DIFFERENT source (a legitimate in-flight append by that source).
        let tmp = tempfile::tempdir().unwrap();
        let src = plugin(tmp.path().join("sts"), "STS", PartitionGranularity::Hour);
        let dest = plugin(tmp.path().join("mts"), "MTS", PartitionGranularity::Day);
        let exec = EtlExecutor::new(src.clone(), dest.clone(), 3600, 0, 100);

        let pv = "SimpleSine";
        let samples: Vec<_> = (0..30)
            .map(|i| sample_at(BASE_SECS + i, i as f64))
            .collect();
        let s_path = write_source_partition(&src, pv, &samples).await;
        let d_path = dest.file_path_for(pv, samples[0].timestamp);
        let ckpt = d_path.with_extension("pb.etl_ckpt");
        let marker = s_path.with_extension("pb.etl_done");

        let dbr = samples[0].value.db_type();
        for s in &samples {
            dest.append_event(pv, dbr, s).await.unwrap();
        }
        dest.flush_writes().await.unwrap();
        // Checkpoint owned by a different (hour-23) source of the same PV/day.
        let other_owner = "SimpleSine:2023_11_14_23.pb";
        dest.create_etl_sidecar(&ckpt, format!("0\n{other_owner}\n").as_bytes())
            .unwrap();
        src.create_etl_sidecar(&marker, b"").unwrap();

        exec.move_file(&s_path).await.unwrap();

        assert!(!s_path.exists());
        assert!(!marker.exists());
        let (_, owner) = read_ckpt(&ckpt).expect("other source's checkpoint intact");
        assert_eq!(owner, other_owner, "recovery must not touch another owner");
    }

    #[test]
    #[should_panic(expected = "coarser-or-equal")]
    fn new_rejects_dest_finer_than_source() {
        let tmp = tempfile::tempdir().unwrap();
        let src = plugin(tmp.path().join("s"), "STS", PartitionGranularity::Day);
        let dest = plugin(tmp.path().join("d"), "MTS", PartitionGranularity::Hour);
        let _ = EtlExecutor::new(src, dest, 3600, 0, 100);
    }

    #[tokio::test]
    async fn move_file_defers_partition_with_pending_inbound_checkpoint() {
        // finding B: an MTS→LTS move must NOT read+delete an MTS partition
        // that a finer STS→MTS copy is still aggregating into. Doing so
        // promotes the uncommitted partial tail and strands the STS→MTS
        // owner-retry on `truncate_partition(<absent D>, anchor>0)` forever.
        // The partition's own inbound `.etl_ckpt` makes it ineligible until
        // the finer tier clears it.
        let tmp = tempfile::tempdir().unwrap();
        let src = plugin(tmp.path().join("mts"), "MTS", PartitionGranularity::Day);
        let dest = plugin(tmp.path().join("lts"), "LTS", PartitionGranularity::Year);
        let exec = EtlExecutor::new(src.clone(), dest.clone(), 3600, 0, 100);

        let pv = "SimpleSine";
        let samples: Vec<_> = (0..30)
            .map(|i| sample_at(BASE_SECS + i, i as f64))
            .collect();
        let s_path = write_source_partition(&src, pv, &samples).await;
        let d_path = dest.file_path_for(pv, samples[0].timestamp);

        // Stage a pending inbound checkpoint on the MTS partition, owned by an
        // in-flight STS (hour) source aggregating into it.
        let inbound_ckpt = s_path.with_extension("pb.etl_ckpt");
        src.create_etl_sidecar(&inbound_ckpt, b"0\nSimpleSine:2023_11_14_22.pb\n")
            .unwrap();

        // Move must DEFER: partition untouched, nothing promoted to LTS.
        exec.move_file(&s_path).await.unwrap();
        assert!(s_path.exists(), "deferred: MTS partition not deleted");
        assert_eq!(count_samples(&s_path), 30, "deferred: partition unchanged");
        assert!(!d_path.exists(), "deferred: nothing written to LTS");
        assert!(
            inbound_ckpt.exists(),
            "deferred: inbound checkpoint left for the finer tier"
        );

        // Finer tier completes and clears its checkpoint → the partition is
        // now eligible and the move proceeds.
        src.remove_etl_sidecar(&inbound_ckpt).unwrap();
        exec.move_file(&s_path).await.unwrap();
        assert!(!s_path.exists(), "moved: MTS partition deleted");
        assert_eq!(
            count_samples(&d_path),
            30,
            "moved: every sample in LTS once"
        );
    }

    #[tokio::test]
    async fn move_file_aborts_on_unreadable_dest_metadata_not_anchoring_zero() {
        // finding C: a non-NotFound stat error on D must NOT be coerced to a
        // 0 anchor. Only a genuinely absent D anchors at 0; an I/O error
        // aborts the move and keeps the source, so no false-0 checkpoint can
        // drive a later `truncate_partition(D, 0)` that wipes prior data.
        let tmp = tempfile::tempdir().unwrap();
        let src = plugin(tmp.path().join("sts"), "STS", PartitionGranularity::Hour);
        let dest = plugin(tmp.path().join("mts"), "MTS", PartitionGranularity::Day);
        let exec = EtlExecutor::new(src.clone(), dest.clone(), 3600, 0, 100);

        let pv = "SimpleSine";
        let samples: Vec<_> = (0..10)
            .map(|i| sample_at(BASE_SECS + i, i as f64))
            .collect();
        let s_path = write_source_partition(&src, pv, &samples).await;
        let d_path = dest.file_path_for(pv, samples[0].timestamp);

        // Force `metadata(d_path)` to fail with a NON-NotFound error: put a
        // regular FILE where D's parent directory should be → ENOTDIR on stat.
        let d_parent = d_path.parent().unwrap();
        std::fs::create_dir_all(d_parent.parent().unwrap()).unwrap();
        std::fs::write(d_parent, b"not a directory").unwrap();

        let err = exec.move_file(&s_path).await.unwrap_err();
        assert!(
            err.to_string().contains("cannot stat dest partition"),
            "aborted at the anchor stat, not coerced to 0: {err}"
        );
        assert!(
            s_path.exists(),
            "source kept when D length can't be trusted"
        );
    }

    #[tokio::test]
    async fn move_file_recopies_when_marker_is_stale_from_backfill() {
        // finding A: a leaked `.etl_done` marker + a backfill that re-creates
        // the same-named partition must NOT delete the new data without
        // copying. The marker records the certified partition's identity; a
        // mismatch against the current source means it was re-created, so the
        // move re-copies (idempotently) instead of deleting uncopied.
        let tmp = tempfile::tempdir().unwrap();
        let src = plugin(tmp.path().join("sts"), "STS", PartitionGranularity::Hour);
        let dest = plugin(tmp.path().join("mts"), "MTS", PartitionGranularity::Day);
        let exec = EtlExecutor::new(src.clone(), dest.clone(), 3600, 0, 100);

        let pv = "SimpleSine";
        // Original partition (20 samples), already durable in D and certified.
        let original: Vec<_> = (0..20)
            .map(|i| sample_at(BASE_SECS + i, i as f64))
            .collect();
        let s_path = write_source_partition(&src, pv, &original).await;
        let d_path = dest.file_path_for(pv, original[0].timestamp);
        let marker = s_path.with_extension("pb.etl_done");

        // D holds the 20 originals (as if S was already copied out).
        let dbr = original[0].value.db_type();
        for e in &original {
            dest.append_event(pv, dbr, e).await.unwrap();
        }
        dest.flush_writes().await.unwrap();

        // Leak a marker certifying the ORIGINAL partition's identity.
        let original_identity = source_identity(&s_path).expect("original source stattable");
        src.create_etl_sidecar(&marker, &marker_contents(original_identity))
            .unwrap();

        // A backfill re-creates the same-named partition with NEW samples not
        // yet in D. Evict the cached writer + unlink first so a fresh file
        // (new inode, new len/mtime) is created at the same path.
        assert!(src.remove_moved_partition(&s_path).unwrap());
        let backfill: Vec<_> = (0..5)
            .map(|i| sample_at(BASE_SECS + 100 + i, 900.0 + i as f64))
            .collect();
        let s_path2 = write_source_partition(&src, pv, &backfill).await;
        assert_eq!(
            s_path2, s_path,
            "backfill re-creates the same partition path"
        );

        exec.move_file(&s_path).await.unwrap();

        assert!(
            !s_path.exists(),
            "backfill partition moved out, not left behind"
        );
        assert!(!marker.exists(), "stale marker cleared");
        assert_eq!(
            count_samples(&d_path),
            20 + 5,
            "backfill copied into D — no delete-without-copy loss"
        );
    }

    #[tokio::test]
    async fn move_file_marker_recovery_deletes_when_identity_matches() {
        // Complement to the stale case: a genuine leaked marker (identity
        // matches the current source) still takes the delete-without-copy
        // recovery path — never a re-copy that would duplicate.
        let tmp = tempfile::tempdir().unwrap();
        let src = plugin(tmp.path().join("sts"), "STS", PartitionGranularity::Hour);
        let dest = plugin(tmp.path().join("mts"), "MTS", PartitionGranularity::Day);
        let exec = EtlExecutor::new(src.clone(), dest.clone(), 3600, 0, 100);

        let pv = "SimpleSine";
        let samples: Vec<_> = (0..20)
            .map(|i| sample_at(BASE_SECS + i, i as f64))
            .collect();
        let s_path = write_source_partition(&src, pv, &samples).await;
        let d_path = dest.file_path_for(pv, samples[0].timestamp);
        let marker = s_path.with_extension("pb.etl_done");

        // D already durably holds the copy.
        let dbr = samples[0].value.db_type();
        for e in &samples {
            dest.append_event(pv, dbr, e).await.unwrap();
        }
        dest.flush_writes().await.unwrap();

        // A genuine marker certifying THIS still-present source.
        let identity = source_identity(&s_path).unwrap();
        src.create_etl_sidecar(&marker, &marker_contents(identity))
            .unwrap();

        exec.move_file(&s_path).await.unwrap();

        assert!(!s_path.exists(), "genuine marker: source deleted");
        assert!(!marker.exists(), "genuine marker: cleared after delete");
        assert_eq!(
            count_samples(&d_path),
            20,
            "genuine marker: no re-copy, samples not duplicated"
        );
    }

    #[tokio::test]
    async fn move_file_preserves_source_payload_metadata_into_fresh_dest() {
        // finding D: the copy must carry the source PayloadInfo's
        // element_count (waveforms) and custom field headers into a fresh
        // dest partition header. Plain `append_event` defaults them,
        // corrupting the type descriptor for waveform / field-carrying PVs.
        let tmp = tempfile::tempdir().unwrap();
        let src = plugin(tmp.path().join("sts"), "STS", PartitionGranularity::Hour);
        let dest = plugin(tmp.path().join("mts"), "MTS", PartitionGranularity::Day);
        let exec = EtlExecutor::new(src.clone(), dest.clone(), 3600, 0, 100);

        let pv = "WaveformPV";
        let meta = AppendMeta {
            element_count: Some(3),
            headers: vec![("EGU".to_string(), "mm".to_string())],
        };
        let samples: Vec<_> = (0..8).map(|i| sample_at(BASE_SECS + i, i as f64)).collect();
        let dbr = samples[0].value.db_type();
        for s in &samples {
            src.append_event_with_meta(pv, dbr, s, &meta).await.unwrap();
        }
        src.flush_writes().await.unwrap();
        let s_path = src.file_path_for(pv, samples[0].timestamp);
        let d_path = dest.file_path_for(pv, samples[0].timestamp);

        // Sanity: the source header round-trips the metadata.
        let src_desc = PbFileReader::open(&s_path).unwrap().description().clone();
        assert_eq!(src_desc.element_count, Some(3));
        assert_eq!(
            src_desc.headers,
            vec![("EGU".to_string(), "mm".to_string())]
        );

        exec.move_file(&s_path).await.unwrap();

        // The fresh dest header must carry the SAME metadata, not defaults.
        let dst_desc = PbFileReader::open(&d_path).unwrap().description().clone();
        assert_eq!(
            dst_desc.element_count,
            Some(3),
            "element_count preserved into fresh dest header"
        );
        assert_eq!(
            dst_desc.headers,
            vec![("EGU".to_string(), "mm".to_string())],
            "field headers preserved into fresh dest header"
        );
    }
}
