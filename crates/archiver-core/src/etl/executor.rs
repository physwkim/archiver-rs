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

        // Reap terminal orphan checkpoints before moving. A checkpoint whose
        // owning source has been deleted (a crash in the commit's
        // delete-source → remove-checkpoint window) is otherwise never cleared
        // once its dest partition is terminal (no further source aggregates
        // into it), and the NEXT tier's `move_file` defers on that checkpoint
        // forever — a permanent migration wedge. This executor is the correct
        // owner of the sweep: it holds BOTH the tier that carries the
        // checkpoint (its dest) and the tier that holds the owner (its source),
        // so it alone can verify owner-gone. The next tier cannot (it does not
        // know where the finer tier lives).
        self.reap_orphan_checkpoints(source_root).await?;

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

    /// Remove orphan checkpoints in the DEST tier whose owning source (in the
    /// SOURCE tier) no longer exists — the terminal-partition wedge fix.
    ///
    /// Held under the move gate so it never races a `move_file` mid-establishing
    /// or mid-removing a checkpoint: while a move holds the gate its owner
    /// source is present, so a gate-held reap only ever sees genuinely orphaned
    /// (owner-gone) or torn checkpoints. A parseable checkpoint whose owner is
    /// still on disk is an in-flight aggregation and is left untouched.
    ///
    /// `source_root` is passed in (already confirmed to exist by the caller) so
    /// the owner's path can be reconstructed by mirroring the checkpoint's
    /// dest-relative sub-directory into the source tier — both tiers lay a PV's
    /// partitions out under the same relative path.
    async fn reap_orphan_checkpoints(&self, source_root: &Path) -> anyhow::Result<()> {
        let _gate = self.move_gate.lock().await;

        let dest_root = self.dest.root_folder();
        for ckpt_path in list_ckpt_files(dest_root)? {
            let ck = match read_ckpt(&ckpt_path) {
                Some(ck) => ck,
                None => {
                    // Torn (crashed mid-create, so it predates any append —
                    // D is at its committed length). Remove it: a torn
                    // checkpoint on a terminal partition wedges the next tier
                    // just as an orphan does.
                    warn!(?ckpt_path, "ETL reaping torn dest checkpoint");
                    self.dest.remove_etl_sidecar(&ckpt_path)?;
                    continue;
                }
            };

            // Reconstruct the owner's path in the source tier by mirroring the
            // checkpoint's sub-directory (relative to the dest root) under the
            // source root.
            let owner_path = match ckpt_path
                .parent()
                .and_then(|p| p.strip_prefix(dest_root).ok())
            {
                Some(rel) => source_root.join(rel).join(&ck.owner),
                None => source_root.join(&ck.owner),
            };
            if owner_path.exists() {
                // In-flight aggregation by a still-present source: leave it.
                continue;
            }

            // Orphan: the owner is gone. A `committed` tail is final — just
            // remove the checkpoint. A `copying` orphan can only arise from an
            // out-of-model external deletion of the source mid-copy; its tail
            // past the anchor is a partial, so roll D back before removing so no
            // uncommitted frames linger. `<D>` is the checkpoint path with the
            // `.etl_ckpt` extension stripped.
            if ck.state == CkptState::Copying {
                let d_path = ckpt_path.with_extension("");
                let dest = self.dest.clone();
                let anchor = ck.anchor;
                tokio::task::spawn_blocking(move || dest.truncate_partition(&d_path, anchor))
                    .await??;
            }
            warn!(
                ?ckpt_path,
                owner = ck.owner,
                "ETL reaping orphaned dest checkpoint (owner source gone)"
            );
            self.dest.remove_etl_sidecar(&ckpt_path)?;
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
    /// the source either fully migrated or untouched. Returns the number of
    /// files ACTUALLY moved out — not the number attempted: a file `move_file`
    /// defers (pending inbound checkpoint, another source owning the dest
    /// checkpoint, or grown/live since the copy) stays in the source tier and
    /// is not counted, so the operator is never told a deferred file migrated.
    pub async fn consolidate_pv(&self, pv: &str) -> anyhow::Result<u64> {
        // Flush any buffered writes for the source tier so we move
        // everything that's been written so far.
        self.source.flush_writes().await?;

        let pv_files =
            crate::storage::plainpb::list_pv_pb_files_pub(self.source.root_folder(), pv)?;
        let attempted = pv_files.len();
        info!(
            pv,
            attempted,
            source = self.source.name(),
            dest = self.dest.name(),
            "Consolidating PV files",
        );
        let mut moved = 0u64;
        for file in &pv_files {
            match self.move_file(file).await {
                Ok(true) => moved += 1,
                Ok(false) => {
                    debug!(?file, "consolidate_pv: deferred, left in source tier");
                }
                Err(e) => {
                    warn!(?file, "consolidate_pv: failed to move file: {e}");
                    return Err(e);
                }
            }
        }
        Ok(moved)
    }

    /// Slot-locked, length-gated delete of a moved source partition, awaited
    /// to completion (never under the copy timeout) so the unlink can't
    /// detach past the gate. `Ok(true)` = removed (durably copied and
    /// byte-for-byte unchanged since the copy read it); `Ok(false)` = KEPT
    /// because it is still live/dirty or a concurrent backfill grew it past
    /// `copied_len` — the caller leaves its checkpoint and re-copies next
    /// cycle.
    async fn delete_moved_source(
        source: &Arc<PlainPbStoragePlugin>,
        path: &Path,
        copied_len: u64,
    ) -> anyhow::Result<bool> {
        let source = source.clone();
        let owned = path.to_path_buf();
        let removed = tokio::task::spawn_blocking(move || {
            source.remove_moved_partition_if_len(&owned, copied_len)
        })
        .await;
        match removed {
            Ok(Ok(outcome)) => Ok(outcome),
            Ok(Err(e)) => Err(anyhow::anyhow!(
                "ETL source delete failed for {path:?}: {e}"
            )),
            Err(e) => Err(anyhow::anyhow!(
                "ETL source-delete task panicked for {path:?}: {e}"
            )),
        }
    }

    /// Move a single PB file from source to destination tier.
    ///
    /// Idempotent by construction: the copy anchors on an owner-stamped
    /// `<D>.pb.etl_ckpt` checkpoint that survives until the source is
    /// deleted, so any retry truncates D back to the anchor and REPLACES
    /// this source's contribution rather than appending a second copy. The
    /// source is deleted LAST and only if it is byte-for-byte the partition
    /// that was copied (a concurrent backfill that grew it is refused), then
    /// the checkpoint is removed.
    ///
    /// Returns `Ok(true)` when the source was actually moved out (copied +
    /// deleted) and `Ok(false)` when the move was DEFERRED — a pending inbound
    /// checkpoint, another source owning the dest checkpoint, or a source that
    /// grew / stayed live since the copy. A deferred file is left in the source
    /// tier for a later cycle; callers that report progress must not count it
    /// as moved.
    async fn move_file(&self, source_path: &Path) -> anyhow::Result<bool> {
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
        // (permanent wedge). Defer — the finer tier commits (deleting this
        // partition itself) or its checkpoint is reaped, then a later cycle
        // moves the partition out cleanly. No-op for the finest tier (nothing
        // feeds it via ETL, so it never carries an inbound checkpoint).
        let inbound_ckpt = source_path.with_extension("pb.etl_ckpt");
        if inbound_ckpt.exists() {
            debug!(
                ?source_path,
                ?inbound_ckpt,
                "ETL deferring: partition has a pending inbound checkpoint"
            );
            return Ok(false);
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
        // The source length as the copy reads it. The commit deletes the
        // source only if it is STILL exactly this long — proof that D holds
        // every byte we copied and no uncopied tail (a concurrent backfill
        // append into this old partition) grew it since. A grown source is
        // refused at the delete and the checkpoint drives a truncate + full
        // re-copy next cycle, so a late-arriving sample can't be deleted
        // uncopied.
        let copied_len = std::fs::metadata(&source_path)?.len();
        let desc = reader.description().clone();
        let dbr_type = desc.db_type;

        // Peek the first sample to locate the single destination partition D.
        // ETL only moves finer→coarser and each sample routes by its own
        // timestamp, so every sample in this source partition nests in the D
        // derived from the first (the construction-time granularity guard
        // makes that hold; the per-sample check below catches a stray
        // out-of-range ts).
        let Some(first_sample) = reader.next_event()? else {
            // Empty source partition (header only) — nothing reached D. Delete
            // it, but still length-gated on `copied_len` so a backfill that
            // raced real samples in since the open is not deleted uncopied (it
            // fails the gate and is re-processed, now non-empty, next cycle).
            // The delete's own outcome IS the move outcome: removed ⇒ moved
            // out, refused (raced backfill) ⇒ deferred.
            let removed = Self::delete_moved_source(&source, &source_path, copied_len).await?;
            return Ok(removed);
        };

        let d_path = dest.file_path_for(&desc.pv_name, first_sample.timestamp);
        let ckpt = d_path.with_extension("pb.etl_ckpt");
        let s_filename = source_path
            .file_name()
            .and_then(|n| n.to_str())
            .ok_or_else(|| anyhow::anyhow!("ETL: source path has no filename: {source_path:?}"))?
            .to_string();
        // btime identity of THIS source instance (see `source_btime_nanos`),
        // stamped into the checkpoint so a later cycle can tell an in-flight
        // copy of this file apart from a same-named partition a backfill
        // re-created after this one was moved out.
        let s_btime = source_btime_nanos(&source_path);

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

        // Establish or resume this source's checkpoint on D and settle on the
        // `anchor` this copy appends from. A retry rolls D back to `anchor` and
        // re-appends, so the copy is idempotent by construction. Invariant:
        // `<D>.pb.etl_ckpt` present ⟺ a copy of `owner` into D is in progress
        // (`Copying`) or finished-but-pending-delete (`Committed`).
        let anchor = match read_ckpt(&ckpt) {
            Some(ck) if ck.owner == s_filename => {
                // `Copying` proves the tail is a partial copy of THIS same
                // instance (the source is deleted only after the `Committed`
                // transition, so a `Copying` checkpoint means it was never
                // deleted). `Committed` + btime match is likewise this
                // instance's own final tail — re-copying it is idempotent.
                // Either way rolling D back to the stored anchor is safe on ANY
                // filesystem; btime is not load-bearing here.
                let same_instance = matches!((ck.btime, s_btime), (Some(a), Some(b)) if a == b);
                if ck.state == CkptState::Copying || same_instance {
                    // Roll D back to the stored anchor (discarding the partial
                    // or this-instance tail) and re-append below. KEEP the
                    // checkpoint's anchor — it is the pre-append length and must
                    // NOT be re-measured. Awaited to completion (never under the
                    // copy timeout) so this truncate can't detach and fire after
                    // the gate is released.
                    let dest_for_trunc = dest.clone();
                    let d_for_trunc = d_path.clone();
                    let trunc_anchor = ck.anchor;
                    tokio::task::spawn_blocking(move || {
                        dest_for_trunc.truncate_partition(&d_for_trunc, trunc_anchor)
                    })
                    .await??;
                    ck.anchor
                } else {
                    // `Committed` + (btime mismatch, or btime unavailable →
                    // treated conservatively as re-created): the tail may belong
                    // to a deleted/older instance, so it must NEVER be truncated
                    // — that truncate is the btime-less silent loss this design
                    // removes. Preserve the committed tail and anchor a FRESH
                    // copy of the current source AFTER it. On a filesystem
                    // without btime a same-instance crash here yields a
                    // recoverable duplicate, never loss. (No append has happened
                    // yet, so re-establishing the checkpoint is crash-safe.)
                    warn!(
                        ?ckpt,
                        owner = ck.owner,
                        "ETL committed-checkpoint owner re-created (or btime unavailable); \
                         preserving its committed tail and re-anchoring"
                    );
                    dest.remove_etl_sidecar(&ckpt)?;
                    dest.create_etl_sidecar(
                        &ckpt,
                        &ckpt_contents(CkptState::Copying, len_before, &s_filename, s_btime),
                    )?;
                    len_before
                }
            }
            Some(ck) => {
                // A DIFFERENT source owns an in-flight append to D.
                let owner_src = source_path
                    .parent()
                    .map(|p| p.join(&ck.owner))
                    .unwrap_or_else(|| PathBuf::from(&ck.owner));
                if owner_src.exists() {
                    // The owner will finish or retry its own copy; appending
                    // now would interleave two sources into D. Defer — a later
                    // cycle retries this source once the owner clears its
                    // checkpoint.
                    debug!(
                        ?source_path,
                        ?ckpt,
                        owner = ck.owner,
                        "ETL deferring: another source owns the dest checkpoint"
                    );
                    return Ok(false);
                }
                // Orphaned checkpoint: the owner committed and was deleted but
                // crashed before clearing it. Its committed tail in D is final;
                // discard the checkpoint and start a fresh copy of THIS source
                // AFTER it (anchor at D's current length).
                warn!(
                    ?ckpt,
                    owner = ck.owner,
                    "ETL removing orphaned dest checkpoint"
                );
                dest.remove_etl_sidecar(&ckpt)?;
                dest.create_etl_sidecar(
                    &ckpt,
                    &ckpt_contents(CkptState::Copying, len_before, &s_filename, s_btime),
                )?;
                len_before
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
                dest.create_etl_sidecar(
                    &ckpt,
                    &ckpt_contents(CkptState::Copying, len_before, &s_filename, s_btime),
                )?;
                len_before
            }
        };

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
            // Durability BEFORE the commit: flush + (under fsync_on_flush)
            // fsync D so the copy survives a crash before we delete the source.
            dest.flush_writes().await?;
            anyhow::Ok(())
        })
        .await
        .map_err(|_| {
            anyhow::anyhow!("ETL move_file copy timed out after {timeout:?} for {source_path:?}")
        })??;

        // The copy is durable in D. Transition the checkpoint
        // `copying → committed` ATOMICALLY (write-temp + rename): the tail
        // `[anchor..]` is now final, so from here NO retry will ever truncate
        // it. This step MUST be crash-atomic — a torn or vanished checkpoint at
        // this instant would let a retry read `None`/`Copying` and re-copy the
        // already-committed tail (a duplicate).
        dest.overwrite_etl_sidecar(
            &ckpt,
            &ckpt_contents(CkptState::Committed, anchor, &s_filename, s_btime),
        )?;

        // Commit. The copy is durable and the checkpoint is `committed`. Two
        // steps, in this order:
        //
        //   1. Delete the source — LAST, and only if it is byte-for-byte the
        //      partition we copied (length-gated on `copied_len`, under the
        //      per-PV slot lock). A concurrent backfill that grew it is
        //      refused here, so a late sample is never deleted uncopied; the
        //      checkpoint then drives a truncate + full re-copy next cycle.
        //   2. Remove the checkpoint — only AFTER the source is gone. While
        //      the source is on disk the checkpoint is the SOLE idempotency
        //      guard, so a crash between the delete and this removal just
        //      leaves a `committed` orphan: reaped by the next move into D
        //      (owner file gone), by the forward tier's orphan sweep, or — if
        //      the source is re-created first — preserved (its committed tail
        //      is never truncated). Either way no loss (a btime-less re-create
        //      may cost a recoverable duplicate).
        //
        // Awaited to completion (never under the copy timeout) so the unlink
        // can't detach and race a later move.
        if !Self::delete_moved_source(&source, &source_path, copied_len).await? {
            // Still live/dirty, or grew an uncopied tail since the copy. Keep
            // BOTH source and checkpoint: the durable copy stays anchored, and
            // the next cycle rolls D back to the anchor and re-copies the
            // (possibly grown) source. Not an error — an expected transient,
            // like the other defer paths above.
            debug!(
                ?source_path,
                "ETL deferring source delete: live/dirty or grew since the copy"
            );
            return Ok(false);
        }
        dest.remove_etl_sidecar(&ckpt)?;
        metrics::counter!(
            "archiver_etl_files_moved_total",
            "source" => source_name,
            "dest" => dest_name,
        )
        .increment(1);
        Ok(true)
    }
}

/// The lifecycle phase of an ETL dest checkpoint. The explicit phase is what
/// lets a retry decide whether the tail `[anchor..]` in D may be truncated
/// WITHOUT depending on the filesystem exposing a birth-time.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum CkptState {
    /// A copy of `owner` into D is in progress; the tail `[anchor..]` is a
    /// PARTIAL copy. A `Copying` checkpoint can only exist while its source is
    /// still on disk (the source is deleted only after the `Committed`
    /// transition), so its tail is provably a partial copy of the SAME source
    /// instance — truncating back to `anchor` is always safe, on any
    /// filesystem, no btime needed.
    Copying,
    /// The copy of `owner` FINISHED and is durable in D; the tail `[anchor..]`
    /// is FINAL and must NEVER be truncated. btime (when available) only
    /// decides, as an optimization, whether a still-present same-named source
    /// is this same instance or a re-created backfill.
    Committed,
}

impl CkptState {
    fn as_str(self) -> &'static str {
        match self {
            CkptState::Copying => "copying",
            CkptState::Committed => "committed",
        }
    }

    fn parse(s: &str) -> Option<Self> {
        match s {
            "copying" => Some(CkptState::Copying),
            "committed" => Some(CkptState::Committed),
            _ => None,
        }
    }
}

/// A parsed ETL dest checkpoint.
struct Ckpt {
    state: CkptState,
    /// D's byte length BEFORE `owner`'s contribution — the rollback point.
    anchor: u64,
    /// The owning source partition's filename.
    owner: String,
    /// The owner's birth-time (btime) in ns, or `None` when the filesystem
    /// records no creation time.
    btime: Option<u128>,
}

/// Serialize an ETL checkpoint sidecar, one field per line:
///   1. `state` — `copying` or `committed` (see [`CkptState`]),
///   2. `anchor` — the dest partition's byte length BEFORE this source's
///      contribution (the rollback point),
///   3. `owner` — the owning source partition's filename,
///   4. `btime` — the owner's birth-time in nanoseconds since the epoch, or
///      empty when the filesystem records no creation time.
///
/// btime distinguishes THIS source instance from a same-named partition a
/// backfill later re-creates: it changes on delete + re-create but NOT on a
/// plain append. Under the explicit `state` it is only an optimization (it
/// never gates the safety of a truncate — the `state` does), so a filesystem
/// that omits it degrades to a recoverable duplicate in a narrow window, never
/// silent loss.
fn ckpt_contents(state: CkptState, anchor: u64, owner: &str, owner_btime: Option<u128>) -> Vec<u8> {
    let btime = owner_btime.map(|b| b.to_string()).unwrap_or_default();
    format!("{}\n{anchor}\n{owner}\n{btime}\n", state.as_str()).into_bytes()
}

/// Birth-time (btime) of a partition file in nanoseconds since the epoch, or
/// `None` if the path is absent/unstattable or the filesystem records no
/// creation time. Unlike mtime, btime does NOT change when a file is merely
/// appended to — only when it is deleted and re-created — so it is the signal
/// a stale checkpoint uses to tell a re-created owner from an in-flight copy.
fn source_btime_nanos(path: &Path) -> Option<u128> {
    let created = std::fs::metadata(path).ok()?.created().ok()?;
    Some(
        created
            .duration_since(std::time::UNIX_EPOCH)
            .ok()?
            .as_nanos(),
    )
}

/// Parse an ETL checkpoint sidecar into a [`Ckpt`]. `btime` is `None` when the
/// recording filesystem lacked btime. Returns `None` for a missing, torn, or
/// otherwise unparseable file (including an unrecognized state token) — the
/// caller treats that uniformly as "no valid checkpoint". Because the
/// checkpoint's establishment is fsync'd before any append byte, a torn file
/// always predates an append, so discarding it and re-anchoring from D's
/// current length is safe.
fn read_ckpt(path: &Path) -> Option<Ckpt> {
    let content = std::fs::read_to_string(path).ok()?;
    let mut lines = content.lines();
    let state = CkptState::parse(lines.next()?.trim())?;
    let anchor: u64 = lines.next()?.trim().parse().ok()?;
    let owner = lines.next()?.trim().to_string();
    if owner.is_empty() {
        return None;
    }
    let btime = lines.next().and_then(|l| l.trim().parse::<u128>().ok());
    Some(Ckpt {
        state,
        anchor,
        owner,
        btime,
    })
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

/// Recursively list all `.pb.etl_ckpt` checkpoint sidecars under a directory.
/// Matches the `etl_ckpt` extension only, so the `.etl_ckpt.tmp` scratch file
/// of an in-flight atomic overwrite (extension `tmp`) is never returned.
fn list_ckpt_files(dir: &Path) -> anyhow::Result<Vec<PathBuf>> {
    let mut files = Vec::new();
    if dir.is_dir() {
        for entry in std::fs::read_dir(dir)? {
            let entry = entry?;
            let path = entry.path();
            if path.is_dir() {
                files.extend(list_ckpt_files(&path)?);
            } else if path.extension().and_then(|e| e.to_str()) == Some("etl_ckpt") {
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

        exec.move_file(&s_path).await.unwrap();

        assert_eq!(count_samples(&d_path), 50, "dest holds every sample once");
        assert!(!s_path.exists(), "source deleted after durable copy");
        assert!(!ckpt.exists(), "checkpoint cleared on commit");
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

        // Stage the failed-attempt state: a 10-sample partial in D and a
        // `copying` owner checkpoint whose anchor is D's pre-append length (0).
        let dbr = samples[0].value.db_type();
        for s in &samples[..10] {
            dest.append_event(pv, dbr, s).await.unwrap();
        }
        dest.flush_writes().await.unwrap();
        let s_filename = s_path.file_name().unwrap().to_str().unwrap();
        dest.create_etl_sidecar(
            &ckpt,
            &ckpt_contents(
                CkptState::Copying,
                0,
                s_filename,
                source_btime_nanos(&s_path),
            ),
        )
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
        // anchor, plus S's `copying` owner checkpoint recorded at the anchor.
        for e in &s[..15] {
            dest.append_event(pv, dbr, e).await.unwrap();
        }
        dest.flush_writes().await.unwrap();
        let s_filename = s_path.file_name().unwrap().to_str().unwrap();
        dest.create_etl_sidecar(
            &ckpt,
            &ckpt_contents(
                CkptState::Copying,
                anchor,
                s_filename,
                source_btime_nanos(&s_path),
            ),
        )
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

        // A DIFFERENT source (still present on disk) owns an in-flight
        // (`copying`) checkpoint on D.
        let owner_name = "SimpleSine:2023_11_14_22.pb";
        let owner_path = s_path.parent().unwrap().join(owner_name);
        std::fs::write(&owner_path, b"partial").unwrap();
        dest.create_etl_sidecar(
            &ckpt,
            &ckpt_contents(CkptState::Copying, 0, owner_name, None),
        )
        .unwrap();

        // Must defer: return Ok without touching D, source, or checkpoint.
        exec.move_file(&s_path).await.unwrap();

        assert!(s_path.exists(), "source kept when deferring");
        assert!(!d_path.exists(), "dest partition untouched when deferring");
        let ck = read_ckpt(&ckpt).expect("owner checkpoint intact");
        assert_eq!(ck.owner, owner_name);
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
    async fn move_file_resumes_via_owned_checkpoint_after_crash_before_delete() {
        // Crash after the copy is durable AND the checkpoint transitioned to
        // `committed`, but before the source delete: the `committed` owned
        // `<D>.pb.etl_ckpt` (btime = THIS source instance) survives. Because
        // the still-present source IS this same instance (btime match), the
        // resume rolls D back to the anchor and RE-COPIES exactly once — never
        // leaving a duplicate — then deletes the source and clears the
        // checkpoint. The redesign has no `.etl_done` marker; the checkpoint
        // alone drives recovery.
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

        // Stage the crash state: D already fully holds S, and S's owned
        // `committed` checkpoint (anchor 0 = D's pre-append length, btime =
        // this source) is still present because the delete hadn't run yet.
        let dbr = samples[0].value.db_type();
        for s in &samples {
            dest.append_event(pv, dbr, s).await.unwrap();
        }
        dest.flush_writes().await.unwrap();
        let s_filename = s_path.file_name().unwrap().to_str().unwrap();
        dest.create_etl_sidecar(
            &ckpt,
            &ckpt_contents(
                CkptState::Committed,
                0,
                s_filename,
                source_btime_nanos(&s_path),
            ),
        )
        .unwrap();

        exec.move_file(&s_path).await.unwrap();

        assert!(!s_path.exists(), "resume deletes the source");
        assert!(!ckpt.exists(), "resume clears its own dest checkpoint");
        assert_eq!(
            count_samples(&d_path),
            30,
            "resume truncated to the anchor + re-copied once, no duplicate"
        );
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
        src.create_etl_sidecar(
            &inbound_ckpt,
            &ckpt_contents(CkptState::Copying, 0, "SimpleSine:2023_11_14_22.pb", None),
        )
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
    async fn move_file_backfill_after_clean_move_copies_without_loss_or_dup() {
        // finding A under the redesign: after a source was cleanly moved out
        // (D holds it, source deleted, checkpoint removed), a backfill
        // re-creates the same-named partition with NEW samples. With no marker
        // and no leftover checkpoint, the re-created source is simply copied
        // fresh — its new samples appended to D, the already-present ones NOT
        // duplicated (they are not in the re-created source), and nothing lost.
        let tmp = tempfile::tempdir().unwrap();
        let src = plugin(tmp.path().join("sts"), "STS", PartitionGranularity::Hour);
        let dest = plugin(tmp.path().join("mts"), "MTS", PartitionGranularity::Day);
        let exec = EtlExecutor::new(src.clone(), dest.clone(), 3600, 0, 100);

        let pv = "SimpleSine";
        let original: Vec<_> = (0..20)
            .map(|i| sample_at(BASE_SECS + i, i as f64))
            .collect();
        let s_path = write_source_partition(&src, pv, &original).await;
        let d_path = dest.file_path_for(pv, original[0].timestamp);

        // D holds the 20 originals (a prior clean move); no ckpt, no marker.
        let dbr = original[0].value.db_type();
        for e in &original {
            dest.append_event(pv, dbr, e).await.unwrap();
        }
        dest.flush_writes().await.unwrap();

        // A backfill re-creates the same-named partition with NEW samples not
        // yet in D. Evict the cached writer + unlink first so a fresh file is
        // created at the same path.
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

        assert!(!s_path.exists(), "backfill partition moved out");
        assert_eq!(
            count_samples(&d_path),
            20 + 5,
            "backfill copied into D — no loss, no duplicate"
        );
    }

    #[tokio::test]
    async fn move_file_recreated_owner_checkpoint_preserves_committed_tail() {
        // Redesign / re-create window: an ORPHAN `committed` checkpoint whose
        // owner was deleted then RE-CREATED (crash after the delete, before the
        // checkpoint removal, then a backfill). The stored anchor points BELOW
        // D's committed data. A `committed` tail is FINAL and is NEVER
        // truncated — so the committed samples survive on EVERY filesystem, not
        // only ones exposing btime. (btime here would merely confirm the
        // re-create; its absence changes nothing because the state, not btime,
        // gates the truncate.) The move re-anchors fresh and appends.
        let tmp = tempfile::tempdir().unwrap();
        let src = plugin(tmp.path().join("sts"), "STS", PartitionGranularity::Hour);
        let dest = plugin(tmp.path().join("mts"), "MTS", PartitionGranularity::Day);
        let exec = EtlExecutor::new(src.clone(), dest.clone(), 3600, 0, 100);

        let pv = "SimpleSine";
        // D already durably holds 20 committed samples from the OLD instance.
        let committed: Vec<_> = (0..20)
            .map(|i| sample_at(BASE_SECS + i, i as f64))
            .collect();
        let dbr = committed[0].value.db_type();
        for e in &committed {
            dest.append_event(pv, dbr, e).await.unwrap();
        }
        dest.flush_writes().await.unwrap();
        let d_path = dest.file_path_for(pv, committed[0].timestamp);
        let ckpt = d_path.with_extension("pb.etl_ckpt");

        // The re-created source: 5 NEW samples (same day, disjoint times).
        let backfill: Vec<_> = (0..5)
            .map(|i| sample_at(BASE_SECS + 100 + i, 900.0 + i as f64))
            .collect();
        let s_path = write_source_partition(&src, pv, &backfill).await;
        let s_filename = s_path.file_name().unwrap().to_str().unwrap();

        // Orphan `committed` checkpoint: owner == this source's name, anchor =
        // 0 (BELOW the committed data), btime = a bogus OLD value. The state is
        // `committed`, so the tail is never truncated regardless of btime; if
        // truncation to 0 were (wrongly) taken it would destroy the 20
        // committed samples and this test would read 5.
        dest.create_etl_sidecar(
            &ckpt,
            &ckpt_contents(CkptState::Committed, 0, s_filename, Some(1)),
        )
        .unwrap();

        exec.move_file(&s_path).await.unwrap();

        assert!(!s_path.exists(), "re-created source moved out");
        assert!(!ckpt.exists(), "checkpoint cleared on commit");
        assert_eq!(
            count_samples(&d_path),
            20 + 5,
            "committed tail preserved (not truncated) + backfill appended"
        );
    }

    #[tokio::test]
    async fn move_file_grown_source_recopied_without_duplication() {
        // DUP-1 closure: a source whose delete was deferred (owned checkpoint
        // kept) then GREW in place via a backfill append (same file → btime
        // unchanged). The retry must truncate D back to the anchor and re-copy
        // the WHOLE grown source exactly once — not append it after the prefix
        // already in D (which would duplicate the original samples).
        let tmp = tempfile::tempdir().unwrap();
        let src = plugin(tmp.path().join("sts"), "STS", PartitionGranularity::Hour);
        let dest = plugin(tmp.path().join("mts"), "MTS", PartitionGranularity::Day);
        let exec = EtlExecutor::new(src.clone(), dest.clone(), 3600, 0, 100);

        let pv = "SimpleSine";
        let original: Vec<_> = (0..20)
            .map(|i| sample_at(BASE_SECS + i, i as f64))
            .collect();
        let s_path = write_source_partition(&src, pv, &original).await;
        let d_path = dest.file_path_for(pv, original[0].timestamp);
        let ckpt = d_path.with_extension("pb.etl_ckpt");
        let s_filename = s_path.file_name().unwrap().to_str().unwrap();

        // Prior attempt copied the 20 originals into D, transitioned the
        // checkpoint to `committed`, then had its delete deferred (the source
        // grew before the gated delete). btime recorded = this source instance,
        // so the retry sees `committed` + btime match and re-copies the whole
        // (now grown) source.
        let dbr = original[0].value.db_type();
        for e in &original {
            dest.append_event(pv, dbr, e).await.unwrap();
        }
        dest.flush_writes().await.unwrap();
        dest.create_etl_sidecar(
            &ckpt,
            &ckpt_contents(
                CkptState::Committed,
                0,
                s_filename,
                source_btime_nanos(&s_path),
            ),
        )
        .unwrap();

        // Backfill appends 5 more to the SAME source file (btime unchanged),
        // flushed so the on-disk source is now 25 samples.
        let grow: Vec<_> = (0..5)
            .map(|i| sample_at(BASE_SECS + 30 + i, 500.0 + i as f64))
            .collect();
        for e in &grow {
            src.append_event(pv, dbr, e).await.unwrap();
        }
        src.flush_writes().await.unwrap();
        assert_eq!(count_samples(&s_path), 25, "source grew to 25");

        exec.move_file(&s_path).await.unwrap();

        assert!(!s_path.exists(), "grown source moved out");
        assert!(!ckpt.exists(), "checkpoint cleared on commit");
        assert_eq!(
            count_samples(&d_path),
            25,
            "D holds each of the 25 once — original 20 truncated, not duplicated"
        );
    }

    #[tokio::test]
    async fn move_file_copying_checkpoint_truncates_partial_without_btime() {
        // Finding B, core win: on a filesystem exposing NO btime, a `copying`
        // checkpoint still safely rolls back a partial tail. The `copying`
        // state PROVES the source was never deleted (deletion happens only
        // after the `committed` transition), so the still-present source is
        // this same instance and truncate + re-copy is correct — no btime
        // needed, no loss, no duplicate. This is the common retry path, and it
        // is clean on btime-less storage where the old btime-only design would
        // have had to guess.
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

        // A 10-sample partial in D and a `copying` checkpoint recorded with NO
        // btime (as a btime-less filesystem would).
        let dbr = samples[0].value.db_type();
        for s in &samples[..10] {
            dest.append_event(pv, dbr, s).await.unwrap();
        }
        dest.flush_writes().await.unwrap();
        let s_filename = s_path.file_name().unwrap().to_str().unwrap();
        dest.create_etl_sidecar(
            &ckpt,
            &ckpt_contents(CkptState::Copying, 0, s_filename, None),
        )
        .unwrap();

        exec.move_file(&s_path).await.unwrap();

        assert_eq!(
            count_samples(&d_path),
            50,
            "copying state truncated the partial without btime — no duplicate"
        );
        assert!(!s_path.exists());
        assert!(!ckpt.exists());
    }

    #[tokio::test]
    async fn move_file_committed_checkpoint_without_btime_never_loses_committed_tail() {
        // Finding B, worst btime-less case: a `committed` orphan checkpoint
        // recorded with NO btime, whose same-named source is still present
        // (in reality this same instance — a crash between the `committed`
        // transition and the source delete). Without btime we cannot PROVE
        // same-vs-re-created, so we take the conservative path and NEVER
        // truncate the committed tail. The committed samples survive on every
        // filesystem (the silent-loss floor is removed); the only cost is a
        // recoverable duplicate (collapsed by read-time timestamp dedup) —
        // never loss.
        let tmp = tempfile::tempdir().unwrap();
        let src = plugin(tmp.path().join("sts"), "STS", PartitionGranularity::Hour);
        let dest = plugin(tmp.path().join("mts"), "MTS", PartitionGranularity::Day);
        let exec = EtlExecutor::new(src.clone(), dest.clone(), 3600, 0, 100);

        let pv = "SimpleSine";
        let samples: Vec<_> = (0..20)
            .map(|i| sample_at(BASE_SECS + i, i as f64))
            .collect();
        // D durably holds the committed 20; the source still holds the SAME 20.
        let s_path = write_source_partition(&src, pv, &samples).await;
        let d_path = dest.file_path_for(pv, samples[0].timestamp);
        let ckpt = d_path.with_extension("pb.etl_ckpt");
        let dbr = samples[0].value.db_type();
        for e in &samples {
            dest.append_event(pv, dbr, e).await.unwrap();
        }
        dest.flush_writes().await.unwrap();
        let s_filename = s_path.file_name().unwrap().to_str().unwrap();
        dest.create_etl_sidecar(
            &ckpt,
            &ckpt_contents(CkptState::Committed, 0, s_filename, None),
        )
        .unwrap();

        exec.move_file(&s_path).await.unwrap();

        // The committed 20 are preserved (never truncated); the source is
        // re-copied after them because same-instance can't be proven without
        // btime → 40. A truncate-to-0 would have destroyed the committed 20 and
        // yielded 20; that is the loss this design forecloses.
        assert_eq!(
            count_samples(&d_path),
            40,
            "committed tail preserved on a btime-less FS (no loss); recoverable duplicate only"
        );
        assert!(!s_path.exists(), "source moved out");
        assert!(!ckpt.exists(), "checkpoint cleared on commit");
    }

    #[tokio::test]
    async fn reap_removes_terminal_orphan_checkpoint_and_keeps_live_one() {
        // Finding A: a `committed` checkpoint whose owner source was deleted
        // (crash in the delete → remove-checkpoint window) is stranded once its
        // dest partition is terminal — no later move into D reaps it, and the
        // next tier's move defers on it forever. run_once's reap, owned by the
        // forward executor (it holds both tiers), removes it. A checkpoint whose
        // owner source is still present is an in-flight aggregation and is KEPT.
        let tmp = tempfile::tempdir().unwrap();
        let src = plugin(tmp.path().join("sts"), "STS", PartitionGranularity::Hour);
        let dest = plugin(tmp.path().join("mts"), "MTS", PartitionGranularity::Day);
        let exec = EtlExecutor::new(src.clone(), dest.clone(), 3600, 0, 100);

        let pv = "SimpleSine";

        // D_orphan: a committed day-partition whose owner hour-source is GONE.
        let orphan_samples: Vec<_> = (0..20)
            .map(|i| sample_at(BASE_SECS + i, i as f64))
            .collect();
        let dbr = orphan_samples[0].value.db_type();
        for e in &orphan_samples {
            dest.append_event(pv, dbr, e).await.unwrap();
        }
        dest.flush_writes().await.unwrap();
        let d_orphan = dest.file_path_for(pv, orphan_samples[0].timestamp);
        let orphan_ckpt = d_orphan.with_extension("pb.etl_ckpt");
        dest.create_etl_sidecar(
            &orphan_ckpt,
            &ckpt_contents(
                CkptState::Committed,
                0,
                "SimpleSine:1999_01_01_00.pb",
                Some(1),
            ),
        )
        .unwrap();

        // A live source partition (a distant day) exists in STS, so the source
        // root exists (the reap is gated on that) and its own checkpoint below
        // has a present owner. One partition → not movable, so run_once won't
        // migrate it.
        let live_samples: Vec<_> = (0..10)
            .map(|i| sample_at(BASE_SECS + 5 * 86_400 + i, i as f64))
            .collect();
        let live_owner_path = write_source_partition(&src, pv, &live_samples).await;
        let live_owner = live_owner_path.file_name().unwrap().to_str().unwrap();
        let d_live = dest.file_path_for(pv, live_samples[0].timestamp);
        let live_ckpt = d_live.with_extension("pb.etl_ckpt");
        dest.create_etl_sidecar(
            &live_ckpt,
            &ckpt_contents(
                CkptState::Copying,
                0,
                live_owner,
                source_btime_nanos(&live_owner_path),
            ),
        )
        .unwrap();

        exec.run_once().await.unwrap();

        assert!(!orphan_ckpt.exists(), "terminal orphan checkpoint reaped");
        assert_eq!(
            count_samples(&d_orphan),
            20,
            "committed tail untouched by reap"
        );
        assert!(live_ckpt.exists(), "live (owner present) checkpoint kept");
    }

    #[tokio::test]
    async fn terminal_orphan_wedge_resolved_by_forward_tier_reap() {
        // The full wedge: STS→MTS crashed after deleting the last STS source of
        // a day but before removing D's checkpoint. D (an MTS day-partition) is
        // terminal — no STS source maps to it again — so MTS→LTS's move_file(D)
        // defers on the orphan inbound checkpoint every cycle. The STS→MTS
        // executor's reap removes the orphan, unwedging the MTS→LTS move.
        let tmp = tempfile::tempdir().unwrap();
        let sts = plugin(tmp.path().join("sts"), "STS", PartitionGranularity::Hour);
        let mts = plugin(tmp.path().join("mts"), "MTS", PartitionGranularity::Day);
        let lts = plugin(tmp.path().join("lts"), "LTS", PartitionGranularity::Year);
        let sts_mts = EtlExecutor::new(sts.clone(), mts.clone(), 3600, 0, 100);
        let mts_lts = EtlExecutor::new(mts.clone(), lts.clone(), 3600, 0, 100);

        let pv = "SimpleSine";
        // D: an MTS day-partition durably holding 20 samples, with a committed
        // orphan checkpoint owned by a now-deleted STS hour-source.
        let samples: Vec<_> = (0..20)
            .map(|i| sample_at(BASE_SECS + i, i as f64))
            .collect();
        let dbr = samples[0].value.db_type();
        for e in &samples {
            mts.append_event(pv, dbr, e).await.unwrap();
        }
        mts.flush_writes().await.unwrap();
        let d_path = mts.file_path_for(pv, samples[0].timestamp);
        let ckpt = d_path.with_extension("pb.etl_ckpt");
        mts.create_etl_sidecar(
            &ckpt,
            &ckpt_contents(
                CkptState::Committed,
                0,
                "SimpleSine:1999_01_01_00.pb",
                Some(1),
            ),
        )
        .unwrap();

        // A live STS partition (distant day) keeps the STS root present so the
        // reap runs; one partition → not movable.
        let keep: Vec<_> = (0..3)
            .map(|i| sample_at(BASE_SECS + 5 * 86_400 + i, i as f64))
            .collect();
        write_source_partition(&sts, pv, &keep).await;

        // Before the reap: MTS→LTS defers on the inbound orphan checkpoint.
        mts_lts.move_file(&d_path).await.unwrap();
        assert!(d_path.exists(), "wedged: D not moved while orphan present");
        let lts_path = lts.file_path_for(pv, samples[0].timestamp);
        assert!(!lts_path.exists(), "wedged: nothing reached LTS");

        // STS→MTS's reap removes the orphan (its owner STS source is gone).
        sts_mts.run_once().await.unwrap();
        assert!(!ckpt.exists(), "orphan reaped by the forward tier");

        // Now MTS→LTS proceeds: D migrates to LTS.
        mts_lts.move_file(&d_path).await.unwrap();
        assert!(!d_path.exists(), "unwedged: D moved out to LTS");
        assert_eq!(count_samples(&lts_path), 20, "D's samples reached LTS once");
    }

    #[tokio::test]
    async fn consolidate_pv_counts_only_actually_moved_not_deferred() {
        // Finding C: consolidate_pv is the count surfaced to the operator via
        // the consolidateDataForPV endpoint. A file `move_file` DEFERS
        // (Ok(false)) is left in the source tier — counting it as moved falsely
        // claims a still-present file migrated. The count must be the number
        // actually moved out, not the number attempted.
        let tmp = tempfile::tempdir().unwrap();
        let src = plugin(tmp.path().join("sts"), "STS", PartitionGranularity::Hour);
        let dest = plugin(tmp.path().join("mts"), "MTS", PartitionGranularity::Day);
        let exec = EtlExecutor::new(src.clone(), dest.clone(), 3600, 0, 100);

        let pv = "SimpleSine";
        // S_move: a normal source partition that migrates cleanly.
        let a: Vec<_> = (0..10)
            .map(|i| sample_at(BASE_SECS + i, i as f64))
            .collect();
        let s_move = write_source_partition(&src, pv, &a).await;
        // S_defer: a source partition carrying a pending INBOUND checkpoint (a
        // finer tier is mid-aggregation into it), so move_file defers it.
        let b: Vec<_> = (0..10)
            .map(|i| sample_at(BASE_SECS + 86_400 + i, i as f64))
            .collect();
        let s_defer = write_source_partition(&src, pv, &b).await;
        src.create_etl_sidecar(
            &s_defer.with_extension("pb.etl_ckpt"),
            &ckpt_contents(CkptState::Copying, 0, "finer-owner.pb", Some(1)),
        )
        .unwrap();

        let moved = exec.consolidate_pv(pv).await.unwrap();

        assert_eq!(moved, 1, "only the clean file counts, not the deferred one");
        assert!(!s_move.exists(), "clean source migrated out");
        assert!(s_defer.exists(), "deferred source stays in the source tier");
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
