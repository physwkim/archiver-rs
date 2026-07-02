use std::collections::{HashMap, HashSet};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, SystemTime};

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
            // Only reap when the owner is DEFINITIVELY absent. `exists()`
            // coerces a transient stat error (ESTALE / EIO on a flaky mount) to
            // `false`; reaping on that would drop a LIVE owner's checkpoint and
            // make the next move re-copy after the committed tail — a duplicate.
            // Ok(true) (present, in-flight) and Err(_) (uncertain) both mean
            // "not provably gone": leave the checkpoint for a healthy-mount
            // cycle — or move_file's owner-retry resume — to handle.
            if !matches!(owner_path.try_exists(), Ok(false)) {
                continue;
            }

            // Orphan: the owner is gone. Remove the checkpoint but NEVER touch
            // D's bytes. The reap cannot prove what the tail past `anchor` is,
            // and truncating it risks permanent loss: with `fsync_on_flush=false`
            // (the default) and source/dest on independent filesystems, the
            // commit's `copying→committed` rename and the source unlink can
            // persist OUT OF ORDER under power loss, leaving a stale `Copying`
            // checkpoint standing over an ALREADY-COMMITTED, durable tail whose
            // source is gone — rolling that tail back to `anchor` would erase
            // data that exists nowhere else. A genuine partial (an out-of-model
            // external source delete mid-copy) is likewise best preserved: there
            // is no source left to re-copy, so its copied prefix is the only
            // remaining data. A still-`Copying` orphan is thus indistinguishable
            // from a reordered-`committed` one; the reap treats both uniformly —
            // preserve D, drop only the checkpoint. The next tier then carries
            // D's tail forward like any other committed data.
            warn!(
                ?ckpt_path,
                owner = ck.owner,
                state = ck.state.as_str(),
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

        let mut reader = match PbFileReader::open(&source_path) {
            Ok(r) => r,
            Err(e)
                if e.downcast_ref::<std::io::Error>()
                    .is_some_and(|io| io.kind() == std::io::ErrorKind::NotFound) =>
            {
                // The source is already gone. `consolidate_pv` lists a PV's
                // files BEFORE taking the per-move gate, so the periodic loop
                // (sharing this executor's `move_gate`) can migrate one first;
                // by the time this call wins the gate the file is deleted.
                // Nothing to move — report DEFERRED, not an error, so
                // consolidate_pv doesn't spuriously hard-fail the whole PV over
                // a file that was in fact correctly migrated.
                debug!(
                    ?source_path,
                    "ETL move_file: source already gone (migrated by a concurrent cycle); skipping"
                );
                return Ok(false);
            }
            Err(e) => return Err(e),
        };
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

        // v0.4.0 upgrade compat. The released binary (v0.4.0, the only tagged
        // release) used a `<S>.pb.etl_done` marker — written AFTER the copy +
        // flush into D, BEFORE the source delete — as its idempotency guard;
        // this redesign retired the marker for the dest checkpoint. A source
        // left in v0.4.0's marker→delete window at upgrade has its full copy
        // already durable in D but NO HEAD `.etl_ckpt`, so establishing a FRESH
        // blind copy below (the `None`/orphan paths) would DUPLICATE it. The
        // marker is exactly a "D already holds S, re-copy idempotently" signal —
        // the same family as a `Committed` owner-retry we can't prove
        // same-instance — so a fresh copy that sees it dedups instead of
        // blind-appending: an unchanged S is fully skipped (no dup), and a
        // backfill that grew S after the marker copies only its new samples (no
        // loss, unlike v0.4.0's unconditional marker-found delete). HEAD never
        // writes this marker, so it is absent in steady state.
        let done_marker = source_path.with_extension("pb.etl_done");

        // Establish or resume this source's checkpoint on D and settle on the
        // `anchor` this copy appends from. A retry rolls D back to `anchor` and
        // re-appends, so the copy is idempotent by construction. Invariant:
        // `<D>.pb.etl_ckpt` present ⟺ a copy of `owner` into D is in progress
        // (`Copying`) or finished-but-pending-delete (`Committed`).
        //
        // Set when the copy below must dedup against D's existing timestamps
        // rather than blind-append: a `Committed` owner-retry (preserve the
        // committed tail, re-copy the source after it), a resumed dedup `Copying`
        // (a `Committed`→dedup transition that crashed mid-copy, persisted as
        // `Copying`/dedup=1), or a v0.4.0-marker'd source whose copy is already
        // in D. D may already hold this source's prior copy, so the copy below
        // dedups against D's timestamps.
        let mut dedup_against_dest = false;
        // Set to `Some(anchor_len)` by an owner-retry that must roll D back to
        // its checkpoint anchor before re-appending. Hoisted OUT of the match so
        // the truncate runs once, awaited to completion OUTSIDE the copy timeout
        // (a detached `truncate_partition` could `set_len` D after the gate is
        // released and roll a later move's bytes off D — silent loss).
        let mut rollback_to: Option<u64> = None;
        let anchor = match read_ckpt(&ckpt) {
            Some(ck) if ck.owner == s_filename => {
                if ck.state == CkptState::Copying {
                    // `Copying`: `[anchor..]` is a PARTIAL copy of THIS instance.
                    // A source is deleted only AFTER its `Committed` transition, so
                    // a `Copying` checkpoint proves its source was never deleted —
                    // this is necessarily the same instance and `[anchor..]` holds
                    // all (and only) its contribution. Roll D back to `anchor` and
                    // re-copy; exact on ANY filesystem, no btime needed. `state`,
                    // and ONLY `state`, gates the truncate — NEVER `dedup`: the
                    // persisted `dedup` mode says only HOW this cycle re-copies
                    // (dedup=1 when a `Committed`→dedup transition crashed mid-copy
                    // and left `Copying`/dedup=1; dedup=0 for a plain partial), not
                    // WHETHER to truncate. Both re-derive `[anchor..]` from the
                    // proven same-instance source, so the truncate is loss-free
                    // either way. KEEP the checkpoint's anchor (the pre-append
                    // length — must NOT be re-measured).
                    rollback_to = Some(ck.anchor);
                    dedup_against_dest = ck.dedup;
                    ck.anchor
                } else {
                    // `Committed`: the copy finished, so `[anchor..]` is a
                    // COMMITTED tail. It may belong to a deleted/older instance (a
                    // re-created owner after a crash in the delete→uncheckpoint
                    // window), and nothing on disk can reliably prove it is or
                    // isn't the same instance (a filesystem's birth time may be
                    // coarse, constant/synthetic, or absent), so it must NEVER be
                    // truncated — a wrong truncate here is silent loss. This holds
                    // for a `dedup` (=1) `Committed` tail too: a re-created SUBSET
                    // or gap-fill source does NOT re-derive the committed
                    // `[anchor..]`, so rolling back to `anchor` (as gating the
                    // truncate on `dedup` rather than `state` would) drops
                    // committed samples the current source no longer holds.
                    // `state`, and only `state`, gates the truncate: a `Committed`
                    // tail is ALWAYS preserved — regardless of the persisted dedup
                    // mode — and the current source is dedup-re-copied AFTER it,
                    // uniformly on every filesystem. Persist the dedup mode and
                    // transition ATOMICALLY (overwrite = tmp+rename): a crash in
                    // this window leaves either the old `Committed` (re-enters
                    // here) or the new dedup `Copying` (resumes as a dedup re-copy)
                    // — never NO checkpoint, which a resume would read as a
                    // dedup-off fresh copy and duplicate the preserved tail. The
                    // dedup copy is idempotent: a true same-instance retry (crash
                    // before the source delete, or a deferred delete of a grown
                    // source) skips every already-present sample — copying nothing,
                    // or only a grown tail; a genuine re-created/backfill source
                    // (disjoint, gap-fill, or superset) copies exactly its
                    // not-yet-present samples (no loss). (No append has happened
                    // yet.)
                    debug!(
                        ?ckpt,
                        owner = ck.owner,
                        "ETL committed-checkpoint owner retry; preserving its \
                         committed tail and re-anchoring (dedup re-copy)"
                    );
                    dest.overwrite_etl_sidecar(
                        &ckpt,
                        &ckpt_contents(CkptState::Copying, len_before, &s_filename, true),
                    )?;
                    dedup_against_dest = true;
                    len_before
                }
            }
            Some(ck) => {
                // A DIFFERENT source owns an in-flight append to D.
                let owner_src = source_path
                    .parent()
                    .map(|p| p.join(&ck.owner))
                    .unwrap_or_else(|| PathBuf::from(&ck.owner));
                // Defer unless the owner is DEFINITIVELY gone. `exists()` reads
                // a transient stat error as `false`, which would drop this
                // OTHER source's live checkpoint below and strand its resume (or
                // duplicate). Ok(true) (present) and Err(_) (uncertain) both
                // mean "do not treat as orphan" — defer.
                if !matches!(owner_src.try_exists(), Ok(false)) {
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
                // If a v0.4.0 marker says D already holds THIS source's copy,
                // dedup the fresh copy against D (skip its prior copy) rather
                // than blind-append it after the orphan's tail — else it
                // duplicates. The orphan owner's tail is in a disjoint range, so
                // the bounded dedup read never touches it.
                if done_marker.exists() {
                    dedup_against_dest = true;
                }
                dest.remove_etl_sidecar(&ckpt)?;
                dest.create_etl_sidecar(
                    &ckpt,
                    &ckpt_contents(
                        CkptState::Copying,
                        len_before,
                        &s_filename,
                        dedup_against_dest,
                    ),
                )?;
                len_before
            }
            None => {
                // Torn (present-but-unparseable) or absent HEAD checkpoint. A
                // torn checkpoint is fsync'd before any append byte, so it always
                // predates an append → D is at its true committed length; discard
                // it and re-anchor from len_before.
                if ckpt.exists() {
                    warn!(?ckpt, "ETL discarding torn dest checkpoint");
                    dest.remove_etl_sidecar(&ckpt)?;
                }
                // v0.4.0 upgrade compat (primary path): no HEAD checkpoint, but a
                // `.etl_done` marker means D already holds this source's copy —
                // dedup the re-copy instead of blind-appending (see the
                // `done_marker` note above).
                if done_marker.exists() {
                    dedup_against_dest = true;
                }
                dest.create_etl_sidecar(
                    &ckpt,
                    &ckpt_contents(
                        CkptState::Copying,
                        len_before,
                        &s_filename,
                        dedup_against_dest,
                    ),
                )?;
                len_before
            }
        };

        // Roll D back to the checkpoint anchor for an owner-retry (dedup or
        // non-dedup). Awaited to completion, never under the copy timeout — a
        // detached `truncate_partition` could `set_len` D after the gate is
        // released and roll a later move's committed bytes off D.
        if let Some(anchor_len) = rollback_to {
            let dest_for_trunc = dest.clone();
            let d_for_trunc = d_path.clone();
            tokio::task::spawn_blocking(move || {
                dest_for_trunc.truncate_partition(&d_for_trunc, anchor_len)
            })
            .await??;
        }

        // When appending over a preserved committed tail, read D's existing
        // timestamps (now rolled back to `anchor`, so this reads exactly the
        // preserved region `[0..anchor]`) so the copy can skip any this source
        // already contributed. Each finer source covers a DISJOINT time
        // sub-range of D, so the only collisions are this source's own prior
        // copy — a same-instance retry then copies nothing (no duplicate), while
        // a re-created/backfill source copies exactly its new samples (no loss).
        // Guarded on `anchor > 0`: nothing sits below a zero anchor to dedup
        // against.
        let dest_ts: Option<HashSet<SystemTime>> = if dedup_against_dest && anchor > 0 {
            // Only D's timestamps within THIS source partition's own time range
            // can collide with a source sample (disjoint sub-ranges), so bound
            // the read to `[S_start, S_end)` — the slice that holds exactly this
            // source's prior copy. first_sample is in this source's range, so
            // its partition boundaries at the SOURCE granularity delimit it.
            let src_g = self.source.partition_granularity();
            let s_start = crate::storage::partition::partition_start(first_sample.timestamp, src_g);
            let s_end =
                crate::storage::partition::next_partition_start(first_sample.timestamp, src_g);
            Some(read_partition_timestamps_in_range(&d_path, s_start, s_end)?)
        } else {
            None
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
            // `None` ⇒ copy everything (the common path). `Some(set)` ⇒ skip any
            // sample D already holds (the preserved-committed-tail re-copy).
            let already_in_dest =
                |ts: SystemTime| dest_ts.as_ref().is_some_and(|set| set.contains(&ts));
            if !already_in_dest(first_sample.timestamp) {
                dest.append_event_with_meta(&desc.pv_name, dbr_type, &first_sample, &append_meta)
                    .await?;
            }
            while let Some(sample) = reader.next_event()? {
                if dest.file_path_for(&desc.pv_name, sample.timestamp) != d_path {
                    return Err(anyhow::anyhow!(
                        "ETL: a sample timestamp in {source_path:?} maps to a different dest \
                         partition than {d_path:?}; refusing to split the copy"
                    ));
                }
                if already_in_dest(sample.timestamp) {
                    continue;
                }
                dest.append_event_with_meta(&desc.pv_name, dbr_type, &sample, &append_meta)
                    .await?;
            }
            // Durability BEFORE the commit. Flush all dest writers, then fsync
            // D's data AND its directory entry UNCONDITIONALLY so the copy
            // survives a crash before we delete the source. The copied samples
            // are already-archived history being RELOCATED, not loss-tolerant
            // fresh ingest — and the source delete is now unconditionally
            // durable, so once it runs there is no original left to re-copy
            // from. Gating D's durability on `fsync_on_flush` (as the ordinary
            // flush path is) would, on the default config, leave a window where
            // a power loss after the durable source delete permanently loses the
            // copy (finding #9 R10). So decouple it, exactly as the checkpoint
            // sidecar and source unlink were decoupled.
            dest.flush_writes().await?;
            dest.sync_partition_durable(&d_path).map_err(|e| {
                anyhow::anyhow!("ETL: durable-sync of dest partition {d_path:?} failed: {e}")
            })?;
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
        // Record the dedup mode. A `Committed` resume is gated on `state` alone
        // — it ALWAYS preserves D and dedup-re-copies the source (see the
        // owner-match arm above), so it does NOT consult this stored flag; on a
        // `Committed` checkpoint the field is informational. It is
        // decision-load-bearing only on a `Copying` checkpoint, where it selects
        // blind vs dedup re-copy of the partial tail. Written faithfully here so
        // the checkpoint records what this copy actually did.
        dest.overwrite_etl_sidecar(
            &ckpt,
            &ckpt_contents(
                CkptState::Committed,
                anchor,
                &s_filename,
                dedup_against_dest,
            ),
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
        //      is never truncated) and its re-copy deduped against D. No loss
        //      and no duplicate on any filesystem.
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
        // The source is gone and its copy is committed in D, so any v0.4.0
        // `.etl_done` marker for it is now superseded — clear it so it does not
        // linger (the source it named no longer exists to re-trigger cleanup).
        // Gated on the dedup mode: a marker only ever drives a dedup copy, so a
        // plain blind move never pays this unlink. Best-effort; a leftover is
        // harmless (HEAD ignores it once the source is gone).
        if dedup_against_dest {
            let _ = std::fs::remove_file(&done_marker);
        }
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
/// lets a retry decide whether the tail `[anchor..]` in D may be truncated —
/// with no dependence on any filesystem-provided birth time.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum CkptState {
    /// A copy of `owner` into D is in progress; the tail `[anchor..]` is a
    /// PARTIAL copy. A `Copying` checkpoint can only exist while its source is
    /// still on disk (the source is deleted only after the `Committed`
    /// transition), so its tail is provably a partial copy of the SAME source
    /// instance — truncating back to `anchor` is always safe, on any
    /// filesystem. Whether the re-copy after that truncate blind-appends or
    /// dedups is the ORTHOGONAL [`Ckpt::dedup`] mode, not the state: a dedup
    /// `Copying` preserves this source's own committed tail BELOW `anchor`, so
    /// its re-copy must dedup against D.
    Copying,
    /// The copy of `owner` FINISHED and is durable in D; the tail `[anchor..]`
    /// is FINAL and must NEVER be truncated. A retry cannot prove whether a
    /// still-present same-named source is this same instance or a re-created
    /// backfill, so it never tries: the tail is preserved and the source is
    /// dedup-re-copied after it, correct either way.
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
    /// The persisted copy MODE. `true` ⟺ the tail `[anchor..]` was appended
    /// over a PRESERVED committed tail that already holds (some of) this
    /// source's data — so a resume must re-copy WITH dedup, never blind-append
    /// (that would duplicate the below-anchor tail). `false` ⟺ `[anchor..]`
    /// holds ALL of this source's contribution (disjoint below), so a resume
    /// truncates + blind re-appends. This is orthogonal to `state` and, unlike
    /// the old in-memory flag, survives a crash/timeout so the mode is never
    /// re-guessed on resume.
    dedup: bool,
}

/// Serialize an ETL checkpoint sidecar, one field per line:
///   1. `state` — `copying` or `committed` (see [`CkptState`]),
///   2. `anchor` — the dest partition's byte length BEFORE this source's
///      contribution (the rollback point),
///   3. `owner` — the owning source partition's filename,
///   4. `dedup` — `1` when the tail was appended over a preserved committed
///      tail that overlaps this source (resume must re-copy WITH dedup), else
///      `0`.
///
/// `dedup` is the persisted copy MODE (see [`Ckpt::dedup`]): it — not `state` —
/// is what tells a resume whether `[anchor..]` may be blind re-appended (mode
/// `0`, tail disjoint below) or must be re-copied with dedup (mode `1`, this
/// source's data also sits below `anchor`). Persisting it means an interrupted
/// dedup re-copy resumes AS a dedup re-copy instead of a blind-append that
/// would duplicate the preserved tail.
///
/// There is deliberately NO birth-time (btime) field: the truncate-safety of a
/// retry is gated solely by `state` (a `Committed` tail is never truncated), so
/// no filesystem-provided creation time is load-bearing and none is recorded.
fn ckpt_contents(state: CkptState, anchor: u64, owner: &str, dedup: bool) -> Vec<u8> {
    let dedup = if dedup { "1" } else { "0" };
    format!("{}\n{anchor}\n{owner}\n{dedup}\n", state.as_str()).into_bytes()
}

/// Parse an ETL checkpoint sidecar into a [`Ckpt`]. Returns `None` for a
/// missing, torn, or otherwise unparseable file (including an unrecognized
/// state token) — the caller treats that uniformly as "no valid checkpoint".
/// Because the checkpoint's establishment is fsync'd before any append byte, a
/// torn file always predates an append, so discarding it and re-anchoring from
/// D's current length is safe.
fn read_ckpt(path: &Path) -> Option<Ckpt> {
    let content = std::fs::read_to_string(path).ok()?;
    let mut lines = content.lines();
    let state = CkptState::parse(lines.next()?.trim())?;
    let anchor: u64 = lines.next()?.trim().parse().ok()?;
    let owner = lines.next()?.trim().to_string();
    if owner.is_empty() {
        return None;
    }
    // `dedup` is the LAST line. New checkpoints are 4 lines
    // (state/anchor/owner/dedup); a checkpoint written by an earlier build of
    // this unreleased series carried a btime as line 4 with dedup last (5
    // lines) — reading the last line parses both identically. A checkpoint with
    // no dedup line at all has a non-`1` last line → `false`, matching the old
    // blind-append semantics.
    let dedup = content
        .lines()
        .last()
        .map(|l| l.trim() == "1")
        .unwrap_or(false);
    Some(Ckpt {
        state,
        anchor,
        owner,
        dedup,
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

/// Collect every sample timestamp already stored in a dest partition so a
/// re-copy over a preserved committed tail can SKIP samples D already holds —
/// making the re-copy idempotent by construction, with no dependence on any
/// filesystem-provided birth time.
/// Only called when D is known non-empty (`len_before > 0`), so a missing file
/// or read error propagates (aborting the move, keeping the source for a retry)
/// rather than silently producing a duplicate.
/// Collect the timestamps in dest partition `path` that fall in the half-open
/// range `[lo, hi)`. Bounding to one source partition's range keeps the dedup
/// set O(source partition) rather than O(dest partition): a coarse LTS-year D
/// holds millions of samples, but — since each finer source covers a DISJOINT
/// sub-range of D — only the one-finer-partition slice `[lo, hi)` that overlaps
/// this source can collide with a source sample. Reading all of D would build a
/// multi-hundred-MB set for nothing (and OOM a constrained node), and this dedup
/// path runs on every committed-owner retry.
fn read_partition_timestamps_in_range(
    path: &Path,
    lo: SystemTime,
    hi: SystemTime,
) -> anyhow::Result<HashSet<SystemTime>> {
    let mut reader = PbFileReader::open(path)?;
    let mut set = HashSet::new();
    while let Some(sample) = reader.next_event()? {
        if sample.timestamp >= lo && sample.timestamp < hi {
            set.insert(sample.timestamp);
        }
    }
    Ok(set)
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
            &ckpt_contents(CkptState::Copying, 0, s_filename, false),
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
            &ckpt_contents(CkptState::Copying, anchor, s_filename, false),
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
            &ckpt_contents(CkptState::Copying, 0, owner_name, false),
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
        // `<D>.pb.etl_ckpt` survives with the still-present source. The resume
        // NEVER truncates a committed tail (it cannot prove same-vs-re-created,
        // and does not try): it preserves the tail and dedup-re-copies the
        // source against D — every sample is already present, so nothing is
        // appended (no duplicate) — then deletes the source and clears the
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
        // `committed` checkpoint (anchor 0 = D's pre-append length) is still
        // present because the delete hadn't run yet.
        let dbr = samples[0].value.db_type();
        for s in &samples {
            dest.append_event(pv, dbr, s).await.unwrap();
        }
        dest.flush_writes().await.unwrap();
        let s_filename = s_path.file_name().unwrap().to_str().unwrap();
        dest.create_etl_sidecar(
            &ckpt,
            &ckpt_contents(CkptState::Committed, 0, s_filename, false),
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
            &ckpt_contents(CkptState::Copying, 0, "SimpleSine:2023_11_14_22.pb", false),
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
        // truncated — the state, not any filesystem birth time, gates the
        // truncate, so the committed samples survive on EVERY filesystem. The
        // move preserves the tail, dedup-re-copies the source after it, and
        // appends the new samples.
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
        // 0 (BELOW the committed data). The state is `committed`, so the tail is
        // never truncated; if truncation to 0 were (wrongly) taken it would
        // destroy the 20 committed samples and this test would read 5.
        dest.create_etl_sidecar(
            &ckpt,
            &ckpt_contents(CkptState::Committed, 0, s_filename, false),
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
        // kept) then GREW in place via a backfill append. The retry must NOT
        // truncate the committed tail and blind re-append (that would duplicate
        // the 20 originals already in D): it preserves the tail and dedup-
        // re-copies the grown source, appending only the 5 new samples — D holds
        // each sample exactly once.
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
        // grew before the gated delete). The retry sees `committed` and
        // dedup-re-copies the (now grown) source against D.
        let dbr = original[0].value.db_type();
        for e in &original {
            dest.append_event(pv, dbr, e).await.unwrap();
        }
        dest.flush_writes().await.unwrap();
        dest.create_etl_sidecar(
            &ckpt,
            &ckpt_contents(CkptState::Committed, 0, s_filename, false),
        )
        .unwrap();

        // Backfill appends 5 more to the SAME source file, flushed so the
        // on-disk source is now 25 samples.
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
    async fn move_file_copying_checkpoint_truncates_partial() {
        // A `copying` checkpoint always safely rolls back a partial tail. The
        // `copying` state PROVES the source was never deleted (deletion happens
        // only after the `committed` transition), so the still-present source is
        // this same instance and truncate + re-copy is correct — no loss, no
        // duplicate. This is the common retry path, and the state alone decides
        // it, with no dependence on any filesystem birth time.
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

        // A 10-sample partial in D and a `copying` checkpoint.
        let dbr = samples[0].value.db_type();
        for s in &samples[..10] {
            dest.append_event(pv, dbr, s).await.unwrap();
        }
        dest.flush_writes().await.unwrap();
        let s_filename = s_path.file_name().unwrap().to_str().unwrap();
        dest.create_etl_sidecar(
            &ckpt,
            &ckpt_contents(CkptState::Copying, 0, s_filename, false),
        )
        .unwrap();

        exec.move_file(&s_path).await.unwrap();

        assert_eq!(
            count_samples(&d_path),
            50,
            "copying state truncated the partial — no duplicate"
        );
        assert!(!s_path.exists());
        assert!(!ckpt.exists());
    }

    #[tokio::test]
    async fn move_file_committed_checkpoint_never_loses_committed_tail() {
        // The set-merge (Round-5 Finding 3): a `committed` checkpoint whose
        // same-named source is still present (in reality this same instance — a
        // crash between the `committed` transition and the source delete).
        // Nothing on disk can PROVE same-vs-re-created, so we never truncate the
        // committed tail (no loss on any filesystem) AND we dedup the re-copy
        // against D. Since D already holds this exact source's 20 committed
        // samples, every source sample is skipped: the retry is idempotent — no
        // loss AND no duplicate.
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
            &ckpt_contents(CkptState::Committed, 0, s_filename, false),
        )
        .unwrap();

        exec.move_file(&s_path).await.unwrap();

        // The committed 20 are preserved (never truncated) AND the re-copy is
        // deduped against D — every source sample is already present, so nothing
        // is appended: D reads 20. A truncate-to-0 would have destroyed the
        // committed 20 (loss → 20 different bytes); a blind re-copy would have
        // yielded 40 (the duplicate the set-merge forecloses).
        assert_eq!(
            count_samples(&d_path),
            20,
            "same-instance retry: tail preserved, re-copy deduped — no loss, no dup"
        );
        assert!(!s_path.exists(), "source moved out");
        assert!(!ckpt.exists(), "checkpoint cleared on commit");
    }

    #[tokio::test]
    async fn move_file_committed_retry_superset_source_dedups_overlap() {
        // Set-merge boundary (Round-5 Finding 3): a committed-owner retry we
        // cannot prove same-instance, where the source OVERLAPS D's committed
        // tail (a replay re-containing already-migrated samples PLUS new ones).
        // The dedup re-copy skips the overlap and appends only the new samples —
        // no duplicate, no loss. A naive "content-equal → skip" would wrongly
        // re-copy the whole superset (duplicating the overlap).
        let tmp = tempfile::tempdir().unwrap();
        let src = plugin(tmp.path().join("sts"), "STS", PartitionGranularity::Hour);
        let dest = plugin(tmp.path().join("mts"), "MTS", PartitionGranularity::Day);
        let exec = EtlExecutor::new(src.clone(), dest.clone(), 3600, 0, 100);

        let pv = "SimpleSine";
        // D holds committed [t0..t19].
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

        // The source is a SUPERSET: [t0..t24] (the 20 already in D + 5 new).
        let superset: Vec<_> = (0..25)
            .map(|i| sample_at(BASE_SECS + i, i as f64))
            .collect();
        let s_path = write_source_partition(&src, pv, &superset).await;
        let s_filename = s_path.file_name().unwrap().to_str().unwrap();
        // Committed + owner match → cannot prove same-instance → preserve tail
        // + dedup re-copy.
        dest.create_etl_sidecar(
            &ckpt,
            &ckpt_contents(CkptState::Committed, 0, s_filename, false),
        )
        .unwrap();

        exec.move_file(&s_path).await.unwrap();

        assert_eq!(
            count_samples(&d_path),
            25,
            "overlap [t0..t19] deduped, [t20..t24] appended — no dup, no loss"
        );
        assert!(!s_path.exists());
        assert!(!ckpt.exists());
    }

    #[tokio::test]
    async fn move_file_committed_retry_gapfill_source_not_lost() {
        // Set-merge boundary (Round-5 Finding 3): a committed-owner retry where
        // the re-created source fills GAPS older than D's newest committed
        // sample. A high-water-mark rule ("copy only ts > max in D") would DROP
        // them (loss); the set-merge keeps them (they are absent from D) — no
        // loss, no dup.
        let tmp = tempfile::tempdir().unwrap();
        let src = plugin(tmp.path().join("sts"), "STS", PartitionGranularity::Hour);
        let dest = plugin(tmp.path().join("mts"), "MTS", PartitionGranularity::Day);
        let exec = EtlExecutor::new(src.clone(), dest.clone(), 3600, 0, 100);

        let pv = "SimpleSine";
        // D holds SPARSE committed [t0, t10, t20, t30].
        let committed: Vec<_> = [0u64, 10, 20, 30]
            .into_iter()
            .map(|i| sample_at(BASE_SECS + i, i as f64))
            .collect();
        let dbr = committed[0].value.db_type();
        for e in &committed {
            dest.append_event(pv, dbr, e).await.unwrap();
        }
        dest.flush_writes().await.unwrap();
        let d_path = dest.file_path_for(pv, committed[0].timestamp);
        let ckpt = d_path.with_extension("pb.etl_ckpt");

        // Re-created source: gap-fillers [t5, t15] — older than t30, absent
        // from D.
        let gaps: Vec<_> = [5u64, 15]
            .into_iter()
            .map(|i| sample_at(BASE_SECS + i, 900.0 + i as f64))
            .collect();
        let s_path = write_source_partition(&src, pv, &gaps).await;
        let s_filename = s_path.file_name().unwrap().to_str().unwrap();
        dest.create_etl_sidecar(
            &ckpt,
            &ckpt_contents(CkptState::Committed, 0, s_filename, false),
        )
        .unwrap();

        exec.move_file(&s_path).await.unwrap();

        assert_eq!(
            count_samples(&d_path),
            4 + 2,
            "gap-fillers [t5,t15] kept (absent from D) — no loss"
        );
        assert!(!s_path.exists());
        assert!(!ckpt.exists());
    }

    #[tokio::test]
    async fn move_file_dedup_copying_resume_same_instance_no_duplicate() {
        // Round-6 Finding 1 (regression). An interrupted dedup re-copy: a
        // `Committed` owner-retry entered the dedup branch (preserve tail +
        // re-anchor), skipped every already-present sample, then
        // crashed/timed-out BEFORE the `committed` transition — leaving a
        // `Copying` checkpoint whose `dedup` mode is now PERSISTED and whose
        // tail `[anchor..]` is empty (all skipped). The resume must re-copy WITH
        // dedup (skip all → append nothing), not blind re-append the source
        // after the anchor. Before `dedup` was persisted the resume read plain
        // `Copying` and blind re-copied, duplicating the preserved tail (D→40).
        let tmp = tempfile::tempdir().unwrap();
        let src = plugin(tmp.path().join("sts"), "STS", PartitionGranularity::Hour);
        let dest = plugin(tmp.path().join("mts"), "MTS", PartitionGranularity::Day);
        let exec = EtlExecutor::new(src.clone(), dest.clone(), 3600, 0, 100);

        let pv = "SimpleSine";
        let samples: Vec<_> = (0..20)
            .map(|i| sample_at(BASE_SECS + i, i as f64))
            .collect();
        // D durably holds this source's committed 20; the source still holds the
        // SAME 20 (this same instance).
        let s_path = write_source_partition(&src, pv, &samples).await;
        let d_path = dest.file_path_for(pv, samples[0].timestamp);
        let ckpt = d_path.with_extension("pb.etl_ckpt");
        let dbr = samples[0].value.db_type();
        for e in &samples {
            dest.append_event(pv, dbr, e).await.unwrap();
        }
        dest.flush_writes().await.unwrap();
        // Anchor at D's full length: the preserved committed tail sits BELOW it,
        // `[anchor..]` is empty (the interrupted dedup copy appended nothing).
        let anchor = std::fs::metadata(&d_path).unwrap().len();
        let s_filename = s_path.file_name().unwrap().to_str().unwrap();
        dest.create_etl_sidecar(
            &ckpt,
            &ckpt_contents(CkptState::Copying, anchor, s_filename, true),
        )
        .unwrap();

        exec.move_file(&s_path).await.unwrap();

        assert_eq!(
            count_samples(&d_path),
            20,
            "dedup Copying resume re-deduped (skipped all) — no dup (blind re-append would give 40)"
        );
        assert!(!s_path.exists(), "source moved out");
        assert!(!ckpt.exists(), "checkpoint cleared on commit");
    }

    #[tokio::test]
    async fn move_file_dedup_copying_resume_truncates_partial_and_rededuplicates() {
        // Round-6 Finding 1, superset boundary. An interrupted dedup re-copy
        // that HAD appended a partial tail past the anchor before dying. The
        // resume must truncate that partial (this cycle's re-derivable work) and
        // re-copy WITH dedup — skipping D's preserved [t0..t19] and re-appending
        // exactly [t20..t24]. No duplicate of the preserved tail, no torn
        // partial left behind.
        let tmp = tempfile::tempdir().unwrap();
        let src = plugin(tmp.path().join("sts"), "STS", PartitionGranularity::Hour);
        let dest = plugin(tmp.path().join("mts"), "MTS", PartitionGranularity::Day);
        let exec = EtlExecutor::new(src.clone(), dest.clone(), 3600, 0, 100);

        let pv = "SimpleSine";
        let full: Vec<_> = (0..25)
            .map(|i| sample_at(BASE_SECS + i, i as f64))
            .collect();
        let d_path = dest.file_path_for(pv, full[0].timestamp);
        let ckpt = d_path.with_extension("pb.etl_ckpt");
        let dbr = full[0].value.db_type();
        // D holds the preserved committed [t0..t19].
        for e in &full[..20] {
            dest.append_event(pv, dbr, e).await.unwrap();
        }
        dest.flush_writes().await.unwrap();
        let anchor = std::fs::metadata(&d_path).unwrap().len();
        // An interrupted dedup copy had appended a partial [t20,t21] past anchor.
        for e in &full[20..22] {
            dest.append_event(pv, dbr, e).await.unwrap();
        }
        dest.flush_writes().await.unwrap();
        // Source is the full superset [t0..t24].
        let s_path = write_source_partition(&src, pv, &full).await;
        let s_filename = s_path.file_name().unwrap().to_str().unwrap();
        dest.create_etl_sidecar(
            &ckpt,
            &ckpt_contents(CkptState::Copying, anchor, s_filename, true),
        )
        .unwrap();

        exec.move_file(&s_path).await.unwrap();

        assert_eq!(
            count_samples(&d_path),
            25,
            "partial [t20,t21] truncated, dedup re-copy skipped [t0..t19] + re-appended [t20..t24] — no dup, no loss"
        );
        assert!(!s_path.exists());
        assert!(!ckpt.exists());
    }

    #[tokio::test]
    async fn move_file_dedup_committed_resume_deferred_delete_no_duplicate() {
        // Round-6 Finding 1, second reachability (no crash needed). A SUCCESSFUL
        // dedup copy whose source delete DEFERRED (source live/grown). Cycle 1
        // committed a `Committed` checkpoint (anchor = the [t0..t19] boundary);
        // the source is still on disk (same instance). On resume the `Committed`
        // state preserves the tail and dedup-re-copies the source — appending
        // nothing (all present) — NOT a blind re-append that would duplicate the
        // preserved [t0..t19] below the anchor (D→45).
        let tmp = tempfile::tempdir().unwrap();
        let src = plugin(tmp.path().join("sts"), "STS", PartitionGranularity::Hour);
        let dest = plugin(tmp.path().join("mts"), "MTS", PartitionGranularity::Day);
        let exec = EtlExecutor::new(src.clone(), dest.clone(), 3600, 0, 100);

        let pv = "SimpleSine";
        let full: Vec<_> = (0..25)
            .map(|i| sample_at(BASE_SECS + i, i as f64))
            .collect();
        let d_path = dest.file_path_for(pv, full[0].timestamp);
        let ckpt = d_path.with_extension("pb.etl_ckpt");
        let dbr = full[0].value.db_type();
        // The completed dedup copy: D holds [t0..t19] preserved + [t20..t24]
        // appended = [t0..t24]; `anchor` is the [t0..t19] boundary.
        for e in &full[..20] {
            dest.append_event(pv, dbr, e).await.unwrap();
        }
        dest.flush_writes().await.unwrap();
        let anchor = std::fs::metadata(&d_path).unwrap().len();
        for e in &full[20..] {
            dest.append_event(pv, dbr, e).await.unwrap();
        }
        dest.flush_writes().await.unwrap();
        // Source still present (delete deferred), SAME instance.
        let s_path = write_source_partition(&src, pv, &full).await;
        let s_filename = s_path.file_name().unwrap().to_str().unwrap();
        dest.create_etl_sidecar(
            &ckpt,
            &ckpt_contents(CkptState::Committed, anchor, s_filename, true),
        )
        .unwrap();

        exec.move_file(&s_path).await.unwrap();

        assert_eq!(
            count_samples(&d_path),
            25,
            "Committed+dedup deferred-delete resume re-deduped — no dup (a blind re-append would give 45)"
        );
        assert!(!s_path.exists());
        assert!(!ckpt.exists());
    }

    #[tokio::test]
    async fn move_file_committed_dedup_recreated_subset_source_never_truncates_tail() {
        // Round-10 protocol Finding 1 (silent loss of committed archive data). A
        // `Committed`/dedup=1 checkpoint whose `anchor` sits BELOW a committed
        // tail, whose owner source was deleted (a crash in the
        // delete→uncheckpoint window) then RE-CREATED as a gap-fill SUBSET that
        // no longer holds the tail's samples. The truncate is gated on `state`,
        // and ONLY `state`: a `Committed` tail is NEVER rolled back. Gating it on
        // `dedup` (the pre-fix bug) would roll D back to `anchor`, dropping the
        // committed tail [t20..t24] the re-created source cannot re-derive —
        // silent loss. Correct behavior: preserve ALL of D and dedup-re-copy the
        // subset AFTER it.
        let tmp = tempfile::tempdir().unwrap();
        let src = plugin(tmp.path().join("sts"), "STS", PartitionGranularity::Hour);
        let dest = plugin(tmp.path().join("mts"), "MTS", PartitionGranularity::Day);
        let exec = EtlExecutor::new(src.clone(), dest.clone(), 3600, 0, 100);

        let pv = "SimpleSine";
        let full: Vec<_> = (0..25)
            .map(|i| sample_at(BASE_SECS + i, i as f64))
            .collect();
        let d_path = dest.file_path_for(pv, full[0].timestamp);
        let ckpt = d_path.with_extension("pb.etl_ckpt");
        let dbr = full[0].value.db_type();
        // D holds [t0..t19] below `anchor` + a committed tail [t20..t24].
        for e in &full[..20] {
            dest.append_event(pv, dbr, e).await.unwrap();
        }
        dest.flush_writes().await.unwrap();
        let anchor = std::fs::metadata(&d_path).unwrap().len();
        for e in &full[20..] {
            dest.append_event(pv, dbr, e).await.unwrap();
        }
        dest.flush_writes().await.unwrap();

        // The owner source was deleted then re-created as a gap-fill SUBSET — a
        // single fresh backfilled sample in the same hour (so the same source
        // filename / checkpoint owner), NOT the [t20..t24] committed tail.
        let subset = vec![sample_at(BASE_SECS + 30, 30.0)];
        let s_path = write_source_partition(&src, pv, &subset).await;
        let s_filename = s_path.file_name().unwrap().to_str().unwrap();
        dest.create_etl_sidecar(
            &ckpt,
            &ckpt_contents(CkptState::Committed, anchor, s_filename, true),
        )
        .unwrap();

        exec.move_file(&s_path).await.unwrap();

        // Committed tail [t20..t24] survives (never truncated); the subset's
        // fresh sample is dedup-appended after it. D = 25 (t0..t24) + 1 = 26.
        // The pre-fix `dedup`-gated truncate rolled D back to `anchor` =
        // |t0..t19|, dropping the 5 tail samples → 21.
        assert_eq!(
            count_samples(&d_path),
            26,
            "committed tail preserved + subset appended (26); a dedup-gated \
             truncate would drop the 5 tail samples (21)"
        );
        assert!(!s_path.exists());
        assert!(!ckpt.exists());
    }

    #[tokio::test]
    async fn move_file_v040_done_marker_dedups_not_reduplicates() {
        // Round-7 Finding 1 (v0.4.0 upgrade regression). The released v0.4.0
        // used a `<S>.pb.etl_done` marker (written after copy+flush into D,
        // before the source delete) as its idempotency guard; this redesign
        // retired it. A source left in that window at upgrade has its full copy
        // in D but NO HEAD checkpoint — a fresh blind re-copy would DUPLICATE it
        // (D→40; retrieval does not collapse it). HEAD must recognize the marker
        // and dedup.
        let tmp = tempfile::tempdir().unwrap();
        let src = plugin(tmp.path().join("sts"), "STS", PartitionGranularity::Hour);
        let dest = plugin(tmp.path().join("mts"), "MTS", PartitionGranularity::Day);
        let exec = EtlExecutor::new(src.clone(), dest.clone(), 3600, 0, 100);

        let pv = "SimpleSine";
        let samples: Vec<_> = (0..20)
            .map(|i| sample_at(BASE_SECS + i, i as f64))
            .collect();
        // D holds S's full v0.4.0 copy; S is still on disk; the v0.4.0 marker is
        // present; there is NO HEAD `.etl_ckpt`.
        let s_path = write_source_partition(&src, pv, &samples).await;
        let d_path = dest.file_path_for(pv, samples[0].timestamp);
        let ckpt = d_path.with_extension("pb.etl_ckpt");
        let dbr = samples[0].value.db_type();
        for e in &samples {
            dest.append_event(pv, dbr, e).await.unwrap();
        }
        dest.flush_writes().await.unwrap();
        let marker = s_path.with_extension("pb.etl_done");
        std::fs::write(&marker, b"").unwrap();
        assert!(!ckpt.exists(), "v0.4.0 wrote no HEAD checkpoint");

        exec.move_file(&s_path).await.unwrap();

        assert_eq!(
            count_samples(&d_path),
            20,
            "v0.4.0 marker recognized: dedup skipped all — no dup (blind copy would give 40)"
        );
        assert!(!s_path.exists(), "source moved out");
        assert!(!marker.exists(), "v0.4.0 marker cleared");
        assert!(!ckpt.exists(), "HEAD checkpoint cleared on commit");
    }

    #[tokio::test]
    async fn move_file_v040_done_marker_grown_source_no_loss() {
        // Round-7 Finding 1, grown-source boundary. A backfill grew S after
        // v0.4.0 wrote the marker (D holds only the original copy). Routing the
        // marker through the dedup re-copy (not v0.4.0's unconditional delete)
        // migrates the new samples too — no loss AND no dup.
        let tmp = tempfile::tempdir().unwrap();
        let src = plugin(tmp.path().join("sts"), "STS", PartitionGranularity::Hour);
        let dest = plugin(tmp.path().join("mts"), "MTS", PartitionGranularity::Day);
        let exec = EtlExecutor::new(src.clone(), dest.clone(), 3600, 0, 100);

        let pv = "SimpleSine";
        let full: Vec<_> = (0..25)
            .map(|i| sample_at(BASE_SECS + i, i as f64))
            .collect();
        let d_path = dest.file_path_for(pv, full[0].timestamp);
        let ckpt = d_path.with_extension("pb.etl_ckpt");
        let dbr = full[0].value.db_type();
        // D holds only v0.4.0's original copy [t0..t19].
        for e in &full[..20] {
            dest.append_event(pv, dbr, e).await.unwrap();
        }
        dest.flush_writes().await.unwrap();
        // The source grew to [t0..t24] after the marker (a backfill); marker set.
        let s_path = write_source_partition(&src, pv, &full).await;
        let marker = s_path.with_extension("pb.etl_done");
        std::fs::write(&marker, b"").unwrap();

        exec.move_file(&s_path).await.unwrap();

        assert_eq!(
            count_samples(&d_path),
            25,
            "marker dedup: [t0..t19] skipped, [t20..t24] migrated — no loss, no dup"
        );
        assert!(!s_path.exists());
        assert!(!marker.exists());
        assert!(!ckpt.exists());
    }

    #[tokio::test]
    async fn move_file_absent_source_is_deferred_not_error() {
        // Round-6 OBS-A: consolidate_pv lists a PV's files BEFORE taking the
        // per-move gate, so the periodic loop (sharing this executor's
        // move_gate) can migrate one first. move_file on the now-absent source
        // must report Ok(false) (nothing to move), not Err — otherwise
        // consolidate_pv spuriously hard-fails the whole PV over a file that was
        // in fact correctly migrated.
        let tmp = tempfile::tempdir().unwrap();
        let src = plugin(tmp.path().join("sts"), "STS", PartitionGranularity::Hour);
        let dest = plugin(tmp.path().join("mts"), "MTS", PartitionGranularity::Day);
        let exec = EtlExecutor::new(src.clone(), dest.clone(), 3600, 0, 100);

        let absent = tmp.path().join("sts").join("SimpleSine:2023_11_14_22.pb");
        assert!(!absent.exists());
        let moved = exec.move_file(&absent).await.unwrap();
        assert!(
            !moved,
            "absent source reported deferred (Ok(false)), not error"
        );
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
                false,
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
            &ckpt_contents(CkptState::Copying, 0, live_owner, false),
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
                false,
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
    async fn reap_copying_orphan_preserves_tail_never_truncates() {
        // Round-5 Finding 1: with fsync_on_flush=false (the default) and
        // source/dest on independent filesystems, the commit's copying→committed
        // rename and the source unlink can persist OUT OF ORDER under power
        // loss, leaving a stale Copying checkpoint standing over an
        // already-committed, durable tail whose source is gone. The reap must
        // NEVER truncate an owner-gone orphan — rolling that tail back to the
        // anchor would erase committed data that exists nowhere else. It removes
        // the checkpoint (to unwedge the next tier) and leaves D byte-for-byte.
        let tmp = tempfile::tempdir().unwrap();
        let src = plugin(tmp.path().join("sts"), "STS", PartitionGranularity::Hour);
        let dest = plugin(tmp.path().join("mts"), "MTS", PartitionGranularity::Day);
        let exec = EtlExecutor::new(src.clone(), dest.clone(), 3600, 0, 100);

        let pv = "SimpleSine";
        // D holds 20 durably-committed samples.
        let samples: Vec<_> = (0..20)
            .map(|i| sample_at(BASE_SECS + i, i as f64))
            .collect();
        let dbr = samples[0].value.db_type();
        for e in &samples {
            dest.append_event(pv, dbr, e).await.unwrap();
        }
        dest.flush_writes().await.unwrap();
        let d_path = dest.file_path_for(pv, samples[0].timestamp);
        let ckpt = d_path.with_extension("pb.etl_ckpt");
        // A stale COPYING checkpoint (owner gone), anchored at 0 — the OLD reap
        // would truncate D to 0, wiping all 20 committed samples.
        dest.create_etl_sidecar(
            &ckpt,
            &ckpt_contents(CkptState::Copying, 0, "SimpleSine:1999_01_01_00.pb", false),
        )
        .unwrap();
        // Source root must exist for the reap to run.
        write_source_partition(&src, pv, &[sample_at(BASE_SECS + 5 * 86_400, 0.0)]).await;

        exec.run_once().await.unwrap();

        assert!(
            !ckpt.exists(),
            "stale copying orphan checkpoint removed (unwedged)"
        );
        assert_eq!(
            count_samples(&d_path),
            20,
            "committed tail preserved — NOT truncated"
        );
    }

    #[tokio::test]
    async fn reap_uncertain_owner_stat_does_not_reap() {
        // Round-5 Finding 2: the reap must not treat a transient stat error
        // (ESTALE / EIO on a flaky mount) as "owner gone" — `exists()` coerces
        // every stat error to `false`, which would reap a live owner's
        // checkpoint and make the next move re-copy after the committed tail (a
        // duplicate). Only a DEFINITIVE Ok(false) reaps. Here the owner's parent
        // is a regular file, so `try_exists` on the owner path returns
        // Err(ENOTDIR) — the "uncertain" case — and the checkpoint must survive.
        let tmp = tempfile::tempdir().unwrap();
        let src = plugin(tmp.path().join("sts"), "STS", PartitionGranularity::Hour);
        let dest = plugin(tmp.path().join("mts"), "MTS", PartitionGranularity::Day);
        let exec = EtlExecutor::new(src.clone(), dest.clone(), 3600, 0, 100);

        // A colon in the PV name → pv_key "Sim/Orphan" → the partition lives in
        // a subdir (`<root>/Sim/Orphan:<part>.pb`), so the reap reconstructs the
        // owner under `source_root/Sim/…`. Making `source_root/Sim` a regular
        // FILE forces `try_exists(owner)` to fail with ENOTDIR.
        let pv = "Sim:Orphan";
        let samples: Vec<_> = (0..20)
            .map(|i| sample_at(BASE_SECS + i, i as f64))
            .collect();
        let dbr = samples[0].value.db_type();
        for e in &samples {
            dest.append_event(pv, dbr, e).await.unwrap();
        }
        dest.flush_writes().await.unwrap();
        let d_path = dest.file_path_for(pv, samples[0].timestamp);
        let ckpt = d_path.with_extension("pb.etl_ckpt");
        dest.create_etl_sidecar(
            &ckpt,
            &ckpt_contents(CkptState::Committed, 0, "Orphan:1999_01_01_00.pb", false),
        )
        .unwrap();

        // source_root must exist for the reap to run; `source_root/Sim` is a
        // regular file so the reap's owner stat errors (ENOTDIR) rather than
        // returning Ok(false).
        std::fs::create_dir_all(src.root_folder()).unwrap();
        std::fs::write(src.root_folder().join("Sim"), b"not a dir").unwrap();

        exec.run_once().await.unwrap();

        assert!(
            ckpt.exists(),
            "uncertain owner stat (ENOTDIR) must NOT reap the checkpoint"
        );
        assert_eq!(count_samples(&d_path), 20, "D untouched");
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
            &ckpt_contents(CkptState::Copying, 0, "finer-owner.pb", false),
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
