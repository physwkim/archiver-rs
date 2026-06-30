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
        // Keyed by PV name now (matching `grouped`), computed once per
        // tick to keep the registry lookup off the per-file path.
        let paused: HashSet<String> = match self.pv_registry.as_ref() {
            Some(reg) => reg
                .pvs_by_status(PvStatus::Paused)
                .map(|recs| recs.into_iter().map(|r| r.pv_name).collect())
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

    /// Move a single PB file from source to destination tier.
    /// Uses copy → durable dest → marker → slot-locked delete for
    /// crash-safe idempotency.
    async fn move_file(&self, source_path: &Path) -> anyhow::Result<()> {
        // Marker from a previous incomplete cleanup (crash after the
        // copy was made durable). The marker is written ONLY after the
        // destination flush below, so its presence proves the dest tier
        // already holds this partition's data — recovery deletes the
        // source without re-copying.
        let marker = source_path.with_extension("pb.etl_done");
        if marker.exists() {
            info!(
                ?source_path,
                "Found ETL marker — previous copy is durable in dest, cleaning up"
            );
            // Delete the source under the per-PV slot lock so no append
            // can land in the just-deleted inode, and a still-live/dirty
            // partition is never destroyed.
            let source = self.source.clone();
            let path = source_path.to_path_buf();
            match tokio::task::spawn_blocking(move || source.remove_moved_partition(&path)).await {
                Ok(Ok(true)) => {}
                Ok(Ok(false)) => {
                    // Source is the live partition with un-copied bytes —
                    // leave it AND the marker; a later cycle cleans up
                    // once it rolls over to non-live. Never delete a
                    // live, dirty partition.
                    warn!(
                        ?source_path,
                        "ETL marker recovery: source is still live/dirty; deferring delete"
                    );
                    return Ok(());
                }
                Ok(Err(e)) => warn!(
                    ?source_path,
                    "ETL marker recovery: failed to remove source: {e}"
                ),
                Err(e) => warn!(
                    ?source_path,
                    "ETL marker recovery: evict task panicked: {e}"
                ),
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
        let source = self.source.clone();
        let source_name = self.source.name().to_string();
        let dest_name = self.dest.name().to_string();

        tokio::time::timeout(timeout, async move {
            let mut reader = PbFileReader::open(&source_path)?;
            let desc = reader.description().clone();
            let dbr_type = desc.db_type;

            // Copy all samples to the destination tier.
            while let Some(sample) = reader.next_event()? {
                dest.append_event(&desc.pv_name, dbr_type, &sample).await?;
            }

            // Durability BEFORE delete: flush the destination tier so the
            // copy is in the page cache and survives a process crash
            // BEFORE we write the marker or touch the source. Only after
            // this does the marker mean "dest durably has it". Without
            // it, a crash after deleting the source but before dest's
            // BufWriter drained would lose the partition outright — and
            // the marker-recovery branch would then delete a source whose
            // data never reached dest.
            dest.flush_writes().await?;

            // Marker AFTER dest is durable — a crash here lets the next
            // run clean up the source without re-copying.
            let marker = source_path.with_extension("pb.etl_done");
            tokio::fs::write(&marker, b"").await?;

            // Slot-locked delete: remove the source under the per-PV slot
            // lock with a live-writer guard. No append can target the
            // inode between the liveness check and the unlink, and a
            // still-dirty live partition is never destroyed.
            let source_for_evict = source.clone();
            let path_for_evict = source_path.clone();
            let removed = tokio::task::spawn_blocking(move || {
                source_for_evict.remove_moved_partition(&path_for_evict)
            })
            .await;
            match removed {
                Ok(Ok(true)) => {
                    tokio::fs::remove_file(&marker).await.ok();
                    metrics::counter!(
                        "archiver_etl_files_moved_total",
                        "source" => source_name,
                        "dest" => dest_name,
                    )
                    .increment(1);
                    anyhow::Ok(())
                }
                Ok(Ok(false)) => {
                    // Still-live/dirty partition reached the mover
                    // (selection should exclude it; consolidate must
                    // pause the PV first). Leave BOTH source and marker:
                    // the dest copy is durable, and a later cycle's
                    // marker-recovery deletes the source once it rolls
                    // over to non-live. No re-copy, no loss.
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
        })
        .await
        .map_err(|_| {
            anyhow::anyhow!("ETL move_file timed out after {timeout:?} for {source_path_disp:?}")
        })??;

        Ok(())
    }
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
}
