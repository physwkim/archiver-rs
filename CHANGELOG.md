# Changelog

## v0.3.1 — 2026-05-08 (binary-only)

### Added

- **`--help` and `--version` flags on the `epics-archiver` binary.**
  Hand-rolled (no `clap` dependency) — handles `-h`/`--help` and
  `-V`/`--version` before reaching the config-load path. Without
  these, `epics-archiver --help` was treated as a config path and
  failed with a misleading "No such file or directory" error.
  README's Installing section claimed `--help` worked; it does now.

### Notes

- This is a binary-only release. The four library crates
  (`archiver-{proto,core,engine,api}`) are unchanged at v0.3.0.

## v0.3.0 — 2026-05-08

### Added

- **pvAccess (PVA) acquisition.** PVs marked `protocol = "pva"` in
  the registry are now acquired via `epics-rs`'s pvAccess client
  alongside Channel Access PVs. NTScalar / NTScalarArray values
  decode through the same `ArchiverSample` pipeline used by CA, so
  storage, retrieval, and the management API are protocol-agnostic.
  PVA monitor subscriptions surface `display.precision`,
  `display.units`, and the IOC-side `timeStamp` like CA's
  DBR_CTRL_* + DBR_TIME_* metadata. The management API shows the
  per-PV protocol in `getPVDetails` (CA / pva) and the PV detail
  modal renders it.
- **PVA archive-fields (NTScalar sub-fields).** `archiveFields`
  values for PVA PVs walk the NTScalar / NTScalarArray substructure
  (`valueAlarm.highAlarmLimit`, `control.limitHigh`, etc.) and
  surface them as per-sample extras using the same string-string
  shape Java archiver uses for `archiveFields` blobs.
- **Sharded write pool.** New `engine.write_shards` /
  `engine.per_shard_buffer` config keys. Sites with many active PVs
  and a fast STS can raise `write_shards` to e.g. 4-16; the engine
  spawns a dispatcher that hashes `pv_name` to a fixed shard
  (per-PV ordering preserved) plus N parallel shard append workers
  feeding a single global flush owner. The dispatcher uses
  `try_send` so a saturated shard's overflow drops on THAT shard
  only (per-PV `buffer_overflow_drops` + a per-shard metric); other
  shards keep flowing. `write_shards = 1` (the default) is the
  legacy single-worker layout with zero overhead.
- **Bounded fd cache.** `storage.max_open_writers_total`
  (process-wide cap shared across STS/MTS/LTS) and per-tier
  `storage.{sts,mts,lts}.max_open_writers`. `PlainPbStoragePlugin`
  draws from a CAS-based atomic permit pool; when at the cap
  `write_cached` proactively LRU-evicts before opening another fd
  (preferred-clean two-pass eviction). Defaults: STS=512,
  MTS/LTS=64. Surfaces `archiver_pb_lru_eviction_loss_total`,
  `archiver_pb_dirty_drop_loss_total`, and
  `archiver_storage_flush_timeouts_total` for observability.
- **Ticker-driven periodic flush.** A `tokio::time::interval`
  fires at `engine.write_period_secs` regardless of sample
  arrival, so a PV that goes silent still gets its buffered bytes
  persisted within one flush_period. The previous "only flush when
  a sample arrived AND elapsed > period" check kept the last
  buffered bytes hostage until shutdown when traffic stopped.
- **Coalescing per-PV pending map** (`PendingReports`). Replaces
  the prior mpsc report channel. Shard workers and their
  spawn_blocking late-success closures coalesce `(pv, ts)` into a
  shared `DashMap` via max-by-arrival; reports never drop, the
  map is bounded by PV count rather than sample rate, and a stale
  late-success cannot clobber a newer hot-path commit. The owner's
  ticker-driven flush snapshots the map, calls
  `flush_ingest_writes`, then commits.

### Changed

- **Write loop architecture (single-owner state transitions).**
  Shard append workers do append-only and report into the shared
  pending map. A single global flush owner is the SOLE consumer of
  pending, the SOLE caller of `flush_ingest_writes`, and the SOLE
  caller of `registry.batch_update_timestamps`. The prior
  per-shard `ts_updates` allowed flush failures to be attributed
  to the wrong shard's commit map; the global owner makes
  attribution structurally unambiguous.
- **Per-PV writer slots in PlainPB.** Replaced the global
  `Mutex<HashMap<String, CachedWriter>>` with
  `Mutex<HashMap<String, Arc<Mutex<PvWriterSlot>>>>`. The outer
  cache mutex is held only briefly for slot insert/lookup/remove;
  all I/O happens under the per-slot mutex. A stuck NFS syscall on
  one PV no longer blocks any other PV's append or flush.
- **`flush_ingest_writes` returns `IngestFlushResult { failed,
  deferred }`** instead of a single `Vec<String>`. `failed` PVs
  lost their bytes (drop the pending entry); `deferred` PVs are
  still buffered (keep the pending entry, retry next cycle). The
  prior single-list lump permanently lost the registry timestamp
  for any PV that went briefly busy and then silent.
- **Single source of truth for dirty-writer drops.** Every
  code path that drops a dirty BufWriter (partition rollover,
  ghost-file reopen, `evict_writer_for_path`, write_cached
  write-error path, LRU eviction, `delete_pv_data`, `rename_pv`)
  now routes through one of two helpers: `drop_dirty_writer`
  (try-flush, on failure record loss + `into_parts` to suppress
  the auto-flush) or `drop_writer_file_gone` (skip flush, record
  loss for any dirty bytes, `into_parts`). Closes the previous
  pattern where ad-hoc `flush().ignore()` could re-issue a failing
  syscall via `BufWriter::Drop`.
- **Tombstone lifecycle for `delete_pv_data` / `rename_pv`.**
  The slot stays in the cache with `dead = true` during the file
  ops so any concurrent `slot_for()` returns the same tombstoned
  slot and the append's dead-check bails before opening a file
  under the soon-to-be-deleted name. RAII `TombstoneCleanupGuard`
  removes the slot from cache on every return path including `?`
  and panic — a partway-failed delete/rename can no longer leave
  the PV permanently undead.
- **Conservative `last_ts` high-water on every storage return.**
  The shard's per-PV ordering check now bumps `last_ts` on Ok,
  Err, panic, AND timeout — not just success. Without it, a
  timed-out append whose spawn_blocking task eventually completed
  late could allow a subsequent older-timestamp sample to slip
  past the ordering check and end up out of order on disk via the
  per-PV slot mutex.
- **Flush truth.** `flush_dirty_writers` and `drop_dirty_writer`
  check `path.exists()` AFTER a successful `flush()`. A flush on
  a deleted inode returns Ok at the syscall level but the bytes
  are not reader-visible; this is now classified as loss and
  recorded for the next ingest flush.
- **Definitive `evict_writer_for_path`.** Fast path uses the
  deterministic `pv_name_to_key` encoding to derive the PV name
  from the file path and look up exactly one slot (single
  blocking lock). Falls back to a full scan only for paths that
  can't be parsed. The previous behaviour silently skipped slots
  held by an in-flight append, leaving a ghost writer that the
  next flush would mistakenly count as healthy.
- **Strict atomic fd reservation.** Replaced the check +
  `fetch_add` race with a CAS-based `try_reserve` (loop with
  `compare_exchange_weak`). The cap is now a hard ceiling under
  concurrency.
- **Ingest-only loss-queue drain.** Read-side `flush_writes`
  (retrieval, ETL) records writer-flush failures into
  `evicted_with_loss` but never drains it; only
  `flush_ingest_writes` (called by the global flush owner) drains.
  Without the separation, a retrieval-triggered flush failure
  could swallow a loss before the global owner ever saw it.
- **Conservative drop of failed PVs.** When `flush_ingest_writes`
  returns `failed = [X]`, the owner removes X from the pending
  map UNCONDITIONALLY — regardless of whether the snapshot's
  value still matches. The loss queue carries only PV names; it
  cannot disambiguate post-snapshot reports for "fresh-writer
  bytes ARE on disk" from "post-loss bytes ALSO gone". Conservative
  drop is the only safe move (under-commit, never over-commit).
- **Decoupled ingest from STS storage failures.** The engine's
  write_loop bounds each `append_event_with_meta` and
  `flush_ingest_writes` with `spawn_blocking` + bounded timeout
  so a stuck NFS syscall parks a blocking-pool thread instead of
  the runtime worker, and the loop continues making progress on
  other PVs. `flush_in_flight` (cleared by an RAII guard so a
  panic inside `block_on` can't leave it stuck `true`) prevents
  stacked spawn_blocking flushes onto a wedged FS.
- **Shutdown phase 2 polls `flush_in_flight`** before the final
  flush so a still-running ticker flush no longer short-circuits
  the shutdown commit.
- **Renamed root binary crate to `epics-archiver`** (was
  `archiver`). Both `archiver` and `archiver-rs` are taken on
  crates.io by unrelated crates. `cargo install epics-archiver`
  now installs the `epics-archiver` binary; the four library
  crates keep the `archiver-{proto,core,engine,api}` names.
- **Bumped `epics-rs` to 0.15.0** and added handling for the new
  `Int64` DBR variants.

### Fixed

- **Partial-header crash recovery.** A crash mid-creation could
  leave a `.pb` file with a 0-byte or partially-written header.
  `file_needs_header` now treats both as "needs fresh header" and
  truncates a partial-header file before re-writing.
- **Partial-sample trim on next open.** `trim_to_last_newline`
  truncates a `.pb` file to its last NEWLINE byte on next open
  after a crash, dropping at most one record but keeping the file
  readable for everything before it.
- **Atomic-at-buffer-layer sample frame.** `write_cached` writes
  the (escaped sample, NEWLINE) pair via a single `write_all` so
  BufWriter never splits a sample's frame across two internal
  flushes. Removes our contribution to the partial-record risk;
  OS-level page-boundary atomicity is still its own concern.
- **`write_cached` poison recovery.** Per-PV slot mutexes recover
  from poisoned state via `unwrap_or_else(|e| e.into_inner())`
  rather than failing every future append for a PV whose writer
  once panicked.
- **EMFILE / ENFILE recovery.** When the OS file-handle table is
  exhausted, `write_cached` evicts an LRU writer and retries the
  open instead of failing every subsequent write until partition
  rollover.
- **ETL ghost-write fix.** After ETL deletes a `.pb` source file,
  the engine's cached BufWriter for that path is evicted so
  subsequent appends don't write into the orphaned inode.
- **DBR_CTRL metadata refresh on reconnect.** Display precision /
  units / alarm limits are re-fetched on CA reconnect rather than
  inheriting stale values from before the disconnect.
- **`getDroppedEventsBufferOverflowReport` bookkeeping.** Per-PV
  `buffer_overflow_drops` counter increments when the dispatcher
  or a shard's mpsc rejects a sample, surfacing in the management
  report.
- **Cancel-bound CA metadata fetch.** Metadata fetch tasks now
  hold a child `CancellationToken` of the PV's main token so
  stopping a PV stops its metadata refresh promptly.
- **Data viewer rework + smaller leak fixes.** UI-side cleanups
  in the management console + fixes to subtle leaks in the
  retrieval path uncovered during the v0.3.0 rework.

## v0.1.5 — 2026-04-14

### Fixed

- **Write-cache file-handle leak in `PlainPbStoragePlugin`.** The cache
  was keyed by partition path and `flush_writes()` only evicted entries
  whose flush failed, so every hourly partition rollover added a new
  `BufWriter` + file descriptor per PV without ever releasing the old
  one. With 500 PVs and hourly STS partitions the cache grew by ~500
  fds/hour, eventually hitting the process fd limit after about a day —
  matching the observed "archiver stops ingesting after ~24 h" symptom.
  Cache is now keyed by PV name (one writer per PV); on partition
  rollover the old writer is flushed and dropped before the new one is
  opened.
- **Monitor-loop reconnection race.** After `wait_connected` timed out,
  the code subscribed to `connection_events()` and waited for a
  `Connected` event, but `tokio::sync::broadcast` only delivers
  messages sent after `subscribe()`. Any `Connected` firing between the
  timeout and the subscribe call was lost and the PV stayed
  disconnected until the next full disconnect/reconnect cycle (a PV
  could go zombie for the remaining lifetime of the process). Subscribe
  before probing, re-probe with a short `wait_connected` to cover the
  subscribe-time window, and handle `broadcast::RecvError::Lagged` by
  re-probing instead of killing the monitor task.
- **Unbounded `/data/getData.raw` blocking task.** The raw-format
  handler spawned a `spawn_blocking` task without the
  `RETRIEVAL_DEADLINE_SECS` (300 s) wall-clock cap that the JSON and
  CSV handlers already enforce. A slow or stalled client could pin a
  blocking worker, the `EventStream`s it owns, and the underlying file
  handles indefinitely. Same 300 s cap now applies to raw.

## v0.1.4 — 2026-04-14

### Added

- **Bundle `ca-repeater-rs` binary alongside `archiver`.** On startup,
  `epics-ca-rs`'s `CaClient::new()` calls `ensure_repeater()`, which
  registers with an existing repeater or spawns one from the sibling
  `ca-repeater-rs` binary next to the running executable (falling back
  to an in-process thread if the binary is absent). Shipping the binary
  lets the repeater run as an independent process — matching the C
  EPICS `caRepeater` model — so a repeater crash or restart doesn't
  share a failure domain with the archiver, and the single
  host-wide repeater is cleanly shared with other CA clients on the
  host (no port-5065 contention within our own process tree).

### Changed

- Upgrade epics-rs pin (`5c27fff` → `7f9a6c0`). No client-facing
  functional changes in this range; the audit commit touches CA
  server-side code (`WRITE_NOTIFY` `write_count` echo, `CLEAR_CHANNEL`
  cleanup) and non-CA records/records-engine paths, none of which
  affect archiver-rs as a CA client.

## v0.1.3 — 2026-04-13

### Changed

- **Upgrade epics-rs** (`d593a27` → `5c27fff`):
  - `5c27fff` ca: Pre-connection subscription, get_with_timeout, tool
    refinements
  - `0b9be88` CA tools: Parallel PV handling, timeout plumbing, error
    distinction
  - `b1ca96a` epics-macros-rs: Resolve epics_base_rs path for umbrella
    crate users
  - `934095b` epics-rs: Add ioc feature to umbrella crate

## v0.1.2 — 2026-04-13

### Changed

- **Upgrade epics-rs** (`64f5977` → `d593a27`):
  - `d593a27` ca: Rewrite monitor flow control to track application backlog
    (C parity) — replaces TCP read count heuristic with actual
    application-level backlog tracking; the old implementation counted TCP
    reads and sent CA_PROTO_EVENTS_OFF after 10 reads, which overshoots on
    fragmented links and stalls remote C IOCs. Server side now properly
    implements EVENTS_OFF/ON with coalesce-while-paused semantics matching
    C EPICS dbEvent.c behavior.
  - `4c589f1` ca-rs: Send NORD elements for waveform instead of
    NELM-padded array — fixes CA clients that compute dimensions from
    element count.

## v0.1.1 — 2026-04-13

### Fixed

- **CA reconnection after IOC restart**: Upgraded epics-rs from v0.8.2
  (`c959530`) to v0.9.0 (`64f5977`), which includes critical fixes for
  beacon anomaly detection and reconnection:
  - `88dd556` ca: Fix reconnection, CPU usage, C interop, and harden robustness
  - `b59bb94` asyn-rs: Remove unbounded sync channel from InterruptManager
  - `4bcd9ea` ca-rs: Use real server IP in search replies and beacons instead of INADDR_ANY

  The previous version's beacon monitor was skipping beacons with
  `available=INADDR_ANY`, which modern IOCs always send. This effectively
  disabled beacon anomaly detection, so when an IOC restarted, the archiver
  could not detect the restart and trigger a re-search. Subscriptions
  became zombies — no data arrived until the archiver process was manually
  restarted. Symptom: PVs like `G:BEAMCURRENT` stopped updating after
  4/10 while `camonitor` (fresh process) worked fine.

## v0.1.0

Initial release.
