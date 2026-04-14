# Changelog

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
