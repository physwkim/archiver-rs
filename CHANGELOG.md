# Changelog

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
