use std::time::{Duration, SystemTime};

use async_trait::async_trait;

use archiver_core::registry::{PvRecord, PvStatus, SampleMode};
use archiver_core::types::ArchDbType;

// --- PvQueryRepository (sync — read-only operations) ---

pub trait PvQueryRepository: Send + Sync {
    fn get_pv(&self, pv: &str) -> anyhow::Result<Option<PvRecord>>;
    fn all_pv_names(&self) -> anyhow::Result<Vec<String>>;
    fn matching_pvs(&self, pattern: &str) -> anyhow::Result<Vec<String>>;
    fn count(&self, status: Option<PvStatus>) -> anyhow::Result<u64>;
    fn all_records(&self) -> anyhow::Result<Vec<PvRecord>>;
    fn pvs_by_status(&self, status: PvStatus) -> anyhow::Result<Vec<PvRecord>>;
    fn recently_added_pvs(&self, since: SystemTime) -> anyhow::Result<Vec<PvRecord>>;
    fn recently_modified_pvs(&self, since: SystemTime) -> anyhow::Result<Vec<PvRecord>>;
    fn silent_pvs(&self, threshold: Duration) -> anyhow::Result<Vec<PvRecord>>;
    /// Return canonical PV name: if `name` is an alias, return its target;
    /// otherwise return the input unchanged. Used by lookup paths.
    fn canonical_name(&self, name: &str) -> anyhow::Result<String>;
    fn aliases_for(&self, target: &str) -> anyhow::Result<Vec<String>>;
    fn all_aliases(&self) -> anyhow::Result<Vec<(String, String)>>;
    fn expanded_pv_names(&self) -> anyhow::Result<Vec<String>>;
    /// Glob match across both real PVs and aliases (Java's
    /// `getMatchingPVs` semantic — c61f1579). Internal-only callers that
    /// shouldn't see aliases stay on `matching_pvs`.
    fn matching_pvs_expanded(&self, pattern: &str) -> anyhow::Result<Vec<String>>;
}

// --- PvCommandRepository (sync — write operations) ---

pub trait PvCommandRepository: Send + Sync {
    fn register_pv(
        &self,
        pv: &str,
        dbr_type: ArchDbType,
        mode: &SampleMode,
        element_count: i32,
    ) -> anyhow::Result<()>;
    fn remove_pv(&self, pv: &str) -> anyhow::Result<bool>;
    fn set_status(&self, pv: &str, status: PvStatus) -> anyhow::Result<bool>;
    fn update_sample_mode(&self, pv: &str, mode: &SampleMode) -> anyhow::Result<bool>;
    fn update_metadata(
        &self,
        pv: &str,
        prec: Option<&str>,
        egu: Option<&str>,
    ) -> anyhow::Result<bool>;
    #[allow(clippy::too_many_arguments)]
    fn import_pv(
        &self,
        pv: &str,
        dbr_type: ArchDbType,
        mode: &SampleMode,
        element_count: i32,
        status: PvStatus,
        created_at: Option<&str>,
        prec: Option<&str>,
        egu: Option<&str>,
        alias_for: Option<&str>,
        archive_fields: &[String],
        policy_name: Option<&str>,
    ) -> anyhow::Result<()>;

    fn update_archive_fields(&self, pv: &str, fields: &[String]) -> anyhow::Result<bool>;
    fn update_policy_name(&self, pv: &str, policy_name: Option<&str>) -> anyhow::Result<bool>;
    fn add_alias(&self, alias: &str, target: &str) -> anyhow::Result<()>;
    fn remove_alias(&self, alias: &str) -> anyhow::Result<bool>;
}

// --- ArchiverQuery (sync — read-only operations on archiver engine) ---

#[async_trait]
pub trait ArchiverQuery: Send + Sync {
    fn get_connection_info(&self, pv: &str) -> Option<ConnectionInfoDto>;
    fn get_never_connected_pvs(&self) -> Vec<String>;
    fn get_currently_disconnected_pvs(&self) -> Vec<String>;
    /// Per-PV counter snapshots for the BPL drop / rate / connection
    /// reports. Returns one entry per actively-archived PV.
    fn all_pv_counters(&self) -> Vec<(String, PvCountersDto)>;
    /// One-shot live CA fetch for `pv`. Returns `None` if the PV is
    /// not actively archived; otherwise `Some(Ok(json_value))` on a
    /// successful read or `Some(Err(message))` on timeout / IOC error.
    async fn live_value(
        &self,
        pv: &str,
        timeout_secs: u64,
    ) -> Option<Result<serde_json::Value, String>>;
    /// Cached snapshot of metadata fields (`HIHI`, `LOLO`, `EGU`, …).
    fn extras_snapshot(&self, pv: &str) -> std::collections::HashMap<String, String>;
}

/// Trait-local counter snapshot. Mirrors `archiver_engine::channel_manager::
/// PvCountersSnapshot`; we re-declare it here so handlers don't depend
/// on the engine crate directly.
#[derive(Debug, Clone)]
pub struct PvCountersDto {
    pub events_received: u64,
    pub events_stored: u64,
    pub first_event_unix_secs: Option<i64>,
    pub buffer_overflow_drops: u64,
    pub timestamp_drops: u64,
    pub type_change_drops: u64,
    pub disconnect_count: u64,
    pub last_disconnect_unix_secs: Option<i64>,
    pub transient_error_count: u64,
    /// Latest CA-reported DBR type when it differed from the archived
    /// type (Java parity 9f2234f). `None` if no mismatch ever observed.
    pub latest_observed_dbr: Option<i32>,
}

// --- ArchiverCommand (async — write operations on archiver engine) ---

#[async_trait]
pub trait ArchiverCommand: Send + Sync {
    async fn archive_pv(&self, pv: &str, mode: &SampleMode) -> anyhow::Result<()>;
    async fn pause_pv(&self, pv: &str) -> anyhow::Result<()>;
    async fn resume_pv(&self, pv: &str) -> anyhow::Result<()>;
    async fn stop_pv(&self, pv: &str) -> anyhow::Result<()>;
    async fn destroy_pv(&self, pv: &str) -> anyhow::Result<()>;
    /// Replace the set of EPICS metadata fields (.HIHI, .LOLO, .EGU, ...)
    /// that the engine samples alongside the main value for `pv`. Persists
    /// to the registry and (re)spawns per-field monitor tasks if the PV
    /// is currently active.
    async fn update_archive_fields(&self, pv: &str, fields: &[String]) -> anyhow::Result<()>;
}

// --- DTOs ---

#[derive(Debug, Clone)]
pub struct ConnectionInfoDto {
    pub connected_since: Option<SystemTime>,
    pub last_event_time: Option<SystemTime>,
    pub is_connected: bool,
    /// Discrete connection state (Java parity dea7acb): Idle, Connecting,
    /// Connected, Disconnected. `None` when the PV has no engine handle.
    pub connection_state: Option<&'static str>,
}

// --- ClusterRouter (async — HTTP-based, mixed read/write) ---

#[derive(Debug, Clone)]
pub struct ResolvedPeerDto {
    pub mgmt_url: String,
    pub retrieval_url: String,
}

#[derive(Debug, Clone)]
pub struct PeerDto {
    pub name: String,
    pub mgmt_url: String,
    pub retrieval_url: String,
}

#[derive(Debug, Clone)]
pub struct ApplianceIdentityDto {
    pub name: String,
    pub mgmt_url: String,
    pub retrieval_url: String,
    pub engine_url: String,
    pub etl_url: String,
}

#[async_trait]
pub trait ClusterRouter: Send + Sync {
    async fn resolve_peer(&self, pv: &str) -> Option<ResolvedPeerDto>;
    fn identity_name(&self) -> &str;
    fn identity(&self) -> ApplianceIdentityDto;
    fn peers(&self) -> Vec<PeerDto>;
    fn find_peer_by_name(&self, name: &str) -> Option<PeerDto>;
    async fn proxy_mgmt_get(
        &self,
        mgmt_url: &str,
        endpoint: &str,
        qs: &str,
    ) -> anyhow::Result<axum::response::Response>;
    async fn proxy_mgmt_post(
        &self,
        mgmt_url: &str,
        endpoint: &str,
        body: axum::body::Bytes,
    ) -> anyhow::Result<axum::response::Response>;
    async fn aggregate_all_pvs(&self) -> Vec<String>;
    /// Java parity (5ebf1e1 + 9b78b21): forward `&limit=N` to peers so a
    /// `?cluster=true&limit=500` against a 100k-PV deployment doesn't
    /// pull every name off every peer just to truncate locally.
    async fn aggregate_all_pvs_limited(&self, limit: Option<i64>) -> Vec<String>;
    async fn aggregate_matching_pvs(&self, pattern: &str) -> Vec<String>;
    /// Java parity (9b78b21): forward `&limit=N` to peers so the
    /// aggregator doesn't pull `peer_default × peers` names just to
    /// truncate locally. `None` keeps peer defaults.
    async fn aggregate_matching_pvs_limited(
        &self,
        pattern: &str,
        limit: Option<i64>,
    ) -> Vec<String>;
    async fn aggregate_pv_count(&self) -> (u64, u64, u64, usize);
    async fn remote_pv_status(&self, pv: &str) -> Option<serde_json::Value>;
    /// Like `remote_pv_status` but distinguishes "all peers responded
    /// negatively" from "at least one peer was unreachable" so callers
    /// can surface `Appliance Down` for the latter (Java parity c2d9f9e).
    async fn remote_pv_status_detailed(&self, pv: &str) -> RemoteStatusOutcomeDto;
    async fn proxy_retrieval(
        &self,
        peer_retrieval_url: &str,
        path: &str,
        query_string: &str,
    ) -> anyhow::Result<axum::response::Response>;
    /// Java parity (26124b6): POST `pvs` to the peer's
    /// `/retrieval/data/getDataAtTime` and return the parsed JSON
    /// response so multi-PV requests can fan out across appliances.
    async fn proxy_data_at_time(
        &self,
        peer_retrieval_url: &str,
        at: Option<&str>,
        pvs: &[String],
    ) -> anyhow::Result<serde_json::Value>;
}

/// Service-layer mirror of `cluster::RemoteStatusOutcome` so handlers can
/// stay decoupled from the concrete `cluster` module.
pub enum RemoteStatusOutcomeDto {
    Found(serde_json::Value),
    NotArchived,
    ApplianceDown,
}
