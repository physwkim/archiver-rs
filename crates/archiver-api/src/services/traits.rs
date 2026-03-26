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
}

// --- PvCommandRepository (sync — write operations) ---

pub trait PvCommandRepository: Send + Sync {
    fn register_pv(&self, pv: &str, dbr_type: ArchDbType, mode: &SampleMode, element_count: i32) -> anyhow::Result<()>;
    fn remove_pv(&self, pv: &str) -> anyhow::Result<bool>;
    fn set_status(&self, pv: &str, status: PvStatus) -> anyhow::Result<bool>;
    fn update_sample_mode(&self, pv: &str, mode: &SampleMode) -> anyhow::Result<bool>;
    fn update_metadata(&self, pv: &str, prec: Option<&str>, egu: Option<&str>) -> anyhow::Result<bool>;
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
    ) -> anyhow::Result<()>;
}

// --- ArchiverQuery (sync — read-only operations on archiver engine) ---

pub trait ArchiverQuery: Send + Sync {
    fn get_connection_info(&self, pv: &str) -> Option<ConnectionInfoDto>;
    fn get_never_connected_pvs(&self) -> Vec<String>;
    fn get_currently_disconnected_pvs(&self) -> Vec<String>;
}

// --- ArchiverCommand (async — write operations on archiver engine) ---

#[async_trait]
pub trait ArchiverCommand: Send + Sync {
    async fn archive_pv(&self, pv: &str, mode: &SampleMode) -> anyhow::Result<()>;
    fn pause_pv(&self, pv: &str) -> anyhow::Result<()>;
    async fn resume_pv(&self, pv: &str) -> anyhow::Result<()>;
    fn stop_pv(&self, pv: &str) -> anyhow::Result<()>;
    fn destroy_pv(&self, pv: &str) -> anyhow::Result<()>;
}

// --- DTOs ---

#[derive(Debug, Clone)]
pub struct ConnectionInfoDto {
    pub connected_since: Option<SystemTime>,
    pub last_event_time: Option<SystemTime>,
    pub is_connected: bool,
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
    async fn proxy_mgmt_get(&self, mgmt_url: &str, endpoint: &str, qs: &str) -> anyhow::Result<axum::response::Response>;
    async fn proxy_mgmt_post(&self, mgmt_url: &str, endpoint: &str, body: axum::body::Bytes) -> anyhow::Result<axum::response::Response>;
    async fn aggregate_all_pvs(&self) -> Vec<String>;
    async fn aggregate_matching_pvs(&self, pattern: &str) -> Vec<String>;
    async fn aggregate_pv_count(&self) -> (u64, u64, u64);
    async fn remote_pv_status(&self, pv: &str) -> Option<serde_json::Value>;
    async fn proxy_retrieval(&self, peer_retrieval_url: &str, path: &str, query_string: &str) -> anyhow::Result<axum::response::Response>;
}
