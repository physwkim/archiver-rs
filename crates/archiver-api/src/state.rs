use std::sync::Arc;

use axum::extract::State;
use axum::http::{HeaderMap, Request, StatusCode};
use axum::middleware::Next;
use axum::response::{IntoResponse, Response};
use subtle::ConstantTimeEq;

use archiver_core::etl::executor::EtlExecutor;
use archiver_core::storage::traits::StoragePlugin;

use crate::security::RateLimiter;
use crate::services::traits::{
    ArchiverCommand, ArchiverQuery, ClusterRouter, PvCommandRepository, PvQueryRepository,
};

/// Failover retrieval configuration surfaced to handlers.
#[derive(Clone)]
pub struct FailoverState {
    pub peers: Vec<String>,
    pub timeout: std::time::Duration,
}

/// Shared application state for API handlers.
#[derive(Clone)]
pub struct AppState {
    pub storage: Arc<dyn StoragePlugin>,
    pub pv_query: Arc<dyn PvQueryRepository>,
    pub pv_cmd: Arc<dyn PvCommandRepository>,
    pub archiver_query: Arc<dyn ArchiverQuery>,
    pub archiver_cmd: Arc<dyn ArchiverCommand>,
    pub cluster: Option<Arc<dyn ClusterRouter>>,
    /// External API keys for client authentication on write endpoints.
    pub api_keys: Option<Vec<String>>,
    /// Cluster-internal shared secret, checked separately from api_keys.
    /// Accepted only on requests that also carry X-Archiver-Proxied.
    pub cluster_api_key: Option<String>,
    pub metrics_handle: Option<metrics_exporter_prometheus::PrometheusHandle>,
    pub rate_limiter: Option<Arc<RateLimiter>>,
    pub trust_proxy_headers: bool,
    /// External archivers consulted for failover-merged retrieval.
    pub failover: Option<Arc<FailoverState>>,
    /// ETL executors keyed by destination tier name (e.g. "MTS", "LTS").
    /// `consolidateDataForPV` chains them to force-move a PV's data toward
    /// the requested tier.
    pub etl_chain: Vec<Arc<EtlExecutor>>,
}

/// Middleware that records HTTP request metrics.
/// Uses the path (without query string) as a label.
pub(crate) async fn http_metrics(
    request: Request<axum::body::Body>,
    next: Next,
) -> Response {
    let method = request.method().to_string();
    // Use only the path component, not the query string, to keep label cardinality bounded.
    let path = request.uri().path().to_string();
    let start = std::time::Instant::now();
    let resp = next.run(request).await;
    let duration = start.elapsed().as_secs_f64();
    let status = resp.status().as_u16().to_string();
    metrics::counter!("archiver_http_requests_total", "method" => method.clone(), "path" => path.clone(), "status" => status).increment(1);
    metrics::histogram!("archiver_http_request_duration_seconds", "method" => method, "path" => path).record(duration);
    resp
}

/// Middleware that checks API keys on mgmt write endpoints.
/// Uses constant-time comparison to prevent timing attacks.
pub(crate) async fn api_key_auth(
    State(state): State<AppState>,
    headers: HeaderMap,
    request: Request<axum::body::Body>,
    next: Next,
) -> Response {
    let path = request.uri().path();

    // Exempt: retrieval endpoints, health, metrics, and all GET on non-write paths.
    let is_exempt = path.starts_with("/retrieval/")
        || path == "/health"
        || path == "/metrics"
        || path == "/mgmt/bpl/health"
        || path.starts_with("/mgmt/ui");

    // Also exempt read-only mgmt GET endpoints (exact path match).
    const READ_ONLY_MGMT_PATHS: &[&str] = &[
        "/mgmt/bpl/getAllPVs",
        "/mgmt/bpl/getMatchingPVs",
        "/mgmt/bpl/getPVStatus",
        "/mgmt/bpl/getPVCount",
        "/mgmt/bpl/getPausedPVsReport",
        "/mgmt/bpl/getNeverConnectedPVs",
        "/mgmt/bpl/getCurrentlyDisconnectedPVs",
        "/mgmt/bpl/getRecentlyAddedPVs",
        "/mgmt/bpl/getRecentlyModifiedPVs",
        "/mgmt/bpl/getSilentPVsReport",
        "/mgmt/bpl/getVersions",
        "/mgmt/bpl/getAppliancesInCluster",
        "/mgmt/bpl/getApplianceInfo",
        "/mgmt/bpl/getPVTypeInfo",
        "/mgmt/bpl/getPVTypeInfoKeys",
        "/mgmt/bpl/getStoresForPV",
        "/mgmt/bpl/getPVDetails",
        "/mgmt/bpl/exportConfig",
        "/mgmt/bpl/getAllAliases",
        "/mgmt/bpl/getAllExpandedPVNames",
        "/mgmt/bpl/getApplianceMetrics",
        "/mgmt/bpl/getStorageUsageForPV",
        "/mgmt/bpl/getPVsForThisAppliance",
        "/mgmt/bpl/getMatchingPVsForAppliance",
        "/mgmt/bpl/getPausedPVsForThisAppliance",
        "/mgmt/bpl/getNeverConnectedPVsForThisAppliance",
        "/mgmt/bpl/getPVsByStorageConsumed",
        "/mgmt/bpl/getEventRateReport",
        "/mgmt/bpl/getLastKnownEventTimeStamp",
        "/mgmt/bpl/getMgmtMetrics",
    ];
    let is_read_only_mgmt = request.method() == axum::http::Method::GET
        && READ_ONLY_MGMT_PATHS.contains(&path);

    if is_exempt || is_read_only_mgmt {
        return next.run(request).await;
    }

    // Extract the provided credential.
    let provided_key = headers
        .get("authorization")
        .and_then(|v| v.to_str().ok())
        .and_then(|v| v.strip_prefix("Bearer "))
        .or_else(|| {
            headers
                .get("x-api-key")
                .and_then(|v| v.to_str().ok())
        });

    // Cluster-internal requests: accept the cluster api_key, but only when
    // the request carries X-Archiver-Proxied (set by ClusterClient, not forgeable
    // without knowing the key — the key itself is the proof of authenticity).
    let is_proxied = request.headers().get("X-Archiver-Proxied").is_some();
    if is_proxied
        && let Some(ref cluster_key) = state.cluster_api_key
            && let Some(key) = provided_key
                && bool::from(cluster_key.as_bytes().ct_eq(key.as_bytes())) {
                    return next.run(request).await;
                }

    // External clients: check against api_keys.
    if let Some(ref keys) = state.api_keys {
        match provided_key {
            Some(key) if keys.iter().any(|k| {
                bool::from(k.as_bytes().ct_eq(key.as_bytes()))
            }) => {}
            _ => return (StatusCode::UNAUTHORIZED, "Invalid or missing API key").into_response(),
        }
    }

    next.run(request).await
}
