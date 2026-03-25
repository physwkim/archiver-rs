pub mod cluster;
pub mod mgmt;
pub mod pv_data;
pub mod pv_input;
pub mod ui;

use std::sync::Arc;

use axum::extract::State;
use axum::http::{HeaderMap, Request, StatusCode};
use axum::middleware::{self, Next};
use axum::response::{IntoResponse, Response};
use axum::routing::get;
use axum::Router;
use tower_http::cors::CorsLayer;
use tower_http::trace::TraceLayer;

use archiver_core::registry::PvRegistry;
use archiver_core::storage::traits::StoragePlugin;
use archiver_engine::channel_manager::ChannelManager;

use crate::cluster::ClusterClient;

/// Shared application state for API handlers.
#[derive(Clone)]
pub struct AppState {
    pub storage: Arc<dyn StoragePlugin>,
    pub channel_mgr: Arc<ChannelManager>,
    pub registry: Arc<PvRegistry>,
    pub cluster: Option<Arc<ClusterClient>>,
    pub api_keys: Option<Vec<String>>,
    pub metrics_handle: Option<metrics_exporter_prometheus::PrometheusHandle>,
}

/// Build the Axum router with all routes.
pub fn build_router(state: AppState) -> Router {
    let mut router = Router::new()
        .merge(pv_data::routes())
        .merge(mgmt::routes())
        .merge(ui::routes())
        .route("/health", get(health))
        .route("/mgmt/bpl/health", get(health));

    if state.cluster.is_some() {
        router = router.merge(cluster::routes());
    }

    if state.metrics_handle.is_some() {
        router = router.route("/metrics", get(metrics_endpoint));
    }

    // Apply auth middleware if API keys are configured.
    if state.api_keys.is_some() {
        router = router.layer(middleware::from_fn_with_state(
            state.clone(),
            api_key_auth,
        ));
    }

    router
        .layer(CorsLayer::permissive())
        .layer(TraceLayer::new_for_http())
        .with_state(state)
}

async fn health() -> impl IntoResponse {
    (StatusCode::OK, "OK")
}

async fn metrics_endpoint(State(state): State<AppState>) -> Response {
    match &state.metrics_handle {
        Some(handle) => {
            let output = handle.render();
            (StatusCode::OK, [("content-type", "text/plain; version=0.0.4")], output).into_response()
        }
        None => (StatusCode::NOT_FOUND, "Metrics not enabled").into_response(),
    }
}

/// Middleware that checks API keys on mgmt write endpoints.
/// Retrieval GET endpoints and health/metrics are exempt.
async fn api_key_auth(
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

    // Also exempt read-only mgmt GET endpoints.
    let is_read_only_mgmt = request.method() == axum::http::Method::GET
        && (path.contains("getAllPVs")
            || path.contains("getMatchingPVs")
            || path.contains("getPVStatus")
            || path.contains("getPVCount")
            || path.contains("getPausedPVsReport")
            || path.contains("getNeverConnectedPVs")
            || path.contains("getCurrentlyDisconnectedPVs")
            || path.contains("getRecentlyAddedPVs")
            || path.contains("getRecentlyModifiedPVs")
            || path.contains("getSilentPVsReport")
            || path.contains("getVersions")
            || path.contains("getAppliancesInCluster")
            || path.contains("getApplianceInfo")
            || path.contains("getPVTypeInfo")
            || path.contains("getPVDetails")
            || path.contains("exportConfig"));

    if is_exempt || is_read_only_mgmt {
        return next.run(request).await;
    }

    if let Some(ref keys) = state.api_keys {
        let provided_key = headers
            .get("authorization")
            .and_then(|v| v.to_str().ok())
            .and_then(|v| v.strip_prefix("Bearer "))
            .or_else(|| {
                headers
                    .get("x-api-key")
                    .and_then(|v| v.to_str().ok())
            });

        match provided_key {
            Some(key) if keys.iter().any(|k| k == key) => {}
            _ => return (StatusCode::UNAUTHORIZED, "Invalid or missing API key").into_response(),
        }
    }

    next.run(request).await
}
