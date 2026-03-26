pub mod cluster;
pub mod dto;
pub mod errors;
pub mod handlers;
pub mod pv_data;
pub mod pv_input;
pub mod security;
pub mod services;
mod state;
pub mod ui;
pub mod usecases;

pub use state::AppState;

use axum::extract::DefaultBodyLimit;
use axum::http::{HeaderValue, Method};
use axum::middleware;
use axum::routing::get;
use axum::Router;
use tower_http::cors::{AllowOrigin, CorsLayer};
use tower_http::trace::TraceLayer;

use archiver_core::config::SecurityConfig;

/// Build the Axum router with all routes and security layers.
pub fn build_router(state: AppState, security: &SecurityConfig) -> Router {
    let mut router = Router::new()
        .merge(pv_data::routes())
        .merge(handlers::mgmt::routes())
        .merge(ui::routes())
        .route("/health", get(handlers::health::health))
        .route("/mgmt/bpl/health", get(handlers::health::health));

    if state.cluster.is_some() {
        router = router.merge(cluster::routes());
    }

    if state.metrics_handle.is_some() {
        router = router.route("/metrics", get(handlers::health::metrics_endpoint));
    }

    // Auth middleware.
    if state.api_keys.is_some() {
        router = router.layer(middleware::from_fn_with_state(
            state.clone(),
            state::api_key_auth,
        ));
    }

    // Build CORS layer.
    let cors_layer = if security.cors_origins.is_empty() {
        CorsLayer::new()
            .allow_methods([Method::GET, Method::POST, Method::OPTIONS])
    } else {
        let origins: Vec<HeaderValue> = security
            .cors_origins
            .iter()
            .filter_map(|s| {
                s.parse().map_err(|e| {
                    tracing::warn!(origin = s, "Invalid CORS origin, skipping: {e}");
                }).ok()
            })
            .collect();
        CorsLayer::new()
            .allow_origin(AllowOrigin::list(origins))
            .allow_methods([Method::GET, Method::POST, Method::OPTIONS])
            .allow_headers(tower_http::cors::Any)
    };

    router
        .layer(DefaultBodyLimit::max(security.max_body_size))
        .layer(middleware::from_fn_with_state(
            state.clone(),
            security::rate_limit,
        ))
        .layer(middleware::from_fn(state::http_metrics))
        .layer(cors_layer)
        .layer(middleware::from_fn(security::security_headers))
        .layer(TraceLayer::new_for_http())
        .with_state(state)
}
