pub mod cluster;
pub mod dto;
pub mod handlers;
pub mod pv_data;
pub mod pv_input;
pub mod services;
mod state;
pub mod ui;

pub use state::AppState;

use axum::middleware;
use axum::routing::get;
use axum::Router;
use tower_http::cors::CorsLayer;
use tower_http::trace::TraceLayer;

/// Build the Axum router with all routes.
pub fn build_router(state: AppState) -> Router {
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

    // Apply auth middleware if API keys are configured.
    if state.api_keys.is_some() {
        router = router.layer(middleware::from_fn_with_state(
            state.clone(),
            state::api_key_auth,
        ));
    }

    router
        .layer(middleware::from_fn(state::http_metrics))
        .layer(CorsLayer::permissive())
        .layer(TraceLayer::new_for_http())
        .with_state(state)
}
