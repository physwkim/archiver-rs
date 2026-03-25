pub mod cluster;
pub mod mgmt;
pub mod pv_data;
pub mod pv_input;
pub mod ui;

use std::sync::Arc;

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
}

/// Build the Axum router with all routes.
pub fn build_router(state: AppState) -> Router {
    let mut router = Router::new()
        .merge(pv_data::routes())
        .merge(mgmt::routes())
        .merge(ui::routes());

    if state.cluster.is_some() {
        router = router.merge(cluster::routes());
    }

    router
        .layer(CorsLayer::permissive())
        .layer(TraceLayer::new_for_http())
        .with_state(state)
}
