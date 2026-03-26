mod config_io;
mod pv_control;
mod pv_query;
mod reports;
mod system;

use axum::http::HeaderMap;
use axum::response::Response;
use axum::routing::{get, post};
use axum::Router;

use crate::dto::mgmt::BulkResult;
use crate::services::traits::ClusterRouter;
use crate::AppState;

pub fn routes() -> Router<AppState> {
    Router::new()
        .route("/mgmt/bpl/getAllPVs", get(pv_query::get_all_pvs))
        .route("/mgmt/bpl/getMatchingPVs", get(pv_query::get_matching_pvs))
        .route("/mgmt/bpl/getPVStatus", get(pv_query::get_pv_status))
        .route("/mgmt/bpl/archivePV", post(pv_control::archive_pv))
        .route("/mgmt/bpl/pauseArchivingPV", get(pv_control::pause_archiving_pv).post(pv_control::bulk_pause_archiving_pv))
        .route("/mgmt/bpl/resumeArchivingPV", get(pv_control::resume_archiving_pv).post(pv_control::bulk_resume_archiving_pv))
        .route("/mgmt/bpl/getPVCount", get(pv_query::get_pv_count))
        .route("/mgmt/bpl/deletePV", get(pv_control::delete_pv))
        .route("/mgmt/bpl/changeArchivalParameters", get(pv_control::change_archival_parameters))
        .route("/mgmt/bpl/getPausedPVsReport", get(reports::get_paused_pvs_report))
        .route("/mgmt/bpl/getNeverConnectedPVs", get(reports::get_never_connected_pvs))
        .route("/mgmt/bpl/getCurrentlyDisconnectedPVs", get(reports::get_currently_disconnected_pvs))
        .route("/mgmt/bpl/getRecentlyAddedPVs", get(reports::get_recently_added_pvs))
        .route("/mgmt/bpl/getRecentlyModifiedPVs", get(reports::get_recently_modified_pvs))
        .route("/mgmt/bpl/getSilentPVsReport", get(reports::get_silent_pvs_report))
        .route("/mgmt/bpl/getVersions", get(system::get_versions))
        .route("/mgmt/bpl/getPVTypeInfo", get(pv_query::get_pv_type_info))
        .route("/mgmt/bpl/getPVDetails", get(pv_query::get_pv_details))
        .route("/mgmt/bpl/abortArchivingPV", get(pv_control::abort_archiving_pv))
        .route("/mgmt/bpl/exportConfig", get(config_io::export_config))
        .route("/mgmt/bpl/importConfig", post(config_io::import_config))
}

/// Try to forward a management GET request to the peer that owns the PV.
/// Returns Some(Response) if proxied, None if should handle locally.
async fn try_mgmt_dispatch(
    state: &AppState,
    pv: &str,
    endpoint: &str,
    qs: &str,
    headers: &HeaderMap,
) -> Option<Response> {
    if headers.get("X-Archiver-Proxied").is_some() {
        return None;
    }
    let cluster = state.cluster.as_ref()?;
    if state.pv_query.get_pv(pv).ok().flatten().is_some() {
        return None;
    }
    let resolved = cluster.resolve_peer(pv).await?;
    cluster
        .proxy_mgmt_get(&resolved.mgmt_url, endpoint, qs)
        .await
        .ok()
}

/// Forward a batch of PV names to a peer and parse the response as Vec<BulkResult>.
async fn forward_pv_batch_to_peer(
    cluster: &dyn ClusterRouter,
    mgmt_url: &str,
    endpoint: &str,
    pv_names: &[String],
) -> Vec<BulkResult> {
    let body = pv_names.join("\n");
    match cluster
        .proxy_mgmt_post(mgmt_url, endpoint, body.into())
        .await
    {
        Ok(resp) => {
            let status_code = resp.status();
            let body_bytes = axum::body::to_bytes(resp.into_body(), 1024 * 1024)
                .await
                .unwrap_or_default();
            if let Ok(peer_results) = serde_json::from_slice::<Vec<BulkResult>>(&body_bytes) {
                peer_results
            } else {
                let msg = String::from_utf8_lossy(&body_bytes);
                pv_names
                    .iter()
                    .map(|pv| BulkResult {
                        pv_name: pv.clone(),
                        status: format!("Peer response ({status_code}): {msg}"),
                    })
                    .collect()
            }
        }
        Err(e) => {
            tracing::warn!(mgmt_url, endpoint, "Peer connection failed: {e}");
            pv_names
                .iter()
                .map(|pv| BulkResult {
                    pv_name: pv.clone(),
                    status: "Peer connection failed".to_string(),
                })
                .collect()
        }
    }
}
