mod aliases;
mod config_io;
mod operations;
mod pv_control;
mod pv_query;
mod reports;
mod system;
mod type_info;

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
        .route("/mgmt/bpl/addAlias", get(aliases::add_alias))
        .route("/mgmt/bpl/removeAlias", get(aliases::remove_alias))
        .route("/mgmt/bpl/getAllAliases", get(aliases::get_all_aliases))
        .route(
            "/mgmt/bpl/getAllExpandedPVNames",
            get(aliases::get_all_expanded_pv_names),
        )
        .route("/mgmt/bpl/putPVTypeInfo", post(type_info::put_pv_type_info))
        .route(
            "/mgmt/bpl/getPVTypeInfoKeys",
            get(type_info::get_pv_type_info_keys),
        )
        .route("/mgmt/bpl/getStoresForPV", get(type_info::get_stores_for_pv))
        .route("/mgmt/bpl/renamePV", get(type_info::rename_pv))
        .route("/mgmt/bpl/modifyMetaFields", get(type_info::modify_meta_fields))
        .route(
            "/mgmt/bpl/getApplianceMetrics",
            get(operations::get_appliance_metrics),
        )
        .route(
            "/mgmt/bpl/consolidateDataForPV",
            get(operations::consolidate_data_for_pv),
        )
        .route(
            "/mgmt/bpl/getStorageUsageForPV",
            get(operations::get_storage_usage_for_pv),
        )
        .route(
            "/mgmt/bpl/resetFailoverCaches",
            get(operations::reset_failover_caches),
        )
}

/// Resolve a possibly-alias PV name to its canonical form. Falls back to
/// the input on lookup error.
pub(super) fn resolve_canonical(state: &AppState, pv: &str) -> String {
    state
        .pv_query
        .canonical_name(pv)
        .unwrap_or_else(|_| pv.to_string())
}

/// Try to forward a management GET request to the peer that owns the PV.
/// Returns Some(Response) if proxied, None if should handle locally.
///
/// Resolves aliases first so a local alias row is treated as a local hit
/// when its target is local, and routed by canonical name when the
/// target lives on a peer.
pub(super) async fn try_mgmt_dispatch(
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
    let canonical = state.pv_query.canonical_name(pv).ok()?;
    // Real PV exists locally → handle locally.
    if let Ok(Some(rec)) = state.pv_query.get_pv(&canonical)
        && rec.alias_for.is_none()
    {
        return None;
    }
    let resolved = cluster.resolve_peer(&canonical).await?;
    cluster
        .proxy_mgmt_get(&resolved.mgmt_url, endpoint, qs)
        .await
        .ok()
}

/// Route PVs to local vs remote batches based on cluster ownership.
/// Returns (local_pvs, remote_batches_by_mgmt_url).
async fn route_pvs(
    state: &AppState,
    pvs: &[String],
    is_proxied: bool,
) -> (Vec<String>, std::collections::HashMap<String, Vec<String>>) {
    let mut local = Vec::new();
    let mut remote: std::collections::HashMap<String, Vec<String>> =
        std::collections::HashMap::new();

    if is_proxied {
        return (pvs.to_vec(), remote);
    }

    let cluster = match state.cluster.as_ref() {
        Some(c) => c,
        None => return (pvs.to_vec(), remote),
    };

    // Pre-fetch all real local PV names and the alias map in two single
    // queries so the per-PV decision below is O(1) lookups instead of
    // N round-trips to SQLite.
    let local_set: std::collections::HashSet<String> = state
        .pv_query
        .all_pv_names()
        .unwrap_or_default()
        .into_iter()
        .collect();
    let alias_map: std::collections::HashMap<String, String> = state
        .pv_query
        .all_aliases()
        .unwrap_or_default()
        .into_iter()
        .collect();

    for pv in pvs {
        let canonical = alias_map.get(pv).cloned().unwrap_or_else(|| pv.clone());
        if local_set.contains(&canonical) {
            local.push(pv.clone());
        } else if let Some(resolved) = cluster.resolve_peer(&canonical).await {
            remote
                .entry(resolved.mgmt_url)
                .or_default()
                .push(pv.clone());
        } else {
            local.push(pv.clone());
        }
    }

    (local, remote)
}

/// Forward all remote PV batches to their respective peers and collect results.
async fn forward_all_peer_batches(
    cluster: &dyn ClusterRouter,
    batches: std::collections::HashMap<String, Vec<String>>,
    endpoint: &str,
) -> Vec<BulkResult> {
    let futs: Vec<_> = batches
        .into_iter()
        .map(|(mgmt_url, batch)| async move {
            forward_pv_batch_to_peer(cluster, &mgmt_url, endpoint, &batch).await
        })
        .collect();
    let results = futures::future::join_all(futs).await;
    results.into_iter().flatten().collect()
}

/// Truncate a string to a safe length for inclusion in client-facing messages.
pub(super) fn truncate_for_display(s: &str, max_len: usize) -> &str {
    if s.len() <= max_len {
        s
    } else {
        // Find a valid UTF-8 boundary.
        let mut end = max_len;
        while end > 0 && !s.is_char_boundary(end) {
            end -= 1;
        }
        &s[..end]
    }
}

/// Forward a batch of PV names to a peer and parse the response as Vec<BulkResult>.
async fn forward_pv_batch_to_peer(
    cluster: &dyn ClusterRouter,
    mgmt_url: &str,
    endpoint: &str,
    pv_names: &[String],
) -> Vec<BulkResult> {
    let body = serde_json::to_string(pv_names).unwrap_or_default();
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
                // Truncate raw peer response to avoid leaking internal details.
                let raw = String::from_utf8_lossy(&body_bytes);
                let msg = truncate_for_display(&raw, 200);
                pv_names
                    .iter()
                    .map(|pv| BulkResult {
                        pv_name: pv.clone(),
                        status: format!("Peer error ({status_code}): {msg}"),
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
