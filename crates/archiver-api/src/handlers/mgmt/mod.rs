mod aliases;
mod config_io;
mod engine;
mod legacy;
mod operations;
mod p2;
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
        // P1 cluster-scoped query aliases ----------------------------------
        .route(
            "/mgmt/bpl/getPVsForThisAppliance",
            get(reports::get_pvs_for_this_appliance),
        )
        .route(
            "/mgmt/bpl/getMatchingPVsForAppliance",
            get(reports::get_matching_pvs_for_appliance),
        )
        .route(
            "/mgmt/bpl/getPausedPVsForThisAppliance",
            get(reports::get_paused_pvs_for_this_appliance),
        )
        .route(
            "/mgmt/bpl/getNeverConnectedPVsForThisAppliance",
            get(reports::get_never_connected_pvs_for_this_appliance),
        )
        // P1 reports --------------------------------------------------------
        .route(
            "/mgmt/bpl/getPVsByStorageConsumed",
            get(reports::get_pvs_by_storage_consumed),
        )
        .route(
            "/mgmt/bpl/getEventRateReport",
            get(reports::get_event_rate_report),
        )
        .route(
            "/mgmt/bpl/getLastKnownEventTimeStamp",
            get(reports::get_last_known_event_timestamp),
        )
        .route("/mgmt/bpl/getMgmtMetrics", get(reports::get_mgmt_metrics))
        // P1 bulk-archived check (POST list) -------------------------------
        .route(
            "/mgmt/bpl/archivedPVsAction",
            post(reports::archived_pvs_action),
        )
        .route(
            "/mgmt/bpl/unarchivedPVsAction",
            post(reports::unarchived_pvs_action),
        )
        .route(
            "/mgmt/bpl/archivedPVsNotInListAction",
            post(reports::archived_pvs_not_in_list_action),
        )
        // P2 endpoints -----------------------------------------------------
        .route("/mgmt/bpl/appendAndAliasPV", get(p2::append_and_alias_pv))
        .route("/mgmt/bpl/changeStore", get(p2::change_store))
        .route("/mgmt/bpl/changeTypeForPV", get(p2::change_type_for_pv))
        .route("/mgmt/bpl/reassignAppliance", get(p2::reassign_appliance))
        .route(
            "/mgmt/bpl/cleanUpAnyImmortalChannels",
            get(p2::clean_up_any_immortal_channels),
        )
        .route(
            "/mgmt/bpl/aggregatedApplianceInfo",
            get(p2::aggregated_appliance_info),
        )
        .route("/mgmt/bpl/metaGetsAction", get(p2::meta_gets_action))
        .route(
            "/mgmt/bpl/PVsMatchingParameter",
            get(p2::pvs_matching_parameter),
        )
        .route("/mgmt/bpl/namedFlagsAll", get(p2::named_flags_all))
        .route("/mgmt/bpl/namedFlagsGet", get(p2::named_flags_get))
        .route("/mgmt/bpl/namedFlagsSet", get(p2::named_flags_set))
        .route(
            "/mgmt/bpl/getApplianceMetricsDetails",
            get(p2::appliance_metrics_details),
        )
        // Drop-event reports (stubs returning [] until engine
        // instrumentation lands).
        .route(
            "/mgmt/bpl/getDroppedEventsBufferOverflowReport",
            get(p2::dropped_events_buffer_overflow_report),
        )
        .route(
            "/mgmt/bpl/getDroppedEventsTimestampReport",
            get(p2::dropped_events_timestamp_report),
        )
        .route(
            "/mgmt/bpl/getDroppedEventsTypeChangeReport",
            get(p2::dropped_events_type_change_report),
        )
        .route(
            "/mgmt/bpl/getLostConnectionsReport",
            get(p2::lost_connections_report),
        )
        .route(
            "/mgmt/bpl/getCreationTimeReportForAppliance",
            get(p2::creation_time_report_for_appliance),
        )
        // Legacy / not-applicable — return 410 Gone with a reason ---------
        .route(
            "/mgmt/bpl/addExternalArchiverServer",
            get(legacy::add_external_archiver_server),
        )
        .route(
            "/mgmt/bpl/addExternalArchiverServerArchives",
            get(legacy::add_external_archiver_server_archives),
        )
        .route(
            "/mgmt/bpl/removeExternalArchiverServer",
            get(legacy::remove_external_archiver_server),
        )
        .route(
            "/mgmt/bpl/channelArchiverListView",
            get(legacy::channel_archiver_list_view),
        )
        .route(
            "/mgmt/bpl/importChannelArchiverConfigAction",
            get(legacy::import_channel_archiver_config),
        )
        .route(
            "/mgmt/bpl/uploadChannelArchiverConfigAction",
            get(legacy::upload_channel_archiver_config),
        )
        .route(
            "/mgmt/bpl/refreshPVDataFromChannelArchivers",
            get(legacy::refresh_pv_data_from_channel_archivers),
        )
        .route(
            "/mgmt/bpl/restartArchiveWorkflowThreadForAppliance",
            get(legacy::restart_archive_workflow_thread),
        )
        .route(
            "/mgmt/bpl/skipAliasCheckAction",
            get(legacy::skip_alias_check_action),
        )
        .route(
            "/mgmt/bpl/syncStaticContentHeadersFooters",
            get(legacy::sync_static_content_headers_footers),
        )
        // Engine BPL — live CA fetch + engine-detailed status -------------
        .route(
            "/mgmt/bpl/getEngineDataAction",
            post(engine::get_engine_data),
        )
        .route(
            "/mgmt/bpl/getDataAtTimeEngine",
            post(engine::get_data_at_time_engine),
        )
        .route(
            "/mgmt/bpl/getLatestMetaDataAction",
            get(engine::get_latest_meta_data),
        )
        .route("/mgmt/bpl/pvStatusAction", get(engine::pv_status_action))
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
