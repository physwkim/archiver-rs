use std::time::{Duration, SystemTime};

use axum::extract::{Query, State};
use axum::http::{HeaderMap, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post};
use axum::Router;
use serde::{Deserialize, Serialize};

use archiver_core::registry::{PvStatus, SampleMode};

use crate::pv_input::{parse_pv_list, PvListInput};
use crate::AppState;

pub fn routes() -> Router<AppState> {
    Router::new()
        .route("/mgmt/bpl/getAllPVs", get(get_all_pvs))
        .route("/mgmt/bpl/getMatchingPVs", get(get_matching_pvs))
        .route("/mgmt/bpl/getPVStatus", get(get_pv_status))
        .route("/mgmt/bpl/archivePV", post(archive_pv))
        .route("/mgmt/bpl/pauseArchivingPV", get(pause_archiving_pv).post(bulk_pause_archiving_pv))
        .route("/mgmt/bpl/resumeArchivingPV", get(resume_archiving_pv).post(bulk_resume_archiving_pv))
        .route("/mgmt/bpl/getPVCount", get(get_pv_count))
        .route("/mgmt/bpl/deletePV", get(delete_pv))
        .route("/mgmt/bpl/changeArchivalParameters", get(change_archival_parameters))
        .route("/mgmt/bpl/getPausedPVsReport", get(get_paused_pvs_report))
        .route("/mgmt/bpl/getNeverConnectedPVs", get(get_never_connected_pvs))
        .route("/mgmt/bpl/getCurrentlyDisconnectedPVs", get(get_currently_disconnected_pvs))
        .route("/mgmt/bpl/getRecentlyAddedPVs", get(get_recently_added_pvs))
        .route("/mgmt/bpl/getRecentlyModifiedPVs", get(get_recently_modified_pvs))
        .route("/mgmt/bpl/getSilentPVsReport", get(get_silent_pvs_report))
        // G4: Operations
        .route("/mgmt/bpl/getVersions", get(get_versions))
        // G5: BPL extensions (Tier 1)
        .route("/mgmt/bpl/getPVTypeInfo", get(get_pv_type_info))
        .route("/mgmt/bpl/getPVDetails", get(get_pv_details))
        .route("/mgmt/bpl/abortArchivingPV", get(abort_archiving_pv))
        .route("/mgmt/bpl/exportConfig", get(export_config))
        .route("/mgmt/bpl/importConfig", post(import_config))
}

// --- Shared types ---

#[derive(Serialize)]
struct BulkResult {
    #[serde(rename = "pvName")]
    pv_name: String,
    status: String,
}

#[derive(Serialize)]
struct ReportEntry {
    #[serde(rename = "pvName")]
    pv_name: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    status: Option<String>,
    #[serde(rename = "lastEvent", skip_serializing_if = "Option::is_none")]
    last_event: Option<String>,
}

// --- Existing endpoints ---

#[derive(Deserialize)]
struct ClusterParam {
    #[serde(default)]
    cluster: Option<bool>,
}

async fn get_all_pvs(
    State(state): State<AppState>,
    Query(cp): Query<ClusterParam>,
) -> Response {
    let mut pvs = match state.registry.all_pv_names() {
        Ok(pvs) => pvs,
        Err(e) => return (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()).into_response(),
    };

    if cp.cluster.unwrap_or(false) {
        if let Some(ref cluster) = state.cluster {
            let remote = cluster.aggregate_all_pvs().await;
            let mut all: std::collections::BTreeSet<String> = pvs.into_iter().collect();
            all.extend(remote);
            pvs = all.into_iter().collect();
        }
    }

    axum::Json(pvs).into_response()
}

#[derive(Deserialize)]
struct MatchingPvsParams {
    pv: String,
    #[serde(default)]
    cluster: Option<bool>,
}

async fn get_matching_pvs(
    State(state): State<AppState>,
    Query(params): Query<MatchingPvsParams>,
) -> Response {
    let mut pvs = match state.registry.matching_pvs(&params.pv) {
        Ok(pvs) => pvs,
        Err(e) => return (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()).into_response(),
    };

    if params.cluster.unwrap_or(false) {
        if let Some(ref cluster) = state.cluster {
            let remote = cluster.aggregate_matching_pvs(&params.pv).await;
            let mut all: std::collections::BTreeSet<String> = pvs.into_iter().collect();
            all.extend(remote);
            pvs = all.into_iter().collect();
        }
    }

    axum::Json(pvs).into_response()
}

#[derive(Deserialize)]
struct PvStatusParams {
    pv: String,
    #[serde(default)]
    cluster: Option<bool>,
}

#[derive(Serialize)]
struct PvStatusResponse {
    pv_name: String,
    status: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    dbr_type: Option<i32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    sample_mode: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    element_count: Option<i32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    last_event_timestamp: Option<String>,
}

async fn get_pv_status(
    State(state): State<AppState>,
    Query(params): Query<PvStatusParams>,
) -> Response {
    match state.registry.get_pv(&params.pv) {
        Ok(Some(record)) => {
            let status_str = match record.status {
                PvStatus::Active => "Being archived",
                PvStatus::Paused => "Paused",
                PvStatus::Error => "Error",
            };
            let sample_mode_str = match &record.sample_mode {
                SampleMode::Monitor => "Monitor".to_string(),
                SampleMode::Scan { period_secs } => format!("Scan @ {period_secs}s"),
            };
            let last_ts = record.last_timestamp.map(|ts| {
                chrono::DateTime::<chrono::Utc>::from(ts).to_rfc3339()
            });
            let resp = PvStatusResponse {
                pv_name: record.pv_name,
                status: status_str.to_string(),
                dbr_type: Some(record.dbr_type as i32),
                sample_mode: Some(sample_mode_str),
                element_count: Some(record.element_count),
                last_event_timestamp: last_ts,
            };
            axum::Json(resp).into_response()
        }
        Ok(None) => {
            // PV not local — try cluster if requested.
            if params.cluster.unwrap_or(false) {
                if let Some(ref cluster) = state.cluster {
                    if let Some(remote_status) = cluster.remote_pv_status(&params.pv).await {
                        return axum::Json(remote_status).into_response();
                    }
                }
            }
            let resp = PvStatusResponse {
                pv_name: params.pv,
                status: "Not being archived".to_string(),
                dbr_type: None,
                sample_mode: None,
                element_count: None,
                last_event_timestamp: None,
            };
            axum::Json(resp).into_response()
        }
        Err(e) => (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()).into_response(),
    }
}

#[derive(Deserialize)]
struct ArchivePvRequest {
    pv: Option<String>,
    #[serde(default)]
    sampling_period: Option<f64>,
    #[serde(default)]
    sampling_method: Option<String>,
    #[serde(default)]
    appliance: Option<String>,
}

/// POST /mgmt/bpl/archivePV
///
/// Accepts either a JSON object `{"pv":"..."}` for single PV, or a JSON
/// array / newline-delimited body for bulk archiving.
async fn archive_pv(
    State(state): State<AppState>,
    headers: HeaderMap,
    body: axum::body::Bytes,
) -> Response {
    let body_str = match String::from_utf8(body.to_vec()) {
        Ok(s) => s,
        Err(_) => return (StatusCode::BAD_REQUEST, "Invalid UTF-8 body").into_response(),
    };

    // Try parsing as single PV request first.
    if let Ok(req) = serde_json::from_str::<ArchivePvRequest>(&body_str) {
        if let Some(pv) = req.pv {
            let sample_mode = parse_sample_mode(req.sampling_method.as_deref(), req.sampling_period);
            return archive_single_pv(&state, &pv, &sample_mode, req.appliance.as_deref(), &headers).await;
        }
    }

    // Try as bulk PV list.
    let pvs = parse_pv_list(&body_str);
    if pvs.is_empty() {
        return (StatusCode::BAD_REQUEST, "No PVs specified").into_response();
    }

    let is_proxied = headers.get("X-Archiver-Proxied").is_some();
    let mut results = Vec::with_capacity(pvs.len());

    // In cluster mode, group by peer.
    if !is_proxied {
        if let Some(ref cluster) = state.cluster {
            let mut remote_batches: std::collections::HashMap<String, Vec<String>> =
                std::collections::HashMap::new();
            let mut local_pvs = Vec::new();
            for pv in &pvs {
                // Check if already archived somewhere in cluster.
                if let Some(resolved) = cluster.resolve_peer(pv).await {
                    results.push(BulkResult {
                        pv_name: pv.clone(),
                        status: "Already archived on peer".to_string(),
                    });
                    continue;
                }
                local_pvs.push(pv.clone());
            }

            for pv in &local_pvs {
                let status = match state.channel_mgr.archive_pv(pv, &SampleMode::Monitor).await {
                    Ok(()) => "Archive request submitted".to_string(),
                    Err(e) => e.to_string(),
                };
                results.push(BulkResult {
                    pv_name: pv.clone(),
                    status,
                });
            }
            return axum::Json(results).into_response();
        }
    }

    for pv in &pvs {
        let status = match state.channel_mgr.archive_pv(pv, &SampleMode::Monitor).await {
            Ok(()) => "Archive request submitted".to_string(),
            Err(e) => e.to_string(),
        };
        results.push(BulkResult {
            pv_name: pv.clone(),
            status,
        });
    }
    axum::Json(results).into_response()
}

async fn archive_single_pv(
    state: &AppState,
    pv: &str,
    sample_mode: &SampleMode,
    appliance: Option<&str>,
    headers: &HeaderMap,
) -> Response {
    let is_proxied = headers.get("X-Archiver-Proxied").is_some();

    // G2: If appliance specified and it's not us, forward to that peer.
    if !is_proxied {
        if let Some(target) = appliance {
            if let Some(ref cluster) = state.cluster {
                if target != cluster.identity().name {
                    if let Some(peer) = cluster.find_peer_by_name(target) {
                        let body = serde_json::json!({ "pv": pv });
                        let bytes = axum::body::Bytes::from(body.to_string());
                        return match cluster.proxy_mgmt_post(&peer.mgmt_url, "archivePV", bytes).await {
                            Ok(resp) => resp,
                            Err(e) => (StatusCode::BAD_GATEWAY, format!("Failed to forward to peer: {e}")).into_response(),
                        };
                    }
                    return (StatusCode::NOT_FOUND, format!("Appliance '{target}' not found in cluster")).into_response();
                }
            }
        }

        // G2: Check if already archived elsewhere in cluster.
        if let Some(ref cluster) = state.cluster {
            if cluster.resolve_peer(pv).await.is_some() {
                return (StatusCode::CONFLICT, format!("PV {pv} is already archived on another appliance")).into_response();
            }
        }
    }

    match state.channel_mgr.archive_pv(pv, sample_mode).await {
        Ok(()) => {
            let msg = format!("Successfully started archiving PV {pv}");
            (StatusCode::OK, msg).into_response()
        }
        Err(e) => {
            let msg = format!("Failed to archive PV {pv}: {e}");
            (StatusCode::INTERNAL_SERVER_ERROR, msg).into_response()
        }
    }
}

fn parse_sample_mode(method: Option<&str>, period: Option<f64>) -> SampleMode {
    match method {
        Some("scan") | Some("Scan") | Some("SCAN") => SampleMode::Scan {
            period_secs: period.unwrap_or(1.0),
        },
        _ => SampleMode::Monitor,
    }
}

// --- Cluster dispatch helper ---

/// Try to forward a management GET request to the peer that owns the PV.
/// Returns Some(Response) if proxied, None if should handle locally.
async fn try_mgmt_dispatch(
    state: &AppState,
    pv: &str,
    endpoint: &str,
    qs: &str,
    headers: &HeaderMap,
) -> Option<Response> {
    // Prevent circular proxying.
    if headers.get("X-Archiver-Proxied").is_some() {
        return None;
    }
    let cluster = state.cluster.as_ref()?;
    // If PV is local, handle locally.
    if state.registry.get_pv(pv).ok().flatten().is_some() {
        return None;
    }
    let resolved = cluster.resolve_peer(pv).await?;
    cluster
        .proxy_mgmt_get(&resolved.mgmt_url, endpoint, qs)
        .await
        .ok()
}

// --- Single pause/resume (GET with ?pv=) ---

#[derive(Deserialize)]
struct PausePvParams {
    pv: String,
}

async fn pause_archiving_pv(
    State(state): State<AppState>,
    headers: HeaderMap,
    Query(params): Query<PausePvParams>,
) -> Response {
    let qs = format!("pv={}", urlencoding::encode(&params.pv));
    if let Some(resp) = try_mgmt_dispatch(&state, &params.pv, "pauseArchivingPV", &qs, &headers).await {
        return resp;
    }
    match state.channel_mgr.pause_pv(&params.pv) {
        Ok(()) => {
            let msg = format!("Successfully paused PV {}", params.pv);
            (StatusCode::OK, msg).into_response()
        }
        Err(e) => {
            let msg = format!("Failed to pause PV {}: {e}", params.pv);
            (StatusCode::INTERNAL_SERVER_ERROR, msg).into_response()
        }
    }
}

async fn resume_archiving_pv(
    State(state): State<AppState>,
    headers: HeaderMap,
    Query(params): Query<PausePvParams>,
) -> Response {
    let qs = format!("pv={}", urlencoding::encode(&params.pv));
    if let Some(resp) = try_mgmt_dispatch(&state, &params.pv, "resumeArchivingPV", &qs, &headers).await {
        return resp;
    }
    match state.channel_mgr.resume_pv(&params.pv).await {
        Ok(()) => {
            let msg = format!("Successfully resumed PV {}", params.pv);
            (StatusCode::OK, msg).into_response()
        }
        Err(e) => {
            let msg = format!("Failed to resume PV {}: {e}", params.pv);
            (StatusCode::INTERNAL_SERVER_ERROR, msg).into_response()
        }
    }
}

// --- Bulk pause/resume (POST with body) ---

async fn bulk_pause_archiving_pv(
    State(state): State<AppState>,
    headers: HeaderMap,
    PvListInput(pvs): PvListInput,
) -> Response {
    let is_proxied = headers.get("X-Archiver-Proxied").is_some();
    let mut results = Vec::with_capacity(pvs.len());

    // Group remote PVs by peer and dispatch in parallel.
    if !is_proxied {
        if let Some(ref cluster) = state.cluster {
            let mut remote_batches: std::collections::HashMap<String, Vec<String>> =
                std::collections::HashMap::new();
            let mut local_pvs = Vec::new();
            for pv in &pvs {
                if state.registry.get_pv(pv).ok().flatten().is_some() {
                    local_pvs.push(pv.clone());
                } else if let Some(resolved) = cluster.resolve_peer(pv).await {
                    remote_batches
                        .entry(resolved.mgmt_url)
                        .or_default()
                        .push(pv.clone());
                } else {
                    local_pvs.push(pv.clone());
                }
            }

            // Dispatch remote batches.
            let futs: Vec<_> = remote_batches
                .into_iter()
                .map(|(mgmt_url, batch)| {
                    let body = batch.join("\n");
                    let cluster = cluster.clone();
                    async move {
                        let _ = cluster
                            .proxy_mgmt_post(&mgmt_url, "pauseArchivingPV", body.into())
                            .await;
                        batch
                            .into_iter()
                            .map(|pv| BulkResult {
                                pv_name: pv,
                                status: "Forwarded to peer".to_string(),
                            })
                            .collect::<Vec<_>>()
                    }
                })
                .collect();
            let remote_results = futures::future::join_all(futs).await;
            for batch_results in remote_results {
                results.extend(batch_results);
            }

            // Handle local PVs.
            for pv in &local_pvs {
                let status = match state.channel_mgr.pause_pv(pv) {
                    Ok(()) => "Successfully paused".to_string(),
                    Err(e) => e.to_string(),
                };
                results.push(BulkResult {
                    pv_name: pv.clone(),
                    status,
                });
            }
            return axum::Json(results).into_response();
        }
    }

    // No cluster or already proxied — handle all locally.
    for pv in &pvs {
        let status = match state.channel_mgr.pause_pv(pv) {
            Ok(()) => "Successfully paused".to_string(),
            Err(e) => e.to_string(),
        };
        results.push(BulkResult {
            pv_name: pv.clone(),
            status,
        });
    }
    axum::Json(results).into_response()
}

async fn bulk_resume_archiving_pv(
    State(state): State<AppState>,
    headers: HeaderMap,
    PvListInput(pvs): PvListInput,
) -> Response {
    let is_proxied = headers.get("X-Archiver-Proxied").is_some();
    let mut results = Vec::with_capacity(pvs.len());

    if !is_proxied {
        if let Some(ref cluster) = state.cluster {
            let mut remote_batches: std::collections::HashMap<String, Vec<String>> =
                std::collections::HashMap::new();
            let mut local_pvs = Vec::new();
            for pv in &pvs {
                if state.registry.get_pv(pv).ok().flatten().is_some() {
                    local_pvs.push(pv.clone());
                } else if let Some(resolved) = cluster.resolve_peer(pv).await {
                    remote_batches
                        .entry(resolved.mgmt_url)
                        .or_default()
                        .push(pv.clone());
                } else {
                    local_pvs.push(pv.clone());
                }
            }

            let futs: Vec<_> = remote_batches
                .into_iter()
                .map(|(mgmt_url, batch)| {
                    let body = batch.join("\n");
                    let cluster = cluster.clone();
                    async move {
                        let _ = cluster
                            .proxy_mgmt_post(&mgmt_url, "resumeArchivingPV", body.into())
                            .await;
                        batch
                            .into_iter()
                            .map(|pv| BulkResult {
                                pv_name: pv,
                                status: "Forwarded to peer".to_string(),
                            })
                            .collect::<Vec<_>>()
                    }
                })
                .collect();
            let remote_results = futures::future::join_all(futs).await;
            for batch_results in remote_results {
                results.extend(batch_results);
            }

            for pv in &local_pvs {
                let status = match state.channel_mgr.resume_pv(pv).await {
                    Ok(()) => "Successfully resumed".to_string(),
                    Err(e) => e.to_string(),
                };
                results.push(BulkResult {
                    pv_name: pv.clone(),
                    status,
                });
            }
            return axum::Json(results).into_response();
        }
    }

    for pv in &pvs {
        let status = match state.channel_mgr.resume_pv(pv).await {
            Ok(()) => "Successfully resumed".to_string(),
            Err(e) => e.to_string(),
        };
        results.push(BulkResult {
            pv_name: pv.clone(),
            status,
        });
    }
    axum::Json(results).into_response()
}

// --- PV Count ---

#[derive(Serialize)]
struct PvCountResponse {
    total: u64,
    active: u64,
    paused: u64,
}

async fn get_pv_count(
    State(state): State<AppState>,
    Query(cp): Query<ClusterParam>,
) -> Response {
    let mut total = state.registry.count(None).unwrap_or(0);
    let mut active = state.registry.count(Some(PvStatus::Active)).unwrap_or(0);
    let mut paused = state.registry.count(Some(PvStatus::Paused)).unwrap_or(0);

    if cp.cluster.unwrap_or(false) {
        if let Some(ref cluster) = state.cluster {
            let (rt, ra, rp) = cluster.aggregate_pv_count().await;
            total += rt;
            active += ra;
            paused += rp;
        }
    }

    let resp = PvCountResponse {
        total,
        active,
        paused,
    };
    axum::Json(resp).into_response()
}

// --- Delete PV ---

#[derive(Deserialize)]
struct DeletePvParams {
    pv: String,
    #[serde(rename = "deleteData", default)]
    delete_data: Option<bool>,
}

async fn delete_pv(
    State(state): State<AppState>,
    headers: HeaderMap,
    Query(params): Query<DeletePvParams>,
) -> Response {
    let mut qs = format!("pv={}", urlencoding::encode(&params.pv));
    if params.delete_data.unwrap_or(false) {
        qs.push_str("&deleteData=true");
    }
    if let Some(resp) = try_mgmt_dispatch(&state, &params.pv, "deletePV", &qs, &headers).await {
        return resp;
    }
    // Stop archiving and remove from registry.
    if let Err(e) = state.channel_mgr.destroy_pv(&params.pv) {
        let msg = format!("Failed to delete PV {}: {e}", params.pv);
        return (StatusCode::INTERNAL_SERVER_ERROR, msg).into_response();
    }

    // Optionally delete data files.
    let mut files_deleted = 0u64;
    if params.delete_data.unwrap_or(false) {
        match state.storage.delete_pv_data(&params.pv).await {
            Ok(n) => files_deleted = n,
            Err(e) => {
                let msg = format!(
                    "PV {} removed from registry but failed to delete data: {e}",
                    params.pv
                );
                return (StatusCode::INTERNAL_SERVER_ERROR, msg).into_response();
            }
        }
    }

    let msg = format!(
        "Successfully deleted PV {} (data files removed: {files_deleted})",
        params.pv
    );
    (StatusCode::OK, msg).into_response()
}

// --- Change archival parameters ---

#[derive(Deserialize)]
struct ChangeParamsQuery {
    pv: String,
    #[serde(default)]
    samplingperiod: Option<f64>,
    #[serde(default)]
    samplingmethod: Option<String>,
}

async fn change_archival_parameters(
    State(state): State<AppState>,
    headers: HeaderMap,
    Query(params): Query<ChangeParamsQuery>,
) -> Response {
    let mut qs = format!("pv={}", urlencoding::encode(&params.pv));
    if let Some(period) = params.samplingperiod {
        qs.push_str(&format!("&samplingperiod={period}"));
    }
    if let Some(ref method) = params.samplingmethod {
        qs.push_str(&format!("&samplingmethod={method}"));
    }
    if let Some(resp) = try_mgmt_dispatch(&state, &params.pv, "changeArchivalParameters", &qs, &headers).await {
        return resp;
    }
    let new_mode = parse_sample_mode(
        params.samplingmethod.as_deref(),
        params.samplingperiod,
    );

    // Update in registry.
    match state.registry.update_sample_mode(&params.pv, &new_mode) {
        Ok(false) => {
            let msg = format!("PV {} not found", params.pv);
            return (StatusCode::NOT_FOUND, msg).into_response();
        }
        Err(e) => {
            let msg = format!("Failed to update parameters for {}: {e}", params.pv);
            return (StatusCode::INTERNAL_SERVER_ERROR, msg).into_response();
        }
        Ok(true) => {}
    }

    // If actively archived, pause and resume with new parameters.
    let record = match state.registry.get_pv(&params.pv) {
        Ok(Some(r)) => r,
        _ => {
            return (
                StatusCode::OK,
                format!("Updated parameters for {}", params.pv),
            )
                .into_response();
        }
    };

    if record.status == PvStatus::Active {
        let _ = state.channel_mgr.pause_pv(&params.pv);
        if let Err(e) = state.channel_mgr.resume_pv(&params.pv).await {
            let msg = format!(
                "Updated parameters but failed to restart {}: {e}",
                params.pv
            );
            return (StatusCode::INTERNAL_SERVER_ERROR, msg).into_response();
        }
    }

    let msg = format!("Successfully updated parameters for {}", params.pv);
    (StatusCode::OK, msg).into_response()
}

// --- Report endpoints ---

async fn get_paused_pvs_report(State(state): State<AppState>) -> Response {
    match state.registry.pvs_by_status(PvStatus::Paused) {
        Ok(records) => {
            let entries: Vec<ReportEntry> = records
                .into_iter()
                .map(|r| ReportEntry {
                    pv_name: r.pv_name,
                    status: Some("Paused".to_string()),
                    last_event: r.last_timestamp.map(|ts| {
                        chrono::DateTime::<chrono::Utc>::from(ts).to_rfc3339()
                    }),
                })
                .collect();
            axum::Json(entries).into_response()
        }
        Err(e) => (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()).into_response(),
    }
}

async fn get_never_connected_pvs(State(state): State<AppState>) -> Response {
    let pvs = state.channel_mgr.get_never_connected_pvs();
    let entries: Vec<ReportEntry> = pvs
        .into_iter()
        .map(|name| ReportEntry {
            pv_name: name,
            status: Some("Never connected".to_string()),
            last_event: None,
        })
        .collect();
    axum::Json(entries).into_response()
}

async fn get_currently_disconnected_pvs(State(state): State<AppState>) -> Response {
    let pvs = state.channel_mgr.get_currently_disconnected_pvs();
    let entries: Vec<ReportEntry> = pvs
        .into_iter()
        .map(|name| ReportEntry {
            pv_name: name,
            status: Some("Disconnected".to_string()),
            last_event: None,
        })
        .collect();
    axum::Json(entries).into_response()
}

async fn get_recently_added_pvs(State(state): State<AppState>) -> Response {
    let since = SystemTime::now() - Duration::from_secs(86400); // 24h
    match state.registry.recently_added_pvs(since) {
        Ok(records) => {
            let entries: Vec<ReportEntry> = records
                .into_iter()
                .map(|r| record_to_report_entry(r))
                .collect();
            axum::Json(entries).into_response()
        }
        Err(e) => (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()).into_response(),
    }
}

async fn get_recently_modified_pvs(State(state): State<AppState>) -> Response {
    let since = SystemTime::now() - Duration::from_secs(86400); // 24h
    match state.registry.recently_modified_pvs(since) {
        Ok(records) => {
            let entries: Vec<ReportEntry> = records
                .into_iter()
                .map(|r| record_to_report_entry(r))
                .collect();
            axum::Json(entries).into_response()
        }
        Err(e) => (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()).into_response(),
    }
}

async fn get_silent_pvs_report(State(state): State<AppState>) -> Response {
    match state.registry.silent_pvs(Duration::from_secs(3600)) {
        // 1 hour threshold
        Ok(records) => {
            let entries: Vec<ReportEntry> = records
                .into_iter()
                .map(|r| record_to_report_entry(r))
                .collect();
            axum::Json(entries).into_response()
        }
        Err(e) => (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()).into_response(),
    }
}

fn record_to_report_entry(r: archiver_core::registry::PvRecord) -> ReportEntry {
    let status_str = match r.status {
        PvStatus::Active => "Being archived",
        PvStatus::Paused => "Paused",
        PvStatus::Error => "Error",
    };
    ReportEntry {
        pv_name: r.pv_name,
        status: Some(status_str.to_string()),
        last_event: r
            .last_timestamp
            .map(|ts| chrono::DateTime::<chrono::Utc>::from(ts).to_rfc3339()),
    }
}

// --- G4: getVersions ---

async fn get_versions() -> Response {
    let resp = serde_json::json!({
        "version": env!("CARGO_PKG_VERSION"),
        "status": "running",
        "archiver": "beamalignment"
    });
    axum::Json(resp).into_response()
}

// --- G5: BPL extensions (Tier 1) ---

#[derive(Deserialize)]
struct PvNameParam {
    pv: String,
}

#[derive(Serialize)]
struct PvTypeInfoResponse {
    #[serde(rename = "pvName")]
    pv_name: String,
    #[serde(rename = "DBRType")]
    dbr_type: i32,
    #[serde(rename = "samplingMethod")]
    sampling_method: String,
    #[serde(rename = "samplingPeriod")]
    sampling_period: f64,
    #[serde(rename = "elementCount")]
    element_count: i32,
    status: String,
    #[serde(rename = "PREC", skip_serializing_if = "Option::is_none")]
    prec: Option<String>,
    #[serde(rename = "EGU", skip_serializing_if = "Option::is_none")]
    egu: Option<String>,
    #[serde(rename = "createdAt")]
    created_at: String,
}

fn record_to_type_info(r: &archiver_core::registry::PvRecord) -> PvTypeInfoResponse {
    let (method, period) = match &r.sample_mode {
        SampleMode::Monitor => ("Monitor".to_string(), 0.0),
        SampleMode::Scan { period_secs } => ("Scan".to_string(), *period_secs),
    };
    let status_str = match r.status {
        PvStatus::Active => "Being archived",
        PvStatus::Paused => "Paused",
        PvStatus::Error => "Error",
    };
    PvTypeInfoResponse {
        pv_name: r.pv_name.clone(),
        dbr_type: r.dbr_type as i32,
        sampling_method: method,
        sampling_period: period,
        element_count: r.element_count,
        status: status_str.to_string(),
        prec: r.prec.clone(),
        egu: r.egu.clone(),
        created_at: r.created_at.to_rfc3339(),
    }
}

async fn get_pv_type_info(
    State(state): State<AppState>,
    Query(params): Query<PvNameParam>,
) -> Response {
    match state.registry.get_pv(&params.pv) {
        Ok(Some(record)) => axum::Json(record_to_type_info(&record)).into_response(),
        Ok(None) => (StatusCode::NOT_FOUND, format!("PV {} not found", params.pv)).into_response(),
        Err(e) => (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()).into_response(),
    }
}

async fn get_pv_details(
    State(state): State<AppState>,
    Query(params): Query<PvNameParam>,
) -> Response {
    let record = match state.registry.get_pv(&params.pv) {
        Ok(Some(r)) => r,
        Ok(None) => return (StatusCode::NOT_FOUND, format!("PV {} not found", params.pv)).into_response(),
        Err(e) => return (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()).into_response(),
    };

    let conn_info = state.channel_mgr.get_connection_info(&params.pv);
    let is_connected = conn_info.as_ref().map(|c| c.is_connected).unwrap_or(false);
    let connected_since = conn_info
        .as_ref()
        .and_then(|c| c.connected_since)
        .map(|ts| chrono::DateTime::<chrono::Utc>::from(ts).to_rfc3339());

    let mut detail = serde_json::to_value(record_to_type_info(&record)).unwrap_or_default();
    if let Some(obj) = detail.as_object_mut() {
        obj.insert("isConnected".to_string(), serde_json::json!(is_connected));
        obj.insert("connectedSince".to_string(), serde_json::json!(connected_since));
    }

    axum::Json(detail).into_response()
}

async fn abort_archiving_pv(
    State(state): State<AppState>,
    headers: HeaderMap,
    Query(params): Query<PvNameParam>,
) -> Response {
    let qs = format!("pv={}", urlencoding::encode(&params.pv));
    if let Some(resp) = try_mgmt_dispatch(&state, &params.pv, "abortArchivingPV", &qs, &headers).await {
        return resp;
    }

    // Only abort if paused or error.
    match state.registry.get_pv(&params.pv) {
        Ok(Some(record)) => {
            if record.status == PvStatus::Active {
                return (StatusCode::BAD_REQUEST, "PV is actively archiving; pause first or use deletePV").into_response();
            }
        }
        Ok(None) => return (StatusCode::NOT_FOUND, format!("PV {} not found", params.pv)).into_response(),
        Err(e) => return (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()).into_response(),
    }

    // Destroy the PV handle but keep data.
    if let Err(e) = state.channel_mgr.destroy_pv(&params.pv) {
        let msg = format!("Failed to abort archiving PV {}: {e}", params.pv);
        return (StatusCode::INTERNAL_SERVER_ERROR, msg).into_response();
    }

    let msg = format!("Successfully aborted archiving PV {} (data retained)", params.pv);
    (StatusCode::OK, msg).into_response()
}

#[derive(Serialize, Deserialize)]
struct ExportRecord {
    #[serde(rename = "pvName")]
    pv_name: String,
    #[serde(rename = "DBRType")]
    dbr_type: i32,
    #[serde(rename = "samplingMethod")]
    sampling_method: String,
    #[serde(rename = "samplingPeriod")]
    sampling_period: f64,
    #[serde(rename = "elementCount")]
    element_count: i32,
    #[serde(rename = "PREC", skip_serializing_if = "Option::is_none")]
    prec: Option<String>,
    #[serde(rename = "EGU", skip_serializing_if = "Option::is_none")]
    egu: Option<String>,
}

async fn export_config(State(state): State<AppState>) -> Response {
    let records = match state.registry.all_records() {
        Ok(r) => r,
        Err(e) => return (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()).into_response(),
    };

    let exports: Vec<ExportRecord> = records
        .into_iter()
        .map(|r| {
            let (method, period) = match &r.sample_mode {
                SampleMode::Monitor => ("Monitor".to_string(), 0.0),
                SampleMode::Scan { period_secs } => ("Scan".to_string(), *period_secs),
            };
            ExportRecord {
                pv_name: r.pv_name,
                dbr_type: r.dbr_type as i32,
                sampling_method: method,
                sampling_period: period,
                element_count: r.element_count,
                prec: r.prec,
                egu: r.egu,
            }
        })
        .collect();

    axum::Json(exports).into_response()
}

async fn import_config(
    State(state): State<AppState>,
    body: axum::body::Bytes,
) -> Response {
    let records: Vec<ExportRecord> = match serde_json::from_slice(&body) {
        Ok(r) => r,
        Err(e) => return (StatusCode::BAD_REQUEST, format!("Invalid JSON: {e}")).into_response(),
    };

    let mut imported = 0u64;
    let mut errors = Vec::new();

    for r in &records {
        let dbr_type = archiver_core::types::ArchDbType::from_i32(r.dbr_type)
            .unwrap_or(archiver_core::types::ArchDbType::ScalarDouble);
        let sample_mode = parse_sample_mode(
            Some(r.sampling_method.as_str()),
            if r.sampling_period > 0.0 { Some(r.sampling_period) } else { None },
        );
        match state.registry.register_pv(&r.pv_name, dbr_type, &sample_mode, r.element_count) {
            Ok(()) => {
                // Update metadata if present.
                if r.prec.is_some() || r.egu.is_some() {
                    let _ = state.registry.update_metadata(
                        &r.pv_name,
                        r.prec.as_deref(),
                        r.egu.as_deref(),
                    );
                }
                imported += 1;
            }
            Err(e) => errors.push(format!("{}: {e}", r.pv_name)),
        }
    }

    let resp = serde_json::json!({
        "imported": imported,
        "total": records.len(),
        "errors": errors,
    });
    axum::Json(resp).into_response()
}
