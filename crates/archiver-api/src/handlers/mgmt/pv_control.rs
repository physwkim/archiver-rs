use axum::extract::{Query, State};
use axum::http::{HeaderMap, StatusCode};
use axum::response::{IntoResponse, Response};

use archiver_core::registry::{PvStatus, SampleMode};

use crate::dto::mgmt::*;
use crate::pv_input::{parse_pv_list, PvListInput};
use crate::AppState;

use super::{forward_pv_batch_to_peer, try_mgmt_dispatch};

/// POST /mgmt/bpl/archivePV
///
/// Accepts either a JSON object `{"pv":"..."}` for single PV, or a JSON
/// array / newline-delimited body for bulk archiving.
pub async fn archive_pv(
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

    // Try as JSON array of ArchivePvRequest objects (with per-PV appliance + sampling).
    if let Ok(reqs) = serde_json::from_str::<Vec<ArchivePvRequest>>(&body_str) {
        let reqs: Vec<_> = reqs.into_iter().filter(|r| r.pv.is_some()).collect();
        if !reqs.is_empty() {
            return bulk_archive_requests(&state, &reqs, &headers).await;
        }
    }

    // Fallback: plain PV list (string array or newline-delimited).
    let pvs = parse_pv_list(&body_str);
    if pvs.is_empty() {
        return (StatusCode::BAD_REQUEST, "No PVs specified").into_response();
    }
    let reqs: Vec<_> = pvs
        .into_iter()
        .map(|pv| ArchivePvRequest {
            pv: Some(pv),
            sampling_period: None,
            sampling_method: None,
            appliance: None,
        })
        .collect();
    bulk_archive_requests(&state, &reqs, &headers).await
}

/// Process a batch of archive requests, distributing to peers by appliance.
async fn bulk_archive_requests(
    state: &AppState,
    reqs: &[ArchivePvRequest],
    headers: &HeaderMap,
) -> Response {
    let is_proxied = headers.get("X-Archiver-Proxied").is_some();
    let mut results = Vec::with_capacity(reqs.len());

    // Group requests by target: None = local, Some(name) = peer appliance.
    let mut local_reqs = Vec::new();
    let mut peer_batches: std::collections::HashMap<String, Vec<&ArchivePvRequest>> =
        std::collections::HashMap::new();

    for req in reqs {
        let pv = req.pv.as_deref().unwrap();

        let target = if is_proxied {
            None
        } else if let Some(ref target_name) = req.appliance {
            if let Some(ref cluster) = state.cluster {
                if target_name != cluster.identity_name() {
                    Some(target_name.clone())
                } else {
                    None
                }
            } else {
                None
            }
        } else if let Some(ref cluster) = state.cluster {
            if cluster.resolve_peer(pv).await.is_some() {
                results.push(BulkResult {
                    pv_name: pv.to_string(),
                    status: "Already archived on peer".to_string(),
                });
                continue;
            }
            None
        } else {
            None
        };

        match target {
            Some(peer_name) => {
                peer_batches
                    .entry(peer_name)
                    .or_default()
                    .push(req);
            }
            None => local_reqs.push(req),
        }
    }

    // Archive local PVs.
    for req in &local_reqs {
        let pv = req.pv.as_deref().unwrap();
        let sample_mode = parse_sample_mode(req.sampling_method.as_deref(), req.sampling_period);
        let status = match state.archiver.archive_pv(pv, &sample_mode).await {
            Ok(()) => "Archive request submitted".to_string(),
            Err(e) => e.to_string(),
        };
        results.push(BulkResult {
            pv_name: pv.to_string(),
            status,
        });
    }

    // Forward batches to peers.
    if let Some(ref cluster) = state.cluster {
        for (peer_name, batch) in &peer_batches {
            let peer = match cluster.find_peer_by_name(peer_name) {
                Some(p) => p,
                None => {
                    for req in batch {
                        results.push(BulkResult {
                            pv_name: req.pv.clone().unwrap_or_default(),
                            status: format!("Appliance '{peer_name}' not found in cluster"),
                        });
                    }
                    continue;
                }
            };

            let peer_body: Vec<serde_json::Value> = batch
                .iter()
                .map(|req| {
                    let mut obj = serde_json::json!({ "pv": req.pv });
                    let sm = parse_sample_mode(req.sampling_method.as_deref(), req.sampling_period);
                    if let SampleMode::Scan { period_secs } = sm {
                        obj["sampling_method"] = serde_json::json!("scan");
                        obj["sampling_period"] = serde_json::json!(period_secs);
                    }
                    obj
                })
                .collect();

            let bytes = axum::body::Bytes::from(serde_json::to_string(&peer_body).unwrap());
            match cluster.proxy_mgmt_post(&peer.mgmt_url, "archivePV", bytes).await {
                Ok(resp) => {
                    let status_code = resp.status();
                    let body_bytes = axum::body::to_bytes(resp.into_body(), 1024 * 1024)
                        .await
                        .unwrap_or_default();
                    if let Ok(peer_results) =
                        serde_json::from_slice::<Vec<BulkResult>>(&body_bytes)
                    {
                        results.extend(peer_results);
                    } else {
                        let msg = String::from_utf8_lossy(&body_bytes);
                        for req in batch {
                            results.push(BulkResult {
                                pv_name: req.pv.clone().unwrap_or_default(),
                                status: format!("Forwarded to {peer_name} ({status_code}): {msg}"),
                            });
                        }
                    }
                }
                Err(e) => {
                    for req in batch {
                        results.push(BulkResult {
                            pv_name: req.pv.clone().unwrap_or_default(),
                            status: format!("Failed to forward to {peer_name}: {e}"),
                        });
                    }
                }
            }
        }
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

    if !is_proxied {
        if let Some(target) = appliance {
            if let Some(ref cluster) = state.cluster {
                if target != cluster.identity_name() {
                    if let Some(peer) = cluster.find_peer_by_name(target) {
                        let mut body = serde_json::json!({ "pv": pv });
                        if let SampleMode::Scan { period_secs } = sample_mode {
                            body["sampling_method"] = serde_json::json!("scan");
                            body["sampling_period"] = serde_json::json!(period_secs);
                        }
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

        if let Some(ref cluster) = state.cluster {
            if cluster.resolve_peer(pv).await.is_some() {
                return (StatusCode::CONFLICT, format!("PV {pv} is already archived on another appliance")).into_response();
            }
        }
    }

    match state.archiver.archive_pv(pv, sample_mode).await {
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

// --- Single pause/resume (GET with ?pv=) ---

pub async fn pause_archiving_pv(
    State(state): State<AppState>,
    headers: HeaderMap,
    Query(params): Query<PausePvParams>,
) -> Response {
    let qs = format!("pv={}", urlencoding::encode(&params.pv));
    if let Some(resp) = try_mgmt_dispatch(&state, &params.pv, "pauseArchivingPV", &qs, &headers).await {
        return resp;
    }
    match state.archiver.pause_pv(&params.pv) {
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

pub async fn resume_archiving_pv(
    State(state): State<AppState>,
    headers: HeaderMap,
    Query(params): Query<PausePvParams>,
) -> Response {
    let qs = format!("pv={}", urlencoding::encode(&params.pv));
    if let Some(resp) = try_mgmt_dispatch(&state, &params.pv, "resumeArchivingPV", &qs, &headers).await {
        return resp;
    }
    match state.archiver.resume_pv(&params.pv).await {
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

pub async fn bulk_pause_archiving_pv(
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
                if state.pv_repo.get_pv(pv).ok().flatten().is_some() {
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
                    let cluster = cluster.clone();
                    async move {
                        forward_pv_batch_to_peer(cluster.as_ref(), &mgmt_url, "pauseArchivingPV", &batch)
                            .await
                    }
                })
                .collect();
            let remote_results = futures::future::join_all(futs).await;
            for batch_results in remote_results {
                results.extend(batch_results);
            }

            for pv in &local_pvs {
                let status = match state.archiver.pause_pv(pv) {
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

    for pv in &pvs {
        let status = match state.archiver.pause_pv(pv) {
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

pub async fn bulk_resume_archiving_pv(
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
                if state.pv_repo.get_pv(pv).ok().flatten().is_some() {
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
                    let cluster = cluster.clone();
                    async move {
                        forward_pv_batch_to_peer(cluster.as_ref(), &mgmt_url, "resumeArchivingPV", &batch)
                            .await
                    }
                })
                .collect();
            let remote_results = futures::future::join_all(futs).await;
            for batch_results in remote_results {
                results.extend(batch_results);
            }

            for pv in &local_pvs {
                let status = match state.archiver.resume_pv(pv).await {
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
        let status = match state.archiver.resume_pv(pv).await {
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

// --- Delete PV ---

pub async fn delete_pv(
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
    if let Err(e) = state.archiver.destroy_pv(&params.pv) {
        let msg = format!("Failed to delete PV {}: {e}", params.pv);
        return (StatusCode::INTERNAL_SERVER_ERROR, msg).into_response();
    }

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

pub async fn change_archival_parameters(
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

    match state.pv_repo.update_sample_mode(&params.pv, &new_mode) {
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

    let record = match state.pv_repo.get_pv(&params.pv) {
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
        let _ = state.archiver.pause_pv(&params.pv);
        if let Err(e) = state.archiver.resume_pv(&params.pv).await {
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

// --- Abort archiving ---

pub async fn abort_archiving_pv(
    State(state): State<AppState>,
    headers: HeaderMap,
    Query(params): Query<PvNameParam>,
) -> Response {
    let qs = format!("pv={}", urlencoding::encode(&params.pv));
    if let Some(resp) = try_mgmt_dispatch(&state, &params.pv, "abortArchivingPV", &qs, &headers).await {
        return resp;
    }

    match state.pv_repo.get_pv(&params.pv) {
        Ok(Some(record)) => {
            if record.status == PvStatus::Active {
                return (StatusCode::BAD_REQUEST, "PV is actively archiving; pause first or use deletePV").into_response();
            }
        }
        Ok(None) => return (StatusCode::NOT_FOUND, format!("PV {} not found", params.pv)).into_response(),
        Err(e) => return (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()).into_response(),
    }

    if let Err(e) = state.archiver.stop_pv(&params.pv) {
        let msg = format!("Failed to abort archiving PV {}: {e}", params.pv);
        return (StatusCode::INTERNAL_SERVER_ERROR, msg).into_response();
    }

    let msg = format!("Successfully aborted archiving PV {} (data retained)", params.pv);
    (StatusCode::OK, msg).into_response()
}
