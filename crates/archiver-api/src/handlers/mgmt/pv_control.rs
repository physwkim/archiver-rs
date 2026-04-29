use axum::extract::{Query, State};
use axum::http::HeaderMap;
use axum::response::{IntoResponse, Response};
use serde::Deserialize;

use archiver_core::registry::SampleMode;

use crate::dto::mgmt::*;
use crate::errors::ApiError;
use crate::pv_input::{parse_pv_list, PvListInput};
use crate::AppState;

use super::{forward_all_peer_batches, route_pvs, try_mgmt_dispatch};

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
        Err(_) => return ApiError::BadRequest("Invalid UTF-8 body".to_string()).into_response(),
    };

    // Try parsing as single PV request first.
    if let Ok(req) = serde_json::from_str::<ArchivePvRequest>(&body_str)
        && let Some(pv) = req.pv {
            let pv = archiver_core::registry::normalize_pv_name(&pv).to_string();
            let sample_mode = parse_sample_mode(req.sampling_method.as_deref(), req.sampling_period);
            return archive_single_pv(&state, &pv, &sample_mode, req.appliance.as_deref(), &headers).await;
        }

    // Try as JSON array of ArchivePvRequest objects (with per-PV appliance + sampling).
    if let Ok(reqs) = serde_json::from_str::<Vec<ArchivePvRequest>>(&body_str) {
        // Normalize each entry's pv field so the per-request channel name
        // matches what the registry stores.
        let reqs: Vec<_> = reqs
            .into_iter()
            .filter_map(|mut r| {
                let pv = r.pv?;
                r.pv = Some(archiver_core::registry::normalize_pv_name(&pv).to_string());
                Some(r)
            })
            .collect();
        if !reqs.is_empty() {
            return bulk_archive_requests(&state, &reqs, &headers).await;
        }
    }

    // Fallback: plain PV list (string array or newline-delimited).
    let pvs = parse_pv_list(&body_str);
    if pvs.is_empty() {
        return ApiError::BadRequest("No PVs specified".to_string()).into_response();
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
        let Some(pv) = req.pv.as_deref() else {
            continue;
        };

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
        let Some(pv) = req.pv.as_deref() else {
            continue;
        };
        let sample_mode = parse_sample_mode(req.sampling_method.as_deref(), req.sampling_period);
        let status = match state.archiver_cmd.archive_pv(pv, &sample_mode).await {
            Ok(()) => "Archive request submitted".to_string(),
            Err(e) => {
                tracing::warn!(pv, "Failed to archive: {e}");
                format!("Failed to archive: {e}")
            }
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

            let json = match serde_json::to_string(&peer_body) {
                Ok(s) => s,
                Err(e) => {
                    tracing::error!("Failed to serialize peer batch: {e}");
                    for req in batch {
                        results.push(BulkResult {
                            pv_name: req.pv.clone().unwrap_or_default(),
                            status: "Failed to serialize request".to_string(),
                        });
                    }
                    continue;
                }
            };
            let bytes = axum::body::Bytes::from(json);
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
                        let raw = String::from_utf8_lossy(&body_bytes);
                        let msg = super::truncate_for_display(&raw, 200);
                        for req in batch {
                            results.push(BulkResult {
                                pv_name: req.pv.clone().unwrap_or_default(),
                                status: format!("Peer {peer_name} error ({status_code}): {msg}"),
                            });
                        }
                    }
                }
                Err(e) => {
                    tracing::warn!(peer_name, "Failed to forward archive batch: {e}");
                    for req in batch {
                        results.push(BulkResult {
                            pv_name: req.pv.clone().unwrap_or_default(),
                            status: format!("Failed to forward to {peer_name}"),
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

    // Refuse to archive PVs whose name already exists as an alias — the user
    // should archive the target PV directly. (Aliases are added with addAlias
    // after archiving the real PV.)
    if let Ok(Some(target)) = state.pv_query.canonical_name(pv).map(|c| {
        if c != pv { Some(c) } else { None }
    }) {
        return ApiError::Conflict(format!(
            "'{pv}' is an alias for '{target}'; archive the target PV instead",
        ))
        .into_response();
    }

    if !is_proxied {
        if let Some(target) = appliance
            && let Some(ref cluster) = state.cluster
                && target != cluster.identity_name() {
                    if let Some(peer) = cluster.find_peer_by_name(target) {
                        let mut body = serde_json::json!({ "pv": pv });
                        if let SampleMode::Scan { period_secs } = sample_mode {
                            body["sampling_method"] = serde_json::json!("scan");
                            body["sampling_period"] = serde_json::json!(period_secs);
                        }
                        let bytes = axum::body::Bytes::from(body.to_string());
                        return match cluster.proxy_mgmt_post(&peer.mgmt_url, "archivePV", bytes).await {
                            Ok(resp) => resp,
                            Err(e) => ApiError::bad_gateway(e).into_response(),
                        };
                    }
                    return ApiError::NotFound(format!("Appliance '{target}' not found in cluster")).into_response();
                }

        if let Some(ref cluster) = state.cluster
            && cluster.resolve_peer(pv).await.is_some() {
                return ApiError::Conflict(format!("PV {pv} is already archived on another appliance")).into_response();
            }
    }

    match crate::usecases::archive_pv::archive_pv(state.archiver_cmd.as_ref(), pv, sample_mode).await {
        Ok(msg) => msg.into_response(),
        Err(e) => e.into_response(),
    }
}

// --- Single pause/resume (GET with ?pv=) ---

pub async fn pause_archiving_pv(
    State(state): State<AppState>,
    headers: HeaderMap,
    Query(params): Query<PausePvParams>,
) -> Result<impl IntoResponse, ApiError> {
    let canonical = super::resolve_canonical(&state, &params.pv);
    let qs = format!("pv={}", urlencoding::encode(&canonical));
    if let Some(resp) = try_mgmt_dispatch(&state, &canonical, "pauseArchivingPV", &qs, &headers).await {
        return Ok(resp);
    }
    state
        .archiver_cmd
        .pause_pv(&canonical)
        .await
        .map_err(|e| ApiError::Internal(format!("Failed to pause PV {canonical}: {e}")))?;
    Ok(format!("Successfully paused PV {canonical}").into_response())
}

pub async fn resume_archiving_pv(
    State(state): State<AppState>,
    headers: HeaderMap,
    Query(params): Query<PausePvParams>,
) -> Result<impl IntoResponse, ApiError> {
    let canonical = super::resolve_canonical(&state, &params.pv);
    let qs = format!("pv={}", urlencoding::encode(&canonical));
    if let Some(resp) = try_mgmt_dispatch(&state, &canonical, "resumeArchivingPV", &qs, &headers).await {
        return Ok(resp);
    }
    state
        .archiver_cmd
        .resume_pv(&canonical)
        .await
        .map_err(|e| ApiError::Internal(format!("Failed to resume PV {canonical}: {e}")))?;
    Ok(format!("Successfully resumed PV {canonical}").into_response())
}

// --- Bulk pause/resume (POST with body) ---

pub async fn bulk_pause_archiving_pv(
    State(state): State<AppState>,
    headers: HeaderMap,
    PvListInput(pvs): PvListInput,
) -> Response {
    let is_proxied = headers.get("X-Archiver-Proxied").is_some();
    let (local_pvs, remote_batches) = route_pvs(&state, &pvs, is_proxied).await;

    let mut results = Vec::with_capacity(pvs.len());

    if let Some(ref cluster) = state.cluster {
        results.extend(
            forward_all_peer_batches(cluster.as_ref(), remote_batches, "pauseArchivingPV").await,
        );
    }

    for pv in &local_pvs {
        let status = match state.archiver_cmd.pause_pv(pv).await {
            Ok(()) => "Successfully paused".to_string(),
            Err(e) => {
                tracing::warn!(pv, "Failed to pause: {e}");
                format!("Failed to pause: {e}")
            }
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
    let (local_pvs, remote_batches) = route_pvs(&state, &pvs, is_proxied).await;

    let mut results = Vec::with_capacity(pvs.len());

    if let Some(ref cluster) = state.cluster {
        results.extend(
            forward_all_peer_batches(cluster.as_ref(), remote_batches, "resumeArchivingPV").await,
        );
    }

    for pv in &local_pvs {
        let status = match state.archiver_cmd.resume_pv(pv).await {
            Ok(()) => "Successfully resumed".to_string(),
            Err(e) => {
                tracing::warn!(pv, "Failed to resume: {e}");
                format!("Failed to resume: {e}")
            }
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
) -> Result<impl IntoResponse, ApiError> {
    let canonical = super::resolve_canonical(&state, &params.pv);
    let mut qs = format!("pv={}", urlencoding::encode(&canonical));
    if params.delete_data.unwrap_or(false) {
        qs.push_str("&deleteData=true");
    }
    if let Some(resp) = try_mgmt_dispatch(&state, &canonical, "deletePV", &qs, &headers).await {
        return Ok(resp);
    }
    let result = crate::usecases::delete_pv::delete_pv(
        state.archiver_cmd.as_ref(),
        state.pv_query.as_ref(),
        state.pv_cmd.as_ref(),
        state.storage.as_ref(),
        &canonical,
        params.delete_data.unwrap_or(false),
    )
    .await?;

    let msg = format!(
        "Successfully deleted PV {} (data files removed: {})",
        result.pv, result.files_deleted
    );
    Ok(msg.into_response())
}

/// `POST /mgmt/bpl/deletePV` with a JSON list of PV names. Java archiver
/// fix 9bf93675 added the bulk POST variant alongside the single-PV GET.
/// Optional `?deleteData=true` query string applies to every PV in the list.
pub async fn bulk_delete_pv(
    State(state): State<AppState>,
    headers: HeaderMap,
    Query(params): Query<DeletePvOptions>,
    PvListInput(pvs): PvListInput,
) -> Response {
    let is_proxied = headers.get("X-Archiver-Proxied").is_some();
    let (local_pvs, remote_batches) = route_pvs(&state, &pvs, is_proxied).await;

    let mut results = Vec::with_capacity(pvs.len());

    if let Some(ref cluster) = state.cluster {
        results.extend(
            forward_all_peer_batches(cluster.as_ref(), remote_batches, "deletePV").await,
        );
    }

    for pv in &local_pvs {
        let status = match crate::usecases::delete_pv::delete_pv(
            state.archiver_cmd.as_ref(),
            state.pv_query.as_ref(),
            state.pv_cmd.as_ref(),
            state.storage.as_ref(),
            pv,
            params.delete_data.unwrap_or(false),
        )
        .await
        {
            Ok(r) => format!("Deleted (files: {})", r.files_deleted),
            Err(e) => {
                tracing::warn!(pv, "Bulk delete failed: {e}");
                format!("Failed: {e}")
            }
        };
        results.push(BulkResult {
            pv_name: pv.clone(),
            status,
        });
    }

    axum::Json(results).into_response()
}

#[derive(Deserialize)]
pub struct DeletePvOptions {
    #[serde(default, rename = "deleteData")]
    pub delete_data: Option<bool>,
}

// --- Change archival parameters ---

pub async fn change_archival_parameters(
    State(state): State<AppState>,
    headers: HeaderMap,
    Query(params): Query<ChangeParamsQuery>,
) -> Result<impl IntoResponse, ApiError> {
    let canonical = super::resolve_canonical(&state, &params.pv);
    let mut qs = format!("pv={}", urlencoding::encode(&canonical));
    if let Some(period) = params.samplingperiod {
        qs.push_str(&format!("&samplingperiod={period}"));
    }
    if let Some(ref method) = params.samplingmethod {
        qs.push_str(&format!("&samplingmethod={method}"));
    }
    if let Some(resp) = try_mgmt_dispatch(&state, &canonical, "changeArchivalParameters", &qs, &headers).await {
        return Ok(resp);
    }
    let new_mode = parse_sample_mode(
        params.samplingmethod.as_deref(),
        params.samplingperiod,
    );

    let msg = crate::usecases::change_parameters::change_parameters(
        state.pv_query.as_ref(),
        state.pv_cmd.as_ref(),
        state.archiver_cmd.as_ref(),
        &canonical,
        &new_mode,
    )
    .await?;

    Ok(msg.into_response())
}

// --- Abort archiving ---

pub async fn abort_archiving_pv(
    State(state): State<AppState>,
    headers: HeaderMap,
    Query(params): Query<PvNameParam>,
) -> Result<impl IntoResponse, ApiError> {
    let canonical = super::resolve_canonical(&state, &params.pv);
    let qs = format!("pv={}", urlencoding::encode(&canonical));
    if let Some(resp) = try_mgmt_dispatch(&state, &canonical, "abortArchivingPV", &qs, &headers).await {
        return Ok(resp);
    }

    let msg = crate::usecases::abort_archiving::abort_archiving(
        state.pv_query.as_ref(),
        state.archiver_cmd.as_ref(),
        &canonical,
    )
    .await?;

    Ok(msg.into_response())
}
