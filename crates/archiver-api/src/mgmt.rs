use std::time::{Duration, SystemTime};

use axum::extract::{Query, State};
use axum::http::StatusCode;
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
}

/// POST /mgmt/bpl/archivePV
///
/// Accepts either a JSON object `{"pv":"..."}` for single PV, or a JSON
/// array / newline-delimited body for bulk archiving.
async fn archive_pv(
    State(state): State<AppState>,
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
            return archive_single_pv(&state, &pv, &sample_mode).await;
        }
    }

    // Try as bulk PV list.
    let pvs = parse_pv_list(&body_str);
    if pvs.is_empty() {
        return (StatusCode::BAD_REQUEST, "No PVs specified").into_response();
    }

    let mut results = Vec::with_capacity(pvs.len());
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

async fn archive_single_pv(state: &AppState, pv: &str, sample_mode: &SampleMode) -> Response {
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

// --- Single pause/resume (GET with ?pv=) ---

#[derive(Deserialize)]
struct PausePvParams {
    pv: String,
}

async fn pause_archiving_pv(
    State(state): State<AppState>,
    Query(params): Query<PausePvParams>,
) -> Response {
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
    Query(params): Query<PausePvParams>,
) -> Response {
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
    PvListInput(pvs): PvListInput,
) -> Response {
    let mut results = Vec::with_capacity(pvs.len());
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
    PvListInput(pvs): PvListInput,
) -> Response {
    let mut results = Vec::with_capacity(pvs.len());
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

async fn get_pv_count(State(state): State<AppState>) -> Response {
    let total = state.registry.count(None).unwrap_or(0);
    let active = state.registry.count(Some(PvStatus::Active)).unwrap_or(0);
    let paused = state.registry.count(Some(PvStatus::Paused)).unwrap_or(0);
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
    Query(params): Query<DeletePvParams>,
) -> Response {
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
    Query(params): Query<ChangeParamsQuery>,
) -> Response {
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
