use std::time::{Duration, SystemTime};

use axum::extract::State;
use axum::response::IntoResponse;

use archiver_core::registry::PvStatus;

use crate::dto::mgmt::*;
use crate::errors::ApiError;
use crate::AppState;

pub async fn get_paused_pvs_report(
    State(state): State<AppState>,
) -> Result<impl IntoResponse, ApiError> {
    let records = state
        .pv_query
        .pvs_by_status(PvStatus::Paused)
        .map_err(ApiError::internal)?;
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
    Ok(axum::Json(entries))
}

pub async fn get_never_connected_pvs(
    State(state): State<AppState>,
) -> Result<impl IntoResponse, ApiError> {
    let pvs = state.archiver_query.get_never_connected_pvs();
    let entries: Vec<ReportEntry> = pvs
        .into_iter()
        .map(|name| ReportEntry {
            pv_name: name,
            status: Some("Never connected".to_string()),
            last_event: None,
        })
        .collect();
    Ok(axum::Json(entries))
}

pub async fn get_currently_disconnected_pvs(
    State(state): State<AppState>,
) -> Result<impl IntoResponse, ApiError> {
    let pvs = state.archiver_query.get_currently_disconnected_pvs();
    let entries: Vec<ReportEntry> = pvs
        .into_iter()
        .map(|name| ReportEntry {
            pv_name: name,
            status: Some("Disconnected".to_string()),
            last_event: None,
        })
        .collect();
    Ok(axum::Json(entries))
}

pub async fn get_recently_added_pvs(
    State(state): State<AppState>,
) -> Result<impl IntoResponse, ApiError> {
    let since = SystemTime::now() - Duration::from_secs(86400); // 24h
    let records = state
        .pv_query
        .recently_added_pvs(since)
        .map_err(ApiError::internal)?;
    let entries: Vec<ReportEntry> = records.into_iter().map(record_to_report_entry).collect();
    Ok(axum::Json(entries))
}

pub async fn get_recently_modified_pvs(
    State(state): State<AppState>,
) -> Result<impl IntoResponse, ApiError> {
    let since = SystemTime::now() - Duration::from_secs(86400); // 24h
    let records = state
        .pv_query
        .recently_modified_pvs(since)
        .map_err(ApiError::internal)?;
    let entries: Vec<ReportEntry> = records.into_iter().map(record_to_report_entry).collect();
    Ok(axum::Json(entries))
}

pub async fn get_silent_pvs_report(
    State(state): State<AppState>,
) -> Result<impl IntoResponse, ApiError> {
    // 1 hour threshold
    let records = state
        .pv_query
        .silent_pvs(Duration::from_secs(3600))
        .map_err(ApiError::internal)?;
    let entries: Vec<ReportEntry> = records.into_iter().map(record_to_report_entry).collect();
    Ok(axum::Json(entries))
}
