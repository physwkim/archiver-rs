use std::time::{Duration, SystemTime};

use axum::extract::State;
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};

use archiver_core::registry::PvStatus;

use crate::dto::mgmt::*;
use crate::AppState;

pub async fn get_paused_pvs_report(State(state): State<AppState>) -> Response {
    match state.pv_repo.pvs_by_status(PvStatus::Paused) {
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

pub async fn get_never_connected_pvs(State(state): State<AppState>) -> Response {
    let pvs = state.archiver.get_never_connected_pvs();
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

pub async fn get_currently_disconnected_pvs(State(state): State<AppState>) -> Response {
    let pvs = state.archiver.get_currently_disconnected_pvs();
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

pub async fn get_recently_added_pvs(State(state): State<AppState>) -> Response {
    let since = SystemTime::now() - Duration::from_secs(86400); // 24h
    match state.pv_repo.recently_added_pvs(since) {
        Ok(records) => {
            let entries: Vec<ReportEntry> = records
                .into_iter()
                .map(record_to_report_entry)
                .collect();
            axum::Json(entries).into_response()
        }
        Err(e) => (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()).into_response(),
    }
}

pub async fn get_recently_modified_pvs(State(state): State<AppState>) -> Response {
    let since = SystemTime::now() - Duration::from_secs(86400); // 24h
    match state.pv_repo.recently_modified_pvs(since) {
        Ok(records) => {
            let entries: Vec<ReportEntry> = records
                .into_iter()
                .map(record_to_report_entry)
                .collect();
            axum::Json(entries).into_response()
        }
        Err(e) => (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()).into_response(),
    }
}

pub async fn get_silent_pvs_report(State(state): State<AppState>) -> Response {
    match state.pv_repo.silent_pvs(Duration::from_secs(3600)) {
        // 1 hour threshold
        Ok(records) => {
            let entries: Vec<ReportEntry> = records
                .into_iter()
                .map(record_to_report_entry)
                .collect();
            axum::Json(entries).into_response()
        }
        Err(e) => (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()).into_response(),
    }
}
