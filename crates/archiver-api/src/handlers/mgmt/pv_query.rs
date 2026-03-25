use axum::extract::{Query, State};
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};

use archiver_core::registry::{PvStatus, SampleMode};

use crate::dto::mgmt::*;
use crate::errors::internal_error;
use crate::AppState;

pub async fn get_all_pvs(
    State(state): State<AppState>,
    Query(cp): Query<ClusterParam>,
) -> Response {
    let mut pvs = match state.pv_repo.all_pv_names() {
        Ok(pvs) => pvs,
        Err(e) => return internal_error(e),
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

pub async fn get_matching_pvs(
    State(state): State<AppState>,
    Query(params): Query<MatchingPvsParams>,
) -> Response {
    let mut pvs = match state.pv_repo.matching_pvs(&params.pv) {
        Ok(pvs) => pvs,
        Err(e) => return internal_error(e),
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

pub async fn get_pv_status(
    State(state): State<AppState>,
    Query(params): Query<PvStatusParams>,
) -> Response {
    match state.pv_repo.get_pv(&params.pv) {
        Ok(Some(record)) => {
            let status_str = match record.status {
                PvStatus::Active => "Being archived",
                PvStatus::Paused => "Paused",
                PvStatus::Error => "Error",
                PvStatus::Inactive => "Inactive",
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
        Err(e) => internal_error(e),
    }
}

pub async fn get_pv_count(
    State(state): State<AppState>,
    Query(cp): Query<ClusterParam>,
) -> Response {
    let mut total = state.pv_repo.count(None).unwrap_or(0);
    let mut active = state.pv_repo.count(Some(PvStatus::Active)).unwrap_or(0);
    let mut paused = state.pv_repo.count(Some(PvStatus::Paused)).unwrap_or(0);

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

pub async fn get_pv_type_info(
    State(state): State<AppState>,
    Query(params): Query<PvNameParam>,
) -> Response {
    match state.pv_repo.get_pv(&params.pv) {
        Ok(Some(record)) => axum::Json(record_to_type_info(&record)).into_response(),
        Ok(None) => (StatusCode::NOT_FOUND, format!("PV {} not found", params.pv)).into_response(),
        Err(e) => internal_error(e),
    }
}

pub async fn get_pv_details(
    State(state): State<AppState>,
    Query(params): Query<PvNameParam>,
) -> Response {
    let record = match state.pv_repo.get_pv(&params.pv) {
        Ok(Some(r)) => r,
        Ok(None) => return (StatusCode::NOT_FOUND, format!("PV {} not found", params.pv)).into_response(),
        Err(e) => return internal_error(e),
    };

    let conn_info = state.archiver.get_connection_info(&params.pv);
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
