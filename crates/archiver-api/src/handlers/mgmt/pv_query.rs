use axum::extract::{Query, State};
use axum::response::IntoResponse;

use archiver_core::registry::{PvStatus, SampleMode};

use crate::dto::mgmt::*;
use crate::errors::ApiError;
use crate::AppState;

pub async fn get_all_pvs(
    State(state): State<AppState>,
    Query(cp): Query<ClusterParam>,
) -> Result<impl IntoResponse, ApiError> {
    let mut pvs = state.pv_query.all_pv_names().map_err(ApiError::internal)?;

    if cp.cluster.unwrap_or(false)
        && let Some(ref cluster) = state.cluster {
            let remote = cluster.aggregate_all_pvs().await;
            let mut all: std::collections::BTreeSet<String> = pvs.into_iter().collect();
            all.extend(remote);
            pvs = all.into_iter().collect();
        }

    Ok(axum::Json(pvs))
}

pub async fn get_matching_pvs(
    State(state): State<AppState>,
    Query(params): Query<MatchingPvsParams>,
) -> Result<impl IntoResponse, ApiError> {
    let mut pvs = state
        .pv_query
        .matching_pvs(&params.pv)
        .map_err(ApiError::internal)?;

    if params.cluster.unwrap_or(false)
        && let Some(ref cluster) = state.cluster {
            let remote = cluster.aggregate_matching_pvs(&params.pv).await;
            let mut all: std::collections::BTreeSet<String> = pvs.into_iter().collect();
            all.extend(remote);
            pvs = all.into_iter().collect();
        }

    Ok(axum::Json(pvs))
}

pub async fn get_pv_status(
    State(state): State<AppState>,
    Query(params): Query<PvStatusParams>,
) -> Result<impl IntoResponse, ApiError> {
    match state.pv_query.get_pv(&params.pv).map_err(ApiError::internal)? {
        Some(record) => {
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
            Ok(axum::Json(resp).into_response())
        }
        None => {
            // PV not local — try cluster if requested.
            if params.cluster.unwrap_or(false)
                && let Some(ref cluster) = state.cluster
                    && let Some(remote_status) = cluster.remote_pv_status(&params.pv).await {
                        return Ok(axum::Json(remote_status).into_response());
                    }
            let resp = PvStatusResponse {
                pv_name: params.pv,
                status: "Not being archived".to_string(),
                dbr_type: None,
                sample_mode: None,
                element_count: None,
                last_event_timestamp: None,
            };
            Ok(axum::Json(resp).into_response())
        }
    }
}

pub async fn get_pv_count(
    State(state): State<AppState>,
    Query(cp): Query<ClusterParam>,
) -> Result<impl IntoResponse, ApiError> {
    let mut total = state.pv_query.count(None).unwrap_or(0);
    let mut active = state.pv_query.count(Some(PvStatus::Active)).unwrap_or(0);
    let mut paused = state.pv_query.count(Some(PvStatus::Paused)).unwrap_or(0);

    let mut failed_peers = None;
    if cp.cluster.unwrap_or(false)
        && let Some(ref cluster) = state.cluster {
            let (rt, ra, rp, failed) = cluster.aggregate_pv_count().await;
            total += rt;
            active += ra;
            paused += rp;
            if failed > 0 {
                failed_peers = Some(failed);
            }
        }

    let resp = PvCountResponse {
        total,
        active,
        paused,
        failed_peers,
    };
    Ok(axum::Json(resp))
}

pub async fn get_pv_type_info(
    State(state): State<AppState>,
    Query(params): Query<PvNameParam>,
) -> Result<impl IntoResponse, ApiError> {
    match state
        .pv_query
        .get_pv(&params.pv)
        .map_err(ApiError::internal)?
    {
        Some(record) => Ok(axum::Json(record_to_type_info(&record))),
        None => Err(ApiError::NotFound(format!("PV {} not found", params.pv))),
    }
}

pub async fn get_pv_details(
    State(state): State<AppState>,
    Query(params): Query<PvNameParam>,
) -> Result<impl IntoResponse, ApiError> {
    let detail = crate::usecases::get_pv_details::get_pv_details(
        state.pv_query.as_ref(),
        state.archiver_query.as_ref(),
        &params.pv,
    )?;

    Ok(axum::Json(detail))
}
