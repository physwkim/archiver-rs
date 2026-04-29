use axum::extract::State;
use axum::response::IntoResponse;

use archiver_core::registry::SampleMode;

use crate::dto::mgmt::*;
use crate::errors::ApiError;
use crate::AppState;

pub async fn export_config(
    State(state): State<AppState>,
) -> Result<impl IntoResponse, ApiError> {
    let records = state.pv_query.all_records().map_err(ApiError::internal)?;

    let exports: Vec<ExportRecord> = records
        .into_iter()
        .map(|r| {
            let (method, period) = match &r.sample_mode {
                SampleMode::Monitor => ("Monitor".to_string(), 0.0),
                SampleMode::Scan { period_secs } => ("Scan".to_string(), *period_secs),
            };
            let status_str = r.status.as_str().to_string();
            let archive_fields = if r.archive_fields.is_empty() {
                None
            } else {
                Some(r.archive_fields)
            };
            ExportRecord {
                pv_name: r.pv_name,
                dbr_type: r.dbr_type as i32,
                sampling_method: method,
                sampling_period: period,
                element_count: r.element_count,
                prec: r.prec,
                egu: r.egu,
                status: Some(status_str),
                created_at: Some(r.created_at.to_rfc3339()),
                alias_for: r.alias_for,
                archive_fields,
                policy_name: r.policy_name,
            }
        })
        .collect();

    Ok(axum::Json(exports))
}

pub async fn import_config(
    State(state): State<AppState>,
    body: axum::body::Bytes,
) -> Result<impl IntoResponse, ApiError> {
    let records: Vec<ExportRecord> = serde_json::from_slice(&body)
        .map_err(|e| ApiError::BadRequest(format!("Invalid JSON: {e}")))?;

    let result = crate::usecases::import_config::import_config(state.pv_cmd.as_ref(), &records)?;

    let resp = serde_json::json!({
        "imported": result.imported,
        "total": result.total,
        "errors": result.errors,
    });
    Ok(axum::Json(resp))
}
