use axum::extract::State;
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};

use archiver_core::registry::{PvStatus, SampleMode};

use crate::dto::mgmt::*;
use crate::AppState;

pub async fn export_config(State(state): State<AppState>) -> Response {
    let records = match state.pv_repo.all_records() {
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
            let status_str = r.status.as_str().to_string();
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
            }
        })
        .collect();

    axum::Json(exports).into_response()
}

pub async fn import_config(
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
        let status = r.status.as_deref()
            .and_then(|s| s.parse().ok())
            .unwrap_or(PvStatus::Active);

        match state.pv_repo.import_pv(
            &r.pv_name,
            dbr_type,
            &sample_mode,
            r.element_count,
            status,
            r.created_at.as_deref(),
            r.prec.as_deref(),
            r.egu.as_deref(),
        ) {
            Ok(()) => imported += 1,
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
