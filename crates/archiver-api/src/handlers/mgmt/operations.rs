//! Operational BPL endpoints: appliance metrics, manual ETL/consolidation,
//! failover-cache reset stub.

use axum::extract::{Query, State};
use axum::response::IntoResponse;
use serde::Deserialize;

use crate::dto::mgmt::PvNameParam;
use crate::errors::ApiError;
use crate::AppState;

/// `GET /mgmt/bpl/getApplianceMetrics`
///
/// Returns total file counts and on-disk size per storage tier, plus the
/// total/active/paused PV counts. Java archiver exposes a similar metrics
/// surface; we return a single appliance object (cluster-wide aggregation
/// is the caller's job).
pub async fn get_appliance_metrics(
    State(state): State<AppState>,
) -> Result<impl IntoResponse, ApiError> {
    let summaries = state
        .storage
        .appliance_metrics()
        .map_err(ApiError::internal)?;

    let total = state.pv_query.count(None).unwrap_or(0);
    let active = state
        .pv_query
        .count(Some(archiver_core::registry::PvStatus::Active))
        .unwrap_or(0);
    let paused = state
        .pv_query
        .count(Some(archiver_core::registry::PvStatus::Paused))
        .unwrap_or(0);

    let stores: Vec<serde_json::Value> = summaries
        .into_iter()
        .map(|s| {
            serde_json::json!({
                "name": s.name,
                "rootFolder": s.root_folder.display().to_string(),
                "granularity": format!("{:?}", s.granularity),
                "totalFiles": s.total_files.unwrap_or(0),
                "totalSizeBytes": s.total_size_bytes.unwrap_or(0),
            })
        })
        .collect();

    let identity = state
        .cluster
        .as_ref()
        .map(|c| c.identity_name().to_string())
        .unwrap_or_else(|| "standalone".to_string());

    Ok(axum::Json(serde_json::json!({
        "appliance": identity,
        "pvCount": {
            "total": total,
            "active": active,
            "paused": paused,
        },
        "stores": stores,
    })))
}

/// `GET /mgmt/bpl/consolidateDataForPV?pv=<name>&storage=<tier>`
///
/// Forces a flush of any buffered writes for `pv`. With `storage=` set, the
/// caller intends a manual ETL of the PV's files from STS into MTS / LTS.
/// Our pipeline runs ETL automatically based on partition age, so the
/// safe operation here is: flush the cache (so reads see fresh data) and
/// report the per-tier file counts so operators can confirm migration.
#[derive(Deserialize)]
pub struct ConsolidateQuery {
    pub pv: String,
    /// Optional store name; ignored other than for echo, since the flush
    /// applies to all tiers atomically.
    #[serde(default)]
    pub storage: Option<String>,
}

pub async fn consolidate_data_for_pv(
    State(state): State<AppState>,
    Query(q): Query<ConsolidateQuery>,
) -> Result<impl IntoResponse, ApiError> {
    if q.pv.is_empty() {
        return Err(ApiError::BadRequest("pv is required".to_string()));
    }
    let canonical = state
        .pv_query
        .canonical_name(&q.pv)
        .map_err(ApiError::internal)?;

    state
        .storage
        .flush_writes()
        .await
        .map_err(ApiError::internal)?;

    let stores = state
        .storage
        .stores_for_pv(&canonical)
        .map_err(ApiError::internal)?;
    let tiers: Vec<serde_json::Value> = stores
        .into_iter()
        .map(|s| {
            serde_json::json!({
                "name": s.name,
                "files": s.pv_file_count.unwrap_or(0),
            })
        })
        .collect();

    Ok(axum::Json(serde_json::json!({
        "status": "ok",
        "pv": canonical,
        "storage": q.storage,
        "tiers": tiers,
    })))
}

/// `GET /mgmt/bpl/getStorageUsageForPV?pv=<name>`
/// Returns per-tier file count for one PV.
pub async fn get_storage_usage_for_pv(
    State(state): State<AppState>,
    Query(p): Query<PvNameParam>,
) -> Result<impl IntoResponse, ApiError> {
    if p.pv.is_empty() {
        return Err(ApiError::BadRequest("pv is required".to_string()));
    }
    let canonical = state
        .pv_query
        .canonical_name(&p.pv)
        .map_err(ApiError::internal)?;
    let stores = state
        .storage
        .stores_for_pv(&canonical)
        .map_err(ApiError::internal)?;
    let mut obj = serde_json::Map::new();
    for s in stores {
        obj.insert(
            s.name,
            serde_json::Value::Number(s.pv_file_count.unwrap_or(0).into()),
        );
    }
    Ok(axum::Json(serde_json::Value::Object(obj)))
}

/// `GET /mgmt/bpl/resetFailoverCaches`
///
/// Java exposes this for clearing the cross-archiver failover lookup cache.
/// We have no failover cache yet (Phase 7 introduces it), so this is a
/// no-op that returns `ok`. When failover lands the impl wires up to the
/// real cache.
pub async fn reset_failover_caches(
    State(_state): State<AppState>,
) -> Result<impl IntoResponse, ApiError> {
    Ok(axum::Json(serde_json::json!({
        "status": "ok",
        "cleared": 0,
    })))
}
