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

/// `GET /mgmt/bpl/consolidateDataForPV?pv=<name>&storage=<TIER>`
///
/// Force-moves all of `pv`'s files toward the requested tier, bypassing the
/// hold/gather rules the periodic ETL respects. `storage=MTS` runs only the
/// STS→MTS executor; `storage=LTS` runs STS→MTS then MTS→LTS so files end
/// up in long-term storage.
///
/// Returns the number of files moved per executor and the resulting
/// per-tier file counts so the caller can verify the migration landed.
#[derive(Deserialize)]
pub struct ConsolidateQuery {
    pub pv: String,
    /// Destination tier name. Case-insensitive. Accepts "MTS" or "LTS".
    /// Empty / missing defaults to "LTS" (the most common operator intent).
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

    // Resolve the requested target tier. Default to the deepest tier (LTS)
    // since that's the typical "consolidate everything" intent.
    let target = q
        .storage
        .as_deref()
        .map(|s| s.trim().to_ascii_uppercase())
        .filter(|s| !s.is_empty())
        .unwrap_or_else(|| "LTS".to_string());

    // Walk the etl_chain in order, running each executor up to (and including)
    // the one whose dest matches the requested tier. The chain is
    // [STS→MTS, MTS→LTS]; consolidating to MTS runs only the first, to LTS
    // runs both.
    let mut moved: Vec<serde_json::Value> = Vec::new();
    let mut found_target = false;
    for executor in &state.etl_chain {
        let dest_name = executor.dest_name().to_ascii_uppercase();
        let count = executor
            .consolidate_pv(&canonical)
            .await
            .map_err(ApiError::internal)?;
        moved.push(serde_json::json!({
            "source": executor.source_name(),
            "dest": executor.dest_name(),
            "files_moved": count,
        }));
        if dest_name == target {
            found_target = true;
            break;
        }
    }
    if !found_target && !state.etl_chain.is_empty() {
        return Err(ApiError::BadRequest(format!(
            "unknown target tier '{target}'; expected one of: {}",
            state
                .etl_chain
                .iter()
                .map(|e| e.dest_name())
                .collect::<Vec<_>>()
                .join(", ")
        )));
    }

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
        "storage": target,
        "moved": moved,
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
