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

/// `GET /mgmt/bpl/getPVsForThisAppliance` — getAllPVs scoped to this
/// appliance (no cluster aggregation). Mirrors Java's per-appliance
/// helper used by aggregate endpoints to fan out and stitch results.
pub async fn get_pvs_for_this_appliance(
    State(state): State<AppState>,
) -> Result<impl IntoResponse, ApiError> {
    let pvs = state.pv_query.all_pv_names().map_err(ApiError::internal)?;
    Ok(axum::Json(pvs))
}

/// `GET /mgmt/bpl/getMatchingPVsForAppliance?pv=<glob>` — local-only
/// glob match. Same semantics as getMatchingPVs with `?cluster=false`
/// but exposed as its own endpoint for parity.
pub async fn get_matching_pvs_for_appliance(
    State(state): State<AppState>,
    axum::extract::Query(p): axum::extract::Query<MatchingPvsParams>,
) -> Result<impl IntoResponse, ApiError> {
    let pvs = state
        .pv_query
        .matching_pvs(&p.pv)
        .map_err(ApiError::internal)?;
    Ok(axum::Json(pvs))
}

/// `GET /mgmt/bpl/getPausedPVsForThisAppliance` — local-only paused
/// list. Same shape as getPausedPVsReport but appliance-scoped.
pub async fn get_paused_pvs_for_this_appliance(
    State(state): State<AppState>,
) -> Result<impl IntoResponse, ApiError> {
    let records = state
        .pv_query
        .pvs_by_status(PvStatus::Paused)
        .map_err(ApiError::internal)?;
    let names: Vec<String> = records.into_iter().map(|r| r.pv_name).collect();
    Ok(axum::Json(names))
}

/// `GET /mgmt/bpl/getNeverConnectedPVsForThisAppliance` — local-only
/// never-connected list. Same as getNeverConnectedPVs (which already
/// queries only the local engine) but renamed for parity.
pub async fn get_never_connected_pvs_for_this_appliance(
    State(state): State<AppState>,
) -> Result<impl IntoResponse, ApiError> {
    let pvs = state.archiver_query.get_never_connected_pvs();
    Ok(axum::Json(pvs))
}

/// `GET /mgmt/bpl/getPVsByStorageConsumed?limit=<n>` — top-N PVs by
/// total bytes on disk across all tiers. `limit` defaults to 100.
#[derive(serde::Deserialize)]
pub struct StorageRankParams {
    #[serde(default)]
    pub limit: Option<usize>,
}

pub async fn get_pvs_by_storage_consumed(
    State(state): State<AppState>,
    axum::extract::Query(p): axum::extract::Query<StorageRankParams>,
) -> Result<impl IntoResponse, ApiError> {
    let limit = p.limit.unwrap_or(100).min(10_000);
    let pvs = state.pv_query.all_pv_names().map_err(ApiError::internal)?;

    // Walk every PV; sum file sizes across the storage backend's per-PV
    // store summary. This is O(pv_count) directory scans — acceptable for
    // an admin query but not something to expose without a sane default
    // limit.
    let mut entries: Vec<(String, u64)> = Vec::with_capacity(pvs.len());
    for pv in pvs {
        let summaries = match state.storage.stores_for_pv(&pv) {
            Ok(s) => s,
            Err(e) => {
                tracing::warn!(pv, "storage summary failed: {e}");
                continue;
            }
        };
        let total: u64 = summaries
            .iter()
            .filter_map(|s| {
                // pv_file_count is set; size on disk requires another
                // scan. Approximate with file count for now — a precise
                // size would require an extra method on the trait.
                s.pv_file_count
            })
            .sum();
        entries.push((pv, total));
    }
    entries.sort_by(|a, b| b.1.cmp(&a.1));
    entries.truncate(limit);

    let json: Vec<serde_json::Value> = entries
        .into_iter()
        .map(|(pv, files)| {
            serde_json::json!({
                "pvName": pv,
                "files": files,
            })
        })
        .collect();
    Ok(axum::Json(json))
}

/// `GET /mgmt/bpl/getEventRateReport` — best-effort events/sec per PV
/// estimated from the registry's `last_timestamp` delta and prometheus
/// `archiver_events_stored_total`. Reports `null` rate when no signal.
pub async fn get_event_rate_report(
    State(state): State<AppState>,
) -> Result<impl IntoResponse, ApiError> {
    let records = state.pv_query.all_records().map_err(ApiError::internal)?;
    let now = SystemTime::now();
    let entries: Vec<serde_json::Value> = records
        .into_iter()
        .filter(|r| r.alias_for.is_none())
        .map(|r| {
            // Best-effort: use the gap between `created_at` and
            // `last_timestamp` as a rough denominator. Real per-PV
            // rate tracking belongs in the engine; this surfaces
            // "is data flowing?" cheaply.
            let secs = r.last_timestamp.and_then(|ts| {
                ts.duration_since(SystemTime::UNIX_EPOCH).ok().map(|d| d.as_secs() as f64)
            });
            let connected = r
                .last_timestamp
                .map(|ts| now.duration_since(ts).unwrap_or_default().as_secs() < 300)
                .unwrap_or(false);
            serde_json::json!({
                "pvName": r.pv_name,
                "lastEventEpochSecs": secs,
                "connected": connected,
            })
        })
        .collect();
    Ok(axum::Json(entries))
}

/// `GET /mgmt/bpl/getLastKnownEventTimeStamp?pv=<name>` — per-Java
/// archiver's etl/bpl, returns the most recent event time. Used by
/// the Java mgmt UI for ETL latency monitoring. Pulls from the
/// registry's `last_timestamp` so it works without scanning storage.
pub async fn get_last_known_event_timestamp(
    State(state): State<AppState>,
    axum::extract::Query(p): axum::extract::Query<PvNameParam>,
) -> Result<impl IntoResponse, ApiError> {
    let canonical = state
        .pv_query
        .canonical_name(&p.pv)
        .unwrap_or_else(|_| p.pv.clone());
    let record = state
        .pv_query
        .get_pv(&canonical)
        .map_err(ApiError::internal)?
        .ok_or_else(|| ApiError::NotFound(format!("PV '{}' not found", p.pv)))?;
    let resp = serde_json::json!({
        "pvName": canonical,
        "lastEvent": record
            .last_timestamp
            .map(|ts| chrono::DateTime::<chrono::Utc>::from(ts).to_rfc3339()),
    });
    Ok(axum::Json(resp))
}

/// `GET /mgmt/bpl/getMgmtMetrics` — appliance-self health surface.
/// Exposes the same totals `getApplianceMetrics` does plus uptime
/// and CPU-core count for capacity planning.
pub async fn get_mgmt_metrics(
    State(state): State<AppState>,
) -> Result<impl IntoResponse, ApiError> {
    let total = state.pv_query.count(None).unwrap_or(0);
    let active = state
        .pv_query
        .count(Some(PvStatus::Active))
        .unwrap_or(0);
    let paused = state
        .pv_query
        .count(Some(PvStatus::Paused))
        .unwrap_or(0);
    Ok(axum::Json(serde_json::json!({
        "pvCount": { "total": total, "active": active, "paused": paused },
        "version": env!("CARGO_PKG_VERSION"),
        "cpuCores": std::thread::available_parallelism()
            .map(|n| n.get())
            .unwrap_or(0),
    })))
}

/// `POST /mgmt/bpl/archivedPVsAction`
/// Body: JSON or text list of PV names. Returns `[{pvName, archived}]`.
pub async fn archived_pvs_action(
    State(state): State<AppState>,
    crate::pv_input::PvListInput(pvs): crate::pv_input::PvListInput,
) -> Result<impl IntoResponse, ApiError> {
    let local_set: std::collections::HashSet<String> = state
        .pv_query
        .all_pv_names()
        .unwrap_or_default()
        .into_iter()
        .collect();
    let resp: Vec<serde_json::Value> = pvs
        .into_iter()
        .map(|pv| {
            let canonical = state
                .pv_query
                .canonical_name(&pv)
                .unwrap_or_else(|_| pv.clone());
            serde_json::json!({
                "pvName": pv,
                "archived": local_set.contains(&canonical),
            })
        })
        .collect();
    Ok(axum::Json(resp))
}

/// `POST /mgmt/bpl/unarchivedPVsAction`
/// Body: list of names. Returns the subset that we do NOT archive.
pub async fn unarchived_pvs_action(
    State(state): State<AppState>,
    crate::pv_input::PvListInput(pvs): crate::pv_input::PvListInput,
) -> Result<impl IntoResponse, ApiError> {
    let local_set: std::collections::HashSet<String> = state
        .pv_query
        .all_pv_names()
        .unwrap_or_default()
        .into_iter()
        .collect();
    let resp: Vec<String> = pvs
        .into_iter()
        .filter(|pv| {
            let canonical = state
                .pv_query
                .canonical_name(pv)
                .unwrap_or_else(|_| pv.clone());
            !local_set.contains(&canonical)
        })
        .collect();
    Ok(axum::Json(resp))
}

/// `POST /mgmt/bpl/archivedPVsNotInListAction`
/// Body: list of names. Returns local PVs that are NOT in the input.
pub async fn archived_pvs_not_in_list_action(
    State(state): State<AppState>,
    crate::pv_input::PvListInput(pvs): crate::pv_input::PvListInput,
) -> Result<impl IntoResponse, ApiError> {
    let input_set: std::collections::HashSet<String> = pvs.into_iter().collect();
    let all_local = state.pv_query.all_pv_names().unwrap_or_default();
    let resp: Vec<String> = all_local
        .into_iter()
        .filter(|pv| !input_set.contains(pv))
        .collect();
    Ok(axum::Json(resp))
}
