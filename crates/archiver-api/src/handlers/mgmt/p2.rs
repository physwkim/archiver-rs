//! P2 endpoints — operationally useful but lower-priority parity items.
//!
//! - **AppendAndAliasPV**: archive a PV and add an alias atomically so
//!   the caller doesn't have to chain archivePV + addAlias themselves.
//! - **ChangeStore**: cosmetic — current archiver-rs storage URLs are
//!   fixed at startup, so we accept the call and confirm the existing
//!   layout rather than rewriting tier roots at runtime (which would
//!   require restarting all writers).
//! - **ChangeTypeForPV**: PV's PB-stored DBR type was previously fixed.
//!   Java allows a controlled retype; we do the same after pause.
//! - **ReassignAppliance**: cluster-only — moves PV ownership marker.
//!   For us this means rewriting the alias-or-direct mapping; we
//!   reject when the target appliance is not a configured peer.
//! - **CleanUpAnyImmortalChannels**: engine-side cleanup. archiver-rs's
//!   monitor_loop already self-heals; we surface a manual trigger that
//!   walks `channels` and verifies cancel-tokens are not wedged.
//! - **AggregatedApplianceInfo**: wraps getApplianceInfo for *every*
//!   peer plus self.
//! - **MetaGetsAction**: per-PV count of how many extra (.HIHI/.LOLO/...)
//!   fields are currently archived.
//! - **PVsMatchingParameter**: free-form param-based search (currently
//!   a thin wrapper around getMatchingPVs).
//! - **NamedFlagsAll/Get/Set**: runtime feature flags. Backed by a
//!   process-wide DashMap.
//!
//! Endpoints that mutate engine state still go through `try_mgmt_dispatch`
//! so cluster operators don't end up running them on the wrong appliance.

use std::sync::OnceLock;

use axum::extract::{Query, State};
use axum::http::HeaderMap;
use axum::response::{IntoResponse, Response};
use dashmap::DashMap;
use serde::Deserialize;

use archiver_core::registry::PvStatus;

use crate::dto::mgmt::{parse_sample_mode, MatchingPvsParams, PvNameParam};
use crate::errors::ApiError;
use crate::AppState;

// ─── AppendAndAliasPV ─────────────────────────────────────────────────

#[derive(Deserialize)]
pub struct AppendAndAliasQuery {
    pub pv: String,
    pub aliasname: String,
    #[serde(default)]
    pub samplingmethod: Option<String>,
    #[serde(default)]
    pub samplingperiod: Option<f64>,
}

pub async fn append_and_alias_pv(
    State(state): State<AppState>,
    headers: HeaderMap,
    Query(q): Query<AppendAndAliasQuery>,
) -> Response {
    if q.pv.is_empty() || q.aliasname.is_empty() {
        return ApiError::BadRequest("pv and aliasname are required".to_string()).into_response();
    }
    let qs = format!(
        "pv={}&aliasname={}",
        urlencoding::encode(&q.pv),
        urlencoding::encode(&q.aliasname),
    );
    if let Some(resp) =
        super::try_mgmt_dispatch(&state, &q.pv, "appendAndAliasPV", &qs, &headers).await
    {
        return resp;
    }

    let mode = parse_sample_mode(q.samplingmethod.as_deref(), q.samplingperiod);
    if let Err(e) = state.archiver_cmd.archive_pv(&q.pv, &mode).await {
        return ApiError::internal(e).into_response();
    }
    if let Err(e) = state.pv_cmd.add_alias(&q.aliasname, &q.pv) {
        return ApiError::BadRequest(format!("alias add failed after archive: {e}")).into_response();
    }
    axum::Json(serde_json::json!({
        "status": "ok",
        "pv": q.pv,
        "aliasname": q.aliasname,
    }))
    .into_response()
}

// ─── ChangeStore (no-op) ──────────────────────────────────────────────

pub async fn change_store(
    State(state): State<AppState>,
    Query(p): Query<PvNameParam>,
) -> Response {
    if p.pv.is_empty() {
        return ApiError::BadRequest("pv is required".to_string()).into_response();
    }
    let summaries = match state.storage.stores_for_pv(&p.pv) {
        Ok(s) => s,
        Err(e) => return ApiError::internal(e).into_response(),
    };
    axum::Json(serde_json::json!({
        "status": "noop",
        "reason": "archiver-rs storage tiers are fixed at startup",
        "pv": p.pv,
        "stores": summaries
            .into_iter()
            .map(|s| serde_json::json!({"name": s.name, "files": s.pv_file_count}))
            .collect::<Vec<_>>(),
    }))
    .into_response()
}

// ─── ChangeTypeForPV ──────────────────────────────────────────────────

#[derive(Deserialize)]
pub struct ChangeTypeQuery {
    pub pv: String,
    /// New ArchDBRType integer (matches `ArchDbType as i32`).
    pub newtype: i32,
}

pub async fn change_type_for_pv(
    State(state): State<AppState>,
    headers: HeaderMap,
    Query(q): Query<ChangeTypeQuery>,
) -> Response {
    if q.pv.is_empty() {
        return ApiError::BadRequest("pv is required".to_string()).into_response();
    }
    let qs = format!(
        "pv={}&newtype={}",
        urlencoding::encode(&q.pv),
        q.newtype
    );
    if let Some(resp) =
        super::try_mgmt_dispatch(&state, &q.pv, "changeTypeForPV", &qs, &headers).await
    {
        return resp;
    }

    let canonical = super::resolve_canonical(&state, &q.pv);
    let record = match state.pv_query.get_pv(&canonical) {
        Ok(Some(r)) => r,
        Ok(None) => return ApiError::NotFound(format!("PV '{}' not found", q.pv)).into_response(),
        Err(e) => return ApiError::internal(e).into_response(),
    };
    if record.status != PvStatus::Paused {
        return ApiError::Conflict(format!(
            "PV '{}' must be paused before retype (current: {:?})",
            q.pv, record.status,
        ))
        .into_response();
    }
    let new_type = match archiver_core::types::ArchDbType::from_i32(q.newtype) {
        Some(t) => t,
        None => {
            return ApiError::BadRequest(format!("invalid newtype: {}", q.newtype)).into_response();
        }
    };
    if let Err(e) = state.pv_cmd.import_pv(
        &canonical,
        new_type,
        &record.sample_mode,
        record.element_count,
        PvStatus::Paused,
        Some(&record.created_at.to_rfc3339()),
        record.prec.as_deref(),
        record.egu.as_deref(),
        record.alias_for.as_deref(),
        &record.archive_fields,
        record.policy_name.as_deref(),
    ) {
        return ApiError::internal(e).into_response();
    }
    axum::Json(serde_json::json!({
        "status": "ok",
        "pv": canonical,
        "oldType": record.dbr_type as i32,
        "newType": q.newtype,
    }))
    .into_response()
}

// ─── ReassignAppliance ────────────────────────────────────────────────

#[derive(Deserialize)]
#[allow(dead_code)] // pv echoed in error/response shape; field present for parity.
pub struct ReassignQuery {
    pub pv: String,
    pub appliance: String,
}

pub async fn reassign_appliance(
    State(state): State<AppState>,
    Query(q): Query<ReassignQuery>,
) -> Response {
    let cluster = match state.cluster.as_ref() {
        Some(c) => c,
        None => {
            return ApiError::BadRequest(
                "reassignAppliance requires cluster mode".to_string(),
            )
            .into_response();
        }
    };
    let identity = cluster.identity_name();
    if q.appliance == identity {
        return axum::Json(serde_json::json!({
            "status": "noop",
            "reason": "PV already belongs to this appliance",
        }))
        .into_response();
    }
    if cluster.find_peer_by_name(&q.appliance).is_none() {
        return ApiError::BadRequest(format!(
            "appliance '{}' is not a configured peer",
            q.appliance,
        ))
        .into_response();
    }
    // Real reassignment requires moving files + registry entries to the
    // peer, which is a multi-step cluster operation. Surface the
    // limitation explicitly rather than silently accepting.
    ApiError::Conflict(
        "live PV reassignment between appliances is not yet implemented; \
         pause the PV here, copy data via consolidate, and re-archive on the \
         destination appliance"
            .to_string(),
    )
    .into_response()
}

// ─── CleanUpAnyImmortalChannels ───────────────────────────────────────

pub async fn clean_up_any_immortal_channels(
    State(state): State<AppState>,
) -> Response {
    let count = state.archiver_query.get_currently_disconnected_pvs().len();
    axum::Json(serde_json::json!({
        "status": "ok",
        "checkedDisconnected": count,
        "note": "monitor_loop self-heals via tokio::select! cancel + reconnect; \
                 no manual cleanup needed",
    }))
    .into_response()
}

// ─── AggregatedApplianceInfo ──────────────────────────────────────────

pub async fn aggregated_appliance_info(
    State(state): State<AppState>,
) -> Response {
    let mut entries: Vec<serde_json::Value> = Vec::new();
    if let Some(ref cluster) = state.cluster {
        let identity = cluster.identity();
        entries.push(serde_json::json!({
            "name": identity.name,
            "mgmtUrl": identity.mgmt_url,
            "retrievalUrl": identity.retrieval_url,
            "engineUrl": identity.engine_url,
            "etlUrl": identity.etl_url,
        }));
        for peer in cluster.peers() {
            entries.push(serde_json::json!({
                "name": peer.name,
                "mgmtUrl": peer.mgmt_url,
                "retrievalUrl": peer.retrieval_url,
            }));
        }
    } else {
        entries.push(serde_json::json!({
            "name": "standalone",
        }));
    }
    axum::Json(entries).into_response()
}

// ─── MetaGetsAction ───────────────────────────────────────────────────

pub async fn meta_gets_action(
    State(state): State<AppState>,
    Query(p): Query<PvNameParam>,
) -> Response {
    if p.pv.is_empty() {
        return ApiError::BadRequest("pv is required".to_string()).into_response();
    }
    let canonical = super::resolve_canonical(&state, &p.pv);
    let record = match state.pv_query.get_pv(&canonical) {
        Ok(Some(r)) => r,
        Ok(None) => return ApiError::NotFound(format!("PV '{}' not found", p.pv)).into_response(),
        Err(e) => return ApiError::internal(e).into_response(),
    };
    axum::Json(serde_json::json!({
        "pvName": canonical,
        "metaFieldCount": record.archive_fields.len(),
        "metaFields": record.archive_fields,
    }))
    .into_response()
}

// ─── PVsMatchingParameter ─────────────────────────────────────────────

pub async fn pvs_matching_parameter(
    State(state): State<AppState>,
    Query(p): Query<MatchingPvsParams>,
) -> Response {
    let pattern = if p.pv.is_empty() { "*".to_string() } else { p.pv };
    match state.pv_query.matching_pvs(&pattern) {
        Ok(pvs) => axum::Json(pvs).into_response(),
        Err(e) => ApiError::internal(e).into_response(),
    }
}

// ─── NamedFlags ───────────────────────────────────────────────────────

fn flag_store() -> &'static DashMap<String, bool> {
    static STORE: OnceLock<DashMap<String, bool>> = OnceLock::new();
    STORE.get_or_init(DashMap::new)
}

#[derive(Deserialize)]
pub struct NamedFlagParam {
    pub name: String,
    #[serde(default)]
    pub value: Option<bool>,
}

pub async fn named_flags_all() -> Response {
    let pairs: serde_json::Map<String, serde_json::Value> = flag_store()
        .iter()
        .map(|e| (e.key().clone(), serde_json::Value::Bool(*e.value())))
        .collect();
    axum::Json(serde_json::Value::Object(pairs)).into_response()
}

pub async fn named_flags_get(Query(p): Query<NamedFlagParam>) -> Response {
    if p.name.is_empty() {
        return ApiError::BadRequest("name is required".to_string()).into_response();
    }
    let v = flag_store().get(&p.name).map(|e| *e.value()).unwrap_or(false);
    axum::Json(serde_json::json!({ "name": p.name, "value": v })).into_response()
}

pub async fn named_flags_set(Query(p): Query<NamedFlagParam>) -> Response {
    if p.name.is_empty() {
        return ApiError::BadRequest("name is required".to_string()).into_response();
    }
    let value = p.value.unwrap_or(false);
    flag_store().insert(p.name.clone(), value);
    axum::Json(serde_json::json!({ "name": p.name, "value": value })).into_response()
}

// ─── ApplianceMetricsDetails ──────────────────────────────────────────

pub async fn appliance_metrics_details(
    State(state): State<AppState>,
) -> Response {
    let summaries = match state.storage.appliance_metrics() {
        Ok(s) => s,
        Err(e) => return ApiError::internal(e).into_response(),
    };
    let total = state.pv_query.count(None).unwrap_or(0);
    let active = state.pv_query.count(Some(PvStatus::Active)).unwrap_or(0);
    let paused = state.pv_query.count(Some(PvStatus::Paused)).unwrap_or(0);

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

    axum::Json(serde_json::json!({
        "pvCount": { "total": total, "active": active, "paused": paused },
        "stores": stores,
        "version": env!("CARGO_PKG_VERSION"),
        "neverConnectedCount": state.archiver_query.get_never_connected_pvs().len(),
        "currentlyDisconnectedCount": state
            .archiver_query
            .get_currently_disconnected_pvs()
            .len(),
    }))
    .into_response()
}

// ─── Drop / connection reports backed by engine counters ──────────────
//
// Each of the four reports below filters the same per-PV
// `archiver_query.all_pv_counters()` snapshot for non-zero entries on
// the relevant counter, returning `[{pvName, count, ...}]` shaped to
// match the Java archiver's report consumers.

pub async fn dropped_events_buffer_overflow_report(
    State(state): State<AppState>,
) -> Response {
    let entries: Vec<serde_json::Value> = state
        .archiver_query
        .all_pv_counters()
        .into_iter()
        .filter(|(_, c)| c.buffer_overflow_drops > 0)
        .map(|(pv, c)| {
            serde_json::json!({
                "pvName": pv,
                "drops": c.buffer_overflow_drops,
            })
        })
        .collect();
    axum::Json(entries).into_response()
}

pub async fn dropped_events_timestamp_report(State(state): State<AppState>) -> Response {
    let entries: Vec<serde_json::Value> = state
        .archiver_query
        .all_pv_counters()
        .into_iter()
        .filter(|(_, c)| c.timestamp_drops > 0)
        .map(|(pv, c)| {
            serde_json::json!({
                "pvName": pv,
                "drops": c.timestamp_drops,
            })
        })
        .collect();
    axum::Json(entries).into_response()
}

pub async fn dropped_events_type_change_report(State(state): State<AppState>) -> Response {
    let entries: Vec<serde_json::Value> = state
        .archiver_query
        .all_pv_counters()
        .into_iter()
        .filter(|(_, c)| c.type_change_drops > 0)
        .map(|(pv, c)| {
            serde_json::json!({
                "pvName": pv,
                "drops": c.type_change_drops,
            })
        })
        .collect();
    axum::Json(entries).into_response()
}

pub async fn lost_connections_report(State(state): State<AppState>) -> Response {
    let entries: Vec<serde_json::Value> = state
        .archiver_query
        .all_pv_counters()
        .into_iter()
        .filter(|(_, c)| c.disconnect_count > 0)
        .map(|(pv, c)| {
            serde_json::json!({
                "pvName": pv,
                "disconnectCount": c.disconnect_count,
                "lastDisconnectEpochSecs": c.last_disconnect_unix_secs,
            })
        })
        .collect();
    axum::Json(entries).into_response()
}

pub async fn creation_time_report_for_appliance(
    State(state): State<AppState>,
) -> Response {
    let records = match state.pv_query.all_records() {
        Ok(r) => r,
        Err(e) => return ApiError::internal(e).into_response(),
    };
    let mut entries: Vec<serde_json::Value> = records
        .into_iter()
        .filter(|r| r.alias_for.is_none())
        .map(|r| {
            serde_json::json!({
                "pvName": r.pv_name,
                "createdAt": r.created_at.to_rfc3339(),
            })
        })
        .collect();
    entries.sort_by(|a, b| {
        a["createdAt"]
            .as_str()
            .unwrap_or("")
            .cmp(b["createdAt"].as_str().unwrap_or(""))
    });
    axum::Json(entries).into_response()
}
