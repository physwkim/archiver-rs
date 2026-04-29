//! Engine-side BPL endpoints — live CA fetch and engine-detailed status.
//!
//! These mirror the Java archiver's `engine/bpl/*` URL family. In Java the
//! engine is a separate process; in archiver-rs everything runs in one
//! process and the engine is reachable through the `archiver_query` trait.
//!
//! Endpoints implemented:
//!
//! - `getEngineDataAction` — POST list of PV names, returns the live
//!   value for each (one-shot CA `get` against the running channel).
//! - `getDataAtTimeEngine` — same body shape; returns each PV's live
//!   value alongside the registry's `last_timestamp` so a Phoebus client
//!   that asks the engine for "current" gets a consistent answer.
//! - `getLatestMetaDataAction` — returns the cached extra-field values
//!   (HIHI/LOLO/EGU/...) for a single PV.
//! - `pvStatusAction` — engine-detailed snapshot for a single PV:
//!   connection state, sample mode, counters, and live extras.

use std::collections::HashMap;

use axum::extract::{Query, State};
use axum::http::HeaderMap;
use axum::response::{IntoResponse, Response};
use serde::Deserialize;

use crate::dto::mgmt::PvNameParam;
use crate::errors::ApiError;
use crate::pv_input::PvListInput;
use crate::AppState;

/// Default per-PV CA timeout for the live-value endpoints, in seconds.
/// Caller may override via `?timeout=<secs>`.
const DEFAULT_LIVE_TIMEOUT_SECS: u64 = 5;

#[derive(Deserialize)]
pub struct LiveTimeoutParam {
    #[serde(default)]
    pub timeout: Option<u64>,
}

/// `POST /mgmt/bpl/getEngineDataAction[?timeout=<secs>]`
/// Body: JSON list of PV names. Returns
/// `{ pvName: { value, error?, ... } }`. Aliases resolve before the CA
/// fetch so the live read hits the canonical IOC PV.
pub async fn get_engine_data(
    State(state): State<AppState>,
    Query(p): Query<LiveTimeoutParam>,
    PvListInput(pvs): PvListInput,
) -> Response {
    let timeout = p.timeout.unwrap_or(DEFAULT_LIVE_TIMEOUT_SECS);
    let mut out = serde_json::Map::new();
    for pv in pvs {
        let canonical = state
            .pv_query
            .canonical_name(&pv)
            .unwrap_or_else(|_| pv.clone());
        let entry = match state.archiver_query.live_value(&canonical, timeout).await {
            Some(Ok(v)) => serde_json::json!({"value": v}),
            Some(Err(e)) => serde_json::json!({"error": e}),
            None => serde_json::json!({"error": "not archived on this appliance"}),
        };
        out.insert(canonical, entry);
    }
    axum::Json(serde_json::Value::Object(out)).into_response()
}

/// `POST /mgmt/bpl/getDataAtTimeEngine[?timeout=<secs>]`
/// Same body+query shape as getEngineDataAction; the response also
/// carries the registry's `lastEvent` timestamp so a Phoebus client can
/// distinguish "live snapshot" from "archived". Falls back to live CA
/// when the PV has never been written.
pub async fn get_data_at_time_engine(
    State(state): State<AppState>,
    Query(p): Query<LiveTimeoutParam>,
    PvListInput(pvs): PvListInput,
) -> Response {
    let timeout = p.timeout.unwrap_or(DEFAULT_LIVE_TIMEOUT_SECS);
    let mut out = serde_json::Map::new();
    for pv in pvs {
        let canonical = state
            .pv_query
            .canonical_name(&pv)
            .unwrap_or_else(|_| pv.clone());

        let last_event = state
            .pv_query
            .get_pv(&canonical)
            .ok()
            .flatten()
            .and_then(|r| {
                r.last_timestamp
                    .map(|ts| chrono::DateTime::<chrono::Utc>::from(ts).to_rfc3339())
            });

        let live = state.archiver_query.live_value(&canonical, timeout).await;
        let (value, error) = match live {
            Some(Ok(v)) => (Some(v), None),
            Some(Err(e)) => (None, Some(e)),
            None => (None, Some("not archived on this appliance".to_string())),
        };
        let mut entry = serde_json::Map::new();
        entry.insert("lastEvent".into(), serde_json::to_value(last_event).unwrap());
        if let Some(v) = value {
            entry.insert("value".into(), v);
        }
        if let Some(e) = error {
            entry.insert("error".into(), serde_json::Value::String(e));
        }
        out.insert(canonical, serde_json::Value::Object(entry));
    }
    axum::Json(serde_json::Value::Object(out)).into_response()
}

/// `GET /mgmt/bpl/getLatestMetaDataAction?pv=<name>`
/// Returns the cached extra-field values for the PV's archive_fields
/// list. Empty object when the PV has no metadata fields configured
/// or hasn't received any field updates yet.
pub async fn get_latest_meta_data(
    State(state): State<AppState>,
    headers: HeaderMap,
    Query(p): Query<PvNameParam>,
) -> Response {
    if p.pv.is_empty() {
        return ApiError::BadRequest("pv is required".to_string()).into_response();
    }
    let canonical = super::resolve_canonical(&state, &p.pv);
    let qs = format!("pv={}", urlencoding::encode(&canonical));
    if let Some(resp) =
        super::try_mgmt_dispatch(&state, &canonical, "getLatestMetaDataAction", &qs, &headers).await
    {
        return resp;
    }
    let extras = state.archiver_query.extras_snapshot(&canonical);
    let map: serde_json::Map<String, serde_json::Value> = extras
        .into_iter()
        .map(|(k, v)| (k, serde_json::Value::String(v)))
        .collect();
    axum::Json(serde_json::Value::Object(map)).into_response()
}

/// `GET /mgmt/bpl/pvStatusAction?pv=<name>`
/// Engine-detailed status: connection info + counters + extras
/// snapshot in one response. Heavier than `/getPVStatus` (which only
/// reads the registry) — used by the management UI's per-PV detail
/// panel.
pub async fn pv_status_action(
    State(state): State<AppState>,
    headers: HeaderMap,
    Query(p): Query<PvNameParam>,
) -> Response {
    if p.pv.is_empty() {
        return ApiError::BadRequest("pv is required".to_string()).into_response();
    }
    let canonical = super::resolve_canonical(&state, &p.pv);
    let qs = format!("pv={}", urlencoding::encode(&canonical));
    if let Some(resp) =
        super::try_mgmt_dispatch(&state, &canonical, "pvStatusAction", &qs, &headers).await
    {
        return resp;
    }

    let record = match state.pv_query.get_pv(&canonical) {
        Ok(Some(r)) => r,
        Ok(None) => return ApiError::NotFound(format!("PV '{}' not found", p.pv)).into_response(),
        Err(e) => return ApiError::internal(e).into_response(),
    };

    let conn = state.archiver_query.get_connection_info(&canonical);
    let counters: HashMap<String, _> = state
        .archiver_query
        .all_pv_counters()
        .into_iter()
        .filter(|(name, _)| name == &canonical)
        .collect();
    let counter_json = counters.get(&canonical).map(|c| {
        serde_json::json!({
            "eventsReceived": c.events_received,
            "eventsStored": c.events_stored,
            "firstEventEpochSecs": c.first_event_unix_secs,
            "bufferOverflowDrops": c.buffer_overflow_drops,
            "timestampDrops": c.timestamp_drops,
            "typeChangeDrops": c.type_change_drops,
            "disconnectCount": c.disconnect_count,
            "lastDisconnectEpochSecs": c.last_disconnect_unix_secs,
        })
    });
    let extras = state.archiver_query.extras_snapshot(&canonical);

    axum::Json(serde_json::json!({
        "pvName": canonical,
        "status": format!("{:?}", record.status),
        "dbrType": record.dbr_type as i32,
        "elementCount": record.element_count,
        "lastEvent": record
            .last_timestamp
            .map(|ts| chrono::DateTime::<chrono::Utc>::from(ts).to_rfc3339()),
        "connection": conn.map(|c| serde_json::json!({
            "isConnected": c.is_connected,
            "connectedSinceEpochSecs": c.connected_since
                .and_then(|t| t.duration_since(std::time::UNIX_EPOCH).ok())
                .map(|d| d.as_secs()),
            "lastEventEpochSecs": c.last_event_time
                .and_then(|t| t.duration_since(std::time::UNIX_EPOCH).ok())
                .map(|d| d.as_secs()),
        })),
        "counters": counter_json,
        "extras": extras,
    }))
    .into_response()
}
