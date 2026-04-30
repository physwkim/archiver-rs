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

use crate::AppState;
use crate::dto::mgmt::PvNameParam;
use crate::errors::ApiError;
use crate::pv_input::PvListInput;

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
    headers: HeaderMap,
    Query(p): Query<LiveTimeoutParam>,
    PvListInput(pvs): PvListInput,
) -> Response {
    let timeout = p.timeout.unwrap_or(DEFAULT_LIVE_TIMEOUT_SECS);
    let qs = p
        .timeout
        .map(|t| format!("timeout={t}"))
        .unwrap_or_default();
    let (local_pvs, remote_results) =
        dispatch_engine_pvs(&state, &headers, "getEngineDataAction", &qs, pvs).await;
    let mut out = serde_json::Map::new();
    for (k, v) in remote_results {
        out.insert(k, v);
    }
    for pv in local_pvs {
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

/// Java parity (26124b6): split a multi-PV engine BPL request into local
/// vs per-peer batches, forward each remote batch, and return a tuple of
/// `(local_pvs_to_handle_here, merged_remote_response_map)`. The
/// `X-Archiver-Proxied` guard prevents loops.
async fn dispatch_engine_pvs(
    state: &AppState,
    headers: &HeaderMap,
    endpoint: &str,
    qs: &str,
    pvs: Vec<String>,
) -> (Vec<String>, Vec<(String, serde_json::Value)>) {
    let is_proxied = headers.get("X-Archiver-Proxied").is_some();
    let mut remote_results: Vec<(String, serde_json::Value)> = Vec::new();
    if is_proxied || state.cluster.is_none() {
        return (pvs, remote_results);
    }
    let cluster = state.cluster.as_ref().unwrap().clone();
    // Pre-fetch local registry state once (Java parity 26124b6 + perf):
    // 2 queries instead of 2N per-PV sqlite hits, same pattern as
    // `route_pvs` in mgmt/mod.rs.
    let local_set: std::collections::HashSet<String> = state
        .pv_query
        .all_pv_names()
        .unwrap_or_default()
        .into_iter()
        .collect();
    let alias_map: std::collections::HashMap<String, String> = state
        .pv_query
        .all_aliases()
        .unwrap_or_default()
        .into_iter()
        .collect();
    let mut local_pvs: Vec<String> = Vec::new();
    let mut by_peer: std::collections::HashMap<String, Vec<String>> =
        std::collections::HashMap::new();
    for pv in pvs {
        let canonical = alias_map.get(&pv).cloned().unwrap_or_else(|| pv.clone());
        if local_set.contains(&canonical) {
            local_pvs.push(pv);
            continue;
        }
        match cluster.resolve_peer(&canonical).await {
            Some(resolved) => by_peer.entry(resolved.mgmt_url).or_default().push(pv),
            None => local_pvs.push(pv),
        }
    }
    let endpoint_with_qs = if qs.is_empty() {
        endpoint.to_string()
    } else {
        format!("{endpoint}?{qs}")
    };
    for (peer_mgmt_url, batch) in by_peer {
        let body = serde_json::to_vec(&batch).unwrap_or_default();
        match cluster
            .proxy_mgmt_post(&peer_mgmt_url, &endpoint_with_qs, body.into())
            .await
        {
            Ok(resp) => {
                let (_, body) = resp.into_parts();
                // 64 MB cap on peer responses — engine BPL replies are
                // bounded by `pvs.len() * O(KB)` in practice (~1 MB for
                // a 1000-PV batch). The bound prevents a buggy or
                // malicious peer from streaming an unbounded body and
                // OOM-ing this appliance.
                const PEER_BODY_MAX: usize = 64 * 1024 * 1024;
                let bytes = match axum::body::to_bytes(body, PEER_BODY_MAX).await {
                    Ok(b) => b,
                    Err(e) => {
                        tracing::warn!(peer = peer_mgmt_url, "engine peer body read failed: {e}");
                        for pv in &batch {
                            remote_results.push((
                                pv.clone(),
                                serde_json::json!({"error": "peer body read failed"}),
                            ));
                        }
                        continue;
                    }
                };
                match serde_json::from_slice::<serde_json::Value>(&bytes) {
                    Ok(serde_json::Value::Object(map)) => {
                        for (k, v) in map {
                            remote_results.push((k, v));
                        }
                    }
                    _ => {
                        for pv in &batch {
                            remote_results.push((
                                pv.clone(),
                                serde_json::json!({"error": "peer returned non-object"}),
                            ));
                        }
                    }
                }
            }
            Err(e) => {
                tracing::warn!(peer = peer_mgmt_url, "engine peer dispatch failed: {e}");
                for pv in &batch {
                    remote_results.push((
                        pv.clone(),
                        serde_json::json!({"error": format!("peer unreachable: {e}")}),
                    ));
                }
            }
        }
    }
    (local_pvs, remote_results)
}

/// `POST /mgmt/bpl/getDataAtTimeEngine[?timeout=<secs>]`
/// Same body+query shape as getEngineDataAction; the response also
/// carries the registry's `lastEvent` timestamp so a Phoebus client can
/// distinguish "live snapshot" from "archived". Falls back to live CA
/// when the PV has never been written.
pub async fn get_data_at_time_engine(
    State(state): State<AppState>,
    headers: HeaderMap,
    Query(p): Query<LiveTimeoutParam>,
    PvListInput(pvs): PvListInput,
) -> Response {
    let timeout = p.timeout.unwrap_or(DEFAULT_LIVE_TIMEOUT_SECS);
    let qs = p
        .timeout
        .map(|t| format!("timeout={t}"))
        .unwrap_or_default();
    let (local_pvs, remote_results) =
        dispatch_engine_pvs(&state, &headers, "getDataAtTimeEngine", &qs, pvs).await;
    let mut out = serde_json::Map::new();
    for (k, v) in remote_results {
        out.insert(k, v);
    }
    for pv in local_pvs {
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
        entry.insert(
            "lastEvent".into(),
            serde_json::to_value(last_event).unwrap_or(serde_json::Value::Null),
        );
        if let Some(v) = value {
            // Java parity (36e06e6): use the same "val" key as the
            // retrieval-side getDataAtTime so clients can parse both
            // responses with one schema.
            entry.insert("val".into(), v);
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
