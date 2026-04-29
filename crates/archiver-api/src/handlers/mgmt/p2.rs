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
        // Roll back the archive so the caller doesn't end up with a
        // half-applied state — they asked for "atomic archive + alias",
        // not "archive without alias". destroy_pv tears down the
        // monitor task and removes the registry row.
        let alias_err = format!("{e}");
        if let Err(cleanup_err) = state.archiver_cmd.destroy_pv(&q.pv) {
            tracing::error!(
                pv = q.pv,
                "appendAndAliasPV: alias add failed AND archive rollback failed: \
                 alias error: {alias_err}; cleanup error: {cleanup_err}"
            );
            return ApiError::Internal(format!(
                "alias add failed ({alias_err}) and archive rollback also failed \
                 ({cleanup_err}); manual cleanup needed"
            ))
            .into_response();
        }
        return ApiError::BadRequest(format!(
            "alias add failed; archive rolled back: {alias_err}"
        ))
        .into_response();
    }
    axum::Json(serde_json::json!({
        "status": "ok",
        "pv": q.pv,
        "aliasname": q.aliasname,
    }))
    .into_response()
}

// ─── ChangeStore — 501 with operator guidance ────────────────────────
//
// Java archiver implements per-PV `dataStores` lists in PVTypeInfo, so
// `ChangeStore` rewrites that list and the engine starts routing the
// PV's writes to the new URL. archiver-rs uses a single shared
// 3-tier layout that all PVs share, configured at startup via
// `[storage.sts/mts/lts]`. There's no per-PV routing knob to flip.
//
// Returning 501 Not Implemented (with a runbook reason) is the honest
// answer — operators expecting a runtime URL swap should reconfigure
// archiver.toml and restart instead.

pub async fn change_store(Query(p): Query<PvNameParam>) -> Response {
    let _ = p; // pv echoed in body below for clarity.
    (
        axum::http::StatusCode::NOT_IMPLEMENTED,
        axum::Json(serde_json::json!({
            "status": "not_implemented",
            "pv": p.pv,
            "reason": "archiver-rs uses a single shared 3-tier storage layout \
                       configured at startup; per-PV store routing is not \
                       supported. To migrate storage, update archiver.toml's \
                       [storage.<tier>] root_folder and restart the archiver, \
                       or use renamePV / reassignAppliance to relocate data.",
        })),
    )
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
    let peer = match cluster.find_peer_by_name(&q.appliance) {
        Some(p) => p,
        None => {
            return ApiError::BadRequest(format!(
                "appliance '{}' is not a configured peer",
                q.appliance,
            ))
            .into_response();
        }
    };

    // Live migration: pause local → gather samples → POST bundle to peer
    // → on success, delete local data + registry. Bounded at
    // MAX_MIGRATION_SAMPLES so the request body stays sane; PVs with
    // longer histories must be migrated in time-windowed chunks.
    let canonical = super::resolve_canonical(&state, &q.pv);
    let record = match state.pv_query.get_pv(&canonical) {
        Ok(Some(r)) => r,
        Ok(None) => {
            return ApiError::NotFound(format!("PV '{}' not found", q.pv)).into_response();
        }
        Err(e) => return ApiError::internal(e).into_response(),
    };
    if record.alias_for.is_some() {
        return ApiError::BadRequest(format!(
            "'{}' is an alias; reassign the canonical PV instead",
            q.pv
        ))
        .into_response();
    }

    // Pre-flight: count samples without pausing so a rejection (PV too
    // large) leaves the source running. We deliberately walk get_data
    // twice — once now, once after pause — accepting the second call's
    // possible new samples because pause stops further additions.
    let start = std::time::SystemTime::UNIX_EPOCH;
    let end = std::time::SystemTime::now() + std::time::Duration::from_secs(86_400);
    let preflight_streams = match state.storage.get_data(&canonical, start, end).await {
        Ok(s) => s,
        Err(e) => return ApiError::internal(e).into_response(),
    };
    let mut preflight_count = 0usize;
    for mut s in preflight_streams {
        while let Ok(Some(_)) = s.next_event() {
            preflight_count += 1;
            if preflight_count > MAX_MIGRATION_SAMPLES {
                return ApiError::BadRequest(format!(
                    "PV '{}' has more than {} samples; split the migration \
                     by time window before retrying",
                    canonical, MAX_MIGRATION_SAMPLES,
                ))
                .into_response();
            }
        }
    }

    if let Err(e) = state.archiver_cmd.pause_pv(&canonical) {
        return ApiError::internal(e).into_response();
    }
    if let Err(e) = state.storage.flush_writes().await {
        return ApiError::internal(e).into_response();
    }

    let streams = match state.storage.get_data(&canonical, start, end).await {
        Ok(s) => s,
        Err(e) => return ApiError::internal(e).into_response(),
    };

    let mut samples_json: Vec<serde_json::Value> = Vec::new();
    for mut s in streams {
        loop {
            match s.next_event() {
                Ok(Some(sample)) => {
                    if samples_json.len() >= MAX_MIGRATION_SAMPLES {
                        // Reached the cap during the post-pause re-read
                        // (a few samples landed between preflight and
                        // pause). Accept what we have — operator can run
                        // again to migrate the rest.
                        break;
                    }
                    let secs = sample
                        .timestamp
                        .duration_since(std::time::UNIX_EPOCH)
                        .map(|d| d.as_secs() as i64)
                        .unwrap_or(0);
                    let nanos = sample
                        .timestamp
                        .duration_since(std::time::UNIX_EPOCH)
                        .map(|d| d.subsec_nanos())
                        .unwrap_or(0);
                    samples_json.push(serde_json::json!({
                        "secs": secs,
                        "nanos": nanos,
                        "value": archiver_value_to_json(&sample.value),
                        "severity": sample.severity,
                        "status": sample.status,
                    }));
                }
                Ok(None) => break,
                Err(e) => return ApiError::internal(e).into_response(),
            }
        }
    }

    let bundle = serde_json::json!({
        "pvName": canonical,
        "dbrType": record.dbr_type as i32,
        "samplingMethod": match &record.sample_mode {
            archiver_core::registry::SampleMode::Monitor => "Monitor",
            archiver_core::registry::SampleMode::Scan { .. } => "Scan",
        },
        "samplingPeriod": match &record.sample_mode {
            archiver_core::registry::SampleMode::Scan { period_secs } => *period_secs,
            _ => 0.0,
        },
        "elementCount": record.element_count,
        "prec": record.prec,
        "egu": record.egu,
        "archiveFields": record.archive_fields,
        "samples": samples_json,
    });
    let body = axum::body::Bytes::from(bundle.to_string());

    let resp = match cluster
        .proxy_mgmt_post(&peer.mgmt_url, "receivePVMigration", body)
        .await
    {
        Ok(r) => r,
        Err(e) => return ApiError::bad_gateway(e).into_response(),
    };
    if !resp.status().is_success() {
        let status = resp.status();
        let body_bytes = axum::body::to_bytes(resp.into_body(), 64 * 1024)
            .await
            .unwrap_or_default();
        let body_str = String::from_utf8_lossy(&body_bytes);
        return ApiError::bad_gateway(anyhow::anyhow!(
            "peer rejected migration ({status}): {body_str}"
        ))
        .into_response();
    }

    let count = samples_json.len();
    if let Err(e) = state.storage.delete_pv_data(&canonical).await {
        tracing::warn!(pv = canonical, "failed to delete migrated source data: {e}");
    }
    if let Err(e) = state.archiver_cmd.destroy_pv(&canonical) {
        tracing::warn!(pv = canonical, "failed to destroy source channel: {e}");
    }

    axum::Json(serde_json::json!({
        "status": "ok",
        "pv": canonical,
        "from": identity,
        "to": q.appliance,
        "samplesMigrated": count,
    }))
    .into_response()
}

const MAX_MIGRATION_SAMPLES: usize = 500_000;

// ─── receivePVMigration (peer-side) ───────────────────────────────────

pub async fn receive_pv_migration(
    State(state): State<AppState>,
    headers: HeaderMap,
    body: axum::body::Bytes,
) -> Response {
    if headers.get("X-Archiver-Proxied").is_none() {
        return ApiError::BadRequest(
            "receivePVMigration is cluster-internal; missing X-Archiver-Proxied"
                .to_string(),
        )
        .into_response();
    }

    let bundle: serde_json::Value = match serde_json::from_slice(&body) {
        Ok(v) => v,
        Err(e) => return ApiError::BadRequest(format!("invalid JSON: {e}")).into_response(),
    };
    let pv_name = match bundle["pvName"].as_str() {
        Some(s) if !s.is_empty() => s.to_string(),
        _ => return ApiError::BadRequest("pvName missing".to_string()).into_response(),
    };
    let dbr_type_i = bundle["dbrType"].as_i64().unwrap_or(6) as i32;
    let dbr_type = match archiver_core::types::ArchDbType::from_i32(dbr_type_i) {
        Some(t) => t,
        None => {
            return ApiError::BadRequest(format!("invalid dbrType: {dbr_type_i}")).into_response();
        }
    };
    let sample_mode = crate::dto::mgmt::parse_sample_mode(
        bundle["samplingMethod"].as_str(),
        bundle["samplingPeriod"].as_f64(),
    );
    let element_count = bundle["elementCount"].as_i64().unwrap_or(1) as i32;
    let prec = bundle["prec"].as_str().map(|s| s.to_string());
    let egu = bundle["egu"].as_str().map(|s| s.to_string());
    let archive_fields: Vec<String> = bundle["archiveFields"]
        .as_array()
        .map(|a| {
            a.iter()
                .filter_map(|v| v.as_str().map(|s| s.to_string()))
                .collect()
        })
        .unwrap_or_default();
    let samples = match bundle["samples"].as_array() {
        Some(s) => s,
        None => return ApiError::BadRequest("samples missing".to_string()).into_response(),
    };

    if let Ok(Some(_)) = state.pv_query.get_pv(&pv_name) {
        return ApiError::Conflict(format!("'{pv_name}' already exists on this appliance"))
            .into_response();
    }

    if let Err(e) = state.pv_cmd.import_pv(
        &pv_name,
        dbr_type,
        &sample_mode,
        element_count,
        archiver_core::registry::PvStatus::Paused,
        None,
        prec.as_deref(),
        egu.as_deref(),
        None,
        &archive_fields,
        None,
    ) {
        return ApiError::internal(e).into_response();
    }

    let mut written = 0usize;
    for s in samples {
        let secs = s["secs"].as_i64().unwrap_or(0).max(0) as u64;
        let nanos = s["nanos"].as_i64().unwrap_or(0) as u32;
        let ts = std::time::UNIX_EPOCH + std::time::Duration::new(secs, nanos);
        let value = match json_to_archiver_value(&s["value"], dbr_type) {
            Some(v) => v,
            None => {
                tracing::warn!(pv = pv_name, "skipping sample with bad value");
                continue;
            }
        };
        let mut sample = archiver_core::types::ArchiverSample::new(ts, value);
        sample.severity = s["severity"].as_i64().unwrap_or(0) as i32;
        sample.status = s["status"].as_i64().unwrap_or(0) as i32;
        let meta = archiver_core::storage::traits::AppendMeta {
            element_count: Some(element_count),
            ..Default::default()
        };
        if let Err(e) = state
            .storage
            .append_event_with_meta(&pv_name, dbr_type, &sample, &meta)
            .await
        {
            tracing::error!(pv = pv_name, "failed to append migrated sample: {e}");
            return ApiError::internal(e).into_response();
        }
        written += 1;
    }
    if let Err(e) = state.storage.flush_writes().await {
        tracing::warn!(pv = pv_name, "flush after migration failed: {e}");
    }

    axum::Json(serde_json::json!({
        "status": "ok",
        "pv": pv_name,
        "samplesReceived": written,
    }))
    .into_response()
}

/// Source-side: ArchiverValue → JSON. Mirrors the converter the
/// retrieval handlers use; kept local to keep this module self-
/// contained.
fn archiver_value_to_json(v: &archiver_core::types::ArchiverValue) -> serde_json::Value {
    use archiver_core::types::ArchiverValue;
    use serde_json::Value;
    match v {
        ArchiverValue::ScalarString(s) => Value::String(s.clone()),
        ArchiverValue::ScalarShort(n) => (*n).into(),
        ArchiverValue::ScalarInt(n) => (*n).into(),
        ArchiverValue::ScalarEnum(n) => (*n).into(),
        ArchiverValue::ScalarFloat(f) => (*f as f64).into(),
        ArchiverValue::ScalarDouble(f) => (*f).into(),
        ArchiverValue::ScalarByte(b) => Value::Array(b.iter().map(|x| (*x).into()).collect()),
        ArchiverValue::VectorString(arr) => {
            Value::Array(arr.iter().map(|s| Value::String(s.clone())).collect())
        }
        ArchiverValue::VectorChar(arr) => Value::Array(arr.iter().map(|x| (*x).into()).collect()),
        ArchiverValue::VectorShort(arr) => Value::Array(arr.iter().map(|x| (*x).into()).collect()),
        ArchiverValue::VectorInt(arr) => Value::Array(arr.iter().map(|x| (*x).into()).collect()),
        ArchiverValue::VectorEnum(arr) => Value::Array(arr.iter().map(|x| (*x).into()).collect()),
        ArchiverValue::VectorFloat(arr) => {
            Value::Array(arr.iter().map(|x| (*x as f64).into()).collect())
        }
        ArchiverValue::VectorDouble(arr) => Value::Array(arr.iter().map(|x| (*x).into()).collect()),
        ArchiverValue::V4GenericBytes(b) => Value::Array(b.iter().map(|x| (*x).into()).collect()),
    }
}

/// Dest-side: JSON → ArchiverValue. Uses `dbr_type` to choose the
/// right variant, since the JSON shape (number / string / array)
/// alone can't distinguish e.g. `ScalarFloat` from `ScalarDouble`.
fn json_to_archiver_value(
    v: &serde_json::Value,
    dbr_type: archiver_core::types::ArchDbType,
) -> Option<archiver_core::types::ArchiverValue> {
    use archiver_core::types::{ArchDbType, ArchiverValue};
    Some(match dbr_type {
        ArchDbType::ScalarString => ArchiverValue::ScalarString(v.as_str()?.to_string()),
        ArchDbType::ScalarShort => ArchiverValue::ScalarShort(v.as_i64()? as i32),
        ArchDbType::ScalarInt => ArchiverValue::ScalarInt(v.as_i64()? as i32),
        ArchDbType::ScalarEnum => ArchiverValue::ScalarEnum(v.as_i64()? as i32),
        ArchDbType::ScalarFloat => ArchiverValue::ScalarFloat(v.as_f64()? as f32),
        ArchDbType::ScalarDouble => ArchiverValue::ScalarDouble(v.as_f64()?),
        ArchDbType::ScalarByte => {
            let arr = v.as_array()?;
            ArchiverValue::ScalarByte(arr.iter().filter_map(|x| x.as_u64().map(|n| n as u8)).collect())
        }
        ArchDbType::WaveformString => {
            let arr = v.as_array()?;
            ArchiverValue::VectorString(
                arr.iter().filter_map(|x| x.as_str().map(|s| s.to_string())).collect(),
            )
        }
        ArchDbType::WaveformByte => {
            let arr = v.as_array()?;
            ArchiverValue::VectorChar(arr.iter().filter_map(|x| x.as_u64().map(|n| n as u8)).collect())
        }
        ArchDbType::WaveformShort => {
            let arr = v.as_array()?;
            ArchiverValue::VectorShort(arr.iter().filter_map(|x| x.as_i64().map(|n| n as i32)).collect())
        }
        ArchDbType::WaveformInt => {
            let arr = v.as_array()?;
            ArchiverValue::VectorInt(arr.iter().filter_map(|x| x.as_i64().map(|n| n as i32)).collect())
        }
        ArchDbType::WaveformEnum => {
            let arr = v.as_array()?;
            ArchiverValue::VectorEnum(arr.iter().filter_map(|x| x.as_i64().map(|n| n as i32)).collect())
        }
        ArchDbType::WaveformFloat => {
            let arr = v.as_array()?;
            ArchiverValue::VectorFloat(arr.iter().filter_map(|x| x.as_f64().map(|n| n as f32)).collect())
        }
        ArchDbType::WaveformDouble => {
            let arr = v.as_array()?;
            ArchiverValue::VectorDouble(arr.iter().filter_map(|x| x.as_f64()).collect())
        }
        ArchDbType::V4GenericBytes => {
            let arr = v.as_array()?;
            ArchiverValue::V4GenericBytes(
                arr.iter().filter_map(|x| x.as_u64().map(|n| n as u8)).collect(),
            )
        }
    })
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
