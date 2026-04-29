use axum::extract::{Query, State};
use axum::http::HeaderMap;
use axum::response::IntoResponse;
use serde::Deserialize;

use archiver_core::registry::{PvStatus, SampleMode};

use crate::dto::mgmt::{record_to_type_info, ClusterParam, PvNameParam};
use crate::errors::ApiError;
use crate::AppState;

/// `GET /mgmt/bpl/modifyMetaFields?pv=<name>&command=add,HIHI,LOLO&command=remove,FOO`
///
/// Each `command` is a comma-separated list whose first token is a verb
/// (`add` / `remove` / `clear`) followed by zero or more field names. The
/// PV must be paused. We parse the raw query manually because axum's
/// `Query<T>` does not collect repeated keys into `Vec<String>`.
pub async fn modify_meta_fields(
    State(state): State<AppState>,
    headers: HeaderMap,
    axum::extract::OriginalUri(uri): axum::extract::OriginalUri,
) -> axum::response::Response {
    let raw = uri.query().unwrap_or("");
    let mut pv = String::new();
    let mut commands: Vec<String> = Vec::new();
    for pair in raw.split('&').filter(|s| !s.is_empty()) {
        let (k, v) = pair.split_once('=').unwrap_or((pair, ""));
        let key = urlencoding::decode(k).unwrap_or_else(|_| k.into()).into_owned();
        let value = urlencoding::decode(&v.replace('+', " "))
            .unwrap_or_else(|_| v.into())
            .into_owned();
        match key.as_str() {
            "pv" => pv = value,
            "command" => commands.push(value),
            _ => {}
        }
    }
    if pv.is_empty() {
        return ApiError::BadRequest("pv is required".to_string()).into_response();
    }
    if commands.is_empty() {
        return ApiError::BadRequest(
            "at least one command parameter is required".to_string(),
        )
        .into_response();
    }

    let canonical = match state.pv_query.canonical_name(&pv) {
        Ok(c) => c,
        Err(e) => return ApiError::internal(e).into_response(),
    };

    // Forward to peer when the canonical PV isn't local. modifyMetaFields
    // mutates engine state (per-field CA monitor tasks), which only the
    // owning appliance can do.
    let mut qs = format!("pv={}", urlencoding::encode(&canonical));
    for cmd in &commands {
        qs.push_str(&format!("&command={}", urlencoding::encode(cmd)));
    }
    if let Some(resp) =
        super::try_mgmt_dispatch(&state, &canonical, "modifyMetaFields", &qs, &headers).await
    {
        return resp;
    }

    let record = match state.pv_query.get_pv(&canonical) {
        Ok(Some(r)) => r,
        Ok(None) => return ApiError::NotFound(format!("PV '{pv}' not found")).into_response(),
        Err(e) => return ApiError::internal(e).into_response(),
    };
    if record.status != PvStatus::Paused {
        return ApiError::Conflict(format!(
            "PV '{pv}' must be paused before modifying meta-fields",
        ))
        .into_response();
    }

    let mut fields: std::collections::BTreeSet<String> =
        record.archive_fields.iter().cloned().collect();
    for cmd in &commands {
        let mut parts = cmd.split(',');
        let verb = match parts.next() {
            Some(v) => v.trim(),
            None => return ApiError::BadRequest("empty command".to_string()).into_response(),
        };
        let args: Vec<String> = parts
            .map(|s| s.trim().to_string())
            .filter(|s| !s.is_empty())
            .collect();
        match verb {
            "add" => {
                for f in args {
                    fields.insert(f);
                }
            }
            "remove" => {
                for f in args {
                    fields.remove(&f);
                }
            }
            "clear" => fields.clear(),
            other => {
                return ApiError::BadRequest(format!(
                    "unknown verb '{other}' (expected add/remove/clear)"
                ))
                .into_response();
            }
        }
    }

    let new_fields: Vec<String> = fields.into_iter().collect();
    if let Err(e) = state
        .archiver_cmd
        .update_archive_fields(&canonical, &new_fields)
        .await
    {
        return ApiError::internal(e).into_response();
    }

    axum::Json(serde_json::json!({
        "status": "ok",
        "pv": canonical,
        "archiveFields": new_fields,
    }))
    .into_response()
}

/// `POST /mgmt/bpl/putPVTypeInfo?pv=<name>&override=<bool>&createnew=<bool>`
///
/// Body: a `PvTypeInfoResponse` JSON object. Updates registry metadata only —
/// it does NOT pause/resume the channel. Per Java semantics: callers are
/// expected to chain pause/resume calls themselves.
#[derive(Deserialize)]
pub struct PutTypeInfoQuery {
    pub pv: String,
    #[serde(default)]
    pub override_existing: Option<bool>,
    #[serde(default)]
    pub createnew: Option<bool>,
    /// Java's spelling
    #[serde(default, rename = "override")]
    pub override_legacy: Option<bool>,
}

#[derive(Deserialize)]
#[allow(dead_code)]
pub struct PutTypeInfoBody {
    /// Java GETs PvTypeInfoResponse, edits, and PUTs the same shape back.
    /// We accept `pvName` in the body for round-trip compatibility but the
    /// query parameter is the authoritative key.
    #[serde(rename = "pvName", default)]
    pub pv_name: Option<String>,
    #[serde(rename = "DBRType", default)]
    pub dbr_type: Option<i32>,
    #[serde(rename = "samplingMethod", default)]
    pub sampling_method: Option<String>,
    #[serde(rename = "samplingPeriod", default)]
    pub sampling_period: Option<f64>,
    #[serde(rename = "elementCount", default)]
    pub element_count: Option<i32>,
    #[serde(rename = "PREC", default)]
    pub prec: Option<String>,
    #[serde(rename = "EGU", default)]
    pub egu: Option<String>,
    #[serde(default)]
    pub status: Option<String>,
    #[serde(rename = "createdAt", default)]
    pub created_at: Option<String>,
    #[serde(rename = "aliasFor", default)]
    pub alias_for: Option<String>,
    #[serde(rename = "archiveFields", default)]
    pub archive_fields: Option<Vec<String>>,
    #[serde(rename = "policyName", default)]
    pub policy_name: Option<String>,
}

pub async fn put_pv_type_info(
    State(state): State<AppState>,
    Query(q): Query<PutTypeInfoQuery>,
    body: axum::body::Bytes,
) -> Result<impl IntoResponse, ApiError> {
    if q.pv.is_empty() {
        return Err(ApiError::BadRequest("pv parameter is required".to_string()));
    }
    let allow_override = q.override_existing.unwrap_or(false) || q.override_legacy.unwrap_or(false);
    let allow_create = q.createnew.unwrap_or(false);
    if !allow_override && !allow_create {
        return Err(ApiError::BadRequest(
            "must specify at least one of override or createnew".to_string(),
        ));
    }

    let parsed: PutTypeInfoBody = serde_json::from_slice(&body)
        .map_err(|e| ApiError::BadRequest(format!("Invalid JSON: {e}")))?;

    // Java parity (39b4c881): if the body's `pvName` is set, it must
    // match the query `pv`. A scripted GET-edit-PUT pipeline that
    // targets the wrong URL would otherwise silently overwrite the
    // URL's PV with a different PV's metadata.
    if let Some(ref body_pv) = parsed.pv_name
        && !body_pv.is_empty()
        && body_pv != &q.pv
    {
        return Err(ApiError::BadRequest(format!(
            "body pvName '{body_pv}' does not match query pv '{}'",
            q.pv
        )));
    }

    let exists = state
        .pv_query
        .get_pv(&q.pv)
        .map_err(ApiError::internal)?
        .is_some();

    if exists && !allow_override {
        return Err(ApiError::Conflict(format!(
            "PV '{}' already exists; pass override=true to update",
            q.pv
        )));
    }
    if !exists && !allow_create {
        return Err(ApiError::NotFound(format!(
            "PV '{}' does not exist; pass createnew=true to add",
            q.pv
        )));
    }

    // Defaults pulled from the existing record on override, or from body alone
    // on create. Required fields (dbr_type, sample mode) must be present on
    // create.
    let existing = if exists {
        state.pv_query.get_pv(&q.pv).map_err(ApiError::internal)?
    } else {
        None
    };

    let dbr_type_i = parsed
        .dbr_type
        .or_else(|| existing.as_ref().map(|r| r.dbr_type as i32))
        .ok_or_else(|| ApiError::BadRequest("DBRType required when creating a new PV".to_string()))?;
    let dbr_type = archiver_core::types::ArchDbType::from_i32(dbr_type_i)
        .ok_or_else(|| ApiError::BadRequest(format!("invalid DBRType: {dbr_type_i}")))?;

    let sampling_method = parsed
        .sampling_method
        .clone()
        .or_else(|| {
            existing.as_ref().map(|r| match r.sample_mode {
                SampleMode::Monitor => "Monitor".to_string(),
                SampleMode::Scan { .. } => "Scan".to_string(),
            })
        })
        .unwrap_or_else(|| "Monitor".to_string());
    let sampling_period = parsed
        .sampling_period
        .or_else(|| {
            existing.as_ref().and_then(|r| match r.sample_mode {
                SampleMode::Scan { period_secs } => Some(period_secs),
                _ => None,
            })
        })
        .unwrap_or(0.0);
    let mode = crate::dto::mgmt::parse_sample_mode(
        Some(sampling_method.as_str()),
        if sampling_period > 0.0 {
            Some(sampling_period)
        } else {
            None
        },
    );

    let element_count = parsed
        .element_count
        .or_else(|| existing.as_ref().map(|r| r.element_count))
        .unwrap_or(1);

    let status = parsed
        .status
        .as_deref()
        .and_then(|s| s.parse().ok())
        .or_else(|| existing.as_ref().map(|r| r.status))
        .unwrap_or(PvStatus::Paused);

    let prec = parsed
        .prec
        .clone()
        .or_else(|| existing.as_ref().and_then(|r| r.prec.clone()));
    let egu = parsed
        .egu
        .clone()
        .or_else(|| existing.as_ref().and_then(|r| r.egu.clone()));
    let alias_for = parsed
        .alias_for
        .clone()
        .or_else(|| existing.as_ref().and_then(|r| r.alias_for.clone()));
    let archive_fields = parsed
        .archive_fields
        .clone()
        .or_else(|| existing.as_ref().map(|r| r.archive_fields.clone()))
        .unwrap_or_default();
    let policy_name = parsed
        .policy_name
        .clone()
        .or_else(|| existing.as_ref().and_then(|r| r.policy_name.clone()));

    state
        .pv_cmd
        .import_pv(
            &q.pv,
            dbr_type,
            &mode,
            element_count,
            status,
            parsed.created_at.as_deref(),
            prec.as_deref(),
            egu.as_deref(),
            alias_for.as_deref(),
            &archive_fields,
            policy_name.as_deref(),
        )
        .map_err(ApiError::internal)?;

    let record = state
        .pv_query
        .get_pv(&q.pv)
        .map_err(ApiError::internal)?
        .ok_or_else(|| ApiError::internal(anyhow::anyhow!("vanished after write")))?;
    Ok(axum::Json(record_to_type_info(&record)))
}

/// `GET /mgmt/bpl/getPVTypeInfoKeys?cluster=true`
/// Returns all PV names that have a typeinfo (real PVs and aliases).
/// With `?cluster=true`, aggregates across peers.
pub async fn get_pv_type_info_keys(
    State(state): State<AppState>,
    Query(cp): Query<ClusterParam>,
) -> Result<impl IntoResponse, ApiError> {
    let mut names = state
        .pv_query
        .expanded_pv_names()
        .map_err(ApiError::internal)?;

    if cp.cluster.unwrap_or(false)
        && let Some(ref cluster) = state.cluster
    {
        let remote = cluster.aggregate_all_pvs().await;
        let mut all: std::collections::BTreeSet<String> = names.into_iter().collect();
        all.extend(remote);
        names = all.into_iter().collect();
    }
    Ok(axum::Json(names))
}

/// `GET /mgmt/bpl/getStoresForPV?pv=<name>`
/// Returns `{"STS": "/path", "MTS": "/path", "LTS": "/path"}` plus per-store
/// PV file count (`{store_name}_files`). Java returns just the URL dict;
/// we add file counts because they're cheap and operationally useful.
pub async fn get_stores_for_pv(
    State(state): State<AppState>,
    headers: HeaderMap,
    Query(params): Query<PvNameParam>,
) -> axum::response::Response {
    if params.pv.is_empty() {
        return ApiError::BadRequest("pv parameter is required".to_string()).into_response();
    }
    // Aliases resolve to the canonical PV.
    let canonical = match state.pv_query.canonical_name(&params.pv) {
        Ok(c) => c,
        Err(e) => return ApiError::internal(e).into_response(),
    };

    // File layout is per-appliance — forward to the peer that owns the PV.
    let qs = format!("pv={}", urlencoding::encode(&canonical));
    if let Some(resp) =
        super::try_mgmt_dispatch(&state, &canonical, "getStoresForPV", &qs, &headers).await
    {
        return resp;
    }

    let summaries = match state.storage.stores_for_pv(&canonical) {
        Ok(s) => s,
        Err(e) => return ApiError::internal(e).into_response(),
    };

    let mut obj = serde_json::Map::new();
    for s in summaries {
        let url = format!(
            "pb://localhost?name={}&rootFolder={}&partitionGranularity={:?}",
            s.name,
            s.root_folder.display(),
            s.granularity,
        );
        obj.insert(s.name.clone(), serde_json::Value::String(url));
        if let Some(count) = s.pv_file_count {
            obj.insert(
                format!("{}_files", s.name),
                serde_json::Value::Number(count.into()),
            );
        }
    }
    axum::Json(serde_json::Value::Object(obj)).into_response()
}

/// `GET /mgmt/bpl/renamePV?pv=<old>&newname=<new>`
///
/// Per Java semantics this is a copy operation: the source PV's typeinfo and
/// data are duplicated under the new name. Both end up paused. The caller
/// decides when to resume the new name and delete the old.
#[derive(Deserialize)]
pub struct RenameQuery {
    pub pv: String,
    pub newname: String,
}

pub async fn rename_pv(
    State(state): State<AppState>,
    headers: HeaderMap,
    Query(q): Query<RenameQuery>,
) -> axum::response::Response {
    if q.pv.is_empty() || q.newname.is_empty() {
        return ApiError::BadRequest("pv and newname are required".to_string()).into_response();
    }
    if q.pv == q.newname {
        return ApiError::BadRequest("pv and newname must differ".to_string()).into_response();
    }

    // Rename moves files on disk — only the appliance that owns the source
    // PV can do it. Forward to the peer when the source is remote.
    let qs = format!(
        "pv={}&newname={}",
        urlencoding::encode(&q.pv),
        urlencoding::encode(&q.newname),
    );
    if let Some(resp) =
        super::try_mgmt_dispatch(&state, &q.pv, "renamePV", &qs, &headers).await
    {
        return resp;
    }

    let source = match state.pv_query.get_pv(&q.pv) {
        Ok(Some(r)) => r,
        Ok(None) => return ApiError::NotFound(format!("PV '{}' not found", q.pv)).into_response(),
        Err(e) => return ApiError::internal(e).into_response(),
    };
    if source.alias_for.is_some() {
        return ApiError::BadRequest(format!(
            "'{}' is an alias; rename the canonical PV instead",
            q.pv
        ))
        .into_response();
    }
    if source.status != PvStatus::Paused {
        return ApiError::Conflict(format!(
            "source PV '{}' must be paused before renaming (current: {:?})",
            q.pv, source.status,
        ))
        .into_response();
    }
    match state.pv_query.get_pv(&q.newname) {
        Ok(Some(_)) => {
            return ApiError::Conflict(format!(
                "destination '{}' already exists",
                q.newname
            ))
            .into_response();
        }
        Ok(None) => {}
        Err(e) => return ApiError::internal(e).into_response(),
    }

    // Copy the data files first; if that fails, the registry change is not
    // applied so the source remains the source of truth.
    let copied = match state.storage.rename_pv(&q.pv, &q.newname).await {
        Ok(c) => c,
        Err(e) => return ApiError::internal(e).into_response(),
    };

    // Insert the destination row mirroring source. Both sides remain paused
    // until the caller resumes / deletes. Java parity (9968c378): inherit
    // the source's `created_at` so age-based reports (silent PVs, recently
    // added) don't reclassify the renamed PV as brand-new.
    let created_at_str = source.created_at.to_rfc3339();
    if let Err(e) = state.pv_cmd.import_pv(
        &q.newname,
        source.dbr_type,
        &source.sample_mode,
        source.element_count,
        PvStatus::Paused,
        Some(&created_at_str),
        source.prec.as_deref(),
        source.egu.as_deref(),
        None, // not an alias
        &source.archive_fields,
        source.policy_name.as_deref(),
    ) {
        return ApiError::internal(e).into_response();
    }

    if let Err(e) = state.pv_cmd.set_status(&q.pv, PvStatus::Paused) {
        return ApiError::internal(e).into_response();
    }

    axum::Json(serde_json::json!({
        "status": "ok",
        "from": q.pv,
        "to": q.newname,
        "files_moved": copied,
    }))
    .into_response()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::dto::mgmt::record_to_type_info;
    use archiver_core::registry::{PvRecord, PvStatus, SampleMode};
    use archiver_core::types::ArchDbType;

    fn sample_record() -> PvRecord {
        PvRecord {
            pv_name: "PV:Test".to_string(),
            dbr_type: ArchDbType::ScalarDouble,
            sample_mode: SampleMode::Monitor,
            status: PvStatus::Active,
            element_count: 1,
            last_timestamp: None,
            created_at: chrono::Utc::now(),
            updated_at: chrono::Utc::now(),
            prec: Some("3".to_string()),
            egu: Some("mA".to_string()),
            alias_for: None,
            archive_fields: vec!["HIHI".to_string()],
            policy_name: Some("ring".to_string()),
        }
    }

    #[test]
    fn put_body_can_decode_serialized_response() {
        // The Java client typically GETs PvTypeInfoResponse, edits, PUTs back.
        // Our PutTypeInfoBody must accept the same shape.
        let resp = record_to_type_info(&sample_record());
        let json = serde_json::to_string(&resp).unwrap();
        let decoded: PutTypeInfoBody = serde_json::from_str(&json).unwrap();
        assert_eq!(decoded.pv_name.as_deref(), Some("PV:Test"));
        assert_eq!(decoded.dbr_type, Some(ArchDbType::ScalarDouble as i32));
        assert_eq!(decoded.sampling_method.as_deref(), Some("Monitor"));
        assert_eq!(decoded.archive_fields.as_deref(), Some(&["HIHI".to_string()][..]));
        assert_eq!(decoded.policy_name.as_deref(), Some("ring"));
    }
}
