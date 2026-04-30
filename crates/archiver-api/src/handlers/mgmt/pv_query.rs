use axum::extract::{Query, State};
use axum::response::{IntoResponse, Response};

use archiver_core::registry::{PvStatus, SampleMode};

use crate::AppState;
use crate::dto::mgmt::*;
use crate::errors::ApiError;

pub async fn get_all_pvs(
    State(state): State<AppState>,
    Query(cp): Query<AllPvsParams>,
) -> Result<impl IntoResponse, ApiError> {
    // Java parity (cb5e83b0): `getAllPVs` includes alias rows. Internal
    // engine/report callers stay on `all_pv_names` which filters them out.
    let mut pvs = state
        .pv_query
        .expanded_pv_names()
        .map_err(ApiError::internal)?;

    // Java parity (5ebf1e1): cap the response so a `?cluster=true` query
    // against a 100k-PV deployment doesn't try to JSON-encode every name.
    let limit = match cp.limit {
        Some(n) if n < 0 => None,
        Some(n) => Some(n as usize),
        None => Some(DEFAULT_MATCHING_PVS_LIMIT),
    };

    // Pre-trim local before merging peer data — otherwise a 100k-PV
    // local appliance still encodes all names just to truncate after.
    if let Some(n) = limit {
        pvs.sort();
        pvs.truncate(n);
    }

    if cp.cluster.unwrap_or(false)
        && let Some(ref cluster) = state.cluster
    {
        // Java parity (9b78b21 piggyback): forward the resolved limit so each
        // peer caps its own reply rather than the aggregator pulling
        // peer_default × peers names off the wire.
        let peer_limit = cp.limit.map(|n| n as i64);
        let remote = cluster.aggregate_all_pvs_limited(peer_limit).await;
        let mut all: std::collections::BTreeSet<String> = pvs.into_iter().collect();
        all.extend(remote);
        pvs = all.into_iter().collect();
    }

    if let Some(n) = limit {
        pvs.sort();
        pvs.truncate(n);
    }

    Ok(axum::Json(pvs))
}

/// Default cap for getMatchingPVs when the caller doesn't pass `limit`.
/// Mirrors Java archiver's behavior of always sending a limit so a `*`
/// query against a cluster of 100k PVs doesn't try to JSON-encode every
/// name. Callers that genuinely want everything pass `limit=-1`.
const DEFAULT_MATCHING_PVS_LIMIT: usize = 500;

/// Java parity (39ba51b): `getMatchingPVs` also accepts a POST body
/// `["pv1","pv2",...]`. The body strings are explicit names or globs
/// — each is run through the same expansion path as the GET form, the
/// union is de-duplicated and limit-capped.
pub async fn get_matching_pvs_post(
    State(state): State<AppState>,
    Query(params): Query<ClusterParam>,
    crate::pv_input::PvListInput(patterns): crate::pv_input::PvListInput,
) -> Result<impl IntoResponse, ApiError> {
    let mut all: std::collections::BTreeSet<String> = std::collections::BTreeSet::new();
    for pat in &patterns {
        let names = state
            .pv_query
            .matching_pvs_expanded(pat)
            .map_err(ApiError::internal)?;
        all.extend(names);
    }

    if params.cluster.unwrap_or(false)
        && let Some(ref cluster) = state.cluster
    {
        for pat in &patterns {
            all.extend(cluster.aggregate_matching_pvs(pat).await);
        }
    }

    let mut pvs: Vec<String> = all.into_iter().collect();
    pvs.truncate(DEFAULT_MATCHING_PVS_LIMIT);
    Ok(axum::Json(pvs))
}

pub async fn get_matching_pvs(
    State(state): State<AppState>,
    Query(params): Query<MatchingPvsParams>,
) -> Result<impl IntoResponse, ApiError> {
    // Java parity (c61f1579): `getMatchingPVs?pv=*` includes aliases.
    let mut pvs = state
        .pv_query
        .matching_pvs_expanded(&params.pv)
        .map_err(ApiError::internal)?;

    // Apply limit. Java's bug 22c9b7fc was a `subList(0, limit)` call that
    // panicked when the result was shorter than limit; guard against the
    // same here even though Vec::truncate is forgiving.
    let limit = match params.limit {
        Some(n) if n < 0 => None, // -1 = unlimited
        Some(n) => Some(n as usize),
        None => Some(DEFAULT_MATCHING_PVS_LIMIT),
    };

    // Pre-trim the local set BEFORE the cluster merge so an unbounded
    // local list doesn't get JSON-encoded just to be truncated after.
    // Same intent as the post-merge truncate below — limit applies on
    // both sides of the union.
    if let Some(n) = limit {
        pvs.sort();
        pvs.truncate(n);
    }

    if params.cluster.unwrap_or(false)
        && let Some(ref cluster) = state.cluster
    {
        // Java parity (9b78b21): forward the resolved limit so each
        // peer caps its own reply rather than the aggregator pulling
        // peer_default × peers names off the wire.
        let peer_limit = params.limit.map(|n| n as i64);
        let remote = cluster
            .aggregate_matching_pvs_limited(&params.pv, peer_limit)
            .await;
        let mut all: std::collections::BTreeSet<String> = pvs.into_iter().collect();
        all.extend(remote);
        pvs = all.into_iter().collect();
    }

    if let Some(n) = limit {
        pvs.sort();
        pvs.truncate(n);
    }

    Ok(axum::Json(pvs))
}

/// Resolve a single PV's status (Java parity c84c12a — factored out of
/// `get_pv_status` so the GET (single-PV) and POST (list-of-PVs)
/// handlers share one code path). The returned `pv_name` always echoes
/// the user-supplied input so request → response matching by name is
/// stable across alias resolution.
async fn resolve_pv_status(
    state: &AppState,
    requested: &str,
    cluster_enabled: bool,
) -> PvStatusResponse {
    let canonical = state
        .pv_query
        .canonical_name(requested)
        .unwrap_or_else(|_| requested.to_string());
    // Four-path lookup (Java parity bc6f477): bare PV / alias / field-
    // stripped / alias-of-field.
    let canonical = match state.pv_query.get_pv(&canonical).ok().flatten() {
        Some(_) => canonical,
        None => archiver_core::registry::strip_field_suffix(&canonical)
            .map(|base| {
                state
                    .pv_query
                    .canonical_name(base)
                    .unwrap_or_else(|_| base.to_string())
            })
            .unwrap_or(canonical),
    };

    if let Ok(Some(record)) = state.pv_query.get_pv(&canonical) {
        let status_str = match record.status {
            PvStatus::Active => "Being archived",
            PvStatus::Paused => "Paused",
            PvStatus::Error => "Error",
            PvStatus::Inactive => "Inactive",
            PvStatus::Alias => "Alias",
        };
        let sample_mode_str = match &record.sample_mode {
            SampleMode::Monitor => "Monitor".to_string(),
            SampleMode::Scan { period_secs } => format!("Scan @ {period_secs}s"),
        };
        let last_ts = record
            .last_timestamp
            .map(|ts| chrono::DateTime::<chrono::Utc>::from(ts).to_rfc3339());
        return PvStatusResponse {
            pv_name: requested.to_string(),
            status: status_str.to_string(),
            dbr_type: Some(record.dbr_type as i32),
            sample_mode: Some(sample_mode_str),
            element_count: Some(record.element_count),
            last_event_timestamp: last_ts,
        };
    }

    // Java parity (c2d9f9e): unreachable peer surfaces as "Appliance
    // Down" rather than masquerading as "Not being archived".
    if cluster_enabled && let Some(ref cluster) = state.cluster {
        use crate::services::traits::RemoteStatusOutcomeDto;
        match cluster.remote_pv_status_detailed(&canonical).await {
            RemoteStatusOutcomeDto::Found(remote_status) => {
                if let Ok(parsed) =
                    serde_json::from_value::<PvStatusResponse>(remote_status.clone())
                {
                    return PvStatusResponse {
                        pv_name: requested.to_string(),
                        ..parsed
                    };
                }
            }
            RemoteStatusOutcomeDto::ApplianceDown => {
                return PvStatusResponse {
                    pv_name: requested.to_string(),
                    status: "Appliance Down".to_string(),
                    dbr_type: None,
                    sample_mode: None,
                    element_count: None,
                    last_event_timestamp: None,
                };
            }
            RemoteStatusOutcomeDto::NotArchived => {}
        }
    }

    PvStatusResponse {
        pv_name: requested.to_string(),
        status: "Not being archived".to_string(),
        dbr_type: None,
        sample_mode: None,
        element_count: None,
        last_event_timestamp: None,
    }
}

pub async fn get_pv_status(
    State(state): State<AppState>,
    Query(params): Query<PvStatusParams>,
) -> Result<impl IntoResponse, ApiError> {
    let resp = resolve_pv_status(&state, &params.pv, params.cluster.unwrap_or(false)).await;
    Ok(axum::Json(resp).into_response())
}

/// Java parity (c84c12a): POST `["pv1","pv2",...]` returns one status
/// entry per PV using the same resolution logic as the GET handler.
#[derive(serde::Deserialize)]
pub struct GetPvStatusBulkParams {
    #[serde(default)]
    pub cluster: Option<bool>,
}

pub async fn get_pv_status_post(
    State(state): State<AppState>,
    headers: axum::http::HeaderMap,
    Query(params): Query<GetPvStatusBulkParams>,
    crate::pv_input::PvListInput(pvs): crate::pv_input::PvListInput,
) -> Response {
    let cluster_enabled = params.cluster.unwrap_or(false);
    let is_proxied = headers.get("X-Archiver-Proxied").is_some();
    let mut out: Vec<PvStatusResponse> = Vec::with_capacity(pvs.len());

    // Java parity (c84c12a): when this query is the entry point (not a
    // proxied dispatch from a peer), group peer-owned PVs by appliance
    // and issue ONE POST per peer instead of N×M HTTP probes.
    let local_pvs = if cluster_enabled && !is_proxied && state.cluster.is_some() {
        let (local, by_peer) = super::route_pvs(&state, &pvs, false).await;
        if let Some(ref cluster) = state.cluster {
            for (mgmt_url, batch) in by_peer {
                let body = serde_json::to_vec(&batch).unwrap_or_default();
                let endpoint = "getPVStatus?cluster=true";
                match cluster
                    .proxy_mgmt_post(&mgmt_url, endpoint, body.into())
                    .await
                {
                    Ok(resp) => {
                        let (_, body) = resp.into_parts();
                        const PEER_BODY_MAX: usize = 64 * 1024 * 1024;
                        match axum::body::to_bytes(body, PEER_BODY_MAX).await {
                            Ok(bytes) => {
                                match serde_json::from_slice::<Vec<PvStatusResponse>>(&bytes) {
                                    Ok(arr) => out.extend(arr),
                                    Err(e) => {
                                        tracing::warn!(
                                            peer = mgmt_url,
                                            "getPVStatus peer parse failed: {e}"
                                        );
                                        out.extend(batch.iter().map(|pv| PvStatusResponse {
                                            pv_name: pv.clone(),
                                            status: "Appliance Down".to_string(),
                                            dbr_type: None,
                                            sample_mode: None,
                                            element_count: None,
                                            last_event_timestamp: None,
                                        }));
                                    }
                                }
                            }
                            Err(e) => {
                                tracing::warn!(
                                    peer = mgmt_url,
                                    "getPVStatus peer body read failed: {e}"
                                );
                                out.extend(batch.iter().map(|pv| PvStatusResponse {
                                    pv_name: pv.clone(),
                                    status: "Appliance Down".to_string(),
                                    dbr_type: None,
                                    sample_mode: None,
                                    element_count: None,
                                    last_event_timestamp: None,
                                }));
                            }
                        }
                    }
                    Err(e) => {
                        tracing::warn!(peer = mgmt_url, "getPVStatus peer dispatch failed: {e}");
                        out.extend(batch.iter().map(|pv| PvStatusResponse {
                            pv_name: pv.clone(),
                            status: "Appliance Down".to_string(),
                            dbr_type: None,
                            sample_mode: None,
                            element_count: None,
                            last_event_timestamp: None,
                        }));
                    }
                }
            }
        }
        local
    } else {
        pvs
    };

    for pv in &local_pvs {
        // Local-only resolution: pass `false` for cluster so we don't
        // re-fan-out for PVs route_pvs already classified as local.
        out.push(resolve_pv_status(&state, pv, false).await);
    }
    axum::Json(out).into_response()
}

pub async fn get_pv_count(
    State(state): State<AppState>,
    Query(cp): Query<ClusterParam>,
) -> Result<impl IntoResponse, ApiError> {
    let mut total = state.pv_query.count(None).unwrap_or(0);
    let mut active = state.pv_query.count(Some(PvStatus::Active)).unwrap_or(0);
    let mut paused = state.pv_query.count(Some(PvStatus::Paused)).unwrap_or(0);

    let mut failed_peers = None;
    if cp.cluster.unwrap_or(false)
        && let Some(ref cluster) = state.cluster
    {
        let (rt, ra, rp, failed) = cluster.aggregate_pv_count().await;
        total += rt;
        active += ra;
        paused += rp;
        if failed > 0 {
            failed_peers = Some(failed);
        }
    }

    let resp = PvCountResponse {
        total,
        active,
        paused,
        failed_peers,
    };
    Ok(axum::Json(resp))
}

pub async fn get_pv_type_info(
    State(state): State<AppState>,
    Query(params): Query<PvNameParam>,
) -> Result<impl IntoResponse, ApiError> {
    let canonical = state
        .pv_query
        .canonical_name(&params.pv)
        .unwrap_or_else(|_| params.pv.clone());
    match state
        .pv_query
        .get_pv(&canonical)
        .map_err(ApiError::internal)?
    {
        Some(record) => Ok(axum::Json(crate::dto::mgmt::record_to_type_info_with_name(
            &record,
            Some(&params.pv),
        ))),
        None => Err(ApiError::NotFound(format!("PV {} not found", params.pv))),
    }
}

pub async fn get_pv_details(
    State(state): State<AppState>,
    Query(params): Query<PvNameParam>,
) -> Result<impl IntoResponse, ApiError> {
    let detail = crate::usecases::get_pv_details::get_pv_details(
        state.pv_query.as_ref(),
        state.archiver_query.as_ref(),
        &params.pv,
    )?;

    Ok(axum::Json(detail))
}
