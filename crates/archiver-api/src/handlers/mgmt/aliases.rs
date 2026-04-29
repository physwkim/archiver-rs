use axum::extract::{Query, State};
use axum::response::IntoResponse;
use serde::Deserialize;

use crate::errors::ApiError;
use crate::AppState;

#[derive(Deserialize)]
pub struct AliasParams {
    /// Real (target) PV name.
    pub pv: String,
    /// Alias name to add or remove.
    pub aliasname: String,
}

/// `GET /mgmt/bpl/addAlias?pv=<target>&aliasname=<alias>`
/// Java-compatible: `pv` is the real PV, `aliasname` is the alias.
pub async fn add_alias(
    State(state): State<AppState>,
    Query(params): Query<AliasParams>,
) -> Result<impl IntoResponse, ApiError> {
    if params.pv.is_empty() || params.aliasname.is_empty() {
        return Err(ApiError::BadRequest(
            "pv and aliasname are required".to_string(),
        ));
    }
    state
        .pv_cmd
        .add_alias(&params.aliasname, &params.pv)
        .map_err(|e| ApiError::BadRequest(format!("{e}")))?;
    let resp = serde_json::json!({
        "status": "ok",
        "pv": params.pv,
        "aliasname": params.aliasname,
    });
    Ok(axum::Json(resp))
}

/// `GET /mgmt/bpl/removeAlias?pv=<target>&aliasname=<alias>`
pub async fn remove_alias(
    State(state): State<AppState>,
    Query(params): Query<AliasParams>,
) -> Result<impl IntoResponse, ApiError> {
    if params.pv.is_empty() || params.aliasname.is_empty() {
        return Err(ApiError::BadRequest(
            "pv and aliasname are required".to_string(),
        ));
    }
    let removed = state
        .pv_cmd
        .remove_alias(&params.aliasname)
        .map_err(ApiError::internal)?;
    let resp = serde_json::json!({
        "status": if removed { "ok" } else { "not_found" },
        "pv": params.pv,
        "aliasname": params.aliasname,
    });
    Ok(axum::Json(resp))
}

/// `GET /mgmt/bpl/getAllAliases`
/// Returns: `[{"aliasName": ..., "srcPVName": ...}, ...]`
pub async fn get_all_aliases(
    State(state): State<AppState>,
) -> Result<impl IntoResponse, ApiError> {
    let pairs = state.pv_query.all_aliases().map_err(ApiError::internal)?;
    let json: Vec<serde_json::Value> = pairs
        .into_iter()
        .map(|(alias, src)| {
            serde_json::json!({
                "aliasName": alias,
                "srcPVName": src,
            })
        })
        .collect();
    Ok(axum::Json(json))
}

/// `GET /mgmt/bpl/getAllExpandedPVNames`
/// All names known to the registry (real PVs + aliases).
pub async fn get_all_expanded_pv_names(
    State(state): State<AppState>,
) -> Result<impl IntoResponse, ApiError> {
    let names = state
        .pv_query
        .expanded_pv_names()
        .map_err(ApiError::internal)?;
    Ok(axum::Json(names))
}
