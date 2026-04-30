use crate::dto::mgmt::record_to_type_info_with_name;
use crate::errors::ApiError;
use crate::services::traits::{ArchiverQuery, PvQueryRepository};

pub fn get_pv_details(
    pv_query: &dyn PvQueryRepository,
    archiver_query: &dyn ArchiverQuery,
    pv: &str,
) -> Result<serde_json::Value, ApiError> {
    // Java parity (c150faad): if `BASE.HIHI` is queried and isn't itself
    // a typeinfo, fall back to `BASE`.
    let record = match pv_query.get_pv(pv).map_err(ApiError::internal)? {
        Some(r) => r,
        None => match archiver_core::registry::strip_field_suffix(pv) {
            Some(base) => pv_query
                .get_pv(base)
                .map_err(ApiError::internal)?
                .ok_or_else(|| ApiError::NotFound(format!("PV {pv} not found")))?,
            None => return Err(ApiError::NotFound(format!("PV {pv} not found"))),
        },
    };

    let conn_info = archiver_query.get_connection_info(pv);
    let is_connected = conn_info.as_ref().map(|c| c.is_connected).unwrap_or(false);
    let connected_since = conn_info
        .as_ref()
        .and_then(|c| c.connected_since)
        .map(|ts| chrono::DateTime::<chrono::Utc>::from(ts).to_rfc3339());
    // Java parity (dea7acb): expose discrete connection state so
    // operators can distinguish never-connected from connecting from
    // confirmed-down.
    let connection_state = conn_info
        .as_ref()
        .and_then(|c| c.connection_state)
        .map(|s| s.to_string());

    let mut detail =
        serde_json::to_value(record_to_type_info_with_name(&record, Some(pv))).unwrap_or_default();
    if let Some(obj) = detail.as_object_mut() {
        obj.insert(
            "isConnected".to_string(),
            serde_json::json!(is_connected),
        );
        obj.insert(
            "connectedSince".to_string(),
            serde_json::json!(connected_since),
        );
        if let Some(state) = connection_state {
            obj.insert(
                "lastConnectionEventState".to_string(),
                serde_json::json!(state),
            );
        }
        // Java parity (5aabb60): include the list of aliases that point at
        // this PV so operators can audit the reverse mapping.
        let aliases = pv_query
            .aliases_for(&record.pv_name)
            .unwrap_or_default();
        if !aliases.is_empty() {
            obj.insert(
                "aliasNamesForRealName".to_string(),
                serde_json::json!(aliases),
            );
        }
    }

    Ok(detail)
}
