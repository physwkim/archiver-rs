use crate::dto::mgmt::record_to_type_info;
use crate::errors::ApiError;
use crate::services::traits::{ArchiverQuery, PvQueryRepository};

pub fn get_pv_details(
    pv_query: &dyn PvQueryRepository,
    archiver_query: &dyn ArchiverQuery,
    pv: &str,
) -> Result<serde_json::Value, ApiError> {
    let record = pv_query
        .get_pv(pv)
        .map_err(ApiError::internal)?
        .ok_or_else(|| ApiError::NotFound(format!("PV {pv} not found")))?;

    let conn_info = archiver_query.get_connection_info(pv);
    let is_connected = conn_info.as_ref().map(|c| c.is_connected).unwrap_or(false);
    let connected_since = conn_info
        .as_ref()
        .and_then(|c| c.connected_since)
        .map(|ts| chrono::DateTime::<chrono::Utc>::from(ts).to_rfc3339());

    let mut detail = serde_json::to_value(record_to_type_info(&record)).unwrap_or_default();
    if let Some(obj) = detail.as_object_mut() {
        obj.insert(
            "isConnected".to_string(),
            serde_json::json!(is_connected),
        );
        obj.insert(
            "connectedSince".to_string(),
            serde_json::json!(connected_since),
        );
    }

    Ok(detail)
}
