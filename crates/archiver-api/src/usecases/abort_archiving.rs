use archiver_core::registry::PvStatus;

use crate::errors::ApiError;
use crate::services::traits::{ArchiverCommand, PvQueryRepository};

pub fn abort_archiving(
    pv_query: &dyn PvQueryRepository,
    archiver_cmd: &dyn ArchiverCommand,
    pv: &str,
) -> Result<String, ApiError> {
    match pv_query.get_pv(pv).map_err(ApiError::internal)? {
        Some(record) => {
            if record.status == PvStatus::Active {
                return Err(ApiError::BadRequest(
                    "PV is actively archiving; pause first or use deletePV".to_string(),
                ));
            }
        }
        None => return Err(ApiError::NotFound(format!("PV {pv} not found"))),
    }

    archiver_cmd
        .stop_pv(pv)
        .map_err(|e| ApiError::Internal(format!("Failed to abort archiving PV {pv}: {e}")))?;

    Ok(format!(
        "Successfully aborted archiving PV {pv} (data retained)"
    ))
}
