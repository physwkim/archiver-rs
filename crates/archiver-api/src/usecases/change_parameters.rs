use archiver_core::registry::{PvStatus, SampleMode};

use crate::errors::ApiError;
use crate::services::traits::{ArchiverCommand, PvCommandRepository, PvQueryRepository};

pub async fn change_parameters(
    pv_query: &dyn PvQueryRepository,
    pv_cmd: &dyn PvCommandRepository,
    archiver_cmd: &dyn ArchiverCommand,
    pv: &str,
    new_mode: &SampleMode,
) -> Result<String, ApiError> {
    match pv_cmd.update_sample_mode(pv, new_mode) {
        Ok(false) => return Err(ApiError::NotFound(format!("PV {pv} not found"))),
        Err(e) => {
            return Err(ApiError::Internal(format!(
                "Failed to update parameters for {pv}: {e}"
            )));
        }
        Ok(true) => {}
    }

    let record = match pv_query.get_pv(pv) {
        Ok(Some(r)) => r,
        _ => return Ok(format!("Updated parameters for {pv}")),
    };

    if record.status == PvStatus::Active {
        if let Err(e) = archiver_cmd.pause_pv(pv).await {
            tracing::warn!(pv, "Failed to pause before parameter change: {e}");
        }
        archiver_cmd.resume_pv(pv).await.map_err(|e| {
            ApiError::Internal(format!(
                "Updated parameters but failed to restart {pv}: {e}"
            ))
        })?;
    }

    Ok(format!("Successfully updated parameters for {pv}"))
}
