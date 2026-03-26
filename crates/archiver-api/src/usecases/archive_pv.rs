use archiver_core::registry::SampleMode;

use crate::errors::ApiError;
use crate::services::traits::ArchiverCommand;

pub async fn archive_pv(
    archiver_cmd: &dyn ArchiverCommand,
    pv: &str,
    sample_mode: &SampleMode,
) -> Result<String, ApiError> {
    archiver_cmd
        .archive_pv(pv, sample_mode)
        .await
        .map_err(|e| {
            tracing::error!(pv, "Failed to archive PV: {e}");
            ApiError::Internal(format!("Failed to archive PV {pv}"))
        })?;

    Ok(format!("Successfully started archiving PV {pv}"))
}
