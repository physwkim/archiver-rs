use archiver_core::storage::traits::StoragePlugin;

use crate::errors::ApiError;
use crate::services::traits::ArchiverCommand;

pub struct DeletePvResult {
    pub pv: String,
    pub files_deleted: u64,
}

pub async fn delete_pv(
    archiver_cmd: &dyn ArchiverCommand,
    storage: &dyn StoragePlugin,
    pv: &str,
    delete_data: bool,
) -> Result<DeletePvResult, ApiError> {
    archiver_cmd
        .destroy_pv(pv)
        .map_err(|e| ApiError::Internal(format!("Failed to delete PV {pv}: {e}")))?;

    let files_deleted = if delete_data {
        storage.delete_pv_data(pv).await.map_err(|e| {
            ApiError::Internal(format!("PV {pv} removed but failed to delete data: {e}"))
        })?
    } else {
        0
    };

    Ok(DeletePvResult {
        pv: pv.to_string(),
        files_deleted,
    })
}
