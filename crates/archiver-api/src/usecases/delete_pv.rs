use archiver_core::storage::traits::StoragePlugin;

use crate::errors::ApiError;
use crate::services::traits::{ArchiverCommand, PvCommandRepository, PvQueryRepository};

pub struct DeletePvResult {
    pub pv: String,
    pub files_deleted: u64,
}

pub async fn delete_pv(
    archiver_cmd: &dyn ArchiverCommand,
    pv_query: &dyn PvQueryRepository,
    pv_cmd: &dyn PvCommandRepository,
    storage: &dyn StoragePlugin,
    pv: &str,
    delete_data: bool,
) -> Result<DeletePvResult, ApiError> {
    // Java parity (d7e4c5e2): drop every alias pointing at this PV
    // BEFORE removing the PV row itself, otherwise the alias rows are
    // orphaned and resolve to NotFound (or worse, route across peers).
    // Done first so a partial failure on the registry side still leaves
    // a consistent state — the PV row is still present.
    if let Ok(aliases) = pv_query.aliases_for(pv) {
        for alias in aliases {
            if let Err(e) = pv_cmd.remove_alias(&alias) {
                tracing::warn!(pv, alias, "deletePV: failed to remove alias: {e}");
            }
        }
    }

    archiver_cmd
        .destroy_pv(pv)
        .await
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
