use crate::errors::ApiError;
use crate::services::traits::ArchiverCommand;

pub async fn abort_archiving(
    archiver_cmd: &dyn ArchiverCommand,
    pv: &str,
) -> Result<String, ApiError> {
    // Java parity (0687be1): abort must succeed unconditionally —
    // including for PVs that never produced a registry entry (CA channel
    // never connected). Java's `AbortArchiveRequest` removed the
    // typeinfo guard entirely so operators are never told a stuck pending
    // archive cannot be aborted. `stop_pv` is itself idempotent on
    // unknown PV names, so this delegate does the right thing whether
    // or not a registry record exists.
    archiver_cmd
        .stop_pv(pv)
        .await
        .map_err(|e| ApiError::Internal(format!("Failed to abort archiving PV {pv}: {e}")))?;

    Ok(format!(
        "Successfully aborted archiving PV {pv} (data retained)"
    ))
}
