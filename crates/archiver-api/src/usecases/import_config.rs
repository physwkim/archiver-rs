use archiver_core::registry::PvStatus;

use crate::dto::mgmt::{ExportRecord, parse_sample_mode};
use crate::errors::ApiError;
use crate::services::traits::PvCommandRepository;

pub struct ImportResult {
    pub imported: u64,
    pub total: usize,
    pub errors: Vec<String>,
}

pub fn import_config(
    pv_cmd: &dyn PvCommandRepository,
    records: &[ExportRecord],
) -> Result<ImportResult, ApiError> {
    let mut imported = 0u64;
    let mut errors = Vec::new();

    for r in records {
        let dbr_type = archiver_core::types::ArchDbType::from_i32(r.dbr_type)
            .unwrap_or(archiver_core::types::ArchDbType::ScalarDouble);
        let sample_mode = parse_sample_mode(
            Some(r.sampling_method.as_str()),
            if r.sampling_period > 0.0 {
                Some(r.sampling_period)
            } else {
                None
            },
        );
        let status = r
            .status
            .as_deref()
            .and_then(|s| s.parse().ok())
            .unwrap_or(PvStatus::Active);

        match pv_cmd.import_pv(
            &r.pv_name,
            dbr_type,
            &sample_mode,
            r.element_count,
            status,
            r.created_at.as_deref(),
            r.prec.as_deref(),
            r.egu.as_deref(),
            r.alias_for.as_deref(),
            r.archive_fields.as_deref().unwrap_or(&[]),
            r.policy_name.as_deref(),
        ) {
            Ok(()) => imported += 1,
            Err(e) => {
                tracing::warn!(pv = r.pv_name, "Import failed: {e}");
                errors.push(format!("{}: {e}", r.pv_name));
            }
        }
    }

    Ok(ImportResult {
        imported,
        total: records.len(),
        errors,
    })
}
