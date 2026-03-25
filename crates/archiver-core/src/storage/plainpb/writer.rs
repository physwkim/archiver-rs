use std::path::Path;

use archiver_proto::epics_event::{self, FieldValue, PayloadInfo};
use prost::Message;
use tokio::io::AsyncWriteExt;

use crate::storage::plainpb::codec;
use crate::types::{ArchDbType, ArchiverSample, ArchiverValue};

/// Writes samples to a PlainPB file in a format compatible with the Java archiver.
///
/// File format:
///   [escaped PayloadInfo protobuf]\n
///   [escaped Sample1 protobuf]\n
///   [escaped Sample2 protobuf]\n
///   ...
pub struct PbFileWriter<'a> {
    path: &'a Path,
}

impl<'a> PbFileWriter<'a> {
    pub fn new(path: &'a Path) -> Self {
        Self { path }
    }

    /// Append a sample to the file, creating it with a PayloadInfo header if it
    /// doesn't exist yet.
    pub async fn append_sample(
        &self,
        pv: &str,
        dbr_type: ArchDbType,
        sample: &ArchiverSample,
    ) -> anyhow::Result<()> {
        self.append_sample_with_meta(pv, dbr_type, sample, None, &[])
            .await
    }

    /// Append a sample with optional metadata for the PayloadInfo header.
    pub async fn append_sample_with_meta(
        &self,
        pv: &str,
        dbr_type: ArchDbType,
        sample: &ArchiverSample,
        element_count: Option<i32>,
        headers: &[(String, String)],
    ) -> anyhow::Result<()> {
        let file_exists = self.path.exists();
        let mut file = tokio::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(self.path)
            .await?;

        if !file_exists {
            let (year, _, _) = sample.decompose_timestamp();
            let header = build_payload_info(pv, dbr_type, year, element_count, headers);
            let header_bytes = header.encode_to_vec();
            let escaped = codec::escape(&header_bytes);
            file.write_all(&escaped).await?;
            file.write_all(&[codec::NEWLINE]).await?;
        }

        let sample_bytes = encode_sample(dbr_type, sample)?;
        let escaped = codec::escape(&sample_bytes);
        file.write_all(&escaped).await?;
        file.write_all(&[codec::NEWLINE]).await?;
        file.flush().await?;

        Ok(())
    }
}

fn build_payload_info(
    pv: &str,
    dbr_type: ArchDbType,
    year: i32,
    element_count: Option<i32>,
    extra_headers: &[(String, String)],
) -> PayloadInfo {
    let headers: Vec<FieldValue> = extra_headers
        .iter()
        .map(|(n, v)| FieldValue {
            name: n.clone(),
            val: v.clone(),
        })
        .collect();

    PayloadInfo {
        r#type: dbr_type as i32,
        pvname: pv.to_string(),
        year,
        element_count,
        unused00: None,
        unused01: None,
        unused02: None,
        unused03: None,
        unused04: None,
        unused05: None,
        unused06: None,
        unused07: None,
        unused08: None,
        unused09: None,
        headers,
    }
}

fn field_values_to_pb(fvs: &[(String, String)]) -> Vec<FieldValue> {
    fvs.iter()
        .map(|(n, v)| FieldValue {
            name: n.clone(),
            val: v.clone(),
        })
        .collect()
}

/// Encode an ArchiverSample into protobuf bytes (before line-escaping).
pub fn encode_sample(dbr_type: ArchDbType, sample: &ArchiverSample) -> anyhow::Result<Vec<u8>> {
    let (_, secs, nanos) = sample.decompose_timestamp();
    let fvs = field_values_to_pb(&sample.field_values);
    let severity = if sample.severity != 0 {
        Some(sample.severity)
    } else {
        None
    };
    let status = if sample.status != 0 {
        Some(sample.status)
    } else {
        None
    };
    let rc = sample.repeat_count;
    let fac = if sample.field_actual_change {
        Some(true)
    } else {
        None
    };

    let bytes = match (&sample.value, dbr_type) {
        (ArchiverValue::ScalarString(v), ArchDbType::ScalarString) => {
            epics_event::ScalarString {
                secondsintoyear: secs,
                nano: nanos,
                val: v.clone(),
                severity,
                status,
                repeatcount: rc,
                fieldvalues: fvs,
                fieldactualchange: fac,
            }
            .encode_to_vec()
        }
        (ArchiverValue::ScalarByte(v), ArchDbType::ScalarByte) => {
            epics_event::ScalarByte {
                secondsintoyear: secs,
                nano: nanos,
                val: v.clone(),
                severity,
                status,
                repeatcount: rc,
                fieldvalues: fvs,
                fieldactualchange: fac,
            }
            .encode_to_vec()
        }
        (ArchiverValue::ScalarShort(v), ArchDbType::ScalarShort) => {
            epics_event::ScalarShort {
                secondsintoyear: secs,
                nano: nanos,
                val: *v,
                severity,
                status,
                repeatcount: rc,
                fieldvalues: fvs,
                fieldactualchange: fac,
            }
            .encode_to_vec()
        }
        (ArchiverValue::ScalarInt(v), ArchDbType::ScalarInt) => {
            epics_event::ScalarInt {
                secondsintoyear: secs,
                nano: nanos,
                val: *v,
                severity,
                status,
                repeatcount: rc,
                fieldvalues: fvs,
                fieldactualchange: fac,
            }
            .encode_to_vec()
        }
        (ArchiverValue::ScalarEnum(v), ArchDbType::ScalarEnum) => {
            epics_event::ScalarEnum {
                secondsintoyear: secs,
                nano: nanos,
                val: *v,
                severity,
                status,
                repeatcount: rc,
                fieldvalues: fvs,
                fieldactualchange: fac,
            }
            .encode_to_vec()
        }
        (ArchiverValue::ScalarFloat(v), ArchDbType::ScalarFloat) => {
            epics_event::ScalarFloat {
                secondsintoyear: secs,
                nano: nanos,
                val: *v,
                severity,
                status,
                repeatcount: rc,
                fieldvalues: fvs,
                fieldactualchange: fac,
            }
            .encode_to_vec()
        }
        (ArchiverValue::ScalarDouble(v), ArchDbType::ScalarDouble) => {
            epics_event::ScalarDouble {
                secondsintoyear: secs,
                nano: nanos,
                val: *v,
                severity,
                status,
                repeatcount: rc,
                fieldvalues: fvs,
                fieldactualchange: fac,
            }
            .encode_to_vec()
        }
        (ArchiverValue::VectorString(v), ArchDbType::WaveformString) => {
            epics_event::VectorString {
                secondsintoyear: secs,
                nano: nanos,
                val: v.clone(),
                severity,
                status,
                repeatcount: rc,
                fieldvalues: fvs,
                fieldactualchange: fac,
            }
            .encode_to_vec()
        }
        (ArchiverValue::VectorChar(v), ArchDbType::WaveformByte) => {
            epics_event::VectorChar {
                secondsintoyear: secs,
                nano: nanos,
                val: v.clone(),
                severity,
                status,
                repeatcount: rc,
                fieldvalues: fvs,
                fieldactualchange: fac,
            }
            .encode_to_vec()
        }
        (ArchiverValue::VectorShort(v), ArchDbType::WaveformShort) => {
            epics_event::VectorShort {
                secondsintoyear: secs,
                nano: nanos,
                val: v.clone(),
                severity,
                status,
                repeatcount: rc,
                fieldvalues: fvs,
                fieldactualchange: fac,
            }
            .encode_to_vec()
        }
        (ArchiverValue::VectorInt(v), ArchDbType::WaveformInt) => {
            epics_event::VectorInt {
                secondsintoyear: secs,
                nano: nanos,
                val: v.clone(),
                severity,
                status,
                repeatcount: rc,
                fieldvalues: fvs,
                fieldactualchange: fac,
            }
            .encode_to_vec()
        }
        (ArchiverValue::VectorEnum(v), ArchDbType::WaveformEnum) => {
            epics_event::VectorEnum {
                secondsintoyear: secs,
                nano: nanos,
                val: v.clone(),
                severity,
                status,
                repeatcount: rc,
                fieldvalues: fvs,
                fieldactualchange: fac,
            }
            .encode_to_vec()
        }
        (ArchiverValue::VectorFloat(v), ArchDbType::WaveformFloat) => {
            epics_event::VectorFloat {
                secondsintoyear: secs,
                nano: nanos,
                val: v.clone(),
                severity,
                status,
                repeatcount: rc,
                fieldvalues: fvs,
                fieldactualchange: fac,
            }
            .encode_to_vec()
        }
        (ArchiverValue::VectorDouble(v), ArchDbType::WaveformDouble) => {
            epics_event::VectorDouble {
                secondsintoyear: secs,
                nano: nanos,
                val: v.clone(),
                severity,
                status,
                repeatcount: rc,
                fieldvalues: fvs,
                fieldactualchange: fac,
            }
            .encode_to_vec()
        }
        (ArchiverValue::V4GenericBytes(v), ArchDbType::V4GenericBytes) => {
            epics_event::V4GenericBytes {
                secondsintoyear: secs,
                nano: nanos,
                val: v.clone(),
                severity,
                status,
                repeatcount: rc,
                fieldvalues: fvs,
                fieldactualchange: fac,
                user_tag: None,
            }
            .encode_to_vec()
        }
        _ => anyhow::bail!(
            "Value type {:?} does not match dbr_type {:?}",
            sample.value.db_type(),
            dbr_type
        ),
    };

    Ok(bytes)
}
