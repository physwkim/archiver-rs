use std::fs::File;
use std::io::{BufRead, BufReader, Seek, SeekFrom};
use std::path::Path;
use std::time::SystemTime;

use archiver_proto::epics_event::{self, PayloadInfo};
use prost::Message;

use crate::storage::plainpb::codec;
use crate::storage::plainpb::search::binary_search_pb_file;
use crate::storage::traits::EventStream;
use crate::types::{ArchDbType, ArchiverSample, ArchiverValue, EventStreamDesc};

/// Reads a PlainPB file, yielding one ArchiverSample per line.
/// Uses binary-safe line reading (read_until) since PB data may contain non-UTF8 bytes.
pub struct PbFileReader {
    desc: EventStreamDesc,
    reader: BufReader<File>,
}

impl PbFileReader {
    /// Open a PB file. Reads and parses the PayloadInfo header from the first line.
    pub fn open(path: &Path) -> anyhow::Result<Self> {
        let file = File::open(path)?;
        let mut reader = BufReader::new(file);

        // Read first line (header).
        let mut header_line = Vec::new();
        reader.read_until(codec::NEWLINE, &mut header_line)?;
        // Strip trailing newline.
        if header_line.last() == Some(&codec::NEWLINE) {
            header_line.pop();
        }

        let header_bytes = codec::unescape(&header_line);
        let payload_info = PayloadInfo::decode(header_bytes.as_slice())?;
        let desc = EventStreamDesc::from_payload_info(&payload_info);

        Ok(Self { desc, reader })
    }

    /// Open a PB file and seek to the first sample >= start_time using binary search.
    /// Falls back to reading from the beginning if binary search finds nothing or fails.
    pub fn open_seeked(path: &Path, start_time: SystemTime) -> anyhow::Result<Self> {
        // First, run binary search on a separate file handle.
        let offset = binary_search_pb_file(path, start_time).ok().flatten();

        // Open the file normally (reads header).
        let file = File::open(path)?;
        let mut reader = BufReader::new(file);

        // Read header.
        let mut header_line = Vec::new();
        reader.read_until(codec::NEWLINE, &mut header_line)?;
        if header_line.last() == Some(&codec::NEWLINE) {
            header_line.pop();
        }
        let header_bytes = codec::unescape(&header_line);
        let payload_info = PayloadInfo::decode(header_bytes.as_slice())?;
        let desc = EventStreamDesc::from_payload_info(&payload_info);

        // Seek to the binary search result if found.
        if let Some(off) = offset {
            reader.seek(SeekFrom::Start(off))?;
        }

        Ok(Self { desc, reader })
    }
}

impl EventStream for PbFileReader {
    fn description(&self) -> &EventStreamDesc {
        &self.desc
    }

    fn next_event(&mut self) -> anyhow::Result<Option<ArchiverSample>> {
        loop {
            let mut line_buf = Vec::new();
            let bytes_read = self.reader.read_until(codec::NEWLINE, &mut line_buf)?;
            if bytes_read == 0 {
                return Ok(None);
            }

            // Strip trailing newline.
            if line_buf.last() == Some(&codec::NEWLINE) {
                line_buf.pop();
            }

            if line_buf.is_empty() {
                continue;
            }

            let raw_bytes = codec::unescape(&line_buf);
            let sample = decode_sample(self.desc.db_type, self.desc.year, &raw_bytes)?;
            return Ok(Some(sample));
        }
    }
}

/// Decode a protobuf sample from raw bytes based on the DBR type.
pub fn decode_sample(
    dbr_type: ArchDbType,
    year: i32,
    data: &[u8],
) -> anyhow::Result<ArchiverSample> {
    match dbr_type {
        ArchDbType::ScalarString => {
            let msg = epics_event::ScalarString::decode(data)?;
            sample_from_parts(
                year,
                msg.secondsintoyear,
                msg.nano,
                ArchiverValue::ScalarString(msg.val),
                msg.severity,
                msg.status,
                msg.repeatcount,
                &msg.fieldvalues,
                msg.fieldactualchange,
            )
        }
        ArchDbType::ScalarByte => {
            let msg = epics_event::ScalarByte::decode(data)?;
            sample_from_parts(
                year,
                msg.secondsintoyear,
                msg.nano,
                ArchiverValue::ScalarByte(msg.val),
                msg.severity,
                msg.status,
                msg.repeatcount,
                &msg.fieldvalues,
                msg.fieldactualchange,
            )
        }
        ArchDbType::ScalarShort => {
            let msg = epics_event::ScalarShort::decode(data)?;
            sample_from_parts(
                year,
                msg.secondsintoyear,
                msg.nano,
                ArchiverValue::ScalarShort(msg.val),
                msg.severity,
                msg.status,
                msg.repeatcount,
                &msg.fieldvalues,
                msg.fieldactualchange,
            )
        }
        ArchDbType::ScalarInt => {
            let msg = epics_event::ScalarInt::decode(data)?;
            sample_from_parts(
                year,
                msg.secondsintoyear,
                msg.nano,
                ArchiverValue::ScalarInt(msg.val),
                msg.severity,
                msg.status,
                msg.repeatcount,
                &msg.fieldvalues,
                msg.fieldactualchange,
            )
        }
        ArchDbType::ScalarEnum => {
            let msg = epics_event::ScalarEnum::decode(data)?;
            sample_from_parts(
                year,
                msg.secondsintoyear,
                msg.nano,
                ArchiverValue::ScalarEnum(msg.val),
                msg.severity,
                msg.status,
                msg.repeatcount,
                &msg.fieldvalues,
                msg.fieldactualchange,
            )
        }
        ArchDbType::ScalarFloat => {
            let msg = epics_event::ScalarFloat::decode(data)?;
            sample_from_parts(
                year,
                msg.secondsintoyear,
                msg.nano,
                ArchiverValue::ScalarFloat(msg.val),
                msg.severity,
                msg.status,
                msg.repeatcount,
                &msg.fieldvalues,
                msg.fieldactualchange,
            )
        }
        ArchDbType::ScalarDouble => {
            let msg = epics_event::ScalarDouble::decode(data)?;
            sample_from_parts(
                year,
                msg.secondsintoyear,
                msg.nano,
                ArchiverValue::ScalarDouble(msg.val),
                msg.severity,
                msg.status,
                msg.repeatcount,
                &msg.fieldvalues,
                msg.fieldactualchange,
            )
        }
        ArchDbType::WaveformString => {
            let msg = epics_event::VectorString::decode(data)?;
            sample_from_parts(
                year,
                msg.secondsintoyear,
                msg.nano,
                ArchiverValue::VectorString(msg.val),
                msg.severity,
                msg.status,
                msg.repeatcount,
                &msg.fieldvalues,
                msg.fieldactualchange,
            )
        }
        ArchDbType::WaveformByte => {
            let msg = epics_event::VectorChar::decode(data)?;
            sample_from_parts(
                year,
                msg.secondsintoyear,
                msg.nano,
                ArchiverValue::VectorChar(msg.val),
                msg.severity,
                msg.status,
                msg.repeatcount,
                &msg.fieldvalues,
                msg.fieldactualchange,
            )
        }
        ArchDbType::WaveformShort => {
            let msg = epics_event::VectorShort::decode(data)?;
            sample_from_parts(
                year,
                msg.secondsintoyear,
                msg.nano,
                ArchiverValue::VectorShort(msg.val),
                msg.severity,
                msg.status,
                msg.repeatcount,
                &msg.fieldvalues,
                msg.fieldactualchange,
            )
        }
        ArchDbType::WaveformInt => {
            let msg = epics_event::VectorInt::decode(data)?;
            sample_from_parts(
                year,
                msg.secondsintoyear,
                msg.nano,
                ArchiverValue::VectorInt(msg.val),
                msg.severity,
                msg.status,
                msg.repeatcount,
                &msg.fieldvalues,
                msg.fieldactualchange,
            )
        }
        ArchDbType::WaveformEnum => {
            let msg = epics_event::VectorEnum::decode(data)?;
            sample_from_parts(
                year,
                msg.secondsintoyear,
                msg.nano,
                ArchiverValue::VectorEnum(msg.val),
                msg.severity,
                msg.status,
                msg.repeatcount,
                &msg.fieldvalues,
                msg.fieldactualchange,
            )
        }
        ArchDbType::WaveformFloat => {
            let msg = epics_event::VectorFloat::decode(data)?;
            sample_from_parts(
                year,
                msg.secondsintoyear,
                msg.nano,
                ArchiverValue::VectorFloat(msg.val),
                msg.severity,
                msg.status,
                msg.repeatcount,
                &msg.fieldvalues,
                msg.fieldactualchange,
            )
        }
        ArchDbType::WaveformDouble => {
            let msg = epics_event::VectorDouble::decode(data)?;
            sample_from_parts(
                year,
                msg.secondsintoyear,
                msg.nano,
                ArchiverValue::VectorDouble(msg.val),
                msg.severity,
                msg.status,
                msg.repeatcount,
                &msg.fieldvalues,
                msg.fieldactualchange,
            )
        }
        ArchDbType::V4GenericBytes => {
            let msg = epics_event::V4GenericBytes::decode(data)?;
            sample_from_parts(
                year,
                msg.secondsintoyear,
                msg.nano,
                ArchiverValue::V4GenericBytes(msg.val),
                msg.severity,
                msg.status,
                msg.repeatcount,
                &msg.fieldvalues,
                msg.fieldactualchange,
            )
        }
    }
}

#[allow(clippy::too_many_arguments)]
fn sample_from_parts(
    year: i32,
    seconds_into_year: u32,
    nanos: u32,
    value: ArchiverValue,
    severity: Option<i32>,
    status: Option<i32>,
    repeat_count: Option<u32>,
    field_values: &[epics_event::FieldValue],
    field_actual_change: Option<bool>,
) -> anyhow::Result<ArchiverSample> {
    let timestamp = ArchiverSample::timestamp_from_epoch_parts(year, seconds_into_year, nanos)
        .ok_or_else(|| anyhow::anyhow!("invalid timestamp: year={year} secs={seconds_into_year} nanos={nanos}"))?;
    Ok(ArchiverSample {
        timestamp,
        value,
        severity: severity.unwrap_or(0),
        status: status.unwrap_or(0),
        repeat_count,
        field_values: field_values
            .iter()
            .map(|fv| (fv.name.clone(), fv.val.clone()))
            .collect(),
        field_actual_change: field_actual_change.unwrap_or(false),
    })
}
