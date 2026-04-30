use std::fs::File;
use std::io::{BufRead, BufReader, Seek, SeekFrom};
use std::path::Path;
use std::time::SystemTime;

use archiver_proto::epics_event::PayloadInfo;
use prost::Message;

use crate::storage::plainpb::codec;
use crate::types::ArchDbType;

use super::reader::decode_sample;

/// Binary search within a PB file to find the position of the first sample
/// at or after the given timestamp.
///
/// Returns the byte offset into the file where the matching line starts,
/// or None if all samples are before the target time.
pub fn binary_search_pb_file(path: &Path, target: SystemTime) -> anyhow::Result<Option<u64>> {
    let file = File::open(path)?;
    let file_len = file.metadata()?.len();
    let mut reader = BufReader::new(file);

    // Read header to get year and dbr_type.
    let mut header_line = Vec::new();
    reader.read_until(codec::NEWLINE, &mut header_line)?;
    if header_line.last() == Some(&codec::NEWLINE) {
        header_line.pop();
    }
    let header_bytes = codec::unescape(&header_line);
    let payload_info = PayloadInfo::decode(header_bytes.as_slice())?;
    let year = payload_info.year;
    let dbr_type = ArchDbType::from_i32(payload_info.r#type).unwrap_or(ArchDbType::ScalarDouble);

    let data_start = reader.stream_position()?;

    if data_start >= file_len {
        return Ok(None);
    }

    let mut low = data_start;
    let mut high = file_len;
    let mut result: Option<u64> = None;

    while low < high {
        let mid = low + (high - low) / 2;
        reader.seek(SeekFrom::Start(mid))?;

        // Skip to next line boundary (we might land mid-line).
        if mid != data_start {
            let mut skip = Vec::new();
            reader.read_until(codec::NEWLINE, &mut skip)?;
        }

        let line_start = reader.stream_position()?;
        if line_start >= file_len {
            high = mid;
            continue;
        }

        let mut line = Vec::new();
        let bytes_read = reader.read_until(codec::NEWLINE, &mut line)?;
        if bytes_read == 0 {
            high = mid;
            continue;
        }
        if line.last() == Some(&codec::NEWLINE) {
            line.pop();
        }
        if line.is_empty() {
            high = mid;
            continue;
        }

        let raw = codec::unescape(&line);
        match decode_sample(dbr_type, year, &raw) {
            Ok(sample) => {
                if sample.timestamp >= target {
                    result = Some(line_start);
                    high = mid;
                } else {
                    low = reader.stream_position()?;
                }
            }
            Err(_) => {
                // Corrupted line — skip forward.
                low = reader.stream_position()?;
            }
        }
    }

    Ok(result)
}
