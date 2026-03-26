use std::time::SystemTime;

use archiver_proto::epics_event::{self, PayloadType};
use chrono::{Datelike, NaiveDateTime, Utc};
use serde::{Deserialize, Serialize};

/// Maps to PayloadType in EPICSEvent.proto and ArchDBRTypes in Java archiver.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[repr(i32)]
pub enum ArchDbType {
    ScalarString = 0,
    ScalarShort = 1,
    ScalarFloat = 2,
    ScalarEnum = 3,
    ScalarByte = 4,
    ScalarInt = 5,
    ScalarDouble = 6,
    WaveformString = 7,
    WaveformShort = 8,
    WaveformFloat = 9,
    WaveformEnum = 10,
    WaveformByte = 11,
    WaveformInt = 12,
    WaveformDouble = 13,
    V4GenericBytes = 14,
}

impl ArchDbType {
    pub fn from_i32(v: i32) -> Option<Self> {
        match v {
            0 => Some(Self::ScalarString),
            1 => Some(Self::ScalarShort),
            2 => Some(Self::ScalarFloat),
            3 => Some(Self::ScalarEnum),
            4 => Some(Self::ScalarByte),
            5 => Some(Self::ScalarInt),
            6 => Some(Self::ScalarDouble),
            7 => Some(Self::WaveformString),
            8 => Some(Self::WaveformShort),
            9 => Some(Self::WaveformFloat),
            10 => Some(Self::WaveformEnum),
            11 => Some(Self::WaveformByte),
            12 => Some(Self::WaveformInt),
            13 => Some(Self::WaveformDouble),
            14 => Some(Self::V4GenericBytes),
            _ => None,
        }
    }

    pub fn to_payload_type(self) -> PayloadType {
        PayloadType::try_from(self as i32).unwrap()
    }

    pub fn is_waveform(self) -> bool {
        matches!(
            self,
            Self::WaveformString
                | Self::WaveformShort
                | Self::WaveformFloat
                | Self::WaveformEnum
                | Self::WaveformByte
                | Self::WaveformInt
                | Self::WaveformDouble
        )
    }
}

/// The unified value type for all archived data.
#[derive(Debug, Clone, PartialEq)]
pub enum ArchiverValue {
    ScalarString(String),
    ScalarByte(Vec<u8>),
    ScalarShort(i32),
    ScalarInt(i32),
    ScalarEnum(i32),
    ScalarFloat(f32),
    ScalarDouble(f64),
    VectorString(Vec<String>),
    VectorChar(Vec<u8>),
    VectorShort(Vec<i32>),
    VectorInt(Vec<i32>),
    VectorEnum(Vec<i32>),
    VectorFloat(Vec<f32>),
    VectorDouble(Vec<f64>),
    V4GenericBytes(Vec<u8>),
}

impl ArchiverValue {
    pub fn db_type(&self) -> ArchDbType {
        match self {
            Self::ScalarString(_) => ArchDbType::ScalarString,
            Self::ScalarByte(_) => ArchDbType::ScalarByte,
            Self::ScalarShort(_) => ArchDbType::ScalarShort,
            Self::ScalarInt(_) => ArchDbType::ScalarInt,
            Self::ScalarEnum(_) => ArchDbType::ScalarEnum,
            Self::ScalarFloat(_) => ArchDbType::ScalarFloat,
            Self::ScalarDouble(_) => ArchDbType::ScalarDouble,
            Self::VectorString(_) => ArchDbType::WaveformString,
            Self::VectorChar(_) => ArchDbType::WaveformByte,
            Self::VectorShort(_) => ArchDbType::WaveformShort,
            Self::VectorInt(_) => ArchDbType::WaveformInt,
            Self::VectorEnum(_) => ArchDbType::WaveformEnum,
            Self::VectorFloat(_) => ArchDbType::WaveformFloat,
            Self::VectorDouble(_) => ArchDbType::WaveformDouble,
            Self::V4GenericBytes(_) => ArchDbType::V4GenericBytes,
        }
    }

    /// Try to extract a f64 representation (for postprocessors like mean/max/min).
    pub fn as_f64(&self) -> Option<f64> {
        match self {
            Self::ScalarDouble(v) => Some(*v),
            Self::ScalarFloat(v) => Some(*v as f64),
            Self::ScalarInt(v) => Some(*v as f64),
            Self::ScalarShort(v) => Some(*v as f64),
            Self::ScalarEnum(v) => Some(*v as f64),
            _ => None,
        }
    }
}

/// A single archived sample — the unified internal representation.
#[derive(Debug, Clone)]
pub struct ArchiverSample {
    pub timestamp: SystemTime,
    pub value: ArchiverValue,
    pub severity: i32,
    pub status: i32,
    pub repeat_count: Option<u32>,
    pub field_values: Vec<(String, String)>,
    pub field_actual_change: bool,
}

impl ArchiverSample {
    pub fn new(timestamp: SystemTime, value: ArchiverValue) -> Self {
        Self {
            timestamp,
            value,
            severity: 0,
            status: 0,
            repeat_count: None,
            field_values: Vec::new(),
            field_actual_change: false,
        }
    }

    /// Decompose timestamp into (year, seconds_into_year, nanos).
    pub fn decompose_timestamp(&self) -> (i32, u32, u32) {
        let datetime = chrono::DateTime::<Utc>::from(self.timestamp);
        let year = datetime.year();
        let year_start =
            NaiveDateTime::parse_from_str(&format!("{year}-01-01 00:00:00"), "%Y-%m-%d %H:%M:%S")
                .unwrap()
                .and_utc();
        let duration = datetime.signed_duration_since(year_start);
        let seconds_into_year = duration.num_seconds() as u32;
        let nanos = datetime.timestamp_subsec_nanos();
        (year, seconds_into_year, nanos)
    }

    /// Reconstruct a SystemTime from year + seconds_into_year + nanos.
    pub fn timestamp_from_epoch_parts(year: i32, seconds_into_year: u32, nanos: u32) -> SystemTime {
        let year_start =
            NaiveDateTime::parse_from_str(&format!("{year}-01-01 00:00:00"), "%Y-%m-%d %H:%M:%S")
                .unwrap()
                .and_utc();
        let ts = year_start
            + chrono::Duration::seconds(seconds_into_year as i64)
            + chrono::Duration::nanoseconds(nanos as i64);
        ts.into()
    }

    /// Create a sample from a UNIX epoch timestamp (seconds as f64).
    pub fn from_unix_timestamp(epoch_secs: f64, value: ArchiverValue) -> Self {
        let secs = epoch_secs as u64;
        let nanos = ((epoch_secs - secs as f64) * 1e9) as u32;
        let ts = SystemTime::UNIX_EPOCH
            + std::time::Duration::new(secs, nanos);
        Self::new(ts, value)
    }
}

/// Description of an event stream (used in reader).
#[derive(Debug, Clone)]
pub struct EventStreamDesc {
    pub pv_name: String,
    pub db_type: ArchDbType,
    pub year: i32,
    pub element_count: Option<i32>,
    pub headers: Vec<(String, String)>,
}

impl EventStreamDesc {
    pub fn from_payload_info(info: &epics_event::PayloadInfo) -> Self {
        let db_type = ArchDbType::from_i32(info.r#type).unwrap_or(ArchDbType::ScalarDouble);
        Self {
            pv_name: info.pvname.clone(),
            db_type,
            year: info.year,
            element_count: info.element_count,
            headers: info
                .headers
                .iter()
                .map(|fv| (fv.name.clone(), fv.val.clone()))
                .collect(),
        }
    }
}
