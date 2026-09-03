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

    pub fn to_payload_type(self) -> Option<PayloadType> {
        PayloadType::try_from(self as i32).ok()
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

    /// The type a PV of this type archives as when its channel carries
    /// `element_count` elements: arrays take the waveform form of a
    /// scalar type; scalars, waveforms and the V4 types are unchanged.
    /// The one element-count rule — the registry applies it to every
    /// row it writes, so callers derive only the native scalar type.
    pub fn with_element_count(self, element_count: i32) -> Self {
        if element_count <= 1 {
            return self;
        }
        match self {
            Self::ScalarString => Self::WaveformString,
            Self::ScalarShort => Self::WaveformShort,
            Self::ScalarFloat => Self::WaveformFloat,
            Self::ScalarEnum => Self::WaveformEnum,
            Self::ScalarByte => Self::WaveformByte,
            Self::ScalarInt => Self::WaveformInt,
            Self::ScalarDouble => Self::WaveformDouble,
            other => other,
        }
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

    /// Convert to another archived type "through a number" — the rule
    /// `changeTypeForPV` applies to a PV's existing partitions (Java
    /// parity: `ThruNumberConversion`). Numeric families convert via
    /// `f64` with `as` semantics (truncation toward zero, saturation at
    /// the target's bounds); strings parse and format; a scalar becomes
    /// a one-element waveform and a waveform collapses to its first
    /// element. `V4GenericBytes` is opaque and converts neither way.
    pub fn convert_to(&self, target: ArchDbType) -> anyhow::Result<ArchiverValue> {
        if self.db_type() == target {
            return Ok(self.clone());
        }
        let elems = self.elements()?;
        let first = || -> anyhow::Result<&Elem> {
            elems.first().ok_or_else(|| {
                anyhow::anyhow!("cannot convert an empty waveform to scalar {target:?}")
            })
        };
        let all_num = || -> anyhow::Result<Vec<f64>> { elems.iter().map(Elem::to_f64).collect() };
        Ok(match target {
            ArchDbType::ScalarString => Self::ScalarString(first()?.to_text()),
            ArchDbType::ScalarByte => Self::ScalarByte(vec![first()?.to_f64()? as i8 as u8]),
            ArchDbType::ScalarShort => Self::ScalarShort(first()?.to_f64()? as i16 as i32),
            ArchDbType::ScalarEnum => Self::ScalarEnum(first()?.to_f64()? as i16 as i32),
            ArchDbType::ScalarInt => Self::ScalarInt(first()?.to_f64()? as i32),
            ArchDbType::ScalarFloat => Self::ScalarFloat(first()?.to_f64()? as f32),
            ArchDbType::ScalarDouble => Self::ScalarDouble(first()?.to_f64()?),
            ArchDbType::WaveformString => {
                Self::VectorString(elems.iter().map(Elem::to_text).collect())
            }
            ArchDbType::WaveformByte => {
                Self::VectorChar(all_num()?.into_iter().map(|f| f as i8 as u8).collect())
            }
            ArchDbType::WaveformShort => {
                Self::VectorShort(all_num()?.into_iter().map(|f| f as i16 as i32).collect())
            }
            ArchDbType::WaveformEnum => {
                Self::VectorEnum(all_num()?.into_iter().map(|f| f as i16 as i32).collect())
            }
            ArchDbType::WaveformInt => {
                Self::VectorInt(all_num()?.into_iter().map(|f| f as i32).collect())
            }
            ArchDbType::WaveformFloat => {
                Self::VectorFloat(all_num()?.into_iter().map(|f| f as f32).collect())
            }
            ArchDbType::WaveformDouble => Self::VectorDouble(all_num()?),
            ArchDbType::V4GenericBytes => {
                anyhow::bail!(
                    "cannot convert {:?} to opaque V4GenericBytes",
                    self.db_type()
                )
            }
        })
    }

    /// Element-wise view for `convert_to`. Bytes are signed (EPICS
    /// DBR_CHAR); a scalar is one element.
    fn elements(&self) -> anyhow::Result<Vec<Elem>> {
        Ok(match self {
            Self::ScalarString(s) => vec![Elem::Text(s.clone())],
            Self::ScalarByte(b) | Self::VectorChar(b) => {
                b.iter().map(|x| Elem::Num(f64::from(*x as i8))).collect()
            }
            Self::ScalarShort(v) | Self::ScalarInt(v) | Self::ScalarEnum(v) => {
                vec![Elem::Num(f64::from(*v))]
            }
            Self::ScalarFloat(v) => vec![Elem::Num(f64::from(*v))],
            Self::ScalarDouble(v) => vec![Elem::Num(*v)],
            Self::VectorString(v) => v.iter().map(|s| Elem::Text(s.clone())).collect(),
            Self::VectorShort(v) | Self::VectorInt(v) | Self::VectorEnum(v) => {
                v.iter().map(|x| Elem::Num(f64::from(*x))).collect()
            }
            Self::VectorFloat(v) => v.iter().map(|x| Elem::Num(f64::from(*x))).collect(),
            Self::VectorDouble(v) => v.iter().map(|x| Elem::Num(*x)).collect(),
            Self::V4GenericBytes(_) => {
                anyhow::bail!("cannot convert opaque V4GenericBytes to another type")
            }
        })
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
    ///
    /// Errors instead of panicking when the timestamp lies outside the
    /// calendar range chrono can represent (|year| > 262143): the PB
    /// frame and the partition name both need a civil year, and a
    /// panic here unwinds whichever task carried the sample (a shard's
    /// append, an ETL move, a retrieval handler).
    pub fn decompose_timestamp(&self) -> anyhow::Result<(i32, u32, u32)> {
        let datetime = utc_datetime_checked(self.timestamp).ok_or_else(|| {
            anyhow::anyhow!(
                "sample timestamp {:?} is outside the representable calendar range",
                self.timestamp
            )
        })?;
        let year = datetime.year();
        // Jan 1 00:00:00 of a year chrono already represents is valid.
        let year_start = NaiveDateTime::new(
            chrono::NaiveDate::from_ymd_opt(year, 1, 1).expect("Jan 1 of an in-range year"),
            chrono::NaiveTime::from_hms_opt(0, 0, 0).expect("midnight"),
        )
        .and_utc();
        let duration = datetime.signed_duration_since(year_start);
        let seconds_into_year = duration.num_seconds() as u32;
        let nanos = datetime.timestamp_subsec_nanos();
        Ok((year, seconds_into_year, nanos))
    }

    /// Reconstruct a SystemTime from year + seconds_into_year + nanos.
    pub fn timestamp_from_epoch_parts(
        year: i32,
        seconds_into_year: u32,
        nanos: u32,
    ) -> Option<SystemTime> {
        let year_start = chrono::NaiveDate::from_ymd_opt(year, 1, 1)?
            .and_hms_opt(0, 0, 0)?
            .and_utc();
        let ts = year_start
            + chrono::Duration::seconds(seconds_into_year as i64)
            + chrono::Duration::nanoseconds(nanos as i64);
        Some(ts.into())
    }

    /// Create a sample from a UNIX epoch timestamp (seconds as f64).
    pub fn from_unix_timestamp(epoch_secs: f64, value: ArchiverValue) -> Self {
        let secs = epoch_secs as u64;
        let nanos = ((epoch_secs - secs as f64) * 1e9) as u32;
        let ts = SystemTime::UNIX_EPOCH + std::time::Duration::new(secs, nanos);
        Self::new(ts, value)
    }
}

/// One element of an [`ArchiverValue`] as `convert_to` sees it.
enum Elem {
    Num(f64),
    Text(String),
}

impl Elem {
    fn to_f64(&self) -> anyhow::Result<f64> {
        match self {
            Self::Num(f) => Ok(*f),
            Self::Text(s) => s
                .trim()
                .parse::<f64>()
                .map_err(|e| anyhow::anyhow!("cannot convert string {s:?} to a number: {e}")),
        }
    }

    fn to_text(&self) -> String {
        match self {
            Self::Num(f) => f.to_string(),
            Self::Text(s) => s.clone(),
        }
    }
}

/// Non-panicking `SystemTime` → `DateTime<Utc>` conversion. `From` in
/// chrono unwraps and panics once the year leaves ±262143; every
/// timestamp that reaches storage or retrieval goes through here (or
/// the clamping variant in `partition`) instead.
pub fn utc_datetime_checked(ts: SystemTime) -> Option<chrono::DateTime<Utc>> {
    let (secs, nanos) = match ts.duration_since(SystemTime::UNIX_EPOCH) {
        Ok(d) => (i64::try_from(d.as_secs()).ok()?, d.subsec_nanos()),
        Err(e) => {
            // Before the epoch: borrow one second so nanos stay positive.
            let d = e.duration();
            let s = i64::try_from(d.as_secs()).ok()?;
            if d.subsec_nanos() == 0 {
                (s.checked_neg()?, 0)
            } else {
                (
                    s.checked_neg()?.checked_sub(1)?,
                    1_000_000_000 - d.subsec_nanos(),
                )
            }
        }
    };
    chrono::DateTime::<Utc>::from_timestamp(secs, nanos)
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

/// Render an [`ArchiverValue`] as JSON.
///
/// Java parity (d4b783a): non-finite floats (`NaN`, `±Inf`) serialize
/// to `null` rather than panicking inside `serde_json::Number::from_f64`'s
/// finite-only contract. Centralised here so every callsite (retrieval,
/// management migration, archiver_control snapshot) stays in lockstep
/// when the rendering rules evolve.
pub fn archiver_value_to_json(v: &ArchiverValue) -> serde_json::Value {
    use serde_json::Value;
    match v {
        ArchiverValue::ScalarString(s) => Value::String(s.clone()),
        ArchiverValue::ScalarShort(n) => (*n).into(),
        ArchiverValue::ScalarInt(n) => (*n).into(),
        ArchiverValue::ScalarEnum(n) => (*n).into(),
        ArchiverValue::ScalarFloat(f) => finite_or_null(*f as f64),
        ArchiverValue::ScalarDouble(f) => finite_or_null(*f),
        ArchiverValue::ScalarByte(b) => Value::Array(b.iter().map(|x| (*x).into()).collect()),
        ArchiverValue::VectorString(arr) => {
            Value::Array(arr.iter().map(|s| Value::String(s.clone())).collect())
        }
        ArchiverValue::VectorChar(arr) => Value::Array(arr.iter().map(|x| (*x).into()).collect()),
        ArchiverValue::VectorShort(arr) => Value::Array(arr.iter().map(|x| (*x).into()).collect()),
        ArchiverValue::VectorInt(arr) => Value::Array(arr.iter().map(|x| (*x).into()).collect()),
        ArchiverValue::VectorEnum(arr) => Value::Array(arr.iter().map(|x| (*x).into()).collect()),
        ArchiverValue::VectorFloat(arr) => {
            Value::Array(arr.iter().map(|x| finite_or_null(*x as f64)).collect())
        }
        ArchiverValue::VectorDouble(arr) => {
            Value::Array(arr.iter().map(|x| finite_or_null(*x)).collect())
        }
        ArchiverValue::V4GenericBytes(b) => Value::Array(b.iter().map(|x| (*x).into()).collect()),
    }
}

/// Map a finite f64 to `Number(n)` or, for `NaN` / `±Inf`, to JSON `null`.
/// Java parity (d4b783a).
pub fn finite_or_null(f: f64) -> serde_json::Value {
    if f.is_finite() {
        f.into()
    } else {
        serde_json::Value::Null
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    #[test]
    fn convert_to_numeric_truncates_toward_zero_and_saturates() {
        let d = ArchiverValue::ScalarDouble(-1.9);
        assert_eq!(
            d.convert_to(ArchDbType::ScalarInt).unwrap(),
            ArchiverValue::ScalarInt(-1)
        );
        assert_eq!(
            ArchiverValue::ScalarDouble(70000.0)
                .convert_to(ArchDbType::ScalarShort)
                .unwrap(),
            ArchiverValue::ScalarShort(i16::MAX as i32)
        );
        assert_eq!(
            ArchiverValue::ScalarInt(300)
                .convert_to(ArchDbType::ScalarByte)
                .unwrap(),
            ArchiverValue::ScalarByte(vec![i8::MAX as u8])
        );
        assert_eq!(
            ArchiverValue::ScalarByte(vec![0xFF])
                .convert_to(ArchDbType::ScalarInt)
                .unwrap(),
            ArchiverValue::ScalarInt(-1),
            "bytes are signed DBR_CHAR"
        );
        assert_eq!(
            ArchiverValue::ScalarDouble(1.5)
                .convert_to(ArchDbType::ScalarFloat)
                .unwrap(),
            ArchiverValue::ScalarFloat(1.5)
        );
    }

    #[test]
    fn convert_to_string_formats_and_parses() {
        assert_eq!(
            ArchiverValue::ScalarDouble(2.5)
                .convert_to(ArchDbType::ScalarString)
                .unwrap(),
            ArchiverValue::ScalarString("2.5".into())
        );
        assert_eq!(
            ArchiverValue::ScalarString(" 42 ".into())
                .convert_to(ArchDbType::ScalarInt)
                .unwrap(),
            ArchiverValue::ScalarInt(42)
        );
        let err = ArchiverValue::ScalarString("abc".into())
            .convert_to(ArchDbType::ScalarDouble)
            .unwrap_err();
        assert!(err.to_string().contains("cannot convert string"));
    }

    #[test]
    fn convert_to_changes_shape_like_java_thru_number() {
        assert_eq!(
            ArchiverValue::ScalarInt(7)
                .convert_to(ArchDbType::WaveformDouble)
                .unwrap(),
            ArchiverValue::VectorDouble(vec![7.0])
        );
        assert_eq!(
            ArchiverValue::VectorDouble(vec![3.7, 9.9])
                .convert_to(ArchDbType::ScalarInt)
                .unwrap(),
            ArchiverValue::ScalarInt(3),
            "waveform → scalar takes the first element"
        );
        let err = ArchiverValue::VectorDouble(vec![])
            .convert_to(ArchDbType::ScalarDouble)
            .unwrap_err();
        assert!(err.to_string().contains("empty waveform"));
        assert_eq!(
            ArchiverValue::VectorInt(vec![1, 2])
                .convert_to(ArchDbType::WaveformString)
                .unwrap(),
            ArchiverValue::VectorString(vec!["1".into(), "2".into()])
        );
    }

    #[test]
    fn convert_to_same_type_and_opaque_bytes() {
        let v = ArchiverValue::VectorFloat(vec![1.0, 2.0]);
        assert_eq!(v.convert_to(ArchDbType::WaveformFloat).unwrap(), v);
        assert!(
            ArchiverValue::V4GenericBytes(vec![1])
                .convert_to(ArchDbType::ScalarDouble)
                .is_err()
        );
        assert!(
            ArchiverValue::ScalarDouble(1.0)
                .convert_to(ArchDbType::V4GenericBytes)
                .is_err()
        );
    }

    #[test]
    fn decompose_timestamp_round_trips_through_epoch_parts() {
        let ts = SystemTime::UNIX_EPOCH + Duration::new(1_700_000_000, 250);
        let s = ArchiverSample::new(ts, ArchiverValue::ScalarDouble(0.0));
        let (year, secs, nanos) = s.decompose_timestamp().unwrap();
        assert_eq!(year, 2023);
        assert_eq!(nanos, 250);
        assert_eq!(
            ArchiverSample::timestamp_from_epoch_parts(year, secs, nanos),
            Some(ts)
        );
    }

    #[test]
    fn decompose_timestamp_errors_instead_of_panicking_out_of_range() {
        // ~1.4e11 years past the epoch: representable as SystemTime,
        // far outside chrono's ±262143-year calendar.
        let ts = SystemTime::UNIX_EPOCH
            .checked_add(Duration::from_secs(1 << 62))
            .expect("SystemTime holds i64 seconds on this platform");
        let s = ArchiverSample::new(ts, ArchiverValue::ScalarDouble(0.0));
        let err = s.decompose_timestamp().unwrap_err();
        assert!(err.to_string().contains("outside the representable"));
        assert!(utc_datetime_checked(ts).is_none());
    }

    #[test]
    fn utc_datetime_checked_handles_pre_epoch_fractions() {
        let ts = SystemTime::UNIX_EPOCH - Duration::new(1, 500_000_000);
        let dt = utc_datetime_checked(ts).unwrap();
        assert_eq!(dt.timestamp(), -2);
        assert_eq!(dt.timestamp_subsec_nanos(), 500_000_000);
        assert_eq!(dt.year(), 1969);
    }
}

#[cfg(test)]
mod element_count_tests {
    use super::ArchDbType;

    #[test]
    fn with_element_count_promotes_only_scalars_and_only_for_arrays() {
        assert_eq!(
            ArchDbType::ScalarDouble.with_element_count(1),
            ArchDbType::ScalarDouble
        );
        assert_eq!(
            ArchDbType::ScalarDouble.with_element_count(2),
            ArchDbType::WaveformDouble
        );
        assert_eq!(
            ArchDbType::ScalarByte.with_element_count(0),
            ArchDbType::ScalarByte
        );
        assert_eq!(
            ArchDbType::WaveformDouble.with_element_count(1),
            ArchDbType::WaveformDouble
        );
        assert_eq!(
            ArchDbType::V4GenericBytes.with_element_count(8),
            ArchDbType::V4GenericBytes
        );
    }
}
