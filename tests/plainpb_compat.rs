//! PlainPB compatibility tests — verifies round-trip write/read and format compatibility.

use std::time::SystemTime;

use archiver_core::storage::plainpb::codec;
use archiver_core::storage::plainpb::PlainPbStoragePlugin;
use archiver_core::storage::partition::PartitionGranularity;
use archiver_core::storage::traits::StoragePlugin;
use archiver_core::types::{ArchDbType, ArchiverSample, ArchiverValue};

use chrono::{TimeZone, Utc};

fn temp_dir() -> tempfile::TempDir {
    tempfile::tempdir().unwrap()
}

#[test]
fn test_codec_roundtrip() {
    let original: Vec<u8> = (0..=255).collect();
    let escaped = codec::escape(&original);
    let unescaped = codec::unescape(&escaped);
    assert_eq!(original, unescaped);
}

#[test]
fn test_codec_specific_bytes() {
    // ESC
    assert_eq!(codec::escape(&[0x1B]), vec![0x1B, 0x01]);
    // LF
    assert_eq!(codec::escape(&[0x0A]), vec![0x1B, 0x02]);
    // CR
    assert_eq!(codec::escape(&[0x0D]), vec![0x1B, 0x03]);
    // Normal byte
    assert_eq!(codec::escape(&[0x42]), vec![0x42]);
}

#[tokio::test]
async fn test_write_read_scalar_double() {
    let dir = temp_dir();
    let plugin = PlainPbStoragePlugin::new("test", dir.path().to_path_buf(), PartitionGranularity::Hour);

    let ts: SystemTime = Utc.with_ymd_and_hms(2024, 6, 15, 10, 30, 0).unwrap().into();
    let sample = ArchiverSample::new(ts, ArchiverValue::ScalarDouble(std::f64::consts::PI));

    plugin.append_event("SIM:Sine", ArchDbType::ScalarDouble, &sample).await.unwrap();

    let start: SystemTime = Utc.with_ymd_and_hms(2024, 6, 15, 10, 0, 0).unwrap().into();
    let end: SystemTime = Utc.with_ymd_and_hms(2024, 6, 15, 11, 0, 0).unwrap().into();
    let mut streams = plugin.get_data("SIM:Sine", start, end).await.unwrap();

    assert_eq!(streams.len(), 1);
    let stream = &mut streams[0];
    assert_eq!(stream.description().pv_name, "SIM:Sine");
    assert_eq!(stream.description().db_type, ArchDbType::ScalarDouble);

    let read_sample = stream.next_event().unwrap().unwrap();
    match &read_sample.value {
        ArchiverValue::ScalarDouble(v) => assert!((v - std::f64::consts::PI).abs() < 1e-10),
        other => panic!("Expected ScalarDouble, got {other:?}"),
    }

    assert!(stream.next_event().unwrap().is_none());
}

#[tokio::test]
async fn test_write_read_scalar_string() {
    let dir = temp_dir();
    let plugin = PlainPbStoragePlugin::new("test", dir.path().to_path_buf(), PartitionGranularity::Day);

    let ts: SystemTime = Utc.with_ymd_and_hms(2024, 6, 15, 10, 30, 0).unwrap().into();
    let sample = ArchiverSample::new(ts, ArchiverValue::ScalarString("hello world".to_string()));

    plugin.append_event("SIM:Name", ArchDbType::ScalarString, &sample).await.unwrap();

    let start: SystemTime = Utc.with_ymd_and_hms(2024, 6, 15, 0, 0, 0).unwrap().into();
    let end: SystemTime = Utc.with_ymd_and_hms(2024, 6, 16, 0, 0, 0).unwrap().into();
    let mut streams = plugin.get_data("SIM:Name", start, end).await.unwrap();

    assert_eq!(streams.len(), 1);
    let read_sample = streams[0].next_event().unwrap().unwrap();
    match &read_sample.value {
        ArchiverValue::ScalarString(v) => assert_eq!(v, "hello world"),
        other => panic!("Expected ScalarString, got {other:?}"),
    }
}

#[tokio::test]
async fn test_write_read_multiple_samples() {
    let dir = temp_dir();
    let plugin = PlainPbStoragePlugin::new("test", dir.path().to_path_buf(), PartitionGranularity::Hour);

    let base_ts: SystemTime = Utc.with_ymd_and_hms(2024, 6, 15, 10, 0, 0).unwrap().into();

    for i in 0..100 {
        let ts = base_ts + std::time::Duration::from_secs(i);
        let val = (i as f64).sin();
        let sample = ArchiverSample::new(ts, ArchiverValue::ScalarDouble(val));
        plugin.append_event("SIM:Sine", ArchDbType::ScalarDouble, &sample).await.unwrap();
    }

    let end: SystemTime = Utc.with_ymd_and_hms(2024, 6, 15, 11, 0, 0).unwrap().into();
    let mut streams = plugin.get_data("SIM:Sine", base_ts, end).await.unwrap();

    assert_eq!(streams.len(), 1);
    let mut count = 0;
    while streams[0].next_event().unwrap().is_some() {
        count += 1;
    }
    assert_eq!(count, 100);
}

#[tokio::test]
async fn test_write_read_waveform() {
    let dir = temp_dir();
    let plugin = PlainPbStoragePlugin::new("test", dir.path().to_path_buf(), PartitionGranularity::Hour);

    let ts: SystemTime = Utc.with_ymd_and_hms(2024, 6, 15, 10, 30, 0).unwrap().into();
    let waveform: Vec<f64> = (0..10).map(|i| i as f64 * 0.1).collect();
    let sample = ArchiverSample::new(ts, ArchiverValue::VectorDouble(waveform.clone()));

    plugin.append_event("SIM:Waveform", ArchDbType::WaveformDouble, &sample).await.unwrap();

    let start: SystemTime = Utc.with_ymd_and_hms(2024, 6, 15, 10, 0, 0).unwrap().into();
    let end: SystemTime = Utc.with_ymd_and_hms(2024, 6, 15, 11, 0, 0).unwrap().into();
    let mut streams = plugin.get_data("SIM:Waveform", start, end).await.unwrap();

    assert_eq!(streams.len(), 1);
    let read_sample = streams[0].next_event().unwrap().unwrap();
    match &read_sample.value {
        ArchiverValue::VectorDouble(v) => {
            assert_eq!(v.len(), 10);
            assert!((v[5] - 0.5).abs() < 1e-10);
        }
        other => panic!("Expected VectorDouble, got {other:?}"),
    }
}

#[tokio::test]
async fn test_file_path_naming() {
    let dir = temp_dir();
    let plugin = PlainPbStoragePlugin::new("test", dir.path().to_path_buf(), PartitionGranularity::Hour);

    let ts: SystemTime = Utc.with_ymd_and_hms(2024, 3, 5, 9, 30, 0).unwrap().into();
    let path = plugin.file_path_for("SIM:Sine", ts);

    // PV "SIM:Sine" → key "SIM/Sine", partition "2024_03_05_09"
    let expected = dir.path().join("SIM/Sine:2024_03_05_09.pb");
    assert_eq!(path, expected);
}

#[tokio::test]
async fn test_last_known_event() {
    let dir = temp_dir();
    let plugin = PlainPbStoragePlugin::new("test", dir.path().to_path_buf(), PartitionGranularity::Hour);

    let ts1: SystemTime = Utc.with_ymd_and_hms(2024, 6, 15, 10, 0, 0).unwrap().into();
    let ts2: SystemTime = Utc.with_ymd_and_hms(2024, 6, 15, 10, 30, 0).unwrap().into();

    plugin.append_event("SIM:Test", ArchDbType::ScalarDouble, &ArchiverSample::new(ts1, ArchiverValue::ScalarDouble(1.0))).await.unwrap();
    plugin.append_event("SIM:Test", ArchDbType::ScalarDouble, &ArchiverSample::new(ts2, ArchiverValue::ScalarDouble(2.0))).await.unwrap();

    let last = plugin.get_last_known_event("SIM:Test").await.unwrap();
    assert!(last.is_some());
    match &last.unwrap().value {
        ArchiverValue::ScalarDouble(v) => assert!((v - 2.0).abs() < 1e-10),
        other => panic!("Expected ScalarDouble, got {other:?}"),
    }
}

// Java parity (d136e5c): exercise every ArchDbType variant + multi-year
// timestamps so type-specific encoding bugs (field ordering, missing fields,
// wrong protobuf type) surface in CI rather than silently in production.

async fn roundtrip_one(
    pv: &str,
    dbr: ArchDbType,
    val: ArchiverValue,
    granularity: PartitionGranularity,
) -> ArchiverValue {
    let dir = temp_dir();
    let plugin = PlainPbStoragePlugin::new("test", dir.path().to_path_buf(), granularity);
    let ts: SystemTime = Utc.with_ymd_and_hms(2024, 6, 15, 10, 30, 0).unwrap().into();
    let sample = ArchiverSample::new(ts, val);
    plugin.append_event(pv, dbr, &sample).await.unwrap();

    let start: SystemTime = Utc.with_ymd_and_hms(2024, 6, 15, 0, 0, 0).unwrap().into();
    let end: SystemTime = Utc.with_ymd_and_hms(2024, 6, 16, 0, 0, 0).unwrap().into();
    let mut streams = plugin.get_data(pv, start, end).await.unwrap();
    assert_eq!(streams.len(), 1);
    assert_eq!(streams[0].description().db_type, dbr);
    let read = streams[0].next_event().unwrap().unwrap();
    assert_eq!(read.timestamp, ts);
    assert!(streams[0].next_event().unwrap().is_none());
    read.value
}

#[tokio::test]
async fn roundtrip_scalar_byte() {
    let v = roundtrip_one(
        "TEST:byte",
        ArchDbType::ScalarByte,
        ArchiverValue::ScalarByte(vec![0x42]),
        PartitionGranularity::Day,
    )
    .await;
    match v {
        ArchiverValue::ScalarByte(b) => assert_eq!(b, vec![0x42]),
        other => panic!("expected ScalarByte, got {other:?}"),
    }
}

#[tokio::test]
async fn roundtrip_scalar_short() {
    let v = roundtrip_one(
        "TEST:short",
        ArchDbType::ScalarShort,
        ArchiverValue::ScalarShort(-12345),
        PartitionGranularity::Day,
    )
    .await;
    match v {
        ArchiverValue::ScalarShort(n) => assert_eq!(n, -12345),
        other => panic!("expected ScalarShort, got {other:?}"),
    }
}

#[tokio::test]
async fn roundtrip_scalar_int() {
    let v = roundtrip_one(
        "TEST:int",
        ArchDbType::ScalarInt,
        ArchiverValue::ScalarInt(-2_000_000_000),
        PartitionGranularity::Day,
    )
    .await;
    match v {
        ArchiverValue::ScalarInt(n) => assert_eq!(n, -2_000_000_000),
        other => panic!("expected ScalarInt, got {other:?}"),
    }
}

#[tokio::test]
async fn roundtrip_scalar_enum() {
    let v = roundtrip_one(
        "TEST:enum",
        ArchDbType::ScalarEnum,
        ArchiverValue::ScalarEnum(7),
        PartitionGranularity::Day,
    )
    .await;
    match v {
        ArchiverValue::ScalarEnum(n) => assert_eq!(n, 7),
        other => panic!("expected ScalarEnum, got {other:?}"),
    }
}

#[tokio::test]
async fn roundtrip_scalar_float() {
    let v = roundtrip_one(
        "TEST:float",
        ArchDbType::ScalarFloat,
        ArchiverValue::ScalarFloat(std::f32::consts::E),
        PartitionGranularity::Day,
    )
    .await;
    match v {
        ArchiverValue::ScalarFloat(f) => {
            assert!((f - std::f32::consts::E).abs() < 1e-6);
        }
        other => panic!("expected ScalarFloat, got {other:?}"),
    }
}

#[tokio::test]
async fn roundtrip_waveform_byte() {
    let v = roundtrip_one(
        "TEST:wbyte",
        ArchDbType::WaveformByte,
        ArchiverValue::VectorChar(vec![0, 1, 2, 3, 4]),
        PartitionGranularity::Day,
    )
    .await;
    match v {
        ArchiverValue::VectorChar(b) => assert_eq!(b, vec![0, 1, 2, 3, 4]),
        other => panic!("expected VectorChar, got {other:?}"),
    }
}

#[tokio::test]
async fn roundtrip_waveform_short() {
    let v = roundtrip_one(
        "TEST:wshort",
        ArchDbType::WaveformShort,
        ArchiverValue::VectorShort(vec![-1, 0, 1, i32::from(i16::MAX)]),
        PartitionGranularity::Day,
    )
    .await;
    match v {
        ArchiverValue::VectorShort(arr) => {
            assert_eq!(arr, vec![-1, 0, 1, i32::from(i16::MAX)]);
        }
        other => panic!("expected VectorShort, got {other:?}"),
    }
}

#[tokio::test]
async fn roundtrip_waveform_int() {
    let v = roundtrip_one(
        "TEST:wint",
        ArchDbType::WaveformInt,
        ArchiverValue::VectorInt(vec![i32::MIN, -1, 0, 1, i32::MAX]),
        PartitionGranularity::Day,
    )
    .await;
    match v {
        ArchiverValue::VectorInt(arr) => {
            assert_eq!(arr, vec![i32::MIN, -1, 0, 1, i32::MAX]);
        }
        other => panic!("expected VectorInt, got {other:?}"),
    }
}

#[tokio::test]
async fn roundtrip_waveform_enum() {
    let v = roundtrip_one(
        "TEST:wenum",
        ArchDbType::WaveformEnum,
        ArchiverValue::VectorEnum(vec![0, 1, 2, 3]),
        PartitionGranularity::Day,
    )
    .await;
    match v {
        ArchiverValue::VectorEnum(arr) => assert_eq!(arr, vec![0, 1, 2, 3]),
        other => panic!("expected VectorEnum, got {other:?}"),
    }
}

#[tokio::test]
async fn roundtrip_waveform_float() {
    let v = roundtrip_one(
        "TEST:wfloat",
        ArchDbType::WaveformFloat,
        ArchiverValue::VectorFloat(vec![0.0_f32, 1.5, -1.5, std::f32::consts::PI]),
        PartitionGranularity::Day,
    )
    .await;
    match v {
        ArchiverValue::VectorFloat(arr) => {
            assert_eq!(arr.len(), 4);
            assert!((arr[3] - std::f32::consts::PI).abs() < 1e-6);
        }
        other => panic!("expected VectorFloat, got {other:?}"),
    }
}

#[tokio::test]
async fn roundtrip_waveform_string() {
    let v = roundtrip_one(
        "TEST:wstring",
        ArchDbType::WaveformString,
        ArchiverValue::VectorString(vec!["foo".into(), "bar".into(), "".into()]),
        PartitionGranularity::Day,
    )
    .await;
    match v {
        ArchiverValue::VectorString(arr) => {
            assert_eq!(arr, vec!["foo".to_string(), "bar".into(), "".into()]);
        }
        other => panic!("expected VectorString, got {other:?}"),
    }
}

/// Java parity (d136e5c): year-relative seconds must round-trip across the
/// realistic deployed range. Catches off-by-one in leap year handling and
/// year-overflow that an i16 would silently truncate.
#[tokio::test]
async fn multi_year_timestamp_roundtrip() {
    for year in [1990, 2000, 2010, 2016, 2020, 2024, 2030] {
        // Mid-year so partition boundaries don't matter, and so we exercise
        // a non-zero secondsintoyear (year start is the trivial case).
        let ts: SystemTime = Utc.with_ymd_and_hms(year, 6, 15, 12, 0, 0).unwrap().into();
        let dir = temp_dir();
        let plugin = PlainPbStoragePlugin::new(
            "test",
            dir.path().to_path_buf(),
            PartitionGranularity::Year,
        );
        let sample = ArchiverSample::new(ts, ArchiverValue::ScalarDouble(year as f64));
        plugin
            .append_event("YEAR:Test", ArchDbType::ScalarDouble, &sample)
            .await
            .unwrap();

        // Span the entire year for the query.
        let qstart: SystemTime = Utc.with_ymd_and_hms(year, 1, 1, 0, 0, 0).unwrap().into();
        let qend: SystemTime = Utc
            .with_ymd_and_hms(year + 1, 1, 1, 0, 0, 0)
            .unwrap()
            .into();
        let mut streams = plugin.get_data("YEAR:Test", qstart, qend).await.unwrap();
        assert_eq!(streams.len(), 1, "year {year}: expected one stream");
        assert_eq!(
            streams[0].description().year,
            year,
            "year {year}: PayloadInfo.year mismatch"
        );
        let read = streams[0]
            .next_event()
            .unwrap()
            .unwrap_or_else(|| panic!("year {year}: no sample read back"));
        assert_eq!(read.timestamp, ts, "year {year}: timestamp mismatch");
        match read.value {
            ArchiverValue::ScalarDouble(v) => {
                assert_eq!(v, year as f64, "year {year}: value mismatch")
            }
            other => panic!("year {year}: expected ScalarDouble, got {other:?}"),
        }
    }
}
