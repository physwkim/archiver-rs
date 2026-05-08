//! PlainPB compatibility tests — verifies round-trip write/read and format compatibility.

use std::time::SystemTime;

use archiver_core::storage::partition::PartitionGranularity;
use archiver_core::storage::plainpb::{FdBudget, PlainPbStoragePlugin};
use archiver_core::storage::plainpb::codec;
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
    let plugin =
        PlainPbStoragePlugin::new("test", dir.path().to_path_buf(), PartitionGranularity::Hour);

    let ts: SystemTime = Utc.with_ymd_and_hms(2024, 6, 15, 10, 30, 0).unwrap().into();
    let sample = ArchiverSample::new(ts, ArchiverValue::ScalarDouble(std::f64::consts::PI));

    plugin
        .append_event("SIM:Sine", ArchDbType::ScalarDouble, &sample)
        .await
        .unwrap();

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
    let plugin =
        PlainPbStoragePlugin::new("test", dir.path().to_path_buf(), PartitionGranularity::Day);

    let ts: SystemTime = Utc.with_ymd_and_hms(2024, 6, 15, 10, 30, 0).unwrap().into();
    let sample = ArchiverSample::new(ts, ArchiverValue::ScalarString("hello world".to_string()));

    plugin
        .append_event("SIM:Name", ArchDbType::ScalarString, &sample)
        .await
        .unwrap();

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
    let plugin =
        PlainPbStoragePlugin::new("test", dir.path().to_path_buf(), PartitionGranularity::Hour);

    let base_ts: SystemTime = Utc.with_ymd_and_hms(2024, 6, 15, 10, 0, 0).unwrap().into();

    for i in 0..100 {
        let ts = base_ts + std::time::Duration::from_secs(i);
        let val = (i as f64).sin();
        let sample = ArchiverSample::new(ts, ArchiverValue::ScalarDouble(val));
        plugin
            .append_event("SIM:Sine", ArchDbType::ScalarDouble, &sample)
            .await
            .unwrap();
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
    let plugin =
        PlainPbStoragePlugin::new("test", dir.path().to_path_buf(), PartitionGranularity::Hour);

    let ts: SystemTime = Utc.with_ymd_and_hms(2024, 6, 15, 10, 30, 0).unwrap().into();
    let waveform: Vec<f64> = (0..10).map(|i| i as f64 * 0.1).collect();
    let sample = ArchiverSample::new(ts, ArchiverValue::VectorDouble(waveform.clone()));

    plugin
        .append_event("SIM:Waveform", ArchDbType::WaveformDouble, &sample)
        .await
        .unwrap();

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
    let plugin =
        PlainPbStoragePlugin::new("test", dir.path().to_path_buf(), PartitionGranularity::Hour);

    let ts: SystemTime = Utc.with_ymd_and_hms(2024, 3, 5, 9, 30, 0).unwrap().into();
    let path = plugin.file_path_for("SIM:Sine", ts);

    // PV "SIM:Sine" → key "SIM/Sine", partition "2024_03_05_09"
    let expected = dir.path().join("SIM/Sine:2024_03_05_09.pb");
    assert_eq!(path, expected);
}

#[tokio::test]
async fn test_last_known_event() {
    let dir = temp_dir();
    let plugin =
        PlainPbStoragePlugin::new("test", dir.path().to_path_buf(), PartitionGranularity::Hour);

    let ts1: SystemTime = Utc.with_ymd_and_hms(2024, 6, 15, 10, 0, 0).unwrap().into();
    let ts2: SystemTime = Utc.with_ymd_and_hms(2024, 6, 15, 10, 30, 0).unwrap().into();

    plugin
        .append_event(
            "SIM:Test",
            ArchDbType::ScalarDouble,
            &ArchiverSample::new(ts1, ArchiverValue::ScalarDouble(1.0)),
        )
        .await
        .unwrap();
    plugin
        .append_event(
            "SIM:Test",
            ArchDbType::ScalarDouble,
            &ArchiverSample::new(ts2, ArchiverValue::ScalarDouble(2.0)),
        )
        .await
        .unwrap();

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
        let plugin =
            PlainPbStoragePlugin::new("test", dir.path().to_path_buf(), PartitionGranularity::Year);
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

// ───── Failure-injection tests ─────
//
// These exercise the PB writer's recovery surface: partial header
// from a previous-process crash, partial trailing sample from a
// mid-flush kill, and the dirty-bit flush gate. They write directly
// to the storage root using `std::fs` so they bypass the cache and
// model the "another process / previous incarnation left this state"
// scenario. The reading test cases use the public StoragePlugin
// surface to confirm recovery is observable end-to-end.

use std::io::Write;

fn pv_path_for(plugin_root: &std::path::Path, pv: &str, ts: SystemTime) -> std::path::PathBuf {
    use std::fs;
    let plugin = PlainPbStoragePlugin::new(
        "tmp",
        plugin_root.to_path_buf(),
        PartitionGranularity::Hour,
    );
    let p = plugin.file_path_for(pv, ts);
    if let Some(parent) = p.parent() {
        fs::create_dir_all(parent).unwrap();
    }
    p
}

#[tokio::test]
async fn injection_partial_header_recovers_on_next_write() {
    let dir = temp_dir();
    let ts: SystemTime = Utc.with_ymd_and_hms(2026, 1, 15, 10, 30, 0).unwrap().into();
    let path = pv_path_for(dir.path(), "TEST:Pv", ts);

    // Simulate a previous-process crash: file exists with 5 bytes of
    // garbage that does NOT parse as PayloadInfo. The writer should
    // detect this on first write_cached call, truncate, and recreate.
    std::fs::write(&path, b"\x01\x02\x03\x04\x05").unwrap();

    let plugin = PlainPbStoragePlugin::new(
        "test",
        dir.path().to_path_buf(),
        PartitionGranularity::Hour,
    );
    let sample = ArchiverSample::new(ts, ArchiverValue::ScalarDouble(42.0));
    plugin
        .append_event("TEST:Pv", ArchDbType::ScalarDouble, &sample)
        .await
        .expect("append must recover from corrupt header");

    let start: SystemTime = Utc.with_ymd_and_hms(2026, 1, 15, 10, 0, 0).unwrap().into();
    let end: SystemTime = Utc.with_ymd_and_hms(2026, 1, 15, 11, 0, 0).unwrap().into();
    let mut streams = plugin.get_data("TEST:Pv", start, end).await.unwrap();
    assert_eq!(streams.len(), 1);
    let read = streams[0].next_event().unwrap().unwrap();
    match read.value {
        ArchiverValue::ScalarDouble(v) => assert_eq!(v, 42.0),
        other => panic!("expected ScalarDouble(42.0), got {other:?}"),
    }
}

#[tokio::test]
async fn injection_partial_trailing_record_gets_trimmed() {
    let dir = temp_dir();
    let ts: SystemTime = Utc.with_ymd_and_hms(2026, 2, 20, 14, 5, 0).unwrap().into();

    // First, create a valid file with one real sample via the plugin.
    let plugin = PlainPbStoragePlugin::new(
        "test",
        dir.path().to_path_buf(),
        PartitionGranularity::Hour,
    );
    let s1 = ArchiverSample::new(ts, ArchiverValue::ScalarDouble(1.0));
    plugin
        .append_event("TEST:Trim", ArchDbType::ScalarDouble, &s1)
        .await
        .unwrap();
    plugin.flush_writes().await.unwrap();

    // Now manually append junk bytes WITHOUT a terminating newline,
    // simulating a writer killed mid-flush. Drop the plugin so the
    // BufWriter's open file handle releases the inode.
    drop(plugin);
    let path = pv_path_for(dir.path(), "TEST:Trim", ts);
    let mut file = std::fs::OpenOptions::new()
        .append(true)
        .open(&path)
        .unwrap();
    file.write_all(b"not-a-real-sample-bytes-no-newline").unwrap();
    drop(file);

    // Reopen the plugin and write another sample — the file_needs_header
    // path runs trim_to_last_newline first, dropping the partial bytes.
    let plugin = PlainPbStoragePlugin::new(
        "test",
        dir.path().to_path_buf(),
        PartitionGranularity::Hour,
    );
    let ts2: SystemTime = Utc.with_ymd_and_hms(2026, 2, 20, 14, 5, 30).unwrap().into();
    let s2 = ArchiverSample::new(ts2, ArchiverValue::ScalarDouble(2.0));
    plugin
        .append_event("TEST:Trim", ArchDbType::ScalarDouble, &s2)
        .await
        .unwrap();

    // Reading should give us back BOTH s1 and s2 cleanly. Without the
    // tail trim, the partial bytes would derail the second-record
    // parse.
    let start: SystemTime = Utc.with_ymd_and_hms(2026, 2, 20, 14, 0, 0).unwrap().into();
    let end: SystemTime = Utc.with_ymd_and_hms(2026, 2, 20, 15, 0, 0).unwrap().into();
    let mut streams = plugin.get_data("TEST:Trim", start, end).await.unwrap();
    let mut values = Vec::new();
    while let Some(sample) = streams[0].next_event().unwrap() {
        if let ArchiverValue::ScalarDouble(v) = sample.value {
            values.push(v);
        }
    }
    assert_eq!(values, vec![1.0, 2.0], "both samples must be readable after trim");
}

#[tokio::test]
async fn injection_zero_byte_file_treated_as_missing() {
    let dir = temp_dir();
    let ts: SystemTime = Utc.with_ymd_and_hms(2026, 3, 10, 8, 0, 0).unwrap().into();

    // Empty file with no bytes — Java parity: same as missing.
    let path = pv_path_for(dir.path(), "TEST:Empty", ts);
    std::fs::File::create(&path).unwrap();
    assert_eq!(std::fs::metadata(&path).unwrap().len(), 0);

    let plugin = PlainPbStoragePlugin::new(
        "test",
        dir.path().to_path_buf(),
        PartitionGranularity::Hour,
    );
    let s = ArchiverSample::new(ts, ArchiverValue::ScalarDouble(99.0));
    plugin
        .append_event("TEST:Empty", ArchDbType::ScalarDouble, &s)
        .await
        .unwrap();

    let start: SystemTime = Utc.with_ymd_and_hms(2026, 3, 10, 7, 0, 0).unwrap().into();
    let end: SystemTime = Utc.with_ymd_and_hms(2026, 3, 10, 9, 0, 0).unwrap().into();
    let mut streams = plugin.get_data("TEST:Empty", start, end).await.unwrap();
    let read = streams[0].next_event().unwrap().unwrap();
    match read.value {
        ArchiverValue::ScalarDouble(v) => assert_eq!(v, 99.0),
        other => panic!("got {other:?}"),
    }
}

#[tokio::test]
async fn injection_flush_after_no_writes_is_noop() {
    // Dirty-bit gate: flush_writes with an empty cache (no PV ever
    // written) must succeed and do nothing. Regression guard for
    // F12 — if the dirty skip ever had a bug that flushed clean
    // writers, this would still pass; but it does verify the empty
    // path doesn't error.
    let dir = temp_dir();
    let plugin = PlainPbStoragePlugin::new(
        "test",
        dir.path().to_path_buf(),
        PartitionGranularity::Hour,
    );
    plugin.flush_writes().await.expect("empty flush is Ok");
    plugin.flush_writes().await.expect("empty flush is repeatable");
}

#[tokio::test]
async fn injection_ingest_flush_returns_empty_vec_on_success() {
    // Contract: `flush_ingest_writes` returns `Ok(Vec::new())` when
    // every dirty writer flushed cleanly. Regression guard for the
    // per-PV failure surface — if the impl ever mistakenly listed
    // healthy PVs as "failed", write_loop would orphan their
    // timestamps from the registry commit.
    let dir = temp_dir();
    let plugin = PlainPbStoragePlugin::new(
        "test",
        dir.path().to_path_buf(),
        PartitionGranularity::Hour,
    );

    let ts: SystemTime = Utc.with_ymd_and_hms(2026, 6, 1, 12, 0, 0).unwrap().into();
    for i in 0..5 {
        let pv = format!("TEST:Pv{i}");
        let s = ArchiverSample::new(ts, ArchiverValue::ScalarDouble(i as f64));
        plugin
            .append_event(&pv, ArchDbType::ScalarDouble, &s)
            .await
            .unwrap();
    }

    let outcome = plugin.flush_ingest_writes().await.unwrap();
    assert!(
        outcome.failed.is_empty() && outcome.deferred.is_empty(),
        "all healthy writers should flush; got failed={:?} deferred={:?}",
        outcome.failed,
        outcome.deferred,
    );

    // Subsequent flush after no new writes — dirty bit clean, still
    // empty failed/deferred. Validates that successful flush clears
    // the dirty flag so the next flush doesn't touch healthy writers
    // again.
    let outcome = plugin.flush_ingest_writes().await.unwrap();
    assert!(outcome.failed.is_empty() && outcome.deferred.is_empty());
}

#[tokio::test]
async fn fd_cap_strict_under_concurrent_appends() {
    // Atomic CAS reservation: with cap=4 and 32 concurrent
    // appends to distinct PVs, `open_writer_count` must NEVER
    // exceed 4 mid-flight (the old fetch_add path could blow
    // through the cap by N concurrent reservations).
    use std::sync::Arc;
    let dir = temp_dir();
    let plugin = Arc::new(PlainPbStoragePlugin::with_max_open_writers(
        "test",
        dir.path().to_path_buf(),
        PartitionGranularity::Hour,
        4,
    ));

    let ts: SystemTime = Utc.with_ymd_and_hms(2026, 7, 1, 12, 0, 0).unwrap().into();

    let mut joins = Vec::new();
    for i in 0..32u32 {
        let plugin = plugin.clone();
        let pv = format!("TEST:CapPv{i}");
        joins.push(tokio::spawn(async move {
            let s = ArchiverSample::new(ts, ArchiverValue::ScalarDouble(i as f64));
            plugin
                .append_event(&pv, ArchDbType::ScalarDouble, &s)
                .await
                .unwrap();
        }));
    }
    for j in joins {
        j.await.unwrap();
    }

    // After all appends settle, the open count must be at the cap.
    // The strict invariant we care about is "never exceeded the cap"
    // — if any reservation had blown past it, the runtime open
    // count would visibly hit 5+ at some point. Without a sampling
    // probe we can at minimum assert the post-condition.
    assert!(
        plugin.open_writer_count() <= 4,
        "open_writer_count exceeded cap: {} > 4",
        plugin.open_writer_count()
    );

    // Sanity: every PV's data made it to disk despite the LRU
    // churn from the cap.
    let outcome = plugin.flush_ingest_writes().await.unwrap();
    // Some may have been LRU-evicted before flush; that's fine —
    // their bytes were flushed at eviction time. Failed list must
    // stay empty (we have a healthy filesystem in tests).
    assert!(
        outcome.failed.is_empty(),
        "no PV should be in failed under healthy fs: {:?}",
        outcome.failed
    );
}

/// Principle 4 — flush truth. A bare `BufWriter::flush()` on a
/// writer whose underlying file has been unlinked still returns
/// Ok (bytes go into the page cache for the deleted inode), but
/// readers will never see them. `flush_dirty_writers` must check
/// `path.exists()` after the flush and downgrade the writer to
/// loss when the file is gone — otherwise the flush owner would
/// commit a stale `last_event` for bytes that no reader can ever
/// see.
#[tokio::test]
async fn flush_to_deleted_inode_records_loss() {
    let dir = temp_dir();
    let plugin = PlainPbStoragePlugin::new(
        "test",
        dir.path().to_path_buf(),
        PartitionGranularity::Hour,
    );

    let ts: SystemTime = Utc.with_ymd_and_hms(2026, 9, 7, 12, 0, 0).unwrap().into();
    let pv = "TEST:DeletedInode";
    plugin
        .append_event(
            pv,
            ArchDbType::ScalarDouble,
            &ArchiverSample::new(ts, ArchiverValue::ScalarDouble(1.0)),
        )
        .await
        .unwrap();

    // Externally delete the file while the writer's fd stays
    // open. A flush via the open fd will succeed at the syscall
    // level — but the bytes are not reader-visible.
    let path = plugin.file_path_for(pv, ts);
    std::fs::remove_file(&path).unwrap();

    // flush_ingest_writes must catch this via the path.exists()
    // check and surface PV in failed (bytes lost) rather than
    // silently mark the writer clean.
    let outcome = plugin.flush_ingest_writes().await.unwrap();
    assert!(
        outcome.failed.iter().any(|p| p == pv),
        "flush on a deleted inode must record loss; got failed={:?}",
        outcome.failed
    );
}

/// Ghost-file path: when a cached writer's underlying file
/// disappears (deleted out from under us by ETL / manual `rm` /
/// flaky NFS) and a subsequent append re-detects the absence,
/// `drop_writer_file_gone` must record loss for the PV's dirty
/// bytes. The next `flush_ingest_writes` surfaces the PV in
/// `failed` so the global flush owner drops its ts_updates.
#[tokio::test]
async fn ghost_file_path_records_loss() {
    let dir = temp_dir();
    let plugin = PlainPbStoragePlugin::new(
        "test",
        dir.path().to_path_buf(),
        PartitionGranularity::Hour,
    );

    let ts1: SystemTime = Utc.with_ymd_and_hms(2026, 9, 1, 12, 0, 0).unwrap().into();
    let ts2 = ts1 + std::time::Duration::from_secs(60);
    let pv = "TEST:Ghost";

    // First append leaves the writer dirty (BufWriter has bytes;
    // no explicit flush yet).
    let s1 = ArchiverSample::new(ts1, ArchiverValue::ScalarDouble(1.0));
    plugin
        .append_event(pv, ArchDbType::ScalarDouble, &s1)
        .await
        .unwrap();

    // External delete of the on-disk file. `drop_writer_file_gone`
    // will fire the next time write_cached probes the path.
    let path = plugin.file_path_for(pv, ts1);
    std::fs::remove_file(&path).unwrap();

    // Second append: ghost-file branch detects the missing file,
    // calls drop_writer_file_gone (dirty=true → loss marker).
    let s2 = ArchiverSample::new(ts2, ArchiverValue::ScalarDouble(2.0));
    plugin
        .append_event(pv, ArchDbType::ScalarDouble, &s2)
        .await
        .unwrap();

    // Loss marker surfaces via the next ingest flush.
    let outcome = plugin.flush_ingest_writes().await.unwrap();
    assert!(
        outcome.failed.iter().any(|p| p == pv),
        "ghost-file path must record loss for {pv}; got failed={:?}",
        outcome.failed
    );
}

/// `evict_writer_for_path`: when ETL removes a `.pb` file and
/// then evicts its writer, dirty bytes must surface as loss.
/// Mirrors the ghost-file path but with the writer eviction
/// driven by an explicit caller rather than detected on next
/// append.
#[tokio::test]
async fn evict_writer_for_path_records_loss_when_dirty() {
    let dir = temp_dir();
    let plugin = PlainPbStoragePlugin::new(
        "test",
        dir.path().to_path_buf(),
        PartitionGranularity::Hour,
    );

    let ts: SystemTime = Utc.with_ymd_and_hms(2026, 9, 2, 12, 0, 0).unwrap().into();
    let pv = "TEST:EvictByPath";
    let s = ArchiverSample::new(ts, ArchiverValue::ScalarDouble(1.0));
    plugin
        .append_event(pv, ArchDbType::ScalarDouble, &s)
        .await
        .unwrap();
    let path = plugin.file_path_for(pv, ts);

    // ETL caller has just `remove_file`-d the path and now wants
    // to drop the cached writer.
    std::fs::remove_file(&path).unwrap();
    assert!(plugin.evict_writer_for_path(&path));

    // Loss marker on the next ingest flush.
    let outcome = plugin.flush_ingest_writes().await.unwrap();
    assert!(
        outcome.failed.iter().any(|p| p == pv),
        "evict_writer_for_path must record loss for dirty writer; got failed={:?}",
        outcome.failed
    );
}

/// `delete_pv_data` records loss for any dirty bytes (file is
/// about to be deleted; flushing is wasted) AND closes the
/// concurrent-append race by tombstoning the slot in the cache
/// during the file-removal window.
#[tokio::test]
async fn delete_pv_data_records_loss_and_tombstones() {
    let dir = temp_dir();
    let plugin = PlainPbStoragePlugin::new(
        "test",
        dir.path().to_path_buf(),
        PartitionGranularity::Hour,
    );

    let ts: SystemTime = Utc.with_ymd_and_hms(2026, 9, 3, 12, 0, 0).unwrap().into();
    let pv = "TEST:Delete";
    let s = ArchiverSample::new(ts, ArchiverValue::ScalarDouble(1.0));
    plugin
        .append_event(pv, ArchDbType::ScalarDouble, &s)
        .await
        .unwrap();

    plugin.delete_pv_data(pv).await.unwrap();

    // Loss marker for the dirty writer's bytes.
    let outcome = plugin.flush_ingest_writes().await.unwrap();
    assert!(
        outcome.failed.iter().any(|p| p == pv),
        "delete_pv_data must record loss for dirty writer; got failed={:?}",
        outcome.failed
    );

    // Phase 3 cleanup: the tombstone slot was removed from the
    // cache, so a fresh append after delete_pv_data succeeds
    // (slot_for inserts a fresh non-dead slot). This proves the
    // tombstone doesn't permanently block legitimate re-archives.
    let ts2: SystemTime = Utc.with_ymd_and_hms(2026, 9, 3, 13, 0, 0).unwrap().into();
    let s2 = ArchiverSample::new(ts2, ArchiverValue::ScalarDouble(2.0));
    plugin
        .append_event(pv, ArchDbType::ScalarDouble, &s2)
        .await
        .expect("post-delete append should succeed on a fresh slot");
}

/// `rename_pv` source: dirty writer for `from` flushes
/// successfully (healthy fs), so no loss marker. Verifies the
/// happy path (loss-on-flush-failure variant requires fault
/// injection and is exercised by the helper's contract).
#[tokio::test]
async fn rename_pv_flushes_source_writer_cleanly() {
    let dir = temp_dir();
    let plugin = PlainPbStoragePlugin::new(
        "test",
        dir.path().to_path_buf(),
        PartitionGranularity::Hour,
    );

    let ts: SystemTime = Utc.with_ymd_and_hms(2026, 9, 4, 12, 0, 0).unwrap().into();
    let from = "TEST:RenameFrom";
    let to = "TEST:RenameTo";
    let s = ArchiverSample::new(ts, ArchiverValue::ScalarDouble(1.0));
    plugin
        .append_event(from, ArchDbType::ScalarDouble, &s)
        .await
        .unwrap();

    let moved = plugin.rename_pv(from, to).await.unwrap();
    assert!(moved >= 1, "rename should have moved at least one file");

    // Healthy fs: flush succeeds during drop_dirty_writer, no
    // loss marker. The next ingest flush has empty failed.
    let outcome = plugin.flush_ingest_writes().await.unwrap();
    assert!(
        !outcome.failed.iter().any(|p| p == from),
        "healthy rename must not record loss for source; got failed={:?}",
        outcome.failed
    );

    // The renamed file must be readable under `to`.
    let dest_path = plugin.file_path_for(to, ts);
    assert!(
        dest_path.exists(),
        "rename should have moved bytes to dest path: {dest_path:?}"
    );
}

/// Partition rollover: when a sample's partition differs from
/// the cached writer's path, write_cached calls drop_dirty_writer
/// on the old writer. Healthy fs → flush succeeds, no loss marker.
/// Together with the helper's flush-failure path (which records
/// loss), this covers both branches of the partition-rollover
/// dirty drop.
#[tokio::test]
async fn partition_rollover_clean_drop_no_loss() {
    let dir = temp_dir();
    let plugin = PlainPbStoragePlugin::new(
        "test",
        dir.path().to_path_buf(),
        PartitionGranularity::Hour,
    );

    let pv = "TEST:Rollover";
    // First sample in hour H.
    let ts1: SystemTime = Utc.with_ymd_and_hms(2026, 9, 5, 12, 0, 0).unwrap().into();
    plugin
        .append_event(
            pv,
            ArchDbType::ScalarDouble,
            &ArchiverSample::new(ts1, ArchiverValue::ScalarDouble(1.0)),
        )
        .await
        .unwrap();

    // Second sample in hour H+1 — different partition, triggers
    // the rollover drop path on the cached writer for hour H.
    let ts2: SystemTime = Utc.with_ymd_and_hms(2026, 9, 5, 13, 0, 0).unwrap().into();
    plugin
        .append_event(
            pv,
            ArchDbType::ScalarDouble,
            &ArchiverSample::new(ts2, ArchiverValue::ScalarDouble(2.0)),
        )
        .await
        .unwrap();

    // Healthy fs: rollover's drop_dirty_writer flushes cleanly,
    // no loss marker.
    let outcome = plugin.flush_ingest_writes().await.unwrap();
    assert!(
        !outcome.failed.iter().any(|p| p == pv),
        "healthy partition rollover must not record loss; got failed={:?}",
        outcome.failed
    );

    // Both files must exist on disk.
    assert!(plugin.file_path_for(pv, ts1).exists());
    assert!(plugin.file_path_for(pv, ts2).exists());
}

#[tokio::test]
async fn shared_fd_budget_caps_three_tiers_combined() {
    // Process-wide fd cap: 3 plugins (STS / MTS / LTS) each
    // drawing from a SHARED FdBudget(cap=4) must NOT collectively
    // exceed 4 open writers. With LRU eviction, sequential appends
    // to many distinct PVs across all 3 tiers can succeed while
    // the live count stays at-or-below the shared cap.
    use std::sync::Arc;
    let dir = temp_dir();
    let sts_dir = dir.path().join("sts");
    let mts_dir = dir.path().join("mts");
    let lts_dir = dir.path().join("lts");
    std::fs::create_dir_all(&sts_dir).unwrap();
    std::fs::create_dir_all(&mts_dir).unwrap();
    std::fs::create_dir_all(&lts_dir).unwrap();

    let shared = FdBudget::new(4);
    let sts = Arc::new(PlainPbStoragePlugin::with_fd_budget(
        "sts",
        sts_dir,
        PartitionGranularity::Hour,
        shared.clone(),
    ));
    let mts = Arc::new(PlainPbStoragePlugin::with_fd_budget(
        "mts",
        mts_dir,
        PartitionGranularity::Day,
        shared.clone(),
    ));
    let lts = Arc::new(PlainPbStoragePlugin::with_fd_budget(
        "lts",
        lts_dir,
        PartitionGranularity::Year,
        shared.clone(),
    ));

    let ts: SystemTime = Utc.with_ymd_and_hms(2026, 8, 1, 12, 0, 0).unwrap().into();

    // Sequential appends across tiers: each new PV either fits
    // under the cap or triggers an LRU eviction in whichever
    // tier currently holds the oldest writer.
    for round in 0..6 {
        for tier_idx in 0..3 {
            let tier: &PlainPbStoragePlugin = match tier_idx {
                0 => &sts,
                1 => &mts,
                _ => &lts,
            };
            let pv = format!("TEST:T{tier_idx}Pv{round}");
            let s = ArchiverSample::new(ts, ArchiverValue::ScalarDouble(round as f64));
            tier.append_event(&pv, ArchDbType::ScalarDouble, &s)
                .await
                .unwrap();
            // Snapshot AFTER each append: the shared count must
            // never have exceeded the cap during this append.
            assert!(
                shared.count() <= 4,
                "shared FdBudget exceeded cap mid-append: count={} > 4 \
                 (round={round} tier={tier_idx} pv={pv})",
                shared.count()
            );
        }
    }

    // After 18 distinct-PV appends, the live count is at-most cap.
    assert!(
        shared.count() <= 4,
        "shared FdBudget exceeded cap at end: {} > 4",
        shared.count()
    );
    // Each tier reports the shared count.
    assert_eq!(
        sts.open_writer_count(),
        shared.count(),
        "sts.open_writer_count() should reflect the shared budget"
    );
    assert_eq!(mts.open_writer_count(), shared.count());
    assert_eq!(lts.open_writer_count(), shared.count());
}
