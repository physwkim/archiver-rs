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
