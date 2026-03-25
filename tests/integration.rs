//! End-to-end integration tests.

use std::sync::Arc;
use std::time::SystemTime;

use archiver_bluesky::documents::BlueskyDocument;
use archiver_bluesky::pv_mapper::PvMapper;
use archiver_core::storage::plainpb::PlainPbStoragePlugin;
use archiver_core::storage::partition::PartitionGranularity;
use archiver_core::storage::traits::{EventStream, StoragePlugin};
use archiver_core::types::{ArchDbType, ArchiverValue};

use chrono::{TimeZone, Utc};

fn temp_dir() -> tempfile::TempDir {
    tempfile::tempdir().unwrap()
}

/// Simulates a full Bluesky scan → PV storage → retrieval workflow.
#[tokio::test]
async fn test_bluesky_scan_to_pv_roundtrip() {
    let dir = temp_dir();
    let storage = PlainPbStoragePlugin::new("test", dir.path().to_path_buf(), PartitionGranularity::Hour);
    let mut mapper = PvMapper::new("BL1");

    // 1. RunStart
    let start_doc = BlueskyDocument::Tuple(
        "start".to_string(),
        serde_json::json!({
            "uid": "run-001",
            "time": 1718445000.0,
            "plan_name": "scan",
            "scan_id": 1,
            "num_points": 5,
            "motors": ["th"]
        }),
    );
    let start_samples = mapper.map_document(&start_doc);
    for (pv, dbr, sample) in &start_samples {
        storage.append_event(pv, *dbr, sample).await.unwrap();
    }

    // 2. EventDescriptor
    let desc_doc = BlueskyDocument::Tuple(
        "descriptor".to_string(),
        serde_json::json!({
            "uid": "desc-001",
            "run_start": "run-001",
            "time": 1718445001.0,
            "data_keys": {
                "th": {"source": "SIM:th.RBV", "dtype": "number", "shape": []},
                "det": {"source": "SIM:det", "dtype": "number", "shape": []}
            },
            "configuration": {
                "det": {
                    "data": {"exposure_time": 0.1, "num_images": 1},
                    "data_keys": {},
                    "timestamps": {}
                }
            },
            "name": "primary"
        }),
    );
    let desc_samples = mapper.map_document(&desc_doc);
    for (pv, dbr, sample) in &desc_samples {
        storage.append_event(pv, *dbr, sample).await.unwrap();
    }

    // 3. Events (5 scan points)
    for i in 0..5 {
        let event_doc = BlueskyDocument::Tuple(
            "event".to_string(),
            serde_json::json!({
                "descriptor": "desc-001",
                "seq_num": i + 1,
                "time": 1718445002.0 + (i as f64),
                "data": {
                    "th": 10.0 + (i as f64),
                    "det": 100.0 * (i as f64 + 1.0)
                },
                "timestamps": {
                    "th": 1718445002.0 + (i as f64),
                    "det": 1718445002.0 + (i as f64)
                },
                "uid": format!("event-{}", i + 1)
            }),
        );
        let event_samples = mapper.map_document(&event_doc);
        for (pv, dbr, sample) in &event_samples {
            storage.append_event(pv, *dbr, sample).await.unwrap();
        }
    }

    // 4. RunStop
    let stop_doc = BlueskyDocument::Tuple(
        "stop".to_string(),
        serde_json::json!({
            "uid": "stop-001",
            "run_start": "run-001",
            "time": 1718445010.0,
            "exit_status": "success"
        }),
    );
    let stop_samples = mapper.map_document(&stop_doc);
    for (pv, dbr, sample) in &stop_samples {
        storage.append_event(pv, *dbr, sample).await.unwrap();
    }

    // 5. Verify: Read back run:active PV — should have 1 (start) and 0 (stop).
    let start_time: SystemTime = Utc.with_ymd_and_hms(2024, 6, 15, 0, 0, 0).unwrap().into();
    let end_time: SystemTime = Utc.with_ymd_and_hms(2024, 6, 16, 0, 0, 0).unwrap().into();

    let mut streams = storage
        .get_data("EXP:BL1:run:active", start_time, end_time)
        .await
        .unwrap();

    assert!(!streams.is_empty());
    let mut values = Vec::new();
    for stream in &mut streams {
        while let Some(sample) = stream.next_event().unwrap() {
            if let ArchiverValue::ScalarEnum(v) = sample.value {
                values.push(v);
            }
        }
    }
    assert_eq!(values, vec![1, 0]); // start=1, stop=0

    // 6. Verify: Read motor readback — should have 5 values.
    let mut streams = storage
        .get_data("EXP:BL1:motor:th:readback", start_time, end_time)
        .await
        .unwrap();

    let mut motor_values = Vec::new();
    for stream in &mut streams {
        while let Some(sample) = stream.next_event().unwrap() {
            if let ArchiverValue::ScalarDouble(v) = sample.value {
                motor_values.push(v);
            }
        }
    }
    assert_eq!(motor_values.len(), 5);
    assert!((motor_values[0] - 10.0).abs() < 1e-10);
    assert!((motor_values[4] - 14.0).abs() < 1e-10);

    // 7. Verify: scan:current_point
    let mut streams = storage
        .get_data("EXP:BL1:scan:current_point", start_time, end_time)
        .await
        .unwrap();

    let mut points = Vec::new();
    for stream in &mut streams {
        while let Some(sample) = stream.next_event().unwrap() {
            if let ArchiverValue::ScalarInt(v) = sample.value {
                points.push(v);
            }
        }
    }
    assert_eq!(points, vec![1, 2, 3, 4, 5]);
}
