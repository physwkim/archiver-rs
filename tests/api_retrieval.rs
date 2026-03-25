use std::sync::Arc;
use std::time::{Duration, SystemTime};

use axum::body::Body;
use axum::http::{Request, StatusCode};
use tower::ServiceExt;

use archiver_core::registry::{PvRegistry, SampleMode};
use archiver_core::storage::partition::PartitionGranularity;
use archiver_core::storage::plainpb::reader::PbFileReader;
use archiver_core::storage::plainpb::PlainPbStoragePlugin;
use archiver_core::storage::traits::{EventStream, StoragePlugin};
use archiver_core::types::{ArchDbType, ArchiverSample, ArchiverValue};
use archiver_engine::channel_manager::ChannelManager;
use archiver_core::config::SecurityConfig;
use archiver_api::{build_router, AppState};
use archiver_api::services::impls::{RegistryRepository, ChannelArchiverControl};

/// Build a test app with sample data pre-populated in storage.
async fn build_retrieval_app() -> (axum::Router, SystemTime, SystemTime, tempfile::TempDir) {
    let dir = tempfile::tempdir().unwrap();
    let storage = Arc::new(PlainPbStoragePlugin::new(
        "sts",
        dir.path().to_path_buf(),
        PartitionGranularity::Hour,
    ));
    let registry = Arc::new(PvRegistry::in_memory().unwrap());

    registry
        .register_pv("SIM:Sine", ArchDbType::ScalarDouble, &SampleMode::Monitor, 1)
        .unwrap();

    // Write 20 samples, 1 second apart.
    let base_time = SystemTime::now() - Duration::from_secs(600);
    for i in 0..20 {
        let ts = base_time + Duration::from_secs(i);
        let val = (i as f64 * 0.1).sin();
        let sample = ArchiverSample::new(ts, ArchiverValue::ScalarDouble(val));
        storage
            .append_event("SIM:Sine", ArchDbType::ScalarDouble, &sample)
            .await
            .unwrap();
    }
    let data_start = base_time;
    let data_end = base_time + Duration::from_secs(19);

    let (channel_mgr, _rx) = ChannelManager::new(storage.clone(), registry.clone(), None)
        .await
        .unwrap();
    let state = AppState {
        storage,
        pv_repo: Arc::new(RegistryRepository::new(registry)),
        archiver: Arc::new(ChannelArchiverControl::new(Arc::new(channel_mgr))),
        cluster: None,
        api_keys: None,
        metrics_handle: None,
        rate_limiter: None,
    };
    (build_router(state, &SecurityConfig::default()), data_start, data_end, dir)
}

fn get_request(uri: &str) -> Request<Body> {
    Request::builder()
        .uri(uri)
        .body(Body::empty())
        .unwrap()
}

async fn body_to_bytes(body: Body) -> Vec<u8> {
    use axum::body::to_bytes;
    to_bytes(body, usize::MAX).await.unwrap().to_vec()
}

async fn body_to_json(body: Body) -> serde_json::Value {
    let bytes = body_to_bytes(body).await;
    serde_json::from_slice(&bytes).unwrap()
}

fn to_rfc3339(t: SystemTime) -> String {
    chrono::DateTime::<chrono::Utc>::from(t)
        .format("%Y-%m-%dT%H:%M:%S.%fZ")
        .to_string()
}

// --- Tests ---

#[tokio::test]
async fn test_get_data_json_basic() {
    let (app, start, end, _dir) = build_retrieval_app().await;
    let from = to_rfc3339(start - Duration::from_secs(1));
    let to = to_rfc3339(end + Duration::from_secs(1));
    let uri = format!("/retrieval/data/getData.json?pv=SIM:Sine&from={from}&to={to}");

    let resp = app.oneshot(get_request(&uri)).await.unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let json = body_to_json(resp.into_body()).await;
    let arr = json.as_array().unwrap();
    assert_eq!(arr.len(), 1);
    assert_eq!(arr[0]["meta"]["name"], "SIM:Sine");
    let data = arr[0]["data"].as_array().unwrap();
    assert_eq!(data.len(), 20);
    // Each sample should have secs, nanos, val, severity, status.
    let first = &data[0];
    assert!(first["secs"].is_number());
    assert!(first["nanos"].is_number());
    assert!(first["val"].is_number());
    assert!(first["severity"].is_number());
    assert!(first["status"].is_number());
}

#[tokio::test]
async fn test_get_data_json_time_filter() {
    let (app, start, _end, _dir) = build_retrieval_app().await;
    // Request only first 5 seconds of data.
    let from = to_rfc3339(start);
    let to = to_rfc3339(start + Duration::from_secs(5));
    let uri = format!("/retrieval/data/getData.json?pv=SIM:Sine&from={from}&to={to}");

    let resp = app.oneshot(get_request(&uri)).await.unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let json = body_to_json(resp.into_body()).await;
    let data = json[0]["data"].as_array().unwrap();
    // Should have at most 6 samples (seconds 0..5 inclusive).
    assert!(data.len() <= 6, "Expected <= 6 samples, got {}", data.len());
    assert!(data.len() >= 4, "Expected >= 4 samples, got {}", data.len());
}

#[tokio::test]
async fn test_get_data_json_limit() {
    let (app, start, end, _dir) = build_retrieval_app().await;
    let from = to_rfc3339(start - Duration::from_secs(1));
    let to = to_rfc3339(end + Duration::from_secs(1));
    let uri = format!("/retrieval/data/getData.json?pv=SIM:Sine&from={from}&to={to}&limit=10");

    let resp = app.oneshot(get_request(&uri)).await.unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let json = body_to_json(resp.into_body()).await;
    let data = json[0]["data"].as_array().unwrap();
    assert_eq!(data.len(), 10);
}

#[tokio::test]
async fn test_get_data_json_not_found() {
    let (app, start, end, _dir) = build_retrieval_app().await;
    let from = to_rfc3339(start);
    let to = to_rfc3339(end);
    let uri = format!("/retrieval/data/getData.json?pv=NONEXIST:PV&from={from}&to={to}");

    let resp = app.oneshot(get_request(&uri)).await.unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let json = body_to_json(resp.into_body()).await;
    let data = json[0]["data"].as_array().unwrap();
    assert!(data.is_empty());
}

#[tokio::test]
async fn test_get_data_csv_basic() {
    let (app, start, end, _dir) = build_retrieval_app().await;
    let from = to_rfc3339(start - Duration::from_secs(1));
    let to = to_rfc3339(end + Duration::from_secs(1));
    let uri = format!("/retrieval/data/getData.csv?pv=SIM:Sine&from={from}&to={to}");

    let resp = app.oneshot(get_request(&uri)).await.unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let ct = resp.headers().get("content-type").unwrap().to_str().unwrap();
    assert_eq!(ct, "text/csv");
    let body = body_to_bytes(resp.into_body()).await;
    let text = String::from_utf8(body).unwrap();
    let lines: Vec<&str> = text.lines().collect();
    // Header + 20 data rows.
    assert_eq!(lines[0], "seconds,nanos,val,severity,status");
    assert_eq!(lines.len(), 21); // header + 20 samples
}

#[tokio::test]
async fn test_get_data_raw_basic() {
    let (app, start, end, _dir) = build_retrieval_app().await;
    let from = to_rfc3339(start - Duration::from_secs(1));
    let to = to_rfc3339(end + Duration::from_secs(1));
    let uri = format!("/retrieval/data/getData.raw?pv=SIM:Sine&from={from}&to={to}");

    let resp = app.oneshot(get_request(&uri)).await.unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let ct = resp.headers().get("content-type").unwrap().to_str().unwrap();
    assert_eq!(ct, "application/x-protobuf");
    let body = body_to_bytes(resp.into_body()).await;
    assert!(!body.is_empty());
}

#[tokio::test]
async fn test_get_data_json_iso_formats() {
    let (app, start, end, _dir) = build_retrieval_app().await;
    // Test with space-separated ISO format.
    let dt_start = chrono::DateTime::<chrono::Utc>::from(start - Duration::from_secs(1));
    let dt_end = chrono::DateTime::<chrono::Utc>::from(end + Duration::from_secs(1));
    let from = dt_start.format("%Y-%m-%dT%H:%M:%S").to_string();
    let to = dt_end.format("%Y-%m-%dT%H:%M:%S").to_string();
    let uri = format!("/retrieval/data/getData.json?pv=SIM:Sine&from={from}&to={to}");

    let resp = app.oneshot(get_request(&uri)).await.unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let json = body_to_json(resp.into_body()).await;
    let data = json[0]["data"].as_array().unwrap();
    assert_eq!(data.len(), 20);
}

#[tokio::test]
async fn test_get_data_json_post_processor() {
    let (app, start, end, _dir) = build_retrieval_app().await;
    let from = to_rfc3339(start - Duration::from_secs(1));
    let to = to_rfc3339(end + Duration::from_secs(1));
    // Use mean_60 post-processor syntax.
    let uri = format!(
        "/retrieval/data/getData.json?pv=mean_60(SIM:Sine)&from={from}&to={to}"
    );

    let resp = app.oneshot(get_request(&uri)).await.unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let json = body_to_json(resp.into_body()).await;
    let arr = json.as_array().unwrap();
    assert_eq!(arr.len(), 1);
    // mean_60 over 20 seconds of data should produce fewer samples than raw.
    let data = arr[0]["data"].as_array().unwrap();
    assert!(data.len() < 20);
}

// --- Binary search / open_seeked tests ---

/// Write a PB file with many samples and return the file path and timestamps.
async fn write_large_pb_file() -> (tempfile::TempDir, std::path::PathBuf, SystemTime, SystemTime) {
    let dir = tempfile::tempdir().unwrap();
    let storage = PlainPbStoragePlugin::new(
        "sts",
        dir.path().to_path_buf(),
        PartitionGranularity::Hour,
    );

    // Write 100 samples, 1 second apart.
    let base_time = SystemTime::now() - Duration::from_secs(200);
    for i in 0..100 {
        let ts = base_time + Duration::from_secs(i);
        let val = i as f64;
        let sample = ArchiverSample::new(ts, ArchiverValue::ScalarDouble(val));
        storage
            .append_event("SIM:Test", ArchDbType::ScalarDouble, &sample)
            .await
            .unwrap();
    }

    let file_path = storage.file_path_for("SIM:Test", base_time);
    let data_end = base_time + Duration::from_secs(99);
    (dir, file_path, base_time, data_end)
}

#[tokio::test]
async fn test_open_seeked_skips_early_samples() {
    let (_dir, path, base_time, _end) = write_large_pb_file().await;
    // Seek to sample 50 (50 seconds after base).
    let seek_time = base_time + Duration::from_secs(50);
    let mut reader = PbFileReader::open_seeked(&path, seek_time).unwrap();

    // First sample should be >= seek_time.
    let first = reader.next_event().unwrap().unwrap();
    assert!(
        first.timestamp >= seek_time,
        "First sample after seek should be >= seek_time"
    );
    // Value should be around 50.0 (since val = i as f64).
    if let ArchiverValue::ScalarDouble(v) = &first.value {
        assert!(*v >= 49.0, "Expected value >= 49.0, got {v}");
    } else {
        panic!("Expected ScalarDouble");
    }
}

#[tokio::test]
async fn test_open_seeked_all_before() {
    let (_dir, path, _base_time, end) = write_large_pb_file().await;
    // Seek past all data.
    let seek_time = end + Duration::from_secs(100);
    let mut reader = PbFileReader::open_seeked(&path, seek_time).unwrap();

    // All samples are before seek_time, so next_event should eventually return None
    // or only samples before seek_time (since binary search returns None, reads from start).
    // The reader will return all samples — caller is responsible for filtering.
    // With binary search returning None, we fall back to start, so we get all samples.
    // This is correct behavior — the caller (collect_samples / drain_stream) filters.
    let first = reader.next_event().unwrap();
    assert!(first.is_some(), "Fallback should read from start");
}

#[tokio::test]
async fn test_open_seeked_fallback() {
    let (_dir, path, base_time, _end) = write_large_pb_file().await;
    // Seek to the very beginning — should get all samples.
    let mut reader = PbFileReader::open_seeked(&path, base_time).unwrap();

    let mut count = 0;
    while reader.next_event().unwrap().is_some() {
        count += 1;
    }
    assert_eq!(count, 100, "Should get all 100 samples");
}

#[tokio::test]
async fn test_get_data_seeked_correctness() {
    // End-to-end: narrow query on large file returns correct samples via storage.get_data.
    let dir = tempfile::tempdir().unwrap();
    let storage = Arc::new(PlainPbStoragePlugin::new(
        "sts",
        dir.path().to_path_buf(),
        PartitionGranularity::Hour,
    ));

    let base_time = SystemTime::now() - Duration::from_secs(200);
    for i in 0..100 {
        let ts = base_time + Duration::from_secs(i);
        let val = i as f64;
        let sample = ArchiverSample::new(ts, ArchiverValue::ScalarDouble(val));
        storage
            .append_event("SIM:Query", ArchDbType::ScalarDouble, &sample)
            .await
            .unwrap();
    }

    // Query a narrow window: samples 60-70.
    let query_start = base_time + Duration::from_secs(60);
    let query_end = base_time + Duration::from_secs(70);
    let streams = storage.get_data("SIM:Query", query_start, query_end).await.unwrap();

    let mut samples = Vec::new();
    for mut stream in streams {
        while let Ok(Some(sample)) = stream.next_event() {
            if sample.timestamp > query_end {
                break;
            }
            if sample.timestamp >= query_start {
                samples.push(sample);
            }
        }
    }

    // Should have ~11 samples (seconds 60 through 70 inclusive).
    assert!(
        samples.len() >= 10 && samples.len() <= 12,
        "Expected 10-12 samples, got {}",
        samples.len()
    );

    // All samples should be in range.
    for s in &samples {
        assert!(s.timestamp >= query_start);
        assert!(s.timestamp <= query_end);
    }
}
