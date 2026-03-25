use std::sync::Arc;
use std::time::{Duration, SystemTime};

use axum::body::Body;
use axum::http::{Request, StatusCode};
use tower::ServiceExt;

use archiver_core::registry::{PvRegistry, PvStatus, SampleMode};
use archiver_core::storage::partition::PartitionGranularity;
use archiver_core::storage::plainpb::PlainPbStoragePlugin;
use archiver_engine::channel_manager::ChannelManager;
use archiver_api::{build_router, AppState};

async fn build_test_app() -> (axum::Router, tempfile::TempDir) {
    let dir = tempfile::tempdir().unwrap();
    let storage = Arc::new(PlainPbStoragePlugin::new(
        "sts",
        dir.path().to_path_buf(),
        PartitionGranularity::Hour,
    ));
    let registry = Arc::new(PvRegistry::in_memory().unwrap());
    let (channel_mgr, _rx) = ChannelManager::new(storage.clone(), registry.clone(), None)
        .await
        .unwrap();
    let state = AppState {
        storage,
        channel_mgr: Arc::new(channel_mgr),
        registry,
        cluster: None,
        api_keys: None,
        metrics_handle: None,
    };
    (build_router(state), dir)
}

async fn build_test_app_with_pvs() -> (axum::Router, Arc<PvRegistry>, tempfile::TempDir) {
    let dir = tempfile::tempdir().unwrap();
    let storage = Arc::new(PlainPbStoragePlugin::new(
        "sts",
        dir.path().to_path_buf(),
        PartitionGranularity::Hour,
    ));
    let registry = Arc::new(PvRegistry::in_memory().unwrap());

    // Register test PVs.
    registry
        .register_pv("SIM:Sine", archiver_core::types::ArchDbType::ScalarDouble, &SampleMode::Monitor, 1)
        .unwrap();
    registry
        .register_pv("SIM:Cosine", archiver_core::types::ArchDbType::ScalarDouble, &SampleMode::Monitor, 1)
        .unwrap();
    registry
        .register_pv("TEST:Counter", archiver_core::types::ArchDbType::ScalarInt, &SampleMode::Scan { period_secs: 1.0 }, 1)
        .unwrap();

    let (channel_mgr, _rx) = ChannelManager::new(storage.clone(), registry.clone(), None)
        .await
        .unwrap();
    let state = AppState {
        storage,
        channel_mgr: Arc::new(channel_mgr),
        registry: registry.clone(),
        cluster: None,
        api_keys: None,
        metrics_handle: None,
    };
    (build_router(state), registry, dir)
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

// --- Tests ---

#[tokio::test]
async fn test_get_all_pvs_empty() {
    let (app, _dir) = build_test_app().await;
    let resp = app.oneshot(get_request("/mgmt/bpl/getAllPVs")).await.unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let json = body_to_json(resp.into_body()).await;
    assert_eq!(json, serde_json::json!([]));
}

#[tokio::test]
async fn test_get_all_pvs_with_data() {
    let (app, _reg, _dir) = build_test_app_with_pvs().await;
    let resp = app.oneshot(get_request("/mgmt/bpl/getAllPVs")).await.unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let json = body_to_json(resp.into_body()).await;
    let pvs = json.as_array().unwrap();
    assert_eq!(pvs.len(), 3);
    // Should contain all three PVs.
    let names: Vec<&str> = pvs.iter().map(|v| v.as_str().unwrap()).collect();
    assert!(names.contains(&"SIM:Sine"));
    assert!(names.contains(&"SIM:Cosine"));
    assert!(names.contains(&"TEST:Counter"));
}

#[tokio::test]
async fn test_get_matching_pvs() {
    let (app, _reg, _dir) = build_test_app_with_pvs().await;
    let resp = app
        .oneshot(get_request("/mgmt/bpl/getMatchingPVs?pv=SIM:*"))
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let json = body_to_json(resp.into_body()).await;
    let pvs = json.as_array().unwrap();
    assert_eq!(pvs.len(), 2);
    let names: Vec<&str> = pvs.iter().map(|v| v.as_str().unwrap()).collect();
    assert!(names.contains(&"SIM:Sine"));
    assert!(names.contains(&"SIM:Cosine"));
}

#[tokio::test]
async fn test_get_matching_pvs_no_match() {
    let (app, _reg, _dir) = build_test_app_with_pvs().await;
    let resp = app
        .oneshot(get_request("/mgmt/bpl/getMatchingPVs?pv=NONEXIST:*"))
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let json = body_to_json(resp.into_body()).await;
    assert_eq!(json, serde_json::json!([]));
}

#[tokio::test]
async fn test_get_pv_status_found() {
    let (app, _reg, _dir) = build_test_app_with_pvs().await;
    let resp = app
        .oneshot(get_request("/mgmt/bpl/getPVStatus?pv=SIM:Sine"))
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let json = body_to_json(resp.into_body()).await;
    assert_eq!(json["pv_name"], "SIM:Sine");
    assert_eq!(json["status"], "Being archived");
    assert!(json["dbr_type"].is_number());
    assert!(json["sample_mode"].is_string());
}

#[tokio::test]
async fn test_get_pv_status_not_found() {
    let (app, _dir) = build_test_app().await;
    let resp = app
        .oneshot(get_request("/mgmt/bpl/getPVStatus?pv=NONEXIST:PV"))
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let json = body_to_json(resp.into_body()).await;
    assert_eq!(json["status"], "Not being archived");
}

#[tokio::test]
async fn test_get_pv_count() {
    let (app, reg, _dir) = build_test_app_with_pvs().await;
    // Pause one PV.
    reg.set_status("TEST:Counter", PvStatus::Paused).unwrap();

    let resp = app
        .oneshot(get_request("/mgmt/bpl/getPVCount"))
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let json = body_to_json(resp.into_body()).await;
    assert_eq!(json["total"], 3);
    assert_eq!(json["active"], 2);
    assert_eq!(json["paused"], 1);
}

#[tokio::test]
async fn test_get_paused_pvs_report() {
    let (app, reg, _dir) = build_test_app_with_pvs().await;
    reg.set_status("SIM:Cosine", PvStatus::Paused).unwrap();

    let resp = app
        .oneshot(get_request("/mgmt/bpl/getPausedPVsReport"))
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let json = body_to_json(resp.into_body()).await;
    let arr = json.as_array().unwrap();
    assert_eq!(arr.len(), 1);
    assert_eq!(arr[0]["pvName"], "SIM:Cosine");
    assert_eq!(arr[0]["status"], "Paused");
}

#[tokio::test]
async fn test_get_recently_added_pvs() {
    let (app, _reg, _dir) = build_test_app_with_pvs().await;
    let resp = app
        .oneshot(get_request("/mgmt/bpl/getRecentlyAddedPVs"))
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let json = body_to_json(resp.into_body()).await;
    let arr = json.as_array().unwrap();
    // All 3 PVs were just created, so all should be recently added.
    assert_eq!(arr.len(), 3);
}

#[tokio::test]
async fn test_get_silent_pvs_report() {
    let (app, reg, _dir) = build_test_app_with_pvs().await;
    // Set a stale timestamp on one PV.
    let stale = SystemTime::now() - Duration::from_secs(7200);
    reg.update_last_timestamp("SIM:Sine", stale).unwrap();

    let resp = app
        .oneshot(get_request("/mgmt/bpl/getSilentPVsReport"))
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let json = body_to_json(resp.into_body()).await;
    let arr = json.as_array().unwrap();
    // SIM:Sine has a 2-hour-old timestamp → silent.
    let names: Vec<&str> = arr.iter().map(|v| v["pvName"].as_str().unwrap()).collect();
    assert!(names.contains(&"SIM:Sine"));
}

#[tokio::test]
async fn test_delete_pv() {
    let (app, reg, _dir) = build_test_app_with_pvs().await;
    assert!(reg.get_pv("TEST:Counter").unwrap().is_some());

    let resp = app
        .oneshot(get_request("/mgmt/bpl/deletePV?pv=TEST:Counter"))
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    // PV should be gone from registry.
    assert!(reg.get_pv("TEST:Counter").unwrap().is_none());
}

#[tokio::test]
async fn test_change_archival_parameters() {
    let (app, reg, _dir) = build_test_app_with_pvs().await;
    let resp = app
        .oneshot(get_request(
            "/mgmt/bpl/changeArchivalParameters?pv=SIM:Sine&samplingperiod=5.0&samplingmethod=scan",
        ))
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let record = reg.get_pv("SIM:Sine").unwrap().unwrap();
    match record.sample_mode {
        SampleMode::Scan { period_secs } => {
            assert!((period_secs - 5.0).abs() < f64::EPSILON);
        }
        _ => panic!("Expected Scan mode"),
    }
}

#[tokio::test]
async fn test_get_appliances_no_cluster() {
    let (app, _dir) = build_test_app().await;
    let resp = app
        .oneshot(get_request("/mgmt/bpl/getAppliancesInCluster"))
        .await
        .unwrap();
    // Route is not registered when cluster is None.
    assert_eq!(resp.status(), StatusCode::NOT_FOUND);
}

// --- G4: Operations tests ---

#[tokio::test]
async fn test_health_endpoint() {
    let (app, _dir) = build_test_app().await;
    let resp = app.oneshot(get_request("/health")).await.unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
}

#[tokio::test]
async fn test_health_mgmt_endpoint() {
    let (app, _dir) = build_test_app().await;
    let resp = app.oneshot(get_request("/mgmt/bpl/health")).await.unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
}

#[tokio::test]
async fn test_get_versions() {
    let (app, _dir) = build_test_app().await;
    let resp = app
        .oneshot(get_request("/mgmt/bpl/getVersions"))
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let json = body_to_json(resp.into_body()).await;
    assert!(json["version"].is_string());
    assert_eq!(json["status"], "running");
}

// --- G5: BPL extension tests ---

#[tokio::test]
async fn test_get_pv_type_info() {
    let (app, _reg, _dir) = build_test_app_with_pvs().await;
    let resp = app
        .oneshot(get_request("/mgmt/bpl/getPVTypeInfo?pv=SIM:Sine"))
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let json = body_to_json(resp.into_body()).await;
    assert_eq!(json["pvName"], "SIM:Sine");
    assert_eq!(json["DBRType"], 6); // ScalarDouble
    assert_eq!(json["samplingMethod"], "Monitor");
    assert_eq!(json["elementCount"], 1);
}

#[tokio::test]
async fn test_get_pv_type_info_not_found() {
    let (app, _dir) = build_test_app().await;
    let resp = app
        .oneshot(get_request("/mgmt/bpl/getPVTypeInfo?pv=NONEXIST"))
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::NOT_FOUND);
}

#[tokio::test]
async fn test_get_pv_details() {
    let (app, _reg, _dir) = build_test_app_with_pvs().await;
    let resp = app
        .oneshot(get_request("/mgmt/bpl/getPVDetails?pv=SIM:Sine"))
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let json = body_to_json(resp.into_body()).await;
    assert_eq!(json["pvName"], "SIM:Sine");
    // Should include connection info fields.
    assert!(json.get("isConnected").is_some());
}

#[tokio::test]
async fn test_abort_archiving_pv() {
    let (app, reg, _dir) = build_test_app_with_pvs().await;
    // Pause first, then abort.
    reg.set_status("TEST:Counter", archiver_core::registry::PvStatus::Paused).unwrap();

    let resp = app
        .oneshot(get_request("/mgmt/bpl/abortArchivingPV?pv=TEST:Counter"))
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    // PV should remain in registry with Inactive status after abort.
    let record = reg.get_pv("TEST:Counter").unwrap().expect("PV should still exist");
    assert_eq!(record.status, archiver_core::registry::PvStatus::Inactive);
}

#[tokio::test]
async fn test_abort_archiving_rejects_active() {
    let (app, _reg, _dir) = build_test_app_with_pvs().await;
    let resp = app
        .oneshot(get_request("/mgmt/bpl/abortArchivingPV?pv=SIM:Sine"))
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
}

#[tokio::test]
async fn test_export_import_roundtrip() {
    use axum::body::Body;
    use axum::http::Request;

    let (app, reg, _dir) = build_test_app_with_pvs().await;

    // Update metadata for one PV.
    reg.update_metadata("SIM:Sine", Some("3"), Some("mm")).unwrap();
    // Pause one PV to test status preservation.
    reg.set_status("SIM:Cosine", PvStatus::Paused).unwrap();

    // Export.
    let resp = app
        .clone()
        .oneshot(get_request("/mgmt/bpl/exportConfig"))
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let export_json = body_to_json(resp.into_body()).await;
    let export_arr = export_json.as_array().unwrap();
    assert_eq!(export_arr.len(), 3);

    // Find the SIM:Sine record to verify metadata was exported.
    let sine = export_arr.iter().find(|r| r["pvName"] == "SIM:Sine").unwrap();
    assert_eq!(sine["PREC"], "3");
    assert_eq!(sine["EGU"], "mm");

    // Verify status and created_at are exported.
    let cosine = export_arr.iter().find(|r| r["pvName"] == "SIM:Cosine").unwrap();
    assert_eq!(cosine["status"], "paused");
    assert!(cosine["createdAt"].is_string());

    // Import into a fresh app.
    let (app2, _dir2) = build_test_app().await;
    let import_body = serde_json::to_string(&export_arr).unwrap();
    let req = Request::builder()
        .method("POST")
        .uri("/mgmt/bpl/importConfig")
        .header("content-type", "application/json")
        .body(Body::from(import_body))
        .unwrap();
    let resp = app2.oneshot(req).await.unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let json = body_to_json(resp.into_body()).await;
    assert_eq!(json["imported"], 3);
}

#[tokio::test]
async fn test_export_import_preserves_status() {
    use axum::body::Body;
    use axum::http::Request;

    let (app, reg, _dir) = build_test_app_with_pvs().await;
    reg.set_status("SIM:Cosine", PvStatus::Paused).unwrap();

    // Export.
    let resp = app
        .clone()
        .oneshot(get_request("/mgmt/bpl/exportConfig"))
        .await
        .unwrap();
    let export_json = body_to_json(resp.into_body()).await;
    let export_arr = export_json.as_array().unwrap();

    // Import into a fresh app and verify status is preserved.
    let dir2 = tempfile::tempdir().unwrap();
    let storage2 = Arc::new(PlainPbStoragePlugin::new(
        "sts",
        dir2.path().to_path_buf(),
        PartitionGranularity::Hour,
    ));
    let registry2 = Arc::new(PvRegistry::in_memory().unwrap());
    let (channel_mgr2, _rx2) = ChannelManager::new(storage2.clone(), registry2.clone(), None)
        .await
        .unwrap();
    let state2 = AppState {
        storage: storage2,
        channel_mgr: Arc::new(channel_mgr2),
        registry: registry2.clone(),
        cluster: None,
        api_keys: None,
        metrics_handle: None,
    };
    let app2 = build_router(state2);

    let import_body = serde_json::to_string(&export_arr).unwrap();
    let req = Request::builder()
        .method("POST")
        .uri("/mgmt/bpl/importConfig")
        .header("content-type", "application/json")
        .body(Body::from(import_body))
        .unwrap();
    let resp = app2.oneshot(req).await.unwrap();
    assert_eq!(resp.status(), StatusCode::OK);

    // Verify SIM:Cosine is paused in the new registry.
    let record = registry2.get_pv("SIM:Cosine").unwrap().unwrap();
    assert_eq!(record.status, PvStatus::Paused);

    // Verify SIM:Sine is still active.
    let record = registry2.get_pv("SIM:Sine").unwrap().unwrap();
    assert_eq!(record.status, PvStatus::Active);

    // Verify created_at is preserved (should match original, not import time).
    let orig_sine = export_arr.iter().find(|r| r["pvName"] == "SIM:Sine").unwrap();
    let orig_created = orig_sine["createdAt"].as_str().unwrap();
    let imported_created = record.created_at.to_rfc3339();
    assert_eq!(orig_created, imported_created, "created_at should be preserved across export/import");
}

#[tokio::test]
async fn test_export_empty() {
    let (app, _dir) = build_test_app().await;
    let resp = app
        .oneshot(get_request("/mgmt/bpl/exportConfig"))
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let json = body_to_json(resp.into_body()).await;
    assert_eq!(json, serde_json::json!([]));
}

// --- R5: Auth middleware tests ---

async fn build_test_app_with_auth() -> (axum::Router, tempfile::TempDir) {
    let dir = tempfile::tempdir().unwrap();
    let storage = Arc::new(PlainPbStoragePlugin::new(
        "sts",
        dir.path().to_path_buf(),
        PartitionGranularity::Hour,
    ));
    let registry = Arc::new(PvRegistry::in_memory().unwrap());
    registry
        .register_pv("SIM:Sine", archiver_core::types::ArchDbType::ScalarDouble, &SampleMode::Monitor, 1)
        .unwrap();
    let (channel_mgr, _rx) = ChannelManager::new(storage.clone(), registry.clone(), None)
        .await
        .unwrap();
    let state = AppState {
        storage,
        channel_mgr: Arc::new(channel_mgr),
        registry,
        cluster: None,
        api_keys: Some(vec!["test-secret-key".to_string()]),
        metrics_handle: None,
    };
    (build_router(state), dir)
}

#[tokio::test]
async fn test_auth_blocks_write_without_key() {
    let (app, _dir) = build_test_app_with_auth().await;
    // POST archivePV without API key should be rejected.
    let req = Request::builder()
        .method("POST")
        .uri("/mgmt/bpl/archivePV")
        .header("content-type", "application/json")
        .body(Body::from(r#"{"pv":"NEW:PV"}"#))
        .unwrap();
    let resp = app.oneshot(req).await.unwrap();
    assert_eq!(resp.status(), StatusCode::UNAUTHORIZED);
}

#[tokio::test]
async fn test_auth_allows_write_with_valid_key() {
    let (app, _dir) = build_test_app_with_auth().await;
    // POST archivePV with valid Bearer token should succeed (or at least not 401).
    let req = Request::builder()
        .method("POST")
        .uri("/mgmt/bpl/archivePV")
        .header("content-type", "application/json")
        .header("authorization", "Bearer test-secret-key")
        .body(Body::from(r#"{"pv":"NEW:PV"}"#))
        .unwrap();
    let resp = app.oneshot(req).await.unwrap();
    // Should not be 401 — the request is authenticated.
    assert_ne!(resp.status(), StatusCode::UNAUTHORIZED);
}

#[tokio::test]
async fn test_auth_allows_read_without_key() {
    let (app, _dir) = build_test_app_with_auth().await;
    // GET getAllPVs should work without API key (read-only exempt).
    let resp = app
        .oneshot(get_request("/mgmt/bpl/getAllPVs"))
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
}

#[tokio::test]
async fn test_auth_rejects_invalid_key() {
    let (app, _dir) = build_test_app_with_auth().await;
    let req = Request::builder()
        .method("POST")
        .uri("/mgmt/bpl/archivePV")
        .header("content-type", "application/json")
        .header("authorization", "Bearer wrong-key")
        .body(Body::from(r#"{"pv":"NEW:PV"}"#))
        .unwrap();
    let resp = app.oneshot(req).await.unwrap();
    assert_eq!(resp.status(), StatusCode::UNAUTHORIZED);
}

// --- G3: Metadata tests ---

#[tokio::test]
async fn test_metadata_prec_egu_in_registry() {
    let reg = PvRegistry::in_memory().unwrap();
    reg.register_pv("TEST:Meta", archiver_core::types::ArchDbType::ScalarDouble, &SampleMode::Monitor, 1)
        .unwrap();

    // Initially null.
    let r = reg.get_pv("TEST:Meta").unwrap().unwrap();
    assert!(r.prec.is_none());
    assert!(r.egu.is_none());

    // Update metadata.
    reg.update_metadata("TEST:Meta", Some("5"), Some("keV")).unwrap();

    let r = reg.get_pv("TEST:Meta").unwrap().unwrap();
    assert_eq!(r.prec.as_deref(), Some("5"));
    assert_eq!(r.egu.as_deref(), Some("keV"));
}

#[tokio::test]
async fn test_metadata_partial_update() {
    let reg = PvRegistry::in_memory().unwrap();
    reg.register_pv("TEST:Partial", archiver_core::types::ArchDbType::ScalarDouble, &SampleMode::Monitor, 1)
        .unwrap();

    // Update only EGU.
    reg.update_metadata("TEST:Partial", None, Some("mm")).unwrap();
    let r = reg.get_pv("TEST:Partial").unwrap().unwrap();
    assert!(r.prec.is_none());
    assert_eq!(r.egu.as_deref(), Some("mm"));

    // Update only PREC — EGU should be preserved.
    reg.update_metadata("TEST:Partial", Some("2"), None).unwrap();
    let r = reg.get_pv("TEST:Partial").unwrap().unwrap();
    assert_eq!(r.prec.as_deref(), Some("2"));
    assert_eq!(r.egu.as_deref(), Some("mm"));
}
