use std::sync::Arc;
use std::time::{Duration, SystemTime};

use axum::body::Body;
use axum::http::{Request, StatusCode};
use tower::ServiceExt;

use archiver_core::registry::{PvRegistry, PvStatus, SampleMode};
use archiver_core::storage::partition::PartitionGranularity;
use archiver_core::storage::plainpb::PlainPbStoragePlugin;
use archiver_engine::channel_manager::ChannelManager;
use archiver_core::config::SecurityConfig;
use archiver_api::{build_router, AppState};
use archiver_api::services::impls::{ChannelArchiverControl, RegistryRepository};

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
    let channel_mgr = Arc::new(channel_mgr);
    let repo = Arc::new(RegistryRepository::new(registry));
    let archiver = Arc::new(ChannelArchiverControl::new(channel_mgr));
    let state = AppState {
        storage,
        pv_query: repo.clone(),
        pv_cmd: repo,
        archiver_query: archiver.clone(),
        archiver_cmd: archiver,
        cluster: None,
        api_keys: None,
        cluster_api_key: None,
        metrics_handle: None,
        rate_limiter: None,
        trust_proxy_headers: false,
        failover: None,
        etl_chain: Vec::new(),
    };
    (build_router(state, &SecurityConfig::default()), dir)
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
    let channel_mgr = Arc::new(channel_mgr);
    let repo = Arc::new(RegistryRepository::new(registry.clone()));
    let archiver = Arc::new(ChannelArchiverControl::new(channel_mgr));
    let state = AppState {
        storage,
        pv_query: repo.clone(),
        pv_cmd: repo,
        archiver_query: archiver.clone(),
        archiver_cmd: archiver,
        cluster: None,
        api_keys: None,
        cluster_api_key: None,
        metrics_handle: None,
        rate_limiter: None,
        trust_proxy_headers: false,
        failover: None,
        etl_chain: Vec::new(),
    };
    (build_router(state, &SecurityConfig::default()), registry, dir)
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
    let channel_mgr2 = Arc::new(channel_mgr2);
    let repo2 = Arc::new(RegistryRepository::new(registry2.clone()));
    let archiver2 = Arc::new(ChannelArchiverControl::new(channel_mgr2));
    let state2 = AppState {
        storage: storage2,
        pv_query: repo2.clone(),
        pv_cmd: repo2,
        archiver_query: archiver2.clone(),
        archiver_cmd: archiver2,
        cluster: None,
        api_keys: None,
        cluster_api_key: None,
        metrics_handle: None,
        rate_limiter: None,
        trust_proxy_headers: false,
        failover: None,
        etl_chain: Vec::new(),
    };
    let app2 = build_router(state2, &SecurityConfig::default());

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
    let channel_mgr = Arc::new(channel_mgr);
    let repo = Arc::new(RegistryRepository::new(registry));
    let archiver = Arc::new(ChannelArchiverControl::new(channel_mgr));
    let state = AppState {
        storage,
        pv_query: repo.clone(),
        pv_cmd: repo,
        archiver_query: archiver.clone(),
        archiver_cmd: archiver,
        cluster: None,
        api_keys: Some(vec!["test-secret-key".to_string()]),
        cluster_api_key: None,
        metrics_handle: None,
        rate_limiter: None,
        trust_proxy_headers: false,
        failover: None,
        etl_chain: Vec::new(),
    };
    (build_router(state, &SecurityConfig::default()), dir)
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

// --- Rate limiting tests ---

#[tokio::test]
async fn test_rate_limiter_blocks_excess_requests() {
    use archiver_api::security::RateLimiter;

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
    let channel_mgr = Arc::new(channel_mgr);
    let repo = Arc::new(RegistryRepository::new(registry));
    let archiver = Arc::new(ChannelArchiverControl::new(channel_mgr));

    // Very restrictive: 1 rps, burst of 2.
    let limiter = Arc::new(RateLimiter::new(1, 2));

    let state = AppState {
        storage,
        pv_query: repo.clone(),
        pv_cmd: repo,
        archiver_query: archiver.clone(),
        archiver_cmd: archiver,
        cluster: None,
        api_keys: None,
        cluster_api_key: None,
        metrics_handle: None,
        rate_limiter: Some(limiter),
        trust_proxy_headers: true,
        failover: None,
        etl_chain: Vec::new(),
    };
    let app = build_router(state, &SecurityConfig::default());

    // The rate limiter reads X-Forwarded-For (trust_proxy_headers=true); simulate requests.
    let mut ok_count = 0;
    let mut throttled_count = 0;

    for _ in 0..5 {
        let req = Request::builder()
            .uri("/health")
            .header("x-forwarded-for", "10.0.0.1")
            .body(Body::empty())
            .unwrap();
        let resp = app.clone().oneshot(req).await.unwrap();
        if resp.status() == StatusCode::TOO_MANY_REQUESTS {
            throttled_count += 1;
        } else {
            ok_count += 1;
        }
    }

    // With burst=2, at most 2 requests should succeed.
    assert!(ok_count <= 2, "Expected at most 2 OK responses, got {ok_count}");
    assert!(throttled_count >= 3, "Expected at least 3 throttled responses, got {throttled_count}");
}

#[tokio::test]
async fn test_rate_limiter_isolates_by_ip() {
    use archiver_api::security::RateLimiter;

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
    let channel_mgr = Arc::new(channel_mgr);
    let repo = Arc::new(RegistryRepository::new(registry));
    let archiver = Arc::new(ChannelArchiverControl::new(channel_mgr));

    // 1 rps, burst 1 — very tight.
    let limiter = Arc::new(RateLimiter::new(1, 1));

    let state = AppState {
        storage,
        pv_query: repo.clone(),
        pv_cmd: repo,
        archiver_query: archiver.clone(),
        archiver_cmd: archiver,
        cluster: None,
        api_keys: None,
        cluster_api_key: None,
        metrics_handle: None,
        rate_limiter: Some(limiter),
        trust_proxy_headers: true,
        failover: None,
        etl_chain: Vec::new(),
    };
    let app = build_router(state, &SecurityConfig::default());

    // First request from IP A — should succeed.
    let req_a = Request::builder()
        .uri("/health")
        .header("x-forwarded-for", "10.0.0.1")
        .body(Body::empty())
        .unwrap();
    let resp = app.clone().oneshot(req_a).await.unwrap();
    assert_eq!(resp.status(), StatusCode::OK);

    // Second request from IP A — should be throttled.
    let req_a2 = Request::builder()
        .uri("/health")
        .header("x-forwarded-for", "10.0.0.1")
        .body(Body::empty())
        .unwrap();
    let resp = app.clone().oneshot(req_a2).await.unwrap();
    assert_eq!(resp.status(), StatusCode::TOO_MANY_REQUESTS);

    // First request from IP B — should succeed (different bucket).
    let req_b = Request::builder()
        .uri("/health")
        .header("x-forwarded-for", "10.0.0.2")
        .body(Body::empty())
        .unwrap();
    let resp = app.clone().oneshot(req_b).await.unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
}

#[tokio::test]
async fn test_add_alias_and_get_all_aliases() {
    let (app, registry, _dir) = build_test_app_with_pvs().await;

    // Add alias DEV:Sine -> SIM:Sine.
    let req = get_request("/mgmt/bpl/addAlias?pv=SIM:Sine&aliasname=DEV:Sine");
    let resp = app.clone().oneshot(req).await.unwrap();
    assert_eq!(resp.status(), StatusCode::OK);

    // Verify in registry directly.
    assert_eq!(
        registry.resolve_alias("DEV:Sine").unwrap().as_deref(),
        Some("SIM:Sine"),
    );

    // getAllAliases returns the pair.
    let req = get_request("/mgmt/bpl/getAllAliases");
    let resp = app.clone().oneshot(req).await.unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let body = body_to_json(resp.into_body()).await;
    let arr = body.as_array().unwrap();
    assert_eq!(arr.len(), 1);
    assert_eq!(arr[0]["aliasName"], "DEV:Sine");
    assert_eq!(arr[0]["srcPVName"], "SIM:Sine");

    // getAllExpandedPVNames includes both alias and target.
    let req = get_request("/mgmt/bpl/getAllExpandedPVNames");
    let resp = app.clone().oneshot(req).await.unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let names: Vec<String> = serde_json::from_value(body_to_json(resp.into_body()).await).unwrap();
    assert!(names.contains(&"DEV:Sine".to_string()));
    assert!(names.contains(&"SIM:Sine".to_string()));
}

#[tokio::test]
async fn test_remove_alias() {
    let (app, _reg, _dir) = build_test_app_with_pvs().await;

    let req = get_request("/mgmt/bpl/addAlias?pv=SIM:Cosine&aliasname=DEV:Cos");
    assert_eq!(app.clone().oneshot(req).await.unwrap().status(), StatusCode::OK);

    let req = get_request("/mgmt/bpl/removeAlias?pv=SIM:Cosine&aliasname=DEV:Cos");
    let resp = app.clone().oneshot(req).await.unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let body = body_to_json(resp.into_body()).await;
    assert_eq!(body["status"], "ok");

    // Removing again returns not_found.
    let req = get_request("/mgmt/bpl/removeAlias?pv=SIM:Cosine&aliasname=DEV:Cos");
    let resp = app.clone().oneshot(req).await.unwrap();
    let body = body_to_json(resp.into_body()).await;
    assert_eq!(body["status"], "not_found");
}

#[tokio::test]
async fn test_get_pv_status_resolves_alias() {
    let (app, _reg, _dir) = build_test_app_with_pvs().await;

    let req = get_request("/mgmt/bpl/addAlias?pv=SIM:Sine&aliasname=DEV:Foo");
    assert_eq!(app.clone().oneshot(req).await.unwrap().status(), StatusCode::OK);

    // Querying the alias returns the target's status.
    let req = get_request("/mgmt/bpl/getPVStatus?pv=DEV:Foo");
    let resp = app.clone().oneshot(req).await.unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let body = body_to_json(resp.into_body()).await;
    assert_eq!(body["pv_name"], "SIM:Sine");
    assert_eq!(body["status"], "Being archived");
}

#[tokio::test]
async fn test_archive_pv_rejects_alias_name() {
    let (app, _reg, _dir) = build_test_app_with_pvs().await;

    let req = get_request("/mgmt/bpl/addAlias?pv=SIM:Sine&aliasname=DEV:Bar");
    assert_eq!(app.clone().oneshot(req).await.unwrap().status(), StatusCode::OK);

    // POST archivePV with the alias name should be rejected.
    let body = serde_json::json!({"pv": "DEV:Bar"}).to_string();
    let req = Request::builder()
        .method("POST")
        .uri("/mgmt/bpl/archivePV")
        .header("content-type", "application/json")
        .body(Body::from(body))
        .unwrap();
    let resp = app.clone().oneshot(req).await.unwrap();
    assert_eq!(resp.status(), StatusCode::CONFLICT);
}

#[tokio::test]
async fn test_get_pv_type_info_keys_includes_aliases() {
    let (app, _reg, _dir) = build_test_app_with_pvs().await;
    let req = get_request("/mgmt/bpl/addAlias?pv=SIM:Sine&aliasname=DEV:Foo");
    assert_eq!(app.clone().oneshot(req).await.unwrap().status(), StatusCode::OK);

    let req = get_request("/mgmt/bpl/getPVTypeInfoKeys");
    let resp = app.clone().oneshot(req).await.unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let names: Vec<String> = serde_json::from_value(body_to_json(resp.into_body()).await).unwrap();
    assert!(names.contains(&"SIM:Sine".to_string()));
    assert!(names.contains(&"DEV:Foo".to_string()));
}

#[tokio::test]
async fn test_put_pv_type_info_creates_and_overrides() {
    let (app, _reg, _dir) = build_test_app_with_pvs().await;

    // Create new PV via PUT.
    let body = serde_json::json!({
        "pvName": "NEW:PV",
        "DBRType": 6,
        "samplingMethod": "Scan",
        "samplingPeriod": 2.0,
        "elementCount": 1,
        "PREC": "4",
        "EGU": "V",
        "archiveFields": ["HIHI", "LOLO"],
        "policyName": "ring",
    });
    let req = Request::builder()
        .method("POST")
        .uri("/mgmt/bpl/putPVTypeInfo?pv=NEW:PV&createnew=true")
        .header("content-type", "application/json")
        .body(Body::from(body.to_string()))
        .unwrap();
    let resp = app.clone().oneshot(req).await.unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let typeinfo = body_to_json(resp.into_body()).await;
    assert_eq!(typeinfo["pvName"], "NEW:PV");
    assert_eq!(typeinfo["samplingMethod"], "Scan");
    assert_eq!(typeinfo["samplingPeriod"], 2.0);
    assert_eq!(typeinfo["archiveFields"][0], "HIHI");
    assert_eq!(typeinfo["policyName"], "ring");

    // Without override: re-PUT should 409.
    let req = Request::builder()
        .method("POST")
        .uri("/mgmt/bpl/putPVTypeInfo?pv=NEW:PV&createnew=true")
        .header("content-type", "application/json")
        .body(Body::from(serde_json::json!({"PREC":"5"}).to_string()))
        .unwrap();
    let resp = app.clone().oneshot(req).await.unwrap();
    assert_eq!(resp.status(), StatusCode::CONFLICT);

    // With override: PREC updates.
    let req = Request::builder()
        .method("POST")
        .uri("/mgmt/bpl/putPVTypeInfo?pv=NEW:PV&override=true")
        .header("content-type", "application/json")
        .body(Body::from(serde_json::json!({"PREC":"5"}).to_string()))
        .unwrap();
    let resp = app.clone().oneshot(req).await.unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let typeinfo = body_to_json(resp.into_body()).await;
    assert_eq!(typeinfo["PREC"], "5");
    // archiveFields preserved across override.
    assert_eq!(typeinfo["archiveFields"][0], "HIHI");
}

#[tokio::test]
async fn test_put_pv_type_info_requires_flag() {
    let (app, _reg, _dir) = build_test_app_with_pvs().await;
    let body = serde_json::json!({"DBRType": 6, "samplingMethod": "Monitor"});
    let req = Request::builder()
        .method("POST")
        .uri("/mgmt/bpl/putPVTypeInfo?pv=SIM:Sine")
        .header("content-type", "application/json")
        .body(Body::from(body.to_string()))
        .unwrap();
    let resp = app.clone().oneshot(req).await.unwrap();
    assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
}

#[tokio::test]
async fn test_get_stores_for_pv() {
    let (app, _reg, _dir) = build_test_app_with_pvs().await;
    let req = get_request("/mgmt/bpl/getStoresForPV?pv=SIM:Sine");
    let resp = app.clone().oneshot(req).await.unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let body = body_to_json(resp.into_body()).await;
    // The test app uses a single tier (sts).
    assert!(body["sts"].as_str().unwrap().starts_with("pb://localhost"));
    assert_eq!(body["sts_files"].as_u64().unwrap(), 0);
}

#[tokio::test]
async fn test_get_stores_for_pv_resolves_alias() {
    let (app, _reg, _dir) = build_test_app_with_pvs().await;
    let req = get_request("/mgmt/bpl/addAlias?pv=SIM:Sine&aliasname=DEV:Foo");
    assert_eq!(app.clone().oneshot(req).await.unwrap().status(), StatusCode::OK);

    let req = get_request("/mgmt/bpl/getStoresForPV?pv=DEV:Foo");
    let resp = app.clone().oneshot(req).await.unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let body = body_to_json(resp.into_body()).await;
    assert!(body.is_object());
}

#[tokio::test]
async fn test_rename_pv_requires_paused_source() {
    let (app, _reg, _dir) = build_test_app_with_pvs().await;
    // Active PV -> rename should be Conflict
    let req = get_request("/mgmt/bpl/renamePV?pv=SIM:Sine&newname=SIM:Sine2");
    let resp = app.clone().oneshot(req).await.unwrap();
    assert_eq!(resp.status(), StatusCode::CONFLICT);
}

#[tokio::test]
async fn test_rename_pv_after_pause_creates_destination() {
    let (app, registry, _dir) = build_test_app_with_pvs().await;

    // Pause source.
    let req = get_request("/mgmt/bpl/pauseArchivingPV?pv=SIM:Cosine");
    assert_eq!(app.clone().oneshot(req).await.unwrap().status(), StatusCode::OK);

    let req = get_request("/mgmt/bpl/renamePV?pv=SIM:Cosine&newname=SIM:CosineRenamed");
    let resp = app.clone().oneshot(req).await.unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let body = body_to_json(resp.into_body()).await;
    assert_eq!(body["from"], "SIM:Cosine");
    assert_eq!(body["to"], "SIM:CosineRenamed");

    // Both should exist and be paused.
    let src = registry.get_pv("SIM:Cosine").unwrap().unwrap();
    assert_eq!(src.status, archiver_core::registry::PvStatus::Paused);
    let dst = registry.get_pv("SIM:CosineRenamed").unwrap().unwrap();
    assert_eq!(dst.status, archiver_core::registry::PvStatus::Paused);
    assert_eq!(dst.dbr_type, src.dbr_type);
}

#[tokio::test]
async fn test_modify_meta_fields_requires_pause() {
    let (app, _reg, _dir) = build_test_app_with_pvs().await;
    // Active PV → 409
    let req = get_request("/mgmt/bpl/modifyMetaFields?pv=SIM:Sine&command=add,HIHI");
    let resp = app.clone().oneshot(req).await.unwrap();
    assert_eq!(resp.status(), StatusCode::CONFLICT);
}

#[tokio::test]
async fn test_modify_meta_fields_add_remove_clear() {
    let (app, registry, _dir) = build_test_app_with_pvs().await;
    let req = get_request("/mgmt/bpl/pauseArchivingPV?pv=SIM:Sine");
    assert_eq!(app.clone().oneshot(req).await.unwrap().status(), StatusCode::OK);

    // Add HIHI, LOLO.
    let req = get_request("/mgmt/bpl/modifyMetaFields?pv=SIM:Sine&command=add,HIHI,LOLO");
    let resp = app.clone().oneshot(req).await.unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let body = body_to_json(resp.into_body()).await;
    let fields: Vec<String> = serde_json::from_value(body["archiveFields"].clone()).unwrap();
    assert!(fields.contains(&"HIHI".to_string()));
    assert!(fields.contains(&"LOLO".to_string()));
    assert_eq!(
        registry.get_pv("SIM:Sine").unwrap().unwrap().archive_fields,
        fields,
    );

    // Remove LOLO, add EGU in one call (multiple `command=`).
    let req = get_request(
        "/mgmt/bpl/modifyMetaFields?pv=SIM:Sine&command=remove,LOLO&command=add,EGU",
    );
    let resp = app.clone().oneshot(req).await.unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let body = body_to_json(resp.into_body()).await;
    let fields: Vec<String> = serde_json::from_value(body["archiveFields"].clone()).unwrap();
    assert!(fields.contains(&"HIHI".to_string()));
    assert!(!fields.contains(&"LOLO".to_string()));
    assert!(fields.contains(&"EGU".to_string()));

    // Clear.
    let req = get_request("/mgmt/bpl/modifyMetaFields?pv=SIM:Sine&command=clear");
    let resp = app.clone().oneshot(req).await.unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let body = body_to_json(resp.into_body()).await;
    let fields: Vec<String> = serde_json::from_value(body["archiveFields"].clone()).unwrap();
    assert!(fields.is_empty());
}

#[tokio::test]
async fn test_modify_meta_fields_invalid_verb() {
    let (app, _reg, _dir) = build_test_app_with_pvs().await;
    let req = get_request("/mgmt/bpl/pauseArchivingPV?pv=SIM:Sine");
    assert_eq!(app.clone().oneshot(req).await.unwrap().status(), StatusCode::OK);

    let req = get_request("/mgmt/bpl/modifyMetaFields?pv=SIM:Sine&command=xyz,HIHI");
    let resp = app.clone().oneshot(req).await.unwrap();
    assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
}

#[tokio::test]
async fn test_get_appliance_metrics_shape() {
    let (app, _reg, _dir) = build_test_app_with_pvs().await;
    let req = get_request("/mgmt/bpl/getApplianceMetrics");
    let resp = app.clone().oneshot(req).await.unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let body = body_to_json(resp.into_body()).await;
    assert_eq!(body["pvCount"]["total"], 3);
    assert_eq!(body["pvCount"]["active"], 3);
    assert_eq!(body["pvCount"]["paused"], 0);
    assert!(!body["stores"].as_array().unwrap().is_empty());
    let store = &body["stores"][0];
    assert!(store["name"].is_string());
    assert!(store["totalFiles"].is_number());
    assert!(store["totalSizeBytes"].is_number());
}

#[tokio::test]
async fn test_consolidate_data_for_pv() {
    let (app, _reg, _dir) = build_test_app_with_pvs().await;
    let req = get_request("/mgmt/bpl/consolidateDataForPV?pv=SIM:Sine&storage=sts");
    let resp = app.clone().oneshot(req).await.unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let body = body_to_json(resp.into_body()).await;
    assert_eq!(body["status"], "ok");
    assert_eq!(body["pv"], "SIM:Sine");
    assert!(body["tiers"].is_array());
}

#[tokio::test]
async fn test_get_storage_usage_for_pv() {
    let (app, _reg, _dir) = build_test_app_with_pvs().await;
    let req = get_request("/mgmt/bpl/getStorageUsageForPV?pv=SIM:Sine");
    let resp = app.clone().oneshot(req).await.unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let body = body_to_json(resp.into_body()).await;
    // Test app uses single-tier storage named "sts", and we haven't written any
    // events, so file count is 0.
    assert_eq!(body["sts"], 0);
}

#[tokio::test]
async fn test_reset_failover_caches_stub() {
    let (app, _reg, _dir) = build_test_app_with_pvs().await;
    let req = get_request("/mgmt/bpl/resetFailoverCaches");
    let resp = app.clone().oneshot(req).await.unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let body = body_to_json(resp.into_body()).await;
    assert_eq!(body["status"], "ok");
}

#[tokio::test]
async fn test_pause_resume_via_alias_targets_real_pv() {
    use archiver_core::registry::PvStatus;
    let (app, registry, _dir) = build_test_app_with_pvs().await;

    // Create alias DEV:Foo -> SIM:Sine.
    let req = get_request("/mgmt/bpl/addAlias?pv=SIM:Sine&aliasname=DEV:Foo");
    assert_eq!(app.clone().oneshot(req).await.unwrap().status(), StatusCode::OK);

    // Pause via alias name.
    let req = get_request("/mgmt/bpl/pauseArchivingPV?pv=DEV:Foo");
    let resp = app.clone().oneshot(req).await.unwrap();
    assert_eq!(resp.status(), StatusCode::OK);

    // The TARGET should be paused.
    let target = registry.get_pv("SIM:Sine").unwrap().unwrap();
    assert_eq!(target.status, PvStatus::Paused);
    // Alias row stays as 'alias' (sentinel), not paused.
    let alias_row = registry.get_pv("DEV:Foo").unwrap().unwrap();
    assert_eq!(alias_row.status, PvStatus::Alias);

    // Resume via alias name.
    let req = get_request("/mgmt/bpl/resumeArchivingPV?pv=DEV:Foo");
    let resp = app.clone().oneshot(req).await.unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let target = registry.get_pv("SIM:Sine").unwrap().unwrap();
    assert_eq!(target.status, PvStatus::Active);
}

#[tokio::test]
async fn test_alias_excluded_from_count_and_status_queries() {
    let (app, registry, _dir) = build_test_app_with_pvs().await;
    // Build state has 3 real PVs.
    let req = get_request("/mgmt/bpl/getPVCount");
    let resp = app.clone().oneshot(req).await.unwrap();
    let body = body_to_json(resp.into_body()).await;
    assert_eq!(body["total"], 3);

    // Add an alias — count must stay 3 (not 4).
    let req = get_request("/mgmt/bpl/addAlias?pv=SIM:Sine&aliasname=DEV:Foo");
    assert_eq!(app.clone().oneshot(req).await.unwrap().status(), StatusCode::OK);
    let req = get_request("/mgmt/bpl/getPVCount");
    let resp = app.clone().oneshot(req).await.unwrap();
    let body = body_to_json(resp.into_body()).await;
    assert_eq!(body["total"], 3, "alias must not bump real-PV count");

    // pvs_by_status(Active) should return 3 reals, not 4.
    use archiver_core::registry::PvStatus;
    let actives = registry.pvs_by_status(PvStatus::Active).unwrap();
    assert_eq!(actives.len(), 3);
    assert!(actives.iter().all(|r| r.alias_for.is_none()));

    // matching_pvs("*") returns 3 reals only.
    let all = registry.matching_pvs("*").unwrap();
    assert_eq!(all.len(), 3);
    assert!(!all.contains(&"DEV:Foo".to_string()));
}

#[tokio::test]
async fn test_restore_from_registry_skips_aliases() {
    use std::sync::Arc;
    use archiver_core::registry::{PvRegistry, PvStatus, SampleMode};
    use archiver_core::storage::partition::PartitionGranularity;
    use archiver_core::storage::plainpb::PlainPbStoragePlugin;
    use archiver_core::types::ArchDbType;
    use archiver_engine::channel_manager::ChannelManager;

    let dir = tempfile::tempdir().unwrap();
    let storage = Arc::new(PlainPbStoragePlugin::new(
        "sts",
        dir.path().to_path_buf(),
        PartitionGranularity::Hour,
    ));
    let registry = Arc::new(PvRegistry::in_memory().unwrap());
    registry.register_pv("REAL:PV", ArchDbType::ScalarDouble, &SampleMode::Monitor, 1).unwrap();
    registry.add_alias("ALIAS:PV", "REAL:PV").unwrap();

    // Sanity: alias has 'alias' status.
    let alias_row = registry.get_pv("ALIAS:PV").unwrap().unwrap();
    assert_eq!(alias_row.status, PvStatus::Alias);

    let (mgr, _rx) = ChannelManager::new(storage.clone(), registry.clone(), None)
        .await
        .unwrap();
    // restore_from_registry must NOT try to archive the alias. We can't
    // verify CA channel creation in a unit test (no IOC), but we can
    // verify pvs_by_status(Active) returns only REAL:PV.
    let actives = registry.pvs_by_status(PvStatus::Active).unwrap();
    assert_eq!(actives.len(), 1);
    assert_eq!(actives[0].pv_name, "REAL:PV");

    // restore is async and tries to connect — short timeout. It's OK if it
    // fails to connect; what matters is it didn't try the alias name. This
    // test exercises the filter path.
    let _ = mgr.restore_from_registry().await;
}

#[tokio::test]
async fn test_consolidate_forwards_to_peer_for_remote_pv() {
    use archiver_api::services::fakes::FakeClusterRouter;
    use archiver_api::services::traits::{ApplianceIdentityDto, PeerDto, ResolvedPeerDto};
    use std::sync::Arc;

    let dir = tempfile::tempdir().unwrap();
    let storage = Arc::new(archiver_core::storage::plainpb::PlainPbStoragePlugin::new(
        "sts",
        dir.path().to_path_buf(),
        archiver_core::storage::partition::PartitionGranularity::Hour,
    ));
    let registry = Arc::new(archiver_core::registry::PvRegistry::in_memory().unwrap());
    // Local has nothing — REMOTE:PV is owned by a peer.
    let (channel_mgr, _rx) = archiver_engine::channel_manager::ChannelManager::new(
        storage.clone(),
        registry.clone(),
        None,
    )
    .await
    .unwrap();
    let channel_mgr = Arc::new(channel_mgr);
    let repo = Arc::new(RegistryRepository::new(registry.clone()));
    let archiver = Arc::new(ChannelArchiverControl::new(channel_mgr));

    let identity = ApplianceIdentityDto {
        name: "appliance0".to_string(),
        mgmt_url: "http://app0/mgmt/bpl".to_string(),
        retrieval_url: "http://app0/retrieval".to_string(),
        engine_url: "http://app0".to_string(),
        etl_url: "http://app0".to_string(),
    };
    let peers = vec![PeerDto {
        name: "appliance1".to_string(),
        mgmt_url: "http://app1/mgmt/bpl".to_string(),
        retrieval_url: "http://app1/retrieval".to_string(),
    }];
    let cluster = Arc::new(
        FakeClusterRouter::new(identity, peers).with_pv_routing(
            "REMOTE:PV",
            ResolvedPeerDto {
                mgmt_url: "http://app1/mgmt/bpl".to_string(),
                retrieval_url: "http://app1/retrieval".to_string(),
            },
        ),
    );

    let state = archiver_api::AppState {
        storage,
        pv_query: repo.clone(),
        pv_cmd: repo,
        archiver_query: archiver.clone(),
        archiver_cmd: archiver,
        cluster: Some(cluster),
        api_keys: None,
        cluster_api_key: None,
        metrics_handle: None,
        rate_limiter: None,
        trust_proxy_headers: false,
        failover: None,
        etl_chain: Vec::new(),
    };
    let app = archiver_api::build_router(state, &archiver_core::config::SecurityConfig::default());

    let req = get_request("/mgmt/bpl/consolidateDataForPV?pv=REMOTE:PV&storage=LTS");
    let resp = app.clone().oneshot(req).await.unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let body = body_to_json(resp.into_body()).await;
    // FakeClusterRouter::proxy_mgmt_get returns {"status": "proxied"}.
    assert_eq!(body["status"], "proxied");

    // Same forward for getStoresForPV / getStorageUsageForPV / renamePV / modifyMetaFields.
    for endpoint in [
        "/mgmt/bpl/getStoresForPV?pv=REMOTE:PV",
        "/mgmt/bpl/getStorageUsageForPV?pv=REMOTE:PV",
        "/mgmt/bpl/modifyMetaFields?pv=REMOTE:PV&command=add,HIHI",
        "/mgmt/bpl/renamePV?pv=REMOTE:PV&newname=REMOTE:PV2",
    ] {
        let req = get_request(endpoint);
        let resp = app.clone().oneshot(req).await.unwrap();
        assert_eq!(resp.status(), StatusCode::OK, "endpoint: {endpoint}");
        let body = body_to_json(resp.into_body()).await;
        assert_eq!(body["status"], "proxied", "endpoint: {endpoint}");
    }
}
