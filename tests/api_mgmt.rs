use std::sync::Arc;
use std::time::{Duration, SystemTime};

use axum::body::Body;
use axum::http::{Request, StatusCode};
use tower::ServiceExt;

use archiver_core::registry::{PvRegistry, PvStatus, SampleMode};
use archiver_core::storage::partition::PartitionGranularity;
use archiver_core::storage::plainpb::PlainPbStoragePlugin;
use archiver_core::storage::traits::StoragePlugin as _;
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
        reassign_appliance_enabled: false,
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
        reassign_appliance_enabled: false,
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
    assert_eq!(json["pvName"], "SIM:Sine");
    assert_eq!(json["status"], "Being archived");
    assert!(json["dbrType"].is_number());
    assert!(json["samplingMethod"].is_string());
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

/// Java parity (0687be1): abort proceeds regardless of registry status —
/// the previous "reject active PVs" guard recreated the typeinfo-stuck
/// bug by forcing operators to pause or delete PVs whose CA channel
/// never connected.
#[tokio::test]
async fn test_abort_archiving_active_succeeds() {
    let (app, reg, _dir) = build_test_app_with_pvs().await;
    let resp = app
        .oneshot(get_request("/mgmt/bpl/abortArchivingPV?pv=SIM:Sine"))
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let record = reg.get_pv("SIM:Sine").unwrap().unwrap();
    assert_eq!(record.status, archiver_core::registry::PvStatus::Inactive);
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
        reassign_appliance_enabled: false,
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
        reassign_appliance_enabled: false,
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
        reassign_appliance_enabled: false,
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
        reassign_appliance_enabled: false,
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

    // Querying the alias returns the target's status, but the response
    // echoes the user-supplied alias name (Java parity, F-12) so clients
    // can match request → response by name.
    let req = get_request("/mgmt/bpl/getPVStatus?pv=DEV:Foo");
    let resp = app.clone().oneshot(req).await.unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let body = body_to_json(resp.into_body()).await;
    assert_eq!(body["pvName"], "DEV:Foo");
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
        reassign_appliance_enabled: false,
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

#[tokio::test]
async fn test_p1_cluster_scoped_aliases() {
    let (app, _reg, _dir) = build_test_app_with_pvs().await;

    for endpoint in [
        "/mgmt/bpl/getPVsForThisAppliance",
        "/mgmt/bpl/getMatchingPVsForAppliance?pv=SIM:*",
        "/mgmt/bpl/getPausedPVsForThisAppliance",
        "/mgmt/bpl/getNeverConnectedPVsForThisAppliance",
    ] {
        let req = get_request(endpoint);
        let resp = app.clone().oneshot(req).await.unwrap();
        assert_eq!(resp.status(), StatusCode::OK, "endpoint: {endpoint}");
    }
}

#[tokio::test]
async fn test_p1_archived_pvs_action_classifies_input() {
    let (app, _reg, _dir) = build_test_app_with_pvs().await;
    let body = serde_json::json!(["SIM:Sine", "NOT:Real"]).to_string();

    let req = Request::builder()
        .method("POST")
        .uri("/mgmt/bpl/archivedPVsAction")
        .header("content-type", "application/json")
        .body(Body::from(body.clone()))
        .unwrap();
    let resp = app.clone().oneshot(req).await.unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let arr = body_to_json(resp.into_body()).await;
    let arr = arr.as_array().unwrap();
    let sine = arr.iter().find(|e| e["pvName"] == "SIM:Sine").unwrap();
    assert_eq!(sine["archived"], true);
    let nope = arr.iter().find(|e| e["pvName"] == "NOT:Real").unwrap();
    assert_eq!(nope["archived"], false);

    // unarchivedPVsAction → only NOT:Real.
    let req = Request::builder()
        .method("POST")
        .uri("/mgmt/bpl/unarchivedPVsAction")
        .header("content-type", "application/json")
        .body(Body::from(body))
        .unwrap();
    let resp = app.clone().oneshot(req).await.unwrap();
    let arr = body_to_json(resp.into_body()).await;
    assert_eq!(arr, serde_json::json!(["NOT:Real"]));

    // archivedPVsNotInListAction with input=[SIM:Sine] → returns SIM:Cosine, TEST:Counter.
    let req = Request::builder()
        .method("POST")
        .uri("/mgmt/bpl/archivedPVsNotInListAction")
        .header("content-type", "application/json")
        .body(Body::from(serde_json::json!(["SIM:Sine"]).to_string()))
        .unwrap();
    let resp = app.clone().oneshot(req).await.unwrap();
    let arr: Vec<String> = serde_json::from_value(body_to_json(resp.into_body()).await).unwrap();
    assert!(arr.contains(&"SIM:Cosine".to_string()));
    assert!(arr.contains(&"TEST:Counter".to_string()));
    assert!(!arr.contains(&"SIM:Sine".to_string()));
}

#[tokio::test]
async fn test_p1_get_last_known_event_timestamp() {
    let (app, _reg, _dir) = build_test_app_with_pvs().await;
    let req = get_request("/mgmt/bpl/getLastKnownEventTimeStamp?pv=SIM:Sine");
    let resp = app.clone().oneshot(req).await.unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let body = body_to_json(resp.into_body()).await;
    assert_eq!(body["pvName"], "SIM:Sine");
    // No samples written yet → null.
    assert!(body["lastEvent"].is_null());

    // Unknown PV → 404.
    let req = get_request("/mgmt/bpl/getLastKnownEventTimeStamp?pv=NOPE");
    let resp = app.clone().oneshot(req).await.unwrap();
    assert_eq!(resp.status(), StatusCode::NOT_FOUND);
}

#[tokio::test]
async fn test_p1_mgmt_metrics_exposes_counts() {
    let (app, _reg, _dir) = build_test_app_with_pvs().await;
    let req = get_request("/mgmt/bpl/getMgmtMetrics");
    let resp = app.clone().oneshot(req).await.unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let body = body_to_json(resp.into_body()).await;
    assert_eq!(body["pvCount"]["total"], 3);
    assert_eq!(body["pvCount"]["active"], 3);
    assert!(body["version"].as_str().is_some());
}

#[tokio::test]
async fn test_p2_named_flags_roundtrip() {
    let (app, _reg, _dir) = build_test_app_with_pvs().await;

    let req = Request::builder()
        .method("POST")
        .uri("/mgmt/bpl/namedFlagsSet?name=alpha&value=true")
        .body(Body::empty())
        .unwrap();
    let resp = app.clone().oneshot(req).await.unwrap();
    assert_eq!(resp.status(), StatusCode::OK);

    let req = get_request("/mgmt/bpl/namedFlagsGet?name=alpha");
    let resp = app.clone().oneshot(req).await.unwrap();
    let body = body_to_json(resp.into_body()).await;
    assert_eq!(body["value"], true);

    let req = get_request("/mgmt/bpl/namedFlagsAll");
    let resp = app.clone().oneshot(req).await.unwrap();
    let body = body_to_json(resp.into_body()).await;
    assert_eq!(body["alpha"], true);
}

#[tokio::test]
async fn test_p2_meta_gets_action() {
    let (app, registry, _dir) = build_test_app_with_pvs().await;
    registry
        .update_archive_fields("SIM:Sine", &["HIHI".to_string(), "LOLO".to_string()])
        .unwrap();
    let req = get_request("/mgmt/bpl/metaGetsAction?pv=SIM:Sine");
    let resp = app.clone().oneshot(req).await.unwrap();
    let body = body_to_json(resp.into_body()).await;
    assert_eq!(body["metaFieldCount"], 2);
    let fields: Vec<String> = serde_json::from_value(body["metaFields"].clone()).unwrap();
    assert!(fields.contains(&"HIHI".to_string()));
}

#[tokio::test]
async fn test_p3_legacy_endpoints_return_410() {
    let (app, _reg, _dir) = build_test_app_with_pvs().await;
    for endpoint in [
        "/mgmt/bpl/addExternalArchiverServer",
        "/mgmt/bpl/channelArchiverListView",
        "/mgmt/bpl/importChannelArchiverConfigAction",
        "/mgmt/bpl/refreshPVDataFromChannelArchivers",
        "/mgmt/bpl/restartArchiveWorkflowThreadForAppliance",
        "/mgmt/bpl/skipAliasCheckAction",
        "/mgmt/bpl/syncStaticContentHeadersFooters",
    ] {
        let req = get_request(endpoint);
        let resp = app.clone().oneshot(req).await.unwrap();
        assert_eq!(resp.status(), StatusCode::GONE, "endpoint: {endpoint}");
        let body = body_to_json(resp.into_body()).await;
        assert_eq!(body["status"], "gone");
        assert!(body["reason"].as_str().is_some());
    }
}

#[tokio::test]
async fn test_p2_change_type_for_pv_requires_pause() {
    let (app, _reg, _dir) = build_test_app_with_pvs().await;
    // Active PV → 409.
    let req = get_request("/mgmt/bpl/changeTypeForPV?pv=SIM:Sine&newtype=2");
    let resp = app.clone().oneshot(req).await.unwrap();
    assert_eq!(resp.status(), StatusCode::CONFLICT);

    // Pause then change → ok.
    let req = get_request("/mgmt/bpl/pauseArchivingPV?pv=SIM:Sine");
    assert_eq!(app.clone().oneshot(req).await.unwrap().status(), StatusCode::OK);
    let req = get_request("/mgmt/bpl/changeTypeForPV?pv=SIM:Sine&newtype=2");
    let resp = app.clone().oneshot(req).await.unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let body = body_to_json(resp.into_body()).await;
    assert_eq!(body["newType"], 2);
}

#[tokio::test]
async fn test_p2_aggregated_appliance_info_standalone() {
    let (app, _reg, _dir) = build_test_app_with_pvs().await;
    let req = get_request("/mgmt/bpl/aggregatedApplianceInfo");
    let resp = app.clone().oneshot(req).await.unwrap();
    let body = body_to_json(resp.into_body()).await;
    let arr = body.as_array().unwrap();
    assert_eq!(arr.len(), 1);
    assert_eq!(arr[0]["name"], "standalone");
}

#[tokio::test]
async fn test_d_consolidate_pv_actually_moves_files() {
    use archiver_core::storage::traits::AppendMeta;
    use archiver_core::types::{ArchDbType, ArchiverSample, ArchiverValue};
    use std::sync::Arc;

    // Two-tier storage so consolidate has somewhere to move data to.
    let dir = tempfile::tempdir().unwrap();
    let sts_dir = dir.path().join("sts");
    let mts_dir = dir.path().join("mts");
    let sts = Arc::new(PlainPbStoragePlugin::new(
        "STS",
        sts_dir.clone(),
        archiver_core::storage::partition::PartitionGranularity::Hour,
    ));
    let mts = Arc::new(PlainPbStoragePlugin::new(
        "MTS",
        mts_dir.clone(),
        archiver_core::storage::partition::PartitionGranularity::Day,
    ));

    // Write a sample to STS so consolidate has something to move.
    let now = std::time::SystemTime::now();
    let sample = ArchiverSample::new(now, ArchiverValue::ScalarDouble(1.0));
    sts.append_event_with_meta("PV:Move", ArchDbType::ScalarDouble, &sample, &AppendMeta::default())
        .await
        .unwrap();
    sts.flush_writes().await.unwrap();

    let executor = archiver_core::etl::executor::EtlExecutor::new(
        sts.clone(),
        mts.clone(),
        3600,
        5,
        3,
    );
    let moved = executor.consolidate_pv("PV:Move").await.unwrap();
    assert_eq!(moved, 1);

    // STS empty, MTS has the sample.
    let sts_streams = sts
        .get_data("PV:Move", now - std::time::Duration::from_secs(60), now + std::time::Duration::from_secs(60))
        .await
        .unwrap();
    assert!(sts_streams.is_empty());
    let mts_streams = mts
        .get_data("PV:Move", now - std::time::Duration::from_secs(60), now + std::time::Duration::from_secs(60))
        .await
        .unwrap();
    assert_eq!(mts_streams.len(), 1);
}

#[tokio::test]
async fn test_d_get_data_at_time_stage_2_walkback() {
    use archiver_core::storage::traits::AppendMeta;
    use archiver_core::types::{ArchDbType, ArchiverSample, ArchiverValue};
    use std::sync::Arc;

    let dir = tempfile::tempdir().unwrap();
    let storage = Arc::new(PlainPbStoragePlugin::new(
        "sts",
        dir.path().to_path_buf(),
        archiver_core::storage::partition::PartitionGranularity::Hour,
    ));
    let registry = Arc::new(PvRegistry::in_memory().unwrap());
    registry
        .register_pv("PV:Walk", ArchDbType::ScalarDouble, &SampleMode::Monitor, 1)
        .unwrap();

    // Two samples: t=10 and t=100.
    let base = std::time::SystemTime::now() - std::time::Duration::from_secs(200);
    for (offset, val) in [(10u64, 1.0), (100u64, 2.0)] {
        let s = ArchiverSample::new(base + std::time::Duration::from_secs(offset), ArchiverValue::ScalarDouble(val));
        storage
            .append_event_with_meta("PV:Walk", ArchDbType::ScalarDouble, &s, &AppendMeta::default())
            .await
            .unwrap();
    }
    storage.flush_writes().await.unwrap();

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
        reassign_appliance_enabled: false,
    };
    let app = build_router(state, &SecurityConfig::default());

    // Query for time between samples — should pick t=10 (last <= target).
    let target = base + std::time::Duration::from_secs(50);
    let target_iso = chrono::DateTime::<chrono::Utc>::from(target).to_rfc3339();
    let body = serde_json::json!(["PV:Walk"]).to_string();
    let req = Request::builder()
        .method("POST")
        .uri(format!("/retrieval/data/getDataAtTime?at={}", urlencoding::encode(&target_iso)))
        .header("content-type", "application/json")
        .body(Body::from(body))
        .unwrap();
    let resp = app.clone().oneshot(req).await.unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let body = body_to_json(resp.into_body()).await;
    assert_eq!(body["PV:Walk"]["val"], 1.0);
}

#[tokio::test]
async fn test_d_update_archive_fields_concurrent_no_duplicate_tasks() {
    use std::sync::Arc;
    let (_app, registry, _dir) = build_test_app_with_pvs().await;
    // Pause SIM:Sine so we can manipulate fields freely (engine accepts
    // updates regardless of status; this is just for clarity).
    registry.set_status("SIM:Sine", PvStatus::Paused).unwrap();

    // Hit update_archive_fields concurrently 16 times with overlapping sets.
    // The per-PV update_lock should serialise them; final state must reflect
    // the last writer, and field_tokens count should equal the final field
    // set (no orphans).
    //
    // We can't observe field_tokens directly through the public API, but we
    // can at least verify the registry persists the last update correctly.
    let cm = Arc::new({
        let dir = tempfile::tempdir().unwrap();
        let storage = Arc::new(PlainPbStoragePlugin::new(
            "sts",
            dir.path().to_path_buf(),
            archiver_core::storage::partition::PartitionGranularity::Hour,
        ));
        let (cm, _rx) = ChannelManager::new(storage, registry.clone(), None).await.unwrap();
        cm
    });

    let mut handles = Vec::new();
    for i in 0..16 {
        let cm = cm.clone();
        let fields: Vec<String> = if i % 2 == 0 {
            vec!["HIHI".into(), "LOLO".into()]
        } else {
            vec!["EGU".into(), "PREC".into()]
        };
        handles.push(tokio::spawn(async move {
            cm.update_archive_fields("SIM:Sine", &fields).await.unwrap();
        }));
    }
    for h in handles {
        h.await.unwrap();
    }
    // Final registry state matches one of the two sets (whichever ran last).
    let r = registry.get_pv("SIM:Sine").unwrap().unwrap();
    assert!(
        r.archive_fields == vec!["HIHI".to_string(), "LOLO".to_string()]
            || r.archive_fields == vec!["EGU".to_string(), "PREC".to_string()],
    );
}

#[tokio::test]
async fn test_engine_pv_status_action_shape() {
    let (app, _reg, _dir) = build_test_app_with_pvs().await;
    let req = get_request("/mgmt/bpl/pvStatusAction?pv=SIM:Sine");
    let resp = app.clone().oneshot(req).await.unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let body = body_to_json(resp.into_body()).await;
    assert_eq!(body["pvName"], "SIM:Sine");
    assert!(body["dbrType"].is_number());
    // No CA traffic in tests, so connection / counters may be null/empty.
    assert!(body["extras"].is_object());
}

#[tokio::test]
async fn test_engine_get_latest_meta_data_empty_when_no_extras() {
    let (app, _reg, _dir) = build_test_app_with_pvs().await;
    let req = get_request("/mgmt/bpl/getLatestMetaDataAction?pv=SIM:Sine");
    let resp = app.clone().oneshot(req).await.unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let body = body_to_json(resp.into_body()).await;
    // No archive_fields configured → empty object.
    assert_eq!(body, serde_json::json!({}));
}

#[tokio::test]
async fn test_engine_data_action_returns_per_pv_entries() {
    let (app, _reg, _dir) = build_test_app_with_pvs().await;
    let body = serde_json::json!(["SIM:Sine", "NOT:Real"]).to_string();
    let req = Request::builder()
        .method("POST")
        .uri("/mgmt/bpl/getEngineDataAction")
        .header("content-type", "application/json")
        .body(Body::from(body))
        .unwrap();
    let resp = app.clone().oneshot(req).await.unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let body = body_to_json(resp.into_body()).await;
    // Both PVs present in response. SIM:Sine has no live CA (no IOC),
    // so we get an error string. NOT:Real reports "not archived".
    assert!(body["SIM:Sine"].is_object());
    assert!(body["NOT:Real"].is_object());
    assert_eq!(
        body["NOT:Real"]["error"].as_str().unwrap(),
        "not archived on this appliance"
    );
}

#[tokio::test]
async fn test_reassign_appliance_validates_target() {
    let (app, _reg, _dir) = build_test_app_with_pvs().await;
    // No cluster configured → 400.
    let req = get_request("/mgmt/bpl/reassignAppliance?pv=SIM:Sine&appliance=other");
    let resp = app.clone().oneshot(req).await.unwrap();
    assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
}

#[tokio::test]
async fn test_change_store_returns_501() {
    let (app, _reg, _dir) = build_test_app_with_pvs().await;
    let req = get_request("/mgmt/bpl/changeStore?pv=SIM:Sine");
    let resp = app.clone().oneshot(req).await.unwrap();
    assert_eq!(resp.status(), StatusCode::NOT_IMPLEMENTED);
    let body = body_to_json(resp.into_body()).await;
    assert_eq!(body["status"], "not_implemented");
    assert!(body["reason"].as_str().unwrap().contains("archiver.toml"));
}

#[tokio::test]
async fn test_receive_pv_migration_requires_proxied_header() {
    let (app, _reg, _dir) = build_test_app_with_pvs().await;
    let body = serde_json::json!({
        "pvName": "MIGRATED:PV",
        "dbrType": 6,
        "samplingMethod": "Monitor",
        "samplingPeriod": 0.0,
        "elementCount": 1,
        "samples": [],
    })
    .to_string();
    let req = Request::builder()
        .method("POST")
        .uri("/mgmt/bpl/receivePVMigration")
        .header("content-type", "application/json")
        .body(Body::from(body))
        .unwrap();
    let resp = app.clone().oneshot(req).await.unwrap();
    assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
}

#[tokio::test]
async fn test_receive_pv_migration_imports_with_proxied_header() {
    let (app, registry, _dir) = build_test_app_with_pvs().await;
    let body = serde_json::json!({
        "pvName": "MIGRATED:PV",
        "dbrType": 6,
        "samplingMethod": "Monitor",
        "samplingPeriod": 0.0,
        "elementCount": 1,
        "archiveFields": ["HIHI"],
        "samples": [
            {"secs": 1700000000, "nanos": 0, "value": 1.5, "severity": 0, "status": 0},
            {"secs": 1700000010, "nanos": 0, "value": 2.5, "severity": 0, "status": 0},
        ],
    })
    .to_string();
    let req = Request::builder()
        .method("POST")
        .uri("/mgmt/bpl/receivePVMigration")
        .header("content-type", "application/json")
        .header("X-Archiver-Proxied", "1")
        .body(Body::from(body))
        .unwrap();
    let resp = app.clone().oneshot(req).await.unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let body = body_to_json(resp.into_body()).await;
    assert_eq!(body["samplesReceived"], 2);
    let r = registry.get_pv("MIGRATED:PV").unwrap().unwrap();
    assert_eq!(r.archive_fields, vec!["HIHI".to_string()]);
    assert_eq!(r.status, archiver_core::registry::PvStatus::Paused);
}
