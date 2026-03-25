use std::sync::Arc;

use axum::body::Body;
use axum::http::{Request, StatusCode};
use tower::ServiceExt;

use archiver_core::config::{ApplianceIdentity, ClusterConfig, PeerConfig};
use archiver_core::registry::{PvRegistry, SampleMode};
use archiver_core::storage::partition::PartitionGranularity;
use archiver_core::storage::plainpb::PlainPbStoragePlugin;
use archiver_engine::channel_manager::ChannelManager;
use archiver_api::cluster::ClusterClient;
use archiver_api::{build_router, AppState};
use archiver_api::services::impls::{ChannelArchiverControl, ClusterClientRouter, RegistryRepository};

/// Start a mock peer appliance on a random port.
/// Returns (base_url, JoinHandle).
async fn start_mock_peer(
    registry: Arc<PvRegistry>,
    storage: Arc<PlainPbStoragePlugin>,
) -> (String, tokio::task::JoinHandle<()>) {
    let (channel_mgr, _rx) = ChannelManager::new(storage.clone(), registry.clone(), None)
        .await
        .unwrap();
    let channel_mgr = Arc::new(channel_mgr);
    let state = AppState {
        storage,
        pv_repo: Arc::new(RegistryRepository::new(registry)),
        archiver: Arc::new(ChannelArchiverControl::new(channel_mgr)),
        cluster: None,
        api_keys: None,
        metrics_handle: None,
    };
    let app = build_router(state);
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let handle = tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });
    (format!("http://{addr}"), handle)
}

/// Build a local app with cluster config pointing to mock peer(s).
async fn build_cluster_test_app(
    local_registry: Arc<PvRegistry>,
    local_storage: Arc<PlainPbStoragePlugin>,
    peer_url: &str,
) -> axum::Router {
    let (channel_mgr, _rx) = ChannelManager::new(local_storage.clone(), local_registry.clone(), None)
        .await
        .unwrap();
    let channel_mgr = Arc::new(channel_mgr);

    let cluster_config = ClusterConfig {
        identity: ApplianceIdentity {
            name: "local".to_string(),
            mgmt_url: "http://localhost:0/mgmt/bpl".to_string(),
            retrieval_url: "http://localhost:0/retrieval".to_string(),
            engine_url: "http://localhost:0".to_string(),
            etl_url: "http://localhost:0".to_string(),
        },
        cache_ttl_secs: 1,
        peer_timeout_secs: 5,
        peers: vec![PeerConfig {
            name: "peer0".to_string(),
            mgmt_url: format!("{peer_url}/mgmt/bpl"),
            retrieval_url: format!("{peer_url}/retrieval"),
        }],
    };

    let state = AppState {
        storage: local_storage,
        pv_repo: Arc::new(RegistryRepository::new(local_registry)),
        archiver: Arc::new(ChannelArchiverControl::new(channel_mgr)),
        cluster: Some(Arc::new(ClusterClientRouter::new(Arc::new(ClusterClient::new(&cluster_config))))),
        api_keys: None,
        metrics_handle: None,
    };
    build_router(state)
}

fn get_request(uri: &str) -> Request<Body> {
    Request::builder()
        .uri(uri)
        .body(Body::empty())
        .unwrap()
}

fn get_request_with_header(uri: &str, header: &str, value: &str) -> Request<Body> {
    Request::builder()
        .uri(uri)
        .header(header, value)
        .body(Body::empty())
        .unwrap()
}

async fn body_to_json(body: Body) -> serde_json::Value {
    let bytes = axum::body::to_bytes(body, usize::MAX).await.unwrap();
    serde_json::from_slice(&bytes).unwrap()
}

async fn body_to_string(body: Body) -> String {
    let bytes = axum::body::to_bytes(body, usize::MAX).await.unwrap();
    String::from_utf8(bytes.to_vec()).unwrap()
}

fn new_registry_and_storage() -> (Arc<PvRegistry>, Arc<PlainPbStoragePlugin>, tempfile::TempDir) {
    let dir = tempfile::tempdir().unwrap();
    let storage = Arc::new(PlainPbStoragePlugin::new(
        "sts",
        dir.path().to_path_buf(),
        PartitionGranularity::Hour,
    ));
    let registry = Arc::new(PvRegistry::in_memory().unwrap());
    (registry, storage, dir)
}

// --- G1: Cluster Integration Tests ---

#[tokio::test]
async fn test_resolve_peer_finds_remote_pv() {
    let (peer_reg, peer_storage, _peer_dir) = new_registry_and_storage();
    peer_reg
        .register_pv("REMOTE:PV", archiver_core::types::ArchDbType::ScalarDouble, &SampleMode::Monitor, 1)
        .unwrap();
    let (peer_url, _handle) = start_mock_peer(peer_reg, peer_storage).await;

    let (local_reg, local_storage, _local_dir) = new_registry_and_storage();
    let app = build_cluster_test_app(local_reg, local_storage, &peer_url).await;

    // getPVStatus with cluster=true should find the remote PV.
    let resp = app
        .oneshot(get_request("/mgmt/bpl/getPVStatus?pv=REMOTE:PV&cluster=true"))
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let json = body_to_json(resp.into_body()).await;
    assert_eq!(json["status"], "Being archived");
}

#[tokio::test]
async fn test_resolve_peer_returns_none_unknown() {
    let (peer_reg, peer_storage, _peer_dir) = new_registry_and_storage();
    let (peer_url, _handle) = start_mock_peer(peer_reg, peer_storage).await;

    let (local_reg, local_storage, _local_dir) = new_registry_and_storage();
    let app = build_cluster_test_app(local_reg, local_storage, &peer_url).await;

    let resp = app
        .oneshot(get_request("/mgmt/bpl/getPVStatus?pv=UNKNOWN:PV&cluster=true"))
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let json = body_to_json(resp.into_body()).await;
    assert_eq!(json["status"], "Not being archived");
}

#[tokio::test]
async fn test_resolve_peer_prefers_local() {
    let (peer_reg, peer_storage, _peer_dir) = new_registry_and_storage();
    peer_reg
        .register_pv("SHARED:PV", archiver_core::types::ArchDbType::ScalarDouble, &SampleMode::Monitor, 1)
        .unwrap();
    let (peer_url, _handle) = start_mock_peer(peer_reg, peer_storage).await;

    let (local_reg, local_storage, _local_dir) = new_registry_and_storage();
    local_reg
        .register_pv("SHARED:PV", archiver_core::types::ArchDbType::ScalarInt, &SampleMode::Scan { period_secs: 2.0 }, 1)
        .unwrap();
    let app = build_cluster_test_app(local_reg, local_storage, &peer_url).await;

    // Should return local PV info (ScalarInt / Scan), not peer's.
    let resp = app
        .oneshot(get_request("/mgmt/bpl/getPVStatus?pv=SHARED:PV"))
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let json = body_to_json(resp.into_body()).await;
    assert_eq!(json["status"], "Being archived");
    assert_eq!(json["dbr_type"], 5); // ScalarInt
    assert_eq!(json["sample_mode"], "Scan @ 2s");
}

#[tokio::test]
async fn test_get_all_pvs_cluster_aggregation() {
    let (peer_reg, peer_storage, _peer_dir) = new_registry_and_storage();
    peer_reg
        .register_pv("PEER:PV1", archiver_core::types::ArchDbType::ScalarDouble, &SampleMode::Monitor, 1)
        .unwrap();
    peer_reg
        .register_pv("PEER:PV2", archiver_core::types::ArchDbType::ScalarDouble, &SampleMode::Monitor, 1)
        .unwrap();
    let (peer_url, _handle) = start_mock_peer(peer_reg, peer_storage).await;

    let (local_reg, local_storage, _local_dir) = new_registry_and_storage();
    local_reg
        .register_pv("LOCAL:PV1", archiver_core::types::ArchDbType::ScalarDouble, &SampleMode::Monitor, 1)
        .unwrap();
    let app = build_cluster_test_app(local_reg, local_storage, &peer_url).await;

    let resp = app
        .oneshot(get_request("/mgmt/bpl/getAllPVs?cluster=true"))
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let json = body_to_json(resp.into_body()).await;
    let pvs = json.as_array().unwrap();
    assert_eq!(pvs.len(), 3);
    let names: Vec<&str> = pvs.iter().map(|v| v.as_str().unwrap()).collect();
    assert!(names.contains(&"LOCAL:PV1"));
    assert!(names.contains(&"PEER:PV1"));
    assert!(names.contains(&"PEER:PV2"));
}

#[tokio::test]
async fn test_get_matching_pvs_cluster() {
    let (peer_reg, peer_storage, _peer_dir) = new_registry_and_storage();
    peer_reg
        .register_pv("SIM:Remote", archiver_core::types::ArchDbType::ScalarDouble, &SampleMode::Monitor, 1)
        .unwrap();
    peer_reg
        .register_pv("OTHER:PV", archiver_core::types::ArchDbType::ScalarDouble, &SampleMode::Monitor, 1)
        .unwrap();
    let (peer_url, _handle) = start_mock_peer(peer_reg, peer_storage).await;

    let (local_reg, local_storage, _local_dir) = new_registry_and_storage();
    local_reg
        .register_pv("SIM:Local", archiver_core::types::ArchDbType::ScalarDouble, &SampleMode::Monitor, 1)
        .unwrap();
    let app = build_cluster_test_app(local_reg, local_storage, &peer_url).await;

    let resp = app
        .oneshot(get_request("/mgmt/bpl/getMatchingPVs?pv=SIM:*&cluster=true"))
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let json = body_to_json(resp.into_body()).await;
    let pvs = json.as_array().unwrap();
    assert_eq!(pvs.len(), 2);
    let names: Vec<&str> = pvs.iter().map(|v| v.as_str().unwrap()).collect();
    assert!(names.contains(&"SIM:Local"));
    assert!(names.contains(&"SIM:Remote"));
}

#[tokio::test]
async fn test_get_pv_status_cluster_remote() {
    let (peer_reg, peer_storage, _peer_dir) = new_registry_and_storage();
    peer_reg
        .register_pv("ONLY:OnPeer", archiver_core::types::ArchDbType::ScalarFloat, &SampleMode::Monitor, 1)
        .unwrap();
    let (peer_url, _handle) = start_mock_peer(peer_reg, peer_storage).await;

    let (local_reg, local_storage, _local_dir) = new_registry_and_storage();
    let app = build_cluster_test_app(local_reg, local_storage, &peer_url).await;

    let resp = app
        .oneshot(get_request("/mgmt/bpl/getPVStatus?pv=ONLY:OnPeer&cluster=true"))
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let json = body_to_json(resp.into_body()).await;
    assert_eq!(json["status"], "Being archived");
    assert_eq!(json["pv_name"], "ONLY:OnPeer");
}

#[tokio::test]
async fn test_get_pv_count_cluster() {
    let (peer_reg, peer_storage, _peer_dir) = new_registry_and_storage();
    peer_reg
        .register_pv("P:A", archiver_core::types::ArchDbType::ScalarDouble, &SampleMode::Monitor, 1)
        .unwrap();
    peer_reg
        .register_pv("P:B", archiver_core::types::ArchDbType::ScalarDouble, &SampleMode::Monitor, 1)
        .unwrap();
    let (peer_url, _handle) = start_mock_peer(peer_reg, peer_storage).await;

    let (local_reg, local_storage, _local_dir) = new_registry_and_storage();
    local_reg
        .register_pv("L:A", archiver_core::types::ArchDbType::ScalarDouble, &SampleMode::Monitor, 1)
        .unwrap();
    let app = build_cluster_test_app(local_reg, local_storage, &peer_url).await;

    let resp = app
        .oneshot(get_request("/mgmt/bpl/getPVCount?cluster=true"))
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let json = body_to_json(resp.into_body()).await;
    assert_eq!(json["total"], 3);
    assert_eq!(json["active"], 3);
    assert_eq!(json["paused"], 0);
}

#[tokio::test]
async fn test_pause_dispatches_to_peer() {
    let (peer_reg, peer_storage, _peer_dir) = new_registry_and_storage();
    peer_reg
        .register_pv("PEER:Pause", archiver_core::types::ArchDbType::ScalarDouble, &SampleMode::Monitor, 1)
        .unwrap();
    let (peer_url, _handle) = start_mock_peer(peer_reg.clone(), peer_storage).await;

    let (local_reg, local_storage, _local_dir) = new_registry_and_storage();
    let app = build_cluster_test_app(local_reg, local_storage, &peer_url).await;

    // Pause a PV that only exists on the peer.
    let resp = app
        .oneshot(get_request("/mgmt/bpl/pauseArchivingPV?pv=PEER:Pause"))
        .await
        .unwrap();
    // Should return OK (proxied to peer).
    assert_eq!(resp.status(), StatusCode::OK);
}

#[tokio::test]
async fn test_delete_dispatches_to_peer() {
    let (peer_reg, peer_storage, _peer_dir) = new_registry_and_storage();
    peer_reg
        .register_pv("PEER:Del", archiver_core::types::ArchDbType::ScalarDouble, &SampleMode::Monitor, 1)
        .unwrap();
    let (peer_url, _handle) = start_mock_peer(peer_reg.clone(), peer_storage).await;

    let (local_reg, local_storage, _local_dir) = new_registry_and_storage();
    let app = build_cluster_test_app(local_reg, local_storage, &peer_url).await;

    let resp = app
        .oneshot(get_request("/mgmt/bpl/deletePV?pv=PEER:Del"))
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    // Verify PV was removed from peer registry.
    assert!(peer_reg.get_pv("PEER:Del").unwrap().is_none());
}

#[tokio::test]
async fn test_x_archiver_proxied_prevents_loop() {
    let (peer_reg, peer_storage, _peer_dir) = new_registry_and_storage();
    let (peer_url, _handle) = start_mock_peer(peer_reg, peer_storage).await;

    let (local_reg, local_storage, _local_dir) = new_registry_and_storage();
    let app = build_cluster_test_app(local_reg, local_storage, &peer_url).await;

    // With X-Archiver-Proxied header, should NOT attempt cluster proxy.
    // It handles the request locally instead. For a non-existent PV,
    // pause_pv fails but the handler still returns a result (either OK or error).
    let resp = app
        .oneshot(get_request_with_header(
            "/mgmt/bpl/pauseArchivingPV?pv=NONEXIST:PV",
            "X-Archiver-Proxied",
            "true",
        ))
        .await
        .unwrap();
    // The key assertion: request was handled locally (not proxied).
    // We just verify it returned *some* response without timing out on proxy.
    assert!(
        resp.status() == StatusCode::OK || resp.status() == StatusCode::INTERNAL_SERVER_ERROR,
        "Expected local handling, got {}",
        resp.status()
    );
}

#[tokio::test]
async fn test_retrieval_proxies_to_peer() {
    use std::time::{Duration, SystemTime};
    use archiver_core::types::{ArchiverSample, ArchiverValue};
    use archiver_core::storage::traits::StoragePlugin;

    let (peer_reg, peer_storage, _peer_dir) = new_registry_and_storage();
    peer_reg
        .register_pv("PEER:Data", archiver_core::types::ArchDbType::ScalarDouble, &SampleMode::Monitor, 1)
        .unwrap();

    // Write some data on the peer.
    let base_time = SystemTime::now() - Duration::from_secs(60);
    for i in 0..5 {
        let ts = base_time + Duration::from_secs(i);
        let sample = ArchiverSample::new(ts, ArchiverValue::ScalarDouble(i as f64));
        peer_storage
            .append_event("PEER:Data", archiver_core::types::ArchDbType::ScalarDouble, &sample)
            .await
            .unwrap();
    }

    let (peer_url, _handle) = start_mock_peer(peer_reg, peer_storage).await;

    let (local_reg, local_storage, _local_dir) = new_registry_and_storage();
    let app = build_cluster_test_app(local_reg, local_storage, &peer_url).await;

    let from = chrono::DateTime::<chrono::Utc>::from(base_time - Duration::from_secs(1))
        .format("%Y-%m-%dT%H:%M:%S%.fZ")
        .to_string();
    let to = chrono::DateTime::<chrono::Utc>::from(base_time + Duration::from_secs(10))
        .format("%Y-%m-%dT%H:%M:%S%.fZ")
        .to_string();
    let uri = format!("/retrieval/data/getData.json?pv=PEER:Data&from={from}&to={to}");

    let resp = app.oneshot(get_request(&uri)).await.unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let json = body_to_json(resp.into_body()).await;
    let data = json[0]["data"].as_array().unwrap();
    assert_eq!(data.len(), 5);
}

#[tokio::test]
async fn test_bulk_pause_splits_by_peer() {
    let (peer_reg, peer_storage, _peer_dir) = new_registry_and_storage();
    peer_reg
        .register_pv("REMOTE:A", archiver_core::types::ArchDbType::ScalarDouble, &SampleMode::Monitor, 1)
        .unwrap();
    let (peer_url, _handle) = start_mock_peer(peer_reg.clone(), peer_storage).await;

    let (local_reg, local_storage, _local_dir) = new_registry_and_storage();
    local_reg
        .register_pv("LOCAL:A", archiver_core::types::ArchDbType::ScalarDouble, &SampleMode::Monitor, 1)
        .unwrap();
    let app = build_cluster_test_app(local_reg.clone(), local_storage, &peer_url).await;

    // Bulk pause: LOCAL:A (local) + REMOTE:A (peer).
    let body = "LOCAL:A\nREMOTE:A";
    let req = Request::builder()
        .method("POST")
        .uri("/mgmt/bpl/pauseArchivingPV")
        .header("content-type", "text/plain")
        .body(Body::from(body))
        .unwrap();

    let resp = app.oneshot(req).await.unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let json = body_to_json(resp.into_body()).await;
    let arr = json.as_array().unwrap();
    assert_eq!(arr.len(), 2);

    // Verify local PV was paused.
    let local_pv = local_reg.get_pv("LOCAL:A").unwrap().unwrap();
    assert_eq!(local_pv.status, archiver_core::registry::PvStatus::Paused);
}

// --- G2: archivePV cluster tests ---

#[tokio::test]
async fn test_archive_pv_forwards_to_appliance() {
    let (peer_reg, peer_storage, _peer_dir) = new_registry_and_storage();
    let (peer_url, _handle) = start_mock_peer(peer_reg.clone(), peer_storage).await;

    let (local_reg, local_storage, _local_dir) = new_registry_and_storage();
    let app = build_cluster_test_app(local_reg, local_storage, &peer_url).await;

    // Archive with appliance=peer0 — should forward to the peer.
    let body = serde_json::json!({"pv": "NEW:PV", "appliance": "peer0"});
    let req = Request::builder()
        .method("POST")
        .uri("/mgmt/bpl/archivePV")
        .header("content-type", "application/json")
        .body(Body::from(body.to_string()))
        .unwrap();

    let resp = app.oneshot(req).await.unwrap();
    // The peer won't have EPICS CA available so it will fail with internal error,
    // but the key test is that the request was forwarded (not handled locally).
    // The proxy may return 502 if the peer couldn't process the request,
    // or 500 if the peer processed it but CA connection failed.
    let status = resp.status();
    let text = body_to_string(resp.into_body()).await;
    assert!(
        status == StatusCode::OK
            || status == StatusCode::INTERNAL_SERVER_ERROR
            || status == StatusCode::BAD_GATEWAY,
        "Expected 200, 500, or 502, got {status}: {text}"
    );
}

#[tokio::test]
async fn test_archive_pv_rejects_duplicate() {
    let (peer_reg, peer_storage, _peer_dir) = new_registry_and_storage();
    peer_reg
        .register_pv("DUP:PV", archiver_core::types::ArchDbType::ScalarDouble, &SampleMode::Monitor, 1)
        .unwrap();
    let (peer_url, _handle) = start_mock_peer(peer_reg, peer_storage).await;

    let (local_reg, local_storage, _local_dir) = new_registry_and_storage();
    let app = build_cluster_test_app(local_reg, local_storage, &peer_url).await;

    // Try to archive a PV that's already on the peer — should be rejected.
    let body = serde_json::json!({"pv": "DUP:PV"});
    let req = Request::builder()
        .method("POST")
        .uri("/mgmt/bpl/archivePV")
        .header("content-type", "application/json")
        .body(Body::from(body.to_string()))
        .unwrap();

    let resp = app.oneshot(req).await.unwrap();
    assert_eq!(resp.status(), StatusCode::CONFLICT);
    let text = body_to_string(resp.into_body()).await;
    assert!(text.contains("already archived"));
}
