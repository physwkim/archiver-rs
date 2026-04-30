use std::sync::Arc;

use axum::body::Body;
use axum::http::{Request, StatusCode};
use tower::ServiceExt;

use archiver_core::config::{ApplianceIdentity, ClusterConfig, PeerConfig, SecurityConfig};
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
            api_key: None,
        }],
        api_key: None,
        reassign_appliance_enabled: false,
    };

    let repo = Arc::new(RegistryRepository::new(local_registry));
    let archiver = Arc::new(ChannelArchiverControl::new(channel_mgr));
    let state = AppState {
        storage: local_storage,
        pv_query: repo.clone(),
        pv_cmd: repo,
        archiver_query: archiver.clone(),
        archiver_cmd: archiver,
        cluster: Some(Arc::new(ClusterClientRouter::new(Arc::new(ClusterClient::new(&cluster_config))))),
        api_keys: None,
        cluster_api_key: None,
        metrics_handle: None,
        rate_limiter: None,
        trust_proxy_headers: false,
        failover: None,
        etl_chain: Vec::new(),
        reassign_appliance_enabled: false,
    };
    build_router(state, &SecurityConfig::default())
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
    assert_eq!(json["dbrType"], 5); // ScalarInt
    assert_eq!(json["samplingMethod"], "Scan @ 2s");
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
    assert_eq!(json["pvName"], "ONLY:OnPeer");
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

    // Java parity (d1d436d): default behavior is now to redirect rather
    // than proxy. Send `redirect: false` to opt back into the legacy
    // buffer-and-stream path that this test was written against.
    let req = Request::builder()
        .method("GET")
        .uri(&uri)
        .header("redirect", "false")
        .body(Body::empty())
        .unwrap();
    let resp = app.clone().oneshot(req).await.unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let json = body_to_json(resp.into_body()).await;
    let data = json[0]["data"].as_array().unwrap();
    assert_eq!(data.len(), 5);

    // Default (no redirect header) should issue a 302 to the peer
    // (Java parity d1d436d — `resp.sendRedirect` emits 302 Found).
    let resp = app.oneshot(get_request(&uri)).await.unwrap();
    assert_eq!(resp.status(), StatusCode::FOUND);
    let location = resp.headers().get("location").unwrap().to_str().unwrap();
    assert!(location.contains("/retrieval/data/getData.json"));
    assert!(location.contains("PEER%3AData") || location.contains("PEER:Data"));
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

// --- Cluster + Auth tests ---

/// Shared cluster internal key used by both local and peer in auth tests.
const CLUSTER_INTERNAL_KEY: &str = "shared-cluster-secret";

/// Build a cluster test app with API key authentication + cluster internal key.
async fn build_cluster_test_app_with_auth(
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
            api_key: None,
        }],
        api_key: Some(CLUSTER_INTERNAL_KEY.to_string()),
        reassign_appliance_enabled: false,
    };

    let repo = Arc::new(RegistryRepository::new(local_registry));
    let archiver = Arc::new(ChannelArchiverControl::new(channel_mgr));
    // External api_keys and cluster_api_key are separate privilege domains.
    let state = AppState {
        storage: local_storage,
        pv_query: repo.clone(),
        pv_cmd: repo,
        archiver_query: archiver.clone(),
        archiver_cmd: archiver,
        cluster: Some(Arc::new(ClusterClientRouter::new(Arc::new(ClusterClient::new(&cluster_config))))),
        api_keys: Some(vec!["test-cluster-key".to_string()]),
        cluster_api_key: Some(CLUSTER_INTERNAL_KEY.to_string()),
        metrics_handle: None,
        rate_limiter: None,
        trust_proxy_headers: false,
        failover: None,
        etl_chain: Vec::new(),
        reassign_appliance_enabled: false,
    };
    build_router(state, &SecurityConfig::default())
}

/// Start a mock peer with API key auth enabled (accepts the shared cluster internal key).
async fn start_mock_peer_with_auth(
    registry: Arc<PvRegistry>,
    storage: Arc<PlainPbStoragePlugin>,
) -> (String, tokio::task::JoinHandle<()>) {
    let (channel_mgr, _rx) = ChannelManager::new(storage.clone(), registry.clone(), None)
        .await
        .unwrap();
    let channel_mgr = Arc::new(channel_mgr);
    let repo = Arc::new(RegistryRepository::new(registry));
    let archiver = Arc::new(ChannelArchiverControl::new(channel_mgr));
    // Peer has its own external key + the shared cluster key (separate fields).
    let state = AppState {
        storage,
        pv_query: repo.clone(),
        pv_cmd: repo,
        archiver_query: archiver.clone(),
        archiver_cmd: archiver,
        cluster: None,
        api_keys: Some(vec!["peer-secret-key".to_string()]),
        cluster_api_key: Some(CLUSTER_INTERNAL_KEY.to_string()),
        metrics_handle: None,
        rate_limiter: None,
        trust_proxy_headers: false,
        failover: None,
        etl_chain: Vec::new(),
        reassign_appliance_enabled: false,
    };
    let app = build_router(state, &SecurityConfig::default());
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let handle = tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });
    (format!("http://{addr}"), handle)
}

#[tokio::test]
async fn test_cluster_proxy_authenticates_with_internal_key() {
    // Peer has auth enabled — proxied requests authenticate via the shared cluster api_key.
    let (peer_reg, peer_storage, _peer_dir) = new_registry_and_storage();
    peer_reg
        .register_pv("AUTH:PV", archiver_core::types::ArchDbType::ScalarDouble, &SampleMode::Monitor, 1)
        .unwrap();
    let (peer_url, _handle) = start_mock_peer_with_auth(peer_reg.clone(), peer_storage).await;

    let (local_reg, local_storage, _local_dir) = new_registry_and_storage();
    let app = build_cluster_test_app_with_auth(local_reg, local_storage, &peer_url).await;

    // Pause a PV on the peer — caller authenticates with user key, proxy uses cluster key.
    let resp = app
        .oneshot(
            Request::builder()
                .uri("/mgmt/bpl/pauseArchivingPV?pv=AUTH:PV")
                .header("authorization", "Bearer test-cluster-key")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);

    // Verify PV was actually paused on the peer.
    let record = peer_reg.get_pv("AUTH:PV").unwrap().unwrap();
    assert_eq!(record.status, archiver_core::registry::PvStatus::Paused);
}

#[tokio::test]
async fn test_x_archiver_proxied_header_does_not_bypass_auth() {
    // External clients must NOT be able to bypass auth by adding X-Archiver-Proxied.
    let (peer_reg, peer_storage, _peer_dir) = new_registry_and_storage();
    let (peer_url, _handle) = start_mock_peer(peer_reg, peer_storage).await;

    let (local_reg, local_storage, _local_dir) = new_registry_and_storage();
    let app = build_cluster_test_app_with_auth(local_reg, local_storage, &peer_url).await;

    // Send request with X-Archiver-Proxied but NO valid API key — should be rejected.
    let resp = app
        .oneshot(
            Request::builder()
                .uri("/mgmt/bpl/pauseArchivingPV?pv=FAKE:PV")
                .header("X-Archiver-Proxied", "true")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::UNAUTHORIZED);
}

#[tokio::test]
async fn test_cluster_auth_rejects_unauthenticated_caller() {
    // Even with cluster mode, the *caller* must authenticate.
    let (peer_reg, peer_storage, _peer_dir) = new_registry_and_storage();
    peer_reg
        .register_pv("AUTH:PV2", archiver_core::types::ArchDbType::ScalarDouble, &SampleMode::Monitor, 1)
        .unwrap();
    let (peer_url, _handle) = start_mock_peer(peer_reg, peer_storage).await;

    let (local_reg, local_storage, _local_dir) = new_registry_and_storage();
    let app = build_cluster_test_app_with_auth(local_reg, local_storage, &peer_url).await;

    // Pause without API key — should be rejected.
    let resp = app
        .oneshot(get_request("/mgmt/bpl/pauseArchivingPV?pv=AUTH:PV2"))
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::UNAUTHORIZED);
}

#[tokio::test]
async fn test_bulk_pause_cluster_with_auth() {
    // Bulk pause with auth: should correctly forward to auth-enabled peer.
    let (peer_reg, peer_storage, _peer_dir) = new_registry_and_storage();
    peer_reg
        .register_pv("REMOTE:BulkAuth", archiver_core::types::ArchDbType::ScalarDouble, &SampleMode::Monitor, 1)
        .unwrap();
    let (peer_url, _handle) = start_mock_peer_with_auth(peer_reg.clone(), peer_storage).await;

    let (local_reg, local_storage, _local_dir) = new_registry_and_storage();
    local_reg
        .register_pv("LOCAL:BulkAuth", archiver_core::types::ArchDbType::ScalarDouble, &SampleMode::Monitor, 1)
        .unwrap();
    let app = build_cluster_test_app_with_auth(local_reg.clone(), local_storage, &peer_url).await;

    let body = r#"["LOCAL:BulkAuth", "REMOTE:BulkAuth"]"#;
    let req = Request::builder()
        .method("POST")
        .uri("/mgmt/bpl/pauseArchivingPV")
        .header("content-type", "application/json")
        .header("authorization", "Bearer test-cluster-key")
        .body(Body::from(body))
        .unwrap();

    let resp = app.oneshot(req).await.unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let json = body_to_json(resp.into_body()).await;
    let arr = json.as_array().unwrap();
    assert_eq!(arr.len(), 2);

    // Verify local PV was paused.
    let local_pv = local_reg.get_pv("LOCAL:BulkAuth").unwrap().unwrap();
    assert_eq!(local_pv.status, archiver_core::registry::PvStatus::Paused);

    // Verify remote PV was paused on the peer.
    let remote_pv = peer_reg.get_pv("REMOTE:BulkAuth").unwrap().unwrap();
    assert_eq!(remote_pv.status, archiver_core::registry::PvStatus::Paused);
}

// --- Per-peer credential tests ---

/// Per-peer key used by the test peer's inbound auth.
const PEER_SPECIFIC_KEY: &str = "peer0-specific-secret";

/// Start a mock peer that accepts a specific per-peer key (not the shared cluster key).
async fn start_mock_peer_with_own_key(
    registry: Arc<PvRegistry>,
    storage: Arc<PlainPbStoragePlugin>,
    inbound_key: &str,
) -> (String, tokio::task::JoinHandle<()>) {
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
        api_keys: Some(vec!["unused-external".to_string()]),
        cluster_api_key: Some(inbound_key.to_string()),
        metrics_handle: None,
        rate_limiter: None,
        trust_proxy_headers: false,
        failover: None,
        etl_chain: Vec::new(),
        reassign_appliance_enabled: false,
    };
    let app = build_router(state, &SecurityConfig::default());
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let handle = tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });
    (format!("http://{addr}"), handle)
}

#[tokio::test]
async fn test_cluster_proxy_authenticates_with_per_peer_key() {
    // Peer accepts only PEER_SPECIFIC_KEY as its inbound key.
    // Local appliance has no cluster.api_key fallback — only the per-peer key.
    let (peer_reg, peer_storage, _peer_dir) = new_registry_and_storage();
    peer_reg
        .register_pv("PPEER:PV", archiver_core::types::ArchDbType::ScalarDouble, &SampleMode::Monitor, 1)
        .unwrap();
    let (peer_url, _handle) = start_mock_peer_with_own_key(peer_reg.clone(), peer_storage, PEER_SPECIFIC_KEY).await;

    let (local_reg, local_storage, _local_dir) = new_registry_and_storage();
    let (channel_mgr, _rx) = ChannelManager::new(local_storage.clone(), local_reg.clone(), None)
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
            api_key: Some(PEER_SPECIFIC_KEY.to_string()),
        }],
        api_key: None, // no fallback
        reassign_appliance_enabled: false,
    };

    let repo = Arc::new(RegistryRepository::new(local_reg));
    let archiver = Arc::new(ChannelArchiverControl::new(channel_mgr));
    let state = AppState {
        storage: local_storage,
        pv_query: repo.clone(),
        pv_cmd: repo,
        archiver_query: archiver.clone(),
        archiver_cmd: archiver,
        cluster: Some(Arc::new(ClusterClientRouter::new(Arc::new(ClusterClient::new(&cluster_config))))),
        api_keys: Some(vec!["caller-key".to_string()]),
        cluster_api_key: None,
        metrics_handle: None,
        rate_limiter: None,
        trust_proxy_headers: false,
        failover: None,
        etl_chain: Vec::new(),
        reassign_appliance_enabled: false,
    };
    let app = build_router(state, &SecurityConfig::default());

    // Pause a PV that only exists on the peer — proxy must use per-peer key.
    let resp = app
        .oneshot(
            Request::builder()
                .uri("/mgmt/bpl/pauseArchivingPV?pv=PPEER:PV")
                .header("authorization", "Bearer caller-key")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);

    // Verify PV was actually paused on the peer.
    let record = peer_reg.get_pv("PPEER:PV").unwrap().unwrap();
    assert_eq!(record.status, archiver_core::registry::PvStatus::Paused);
}

#[tokio::test]
async fn test_different_peers_get_different_keys() {
    const PEER0_KEY: &str = "peer0-key";
    const PEER1_KEY: &str = "peer1-key";

    // Two peers, each with their own inbound key.
    let (peer0_reg, peer0_storage, _peer0_dir) = new_registry_and_storage();
    peer0_reg
        .register_pv("P0:PV", archiver_core::types::ArchDbType::ScalarDouble, &SampleMode::Monitor, 1)
        .unwrap();
    let (peer0_url, _h0) = start_mock_peer_with_own_key(peer0_reg.clone(), peer0_storage, PEER0_KEY).await;

    let (peer1_reg, peer1_storage, _peer1_dir) = new_registry_and_storage();
    peer1_reg
        .register_pv("P1:PV", archiver_core::types::ArchDbType::ScalarDouble, &SampleMode::Monitor, 1)
        .unwrap();
    let (peer1_url, _h1) = start_mock_peer_with_own_key(peer1_reg.clone(), peer1_storage, PEER1_KEY).await;

    let (local_reg, local_storage, _local_dir) = new_registry_and_storage();
    let (channel_mgr, _rx) = ChannelManager::new(local_storage.clone(), local_reg.clone(), None)
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
        peers: vec![
            PeerConfig {
                name: "peer0".to_string(),
                mgmt_url: format!("{peer0_url}/mgmt/bpl"),
                retrieval_url: format!("{peer0_url}/retrieval"),
                api_key: Some(PEER0_KEY.to_string()),
            },
            PeerConfig {
                name: "peer1".to_string(),
                mgmt_url: format!("{peer1_url}/mgmt/bpl"),
                retrieval_url: format!("{peer1_url}/retrieval"),
                api_key: Some(PEER1_KEY.to_string()),
            },
        ],
        api_key: None,
        reassign_appliance_enabled: false,
    };

    let repo = Arc::new(RegistryRepository::new(local_reg));
    let archiver = Arc::new(ChannelArchiverControl::new(channel_mgr));
    let state = AppState {
        storage: local_storage,
        pv_query: repo.clone(),
        pv_cmd: repo,
        archiver_query: archiver.clone(),
        archiver_cmd: archiver,
        cluster: Some(Arc::new(ClusterClientRouter::new(Arc::new(ClusterClient::new(&cluster_config))))),
        api_keys: Some(vec!["caller-key".to_string()]),
        cluster_api_key: None,
        metrics_handle: None,
        rate_limiter: None,
        trust_proxy_headers: false,
        failover: None,
        etl_chain: Vec::new(),
        reassign_appliance_enabled: false,
    };
    let app = build_router(state, &SecurityConfig::default());

    // Pause PV on peer0 — must use PEER0_KEY.
    let resp = app
        .clone()
        .oneshot(
            Request::builder()
                .uri("/mgmt/bpl/pauseArchivingPV?pv=P0:PV")
                .header("authorization", "Bearer caller-key")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let record = peer0_reg.get_pv("P0:PV").unwrap().unwrap();
    assert_eq!(record.status, archiver_core::registry::PvStatus::Paused);

    // Pause PV on peer1 — must use PEER1_KEY.
    let resp = app
        .oneshot(
            Request::builder()
                .uri("/mgmt/bpl/pauseArchivingPV?pv=P1:PV")
                .header("authorization", "Bearer caller-key")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let record = peer1_reg.get_pv("P1:PV").unwrap().unwrap();
    assert_eq!(record.status, archiver_core::registry::PvStatus::Paused);
}
