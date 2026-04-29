//! End-to-end failover retrieval test. Spins up a tiny in-process HTTP server
//! impersonating a peer archiver that returns a hand-crafted PB body, and
//! verifies that the local archiver merges + dedupes peer samples with its
//! local stream.

use std::sync::Arc;
use std::time::{Duration, SystemTime};

use axum::body::{to_bytes, Body};
use axum::http::{Request, StatusCode};
use prost::Message;
use tokio::net::TcpListener;
use tower::ServiceExt;

use archiver_api::services::impls::{ChannelArchiverControl, RegistryRepository};
use archiver_api::{build_router, AppState, FailoverState};
use archiver_core::config::SecurityConfig;
use archiver_core::registry::{PvRegistry, SampleMode};
use archiver_core::storage::partition::PartitionGranularity;
use archiver_core::storage::plainpb::PlainPbStoragePlugin;
use archiver_core::storage::traits::{AppendMeta, StoragePlugin};
use archiver_core::types::{ArchDbType, ArchiverSample, ArchiverValue};
use archiver_engine::channel_manager::ChannelManager;
use archiver_proto::epics_event;

/// Build a getData.raw response body with the given samples. Same on-the-wire
/// format the local PlainPB writer produces.
fn build_pb_body(pv: &str, year: i32, samples: &[(u32, u32, f64)]) -> Vec<u8> {
    use archiver_core::storage::plainpb::codec;

    let header = epics_event::PayloadInfo {
        r#type: ArchDbType::ScalarDouble as i32,
        pvname: pv.to_string(),
        year,
        element_count: Some(1),
        unused00: None,
        unused01: None,
        unused02: None,
        unused03: None,
        unused04: None,
        unused05: None,
        unused06: None,
        unused07: None,
        unused08: None,
        unused09: None,
        headers: Vec::new(),
    };
    let header_bytes = header.encode_to_vec();
    let mut out = codec::escape(&header_bytes);
    out.push(codec::NEWLINE);

    for (secs, nanos, val) in samples {
        let sample = epics_event::ScalarDouble {
            secondsintoyear: *secs,
            nano: *nanos,
            val: *val,
            severity: None,
            status: None,
            repeatcount: None,
            fieldvalues: Vec::new(),
            fieldactualchange: None,
        };
        let bytes = sample.encode_to_vec();
        let escaped = codec::escape(&bytes);
        out.extend_from_slice(&escaped);
        out.push(codec::NEWLINE);
    }
    out
}

/// Spawn an HTTP server that serves a fixed PB body for any /data/getData.raw
/// request. Returns the bound base URL like "http://127.0.0.1:54321/retrieval".
async fn start_fake_peer(body: Vec<u8>) -> (String, tokio::task::JoinHandle<()>) {
    use axum::routing::get;
    use axum::Router;
    use std::sync::Mutex;

    let body = Arc::new(Mutex::new(body));
    let body_clone = body.clone();
    let app = Router::new().route(
        "/retrieval/data/getData.raw",
        get(move || {
            let b = body_clone.clone();
            async move {
                let body = b.lock().unwrap().clone();
                axum::http::Response::builder()
                    .status(200)
                    .header("content-type", "application/octet-stream")
                    .body(axum::body::Body::from(body))
                    .unwrap()
            }
        }),
    );

    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let handle = tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });
    (format!("http://{addr}/retrieval"), handle)
}

#[tokio::test]
async fn failover_merges_and_dedupes_peer_samples() {
    let dir = tempfile::tempdir().unwrap();
    let storage = Arc::new(PlainPbStoragePlugin::new(
        "sts",
        dir.path().to_path_buf(),
        PartitionGranularity::Year,
    ));
    let registry = Arc::new(PvRegistry::in_memory().unwrap());
    registry
        .register_pv("RING:Current", ArchDbType::ScalarDouble, &SampleMode::Monitor, 1)
        .unwrap();

    // Local samples: t=10s and t=30s (in 2024).
    let year = 2024i32;
    let local_year_start = chrono::NaiveDate::from_ymd_opt(year, 1, 1)
        .unwrap()
        .and_hms_opt(0, 0, 0)
        .unwrap()
        .and_utc();
    let mk_ts = |secs: u32| -> SystemTime {
        let dt = local_year_start + chrono::Duration::seconds(secs as i64);
        dt.into()
    };
    for (secs, val) in [(10u32, 1.0), (30u32, 3.0)] {
        let sample = ArchiverSample::new(mk_ts(secs), ArchiverValue::ScalarDouble(val));
        storage
            .append_event_with_meta(
                "RING:Current",
                ArchDbType::ScalarDouble,
                &sample,
                &AppendMeta::default(),
            )
            .await
            .unwrap();
    }
    storage.flush_writes().await.unwrap();

    // Peer samples: t=20s (unique) and t=30s (duplicate).
    let peer_body = build_pb_body(
        "RING:Current",
        year,
        &[(20, 0, 2.0), (30, 0, 99.0 /* would-be dup */)],
    );
    let (peer_url, _peer_task) = start_fake_peer(peer_body).await;

    let (channel_mgr, _rx) = ChannelManager::new(storage.clone(), registry.clone(), None)
        .await
        .unwrap();
    let channel_mgr = Arc::new(channel_mgr);
    let repo = Arc::new(RegistryRepository::new(registry.clone()));
    let archiver = Arc::new(ChannelArchiverControl::new(channel_mgr));

    let state = AppState {
        storage: storage.clone(),
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
        failover: Some(Arc::new(FailoverState {
            peers: vec![peer_url],
            timeout: Duration::from_secs(5),
        })),
        etl_chain: Vec::new(),
    };

    let app = build_router(state, &SecurityConfig::default());
    let from = chrono::DateTime::<chrono::Utc>::from(mk_ts(0)).to_rfc3339();
    let to = chrono::DateTime::<chrono::Utc>::from(mk_ts(60)).to_rfc3339();
    let req = Request::builder()
        .uri(format!(
            "/retrieval/data/getData.json?pv=RING:Current&from={}&to={}",
            urlencoding::encode(&from),
            urlencoding::encode(&to),
        ))
        .body(Body::empty())
        .unwrap();
    let resp = app.oneshot(req).await.unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let bytes = to_bytes(resp.into_body(), 1_048_576).await.unwrap();
    let json: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
    let samples = &json[0]["data"];
    let arr = samples.as_array().unwrap();
    // Expect: 1.0 (local t=10), 2.0 (peer t=20, unique), 3.0 (local t=30 — dedup wins).
    let values: Vec<f64> = arr.iter().map(|s| s["val"].as_f64().unwrap()).collect();
    assert_eq!(values, vec![1.0, 2.0, 3.0]);
}

#[tokio::test]
async fn failover_tolerates_unreachable_peer() {
    let dir = tempfile::tempdir().unwrap();
    let storage = Arc::new(PlainPbStoragePlugin::new(
        "sts",
        dir.path().to_path_buf(),
        PartitionGranularity::Year,
    ));
    let registry = Arc::new(PvRegistry::in_memory().unwrap());
    registry
        .register_pv("PV:Local", ArchDbType::ScalarDouble, &SampleMode::Monitor, 1)
        .unwrap();

    let year = 2024i32;
    let year_start = chrono::NaiveDate::from_ymd_opt(year, 1, 1)
        .unwrap()
        .and_hms_opt(0, 0, 0)
        .unwrap()
        .and_utc();
    let ts = |secs: u32| SystemTime::from(year_start + chrono::Duration::seconds(secs as i64));
    let sample = ArchiverSample::new(ts(5), ArchiverValue::ScalarDouble(42.0));
    storage
        .append_event_with_meta("PV:Local", ArchDbType::ScalarDouble, &sample, &AppendMeta::default())
        .await
        .unwrap();
    storage.flush_writes().await.unwrap();

    let (channel_mgr, _rx) = ChannelManager::new(storage.clone(), registry.clone(), None)
        .await
        .unwrap();
    let channel_mgr = Arc::new(channel_mgr);
    let repo = Arc::new(RegistryRepository::new(registry.clone()));
    let archiver = Arc::new(ChannelArchiverControl::new(channel_mgr));

    let state = AppState {
        storage: storage.clone(),
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
        failover: Some(Arc::new(FailoverState {
            peers: vec!["http://127.0.0.1:1/retrieval".to_string()], // dead port
            timeout: Duration::from_secs(1),
        })),
        etl_chain: Vec::new(),
    };

    let app = build_router(state, &SecurityConfig::default());
    let from = chrono::DateTime::<chrono::Utc>::from(ts(0)).to_rfc3339();
    let to_t = chrono::DateTime::<chrono::Utc>::from(ts(60)).to_rfc3339();
    let req = Request::builder()
        .uri(format!(
            "/retrieval/data/getData.json?pv=PV:Local&from={}&to={}",
            urlencoding::encode(&from),
            urlencoding::encode(&to_t),
        ))
        .body(Body::empty())
        .unwrap();
    let resp = app.oneshot(req).await.unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let bytes = to_bytes(resp.into_body(), 1_048_576).await.unwrap();
    let json: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
    let arr = json[0]["data"].as_array().unwrap();
    // Local-only sample survives.
    assert_eq!(arr.len(), 1);
    assert_eq!(arr[0]["val"], 42.0);
}
