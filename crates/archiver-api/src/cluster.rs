use std::time::{Duration, Instant};

use axum::extract::{Query, State};
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use axum::routing::get;
use axum::Router;
use dashmap::DashMap;
use serde::{Deserialize, Serialize};
use tracing::warn;

use archiver_core::config::{ApplianceIdentity, ClusterConfig, PeerConfig};

use crate::AppState;

/// Cached peer routing entry for a PV.
struct CachedPeer {
    retrieval_url: String,
    expires_at: Instant,
}

/// Client for cluster-mode operations: PV routing, proxying, and aggregation.
pub struct ClusterClient {
    http_client: reqwest::Client,
    identity: ApplianceIdentity,
    peers: Vec<PeerConfig>,
    pv_cache: DashMap<String, CachedPeer>,
    cache_ttl: Duration,
}

impl ClusterClient {
    pub fn new(config: &ClusterConfig) -> Self {
        let http_client = reqwest::Client::builder()
            .timeout(Duration::from_secs(config.peer_timeout_secs))
            .build()
            .expect("Failed to build HTTP client");
        Self {
            http_client,
            identity: config.identity.clone(),
            peers: config.peers.clone(),
            pv_cache: DashMap::new(),
            cache_ttl: Duration::from_secs(config.cache_ttl_secs),
        }
    }

    pub fn identity(&self) -> &ApplianceIdentity {
        &self.identity
    }

    pub fn peers(&self) -> &[PeerConfig] {
        &self.peers
    }

    /// Resolve which peer archives the given PV.
    /// Returns the peer's retrieval URL, or None if no peer archives it.
    pub async fn resolve_peer(&self, pv: &str) -> Option<String> {
        // Check cache first.
        if let Some(entry) = self.pv_cache.get(pv) {
            if entry.expires_at > Instant::now() {
                return Some(entry.retrieval_url.clone());
            }
            // Expired — drop ref before removing.
            drop(entry);
            self.pv_cache.remove(pv);
        }

        // Query all peers concurrently for PV status.
        let futures: Vec<_> = self
            .peers
            .iter()
            .map(|peer| {
                let url = format!(
                    "{}/getPVStatus?pv={}",
                    peer.mgmt_url,
                    urlencoding::encode(pv)
                );
                let retrieval_url = peer.retrieval_url.clone();
                let client = self.http_client.clone();
                async move {
                    let resp = client.get(&url).send().await.ok()?;
                    let body: serde_json::Value = resp.json().await.ok()?;
                    let status = body.get("status")?.as_str()?;
                    if status == "Being archived" {
                        Some(retrieval_url)
                    } else {
                        None
                    }
                }
            })
            .collect();

        let results = futures::future::join_all(futures).await;
        for result in results {
            if let Some(retrieval_url) = result {
                self.pv_cache.insert(
                    pv.to_string(),
                    CachedPeer {
                        retrieval_url: retrieval_url.clone(),
                        expires_at: Instant::now() + self.cache_ttl,
                    },
                );
                return Some(retrieval_url);
            }
        }

        None
    }

    /// Proxy a retrieval request to a remote peer, streaming the response back.
    pub async fn proxy_retrieval(
        &self,
        peer_retrieval_url: &str,
        path: &str,
        query_string: &str,
    ) -> anyhow::Result<Response> {
        let url = if query_string.is_empty() {
            format!("{peer_retrieval_url}/{path}")
        } else {
            format!("{peer_retrieval_url}/{path}?{query_string}")
        };

        let resp = self
            .http_client
            .get(&url)
            .header("X-Archiver-Proxied", "true")
            .send()
            .await?;

        let status = axum::http::StatusCode::from_u16(resp.status().as_u16())
            .unwrap_or(StatusCode::BAD_GATEWAY);

        let mut builder = axum::http::Response::builder().status(status);

        // Forward content-type header.
        if let Some(ct) = resp.headers().get(reqwest::header::CONTENT_TYPE) {
            if let Ok(ct_str) = ct.to_str() {
                builder = builder.header(axum::http::header::CONTENT_TYPE, ct_str);
            }
        }

        // Stream the body.
        let stream = resp.bytes_stream();
        let body = axum::body::Body::from_stream(stream);
        Ok(builder.body(body).unwrap().into_response())
    }

    /// Aggregate all PV names from all peers + dedup + sort.
    pub async fn aggregate_all_pvs(&self) -> Vec<String> {
        let futures: Vec<_> = self
            .peers
            .iter()
            .map(|peer| {
                let url = format!("{}/getAllPVs", peer.mgmt_url);
                let client = self.http_client.clone();
                async move {
                    match client.get(&url).send().await {
                        Ok(resp) => resp.json::<Vec<String>>().await.unwrap_or_default(),
                        Err(e) => {
                            warn!(peer = url, "Failed to get all PVs from peer: {e}");
                            Vec::new()
                        }
                    }
                }
            })
            .collect();

        let results = futures::future::join_all(futures).await;
        let mut all: std::collections::BTreeSet<String> = std::collections::BTreeSet::new();
        for pvs in results {
            all.extend(pvs);
        }
        all.into_iter().collect()
    }

    /// Aggregate matching PVs from all peers.
    pub async fn aggregate_matching_pvs(&self, pattern: &str) -> Vec<String> {
        let futures: Vec<_> = self
            .peers
            .iter()
            .map(|peer| {
                let url = format!(
                    "{}/getMatchingPVs?pv={}",
                    peer.mgmt_url,
                    urlencoding::encode(pattern)
                );
                let client = self.http_client.clone();
                async move {
                    match client.get(&url).send().await {
                        Ok(resp) => resp.json::<Vec<String>>().await.unwrap_or_default(),
                        Err(e) => {
                            warn!(peer = url, "Failed to get matching PVs from peer: {e}");
                            Vec::new()
                        }
                    }
                }
            })
            .collect();

        let results = futures::future::join_all(futures).await;
        let mut all: std::collections::BTreeSet<String> = std::collections::BTreeSet::new();
        for pvs in results {
            all.extend(pvs);
        }
        all.into_iter().collect()
    }

    /// Query PV status from a remote peer.
    pub async fn remote_pv_status(&self, pv: &str) -> Option<serde_json::Value> {
        let futures: Vec<_> = self
            .peers
            .iter()
            .map(|peer| {
                let url = format!(
                    "{}/getPVStatus?pv={}",
                    peer.mgmt_url,
                    urlencoding::encode(pv)
                );
                let client = self.http_client.clone();
                async move {
                    let resp = client.get(&url).send().await.ok()?;
                    let body: serde_json::Value = resp.json().await.ok()?;
                    let status = body.get("status")?.as_str()?;
                    if status != "Not being archived" {
                        Some(body)
                    } else {
                        None
                    }
                }
            })
            .collect();

        let results = futures::future::join_all(futures).await;
        results.into_iter().flatten().next()
    }
}

// --- BPL Cluster Endpoints ---

pub fn routes() -> Router<AppState> {
    Router::new()
        .route(
            "/mgmt/bpl/getAppliancesInCluster",
            get(get_appliances_in_cluster),
        )
        .route("/mgmt/bpl/getApplianceInfo", get(get_appliance_info))
}

#[derive(Serialize)]
struct ApplianceInfoResponse {
    identity: String,
    #[serde(rename = "mgmtURL")]
    mgmt_url: String,
    #[serde(rename = "retrievalURL")]
    retrieval_url: String,
    #[serde(rename = "engineURL")]
    engine_url: String,
    #[serde(rename = "etlURL")]
    etl_url: String,
}

async fn get_appliances_in_cluster(State(state): State<AppState>) -> Response {
    let Some(ref cluster) = state.cluster else {
        return axum::Json(Vec::<ApplianceInfoResponse>::new()).into_response();
    };

    let identity = cluster.identity();
    let mut appliances = vec![ApplianceInfoResponse {
        identity: identity.name.clone(),
        mgmt_url: identity.mgmt_url.clone(),
        retrieval_url: identity.retrieval_url.clone(),
        engine_url: identity.engine_url.clone(),
        etl_url: identity.etl_url.clone(),
    }];

    for peer in cluster.peers() {
        appliances.push(ApplianceInfoResponse {
            identity: peer.name.clone(),
            mgmt_url: peer.mgmt_url.clone(),
            retrieval_url: peer.retrieval_url.clone(),
            engine_url: String::new(),
            etl_url: String::new(),
        });
    }

    axum::Json(appliances).into_response()
}

#[derive(Deserialize)]
struct ApplianceInfoParams {
    #[serde(default)]
    id: Option<String>,
}

async fn get_appliance_info(
    State(state): State<AppState>,
    Query(params): Query<ApplianceInfoParams>,
) -> Response {
    let Some(ref cluster) = state.cluster else {
        return (StatusCode::NOT_FOUND, "Cluster not configured").into_response();
    };

    let identity = cluster.identity();
    let target = params.id.as_deref().unwrap_or(&identity.name);

    if target == identity.name {
        let resp = ApplianceInfoResponse {
            identity: identity.name.clone(),
            mgmt_url: identity.mgmt_url.clone(),
            retrieval_url: identity.retrieval_url.clone(),
            engine_url: identity.engine_url.clone(),
            etl_url: identity.etl_url.clone(),
        };
        return axum::Json(resp).into_response();
    }

    for peer in cluster.peers() {
        if peer.name == target {
            let resp = ApplianceInfoResponse {
                identity: peer.name.clone(),
                mgmt_url: peer.mgmt_url.clone(),
                retrieval_url: peer.retrieval_url.clone(),
                engine_url: String::new(),
                etl_url: String::new(),
            };
            return axum::Json(resp).into_response();
        }
    }

    (StatusCode::NOT_FOUND, "Appliance not found").into_response()
}

#[cfg(test)]
mod tests {
    use super::*;
    use archiver_core::config::{ApplianceIdentity, ClusterConfig, PeerConfig};

    fn test_config() -> ClusterConfig {
        ClusterConfig {
            identity: ApplianceIdentity {
                name: "app0".to_string(),
                mgmt_url: "http://app0:17665/mgmt/bpl".to_string(),
                retrieval_url: "http://app0:17665/retrieval".to_string(),
                engine_url: "http://app0:17665".to_string(),
                etl_url: "http://app0:17665".to_string(),
            },
            cache_ttl_secs: 10,
            peer_timeout_secs: 5,
            peers: vec![PeerConfig {
                name: "app1".to_string(),
                mgmt_url: "http://app1:17665/mgmt/bpl".to_string(),
                retrieval_url: "http://app1:17665/retrieval".to_string(),
            }],
        }
    }

    #[test]
    fn cache_insert_and_lookup() {
        let client = ClusterClient::new(&test_config());

        // Insert a cached peer.
        client.pv_cache.insert(
            "TEST:PV".to_string(),
            CachedPeer {
                retrieval_url: "http://app1:17665/retrieval".to_string(),
                expires_at: Instant::now() + Duration::from_secs(60),
            },
        );

        // Should find it.
        let entry = client.pv_cache.get("TEST:PV").unwrap();
        assert_eq!(entry.retrieval_url, "http://app1:17665/retrieval");
    }

    #[test]
    fn cache_expiry() {
        let client = ClusterClient::new(&test_config());

        // Insert an already-expired entry.
        client.pv_cache.insert(
            "TEST:EXPIRED".to_string(),
            CachedPeer {
                retrieval_url: "http://app1:17665/retrieval".to_string(),
                expires_at: Instant::now() - Duration::from_secs(1),
            },
        );

        // Direct cache check — it's there but expired.
        let entry = client.pv_cache.get("TEST:EXPIRED").unwrap();
        assert!(entry.expires_at < Instant::now());
    }

    #[test]
    fn get_appliances_returns_self_and_peers() {
        let config = test_config();
        let client = ClusterClient::new(&config);

        assert_eq!(client.identity().name, "app0");
        assert_eq!(client.peers().len(), 1);
        assert_eq!(client.peers()[0].name, "app1");
    }
}
