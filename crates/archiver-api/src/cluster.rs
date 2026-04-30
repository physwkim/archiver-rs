use std::collections::HashMap;
use std::time::{Duration, Instant};

use axum::Router;
use axum::extract::{Query, State};
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use axum::routing::get;
use dashmap::DashMap;
use serde::{Deserialize, Serialize};
use tracing::warn;

use archiver_core::config::{ApplianceIdentity, ClusterConfig, PeerConfig};

use crate::AppState;

/// Cached peer routing entry for a PV.
struct CachedPeer {
    retrieval_url: String,
    mgmt_url: String,
    expires_at: Instant,
}

/// Resolved peer info returned by resolve_peer.
pub struct ResolvedPeer {
    pub retrieval_url: String,
    pub mgmt_url: String,
}

/// Outcome of a cluster-wide getPVStatus probe. Java parity (c2d9f9e):
/// callers must distinguish "no peer has this PV" from "a peer that might
/// have it is unreachable" so the second case can surface as
/// `Appliance Down` rather than the misleading `Not being archived`.
pub enum RemoteStatusOutcome {
    /// A peer claims it is archiving the PV (carrier).
    Found(serde_json::Value),
    /// All peers responded and none of them archive the PV.
    NotArchived,
    /// At least one peer was unreachable and no other peer claimed
    /// the PV — the answer is unknowable from where we sit.
    ApplianceDown,
}

/// Client for cluster-mode operations: PV routing, proxying, and aggregation.
pub struct ClusterClient {
    http_client: reqwest::Client,
    identity: ApplianceIdentity,
    peers: Vec<PeerConfig>,
    pv_cache: DashMap<String, CachedPeer>,
    cache_ttl: Duration,
    /// Fallback cluster API key for inter-peer authentication.
    api_key: Option<String>,
    /// Per-peer outbound credentials: URL → key.
    peer_keys: HashMap<String, String>,
}

impl ClusterClient {
    pub fn new(config: &ClusterConfig) -> Self {
        let http_client = reqwest::Client::builder()
            .timeout(Duration::from_secs(config.peer_timeout_secs))
            .build()
            .expect("reqwest Client::builder with timeout should never fail");
        let mut peer_keys = HashMap::new();
        for p in &config.peers {
            if let Some(ref key) = p.api_key {
                peer_keys.insert(p.mgmt_url.clone(), key.clone());
                peer_keys.insert(p.retrieval_url.clone(), key.clone());
            }
        }
        Self {
            http_client,
            identity: config.identity.clone(),
            peers: config.peers.clone(),
            pv_cache: DashMap::new(),
            cache_ttl: Duration::from_secs(config.cache_ttl_secs),
            api_key: config.api_key.clone(),
            peer_keys,
        }
    }

    pub fn identity(&self) -> &ApplianceIdentity {
        &self.identity
    }

    /// Apply the appropriate outbound credential. Uses the per-peer key if one
    /// is configured for `peer_url`, otherwise falls back to the cluster-level key.
    fn apply_auth(
        &self,
        builder: reqwest::RequestBuilder,
        peer_url: &str,
    ) -> reqwest::RequestBuilder {
        let key = self.peer_keys.get(peer_url).or(self.api_key.as_ref());
        if let Some(key) = key {
            builder.header(reqwest::header::AUTHORIZATION, format!("Bearer {key}"))
        } else {
            builder
        }
    }

    /// Build an authenticated peer GET request — adds `X-Archiver-Proxied`
    /// to break loops and the cluster bearer credential. Every peer-side
    /// aggregation should go through this helper; raw `client.get(url)`
    /// silently 401s in any cluster that sets `cluster.api_key`.
    fn peer_get(&self, peer_url: &str, url: &str) -> reqwest::RequestBuilder {
        let req = self
            .http_client
            .get(url)
            .header("X-Archiver-Proxied", "true");
        self.apply_auth(req, peer_url)
    }

    /// Remove all expired entries from the PV routing cache.
    pub fn cleanup_cache(&self) {
        let now = Instant::now();
        self.pv_cache.retain(|_, entry| entry.expires_at > now);
    }

    pub fn peers(&self) -> &[PeerConfig] {
        &self.peers
    }

    /// Find a peer by appliance name.
    pub fn find_peer_by_name(&self, name: &str) -> Option<&PeerConfig> {
        self.peers.iter().find(|p| p.name == name)
    }

    /// Resolve which peer archives the given PV.
    /// Returns the peer's retrieval and mgmt URLs, or None if no peer archives it.
    pub async fn resolve_peer(&self, pv: &str) -> Option<ResolvedPeer> {
        // Check cache first.
        if let Some(entry) = self.pv_cache.get(pv) {
            if entry.expires_at > Instant::now() {
                return Some(ResolvedPeer {
                    retrieval_url: entry.retrieval_url.clone(),
                    mgmt_url: entry.mgmt_url.clone(),
                });
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
                let mgmt_url = peer.mgmt_url.clone();
                let req = self.peer_get(&peer.mgmt_url, &url);
                async move {
                    let resp = req.send().await.ok()?;
                    let body: serde_json::Value = resp.json().await.ok()?;
                    let status = body.get("status")?.as_str()?;
                    if status == "Being archived" {
                        Some((retrieval_url, mgmt_url))
                    } else {
                        None
                    }
                }
            })
            .collect();

        let results = futures::future::join_all(futures).await;
        if let Some((retrieval_url, mgmt_url)) = results.into_iter().flatten().next() {
            self.pv_cache.insert(
                pv.to_string(),
                CachedPeer {
                    retrieval_url: retrieval_url.clone(),
                    mgmt_url: mgmt_url.clone(),
                    expires_at: Instant::now() + self.cache_ttl,
                },
            );
            return Some(ResolvedPeer {
                retrieval_url,
                mgmt_url,
            });
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

        let req = self
            .http_client
            .get(&url)
            .header("X-Archiver-Proxied", "true");
        let resp = self.apply_auth(req, peer_retrieval_url).send().await?;

        let status = axum::http::StatusCode::from_u16(resp.status().as_u16())
            .unwrap_or(StatusCode::BAD_GATEWAY);

        let mut builder = axum::http::Response::builder().status(status);

        // Forward content-type header.
        if let Some(ct) = resp.headers().get(reqwest::header::CONTENT_TYPE)
            && let Ok(ct_str) = ct.to_str()
        {
            builder = builder.header(axum::http::header::CONTENT_TYPE, ct_str);
        }

        // Stream the body.
        let stream = resp.bytes_stream();
        let body = axum::body::Body::from_stream(stream);
        Ok(builder
            .body(body)
            .unwrap_or_else(|_| StatusCode::INTERNAL_SERVER_ERROR.into_response())
            .into_response())
    }

    /// Proxy a management GET request to a remote peer.
    pub async fn proxy_mgmt_get(
        &self,
        peer_mgmt_url: &str,
        endpoint: &str,
        query_string: &str,
    ) -> anyhow::Result<Response> {
        let url = if query_string.is_empty() {
            format!("{peer_mgmt_url}/{endpoint}")
        } else {
            format!("{peer_mgmt_url}/{endpoint}?{query_string}")
        };

        let req = self
            .http_client
            .get(&url)
            .header("X-Archiver-Proxied", "true");
        let resp = self.apply_auth(req, peer_mgmt_url).send().await?;

        let status = axum::http::StatusCode::from_u16(resp.status().as_u16())
            .unwrap_or(StatusCode::BAD_GATEWAY);

        let mut builder = axum::http::Response::builder().status(status);
        if let Some(ct) = resp.headers().get(reqwest::header::CONTENT_TYPE)
            && let Ok(ct_str) = ct.to_str()
        {
            builder = builder.header(axum::http::header::CONTENT_TYPE, ct_str);
        }

        let stream = resp.bytes_stream();
        let body = axum::body::Body::from_stream(stream);
        Ok(builder
            .body(body)
            .unwrap_or_else(|_| StatusCode::INTERNAL_SERVER_ERROR.into_response())
            .into_response())
    }

    /// Proxy a management POST request to a remote peer.
    pub async fn proxy_mgmt_post(
        &self,
        peer_mgmt_url: &str,
        endpoint: &str,
        body: axum::body::Bytes,
    ) -> anyhow::Result<Response> {
        let url = format!("{peer_mgmt_url}/{endpoint}");

        let req = self
            .http_client
            .post(&url)
            .header("X-Archiver-Proxied", "true")
            .header(reqwest::header::CONTENT_TYPE, "application/json")
            .body(body);
        let resp = self.apply_auth(req, peer_mgmt_url).send().await?;

        let status = axum::http::StatusCode::from_u16(resp.status().as_u16())
            .unwrap_or(StatusCode::BAD_GATEWAY);

        let mut builder = axum::http::Response::builder().status(status);
        if let Some(ct) = resp.headers().get(reqwest::header::CONTENT_TYPE)
            && let Ok(ct_str) = ct.to_str()
        {
            builder = builder.header(axum::http::header::CONTENT_TYPE, ct_str);
        }

        let stream = resp.bytes_stream();
        let body = axum::body::Body::from_stream(stream);
        Ok(builder
            .body(body)
            .unwrap_or_else(|_| StatusCode::INTERNAL_SERVER_ERROR.into_response())
            .into_response())
    }

    /// Aggregate PV count from all peers.
    /// Returns (total, active, paused, failed_peer_count).
    pub async fn aggregate_pv_count(&self) -> (u64, u64, u64, usize) {
        let futures: Vec<_> = self
            .peers
            .iter()
            .map(|peer| {
                let url = format!("{}/getPVCount", peer.mgmt_url);
                let req = self.peer_get(&peer.mgmt_url, &url);
                async move {
                    match req.send().await {
                        Ok(resp) => {
                            let body: serde_json::Value = resp.json().await.unwrap_or_default();
                            let total = body.get("total").and_then(|v| v.as_u64()).unwrap_or(0);
                            let active = body.get("active").and_then(|v| v.as_u64()).unwrap_or(0);
                            let paused = body.get("paused").and_then(|v| v.as_u64()).unwrap_or(0);
                            Ok((total, active, paused))
                        }
                        Err(e) => {
                            warn!(peer = url, "Failed to get PV count from peer: {e}");
                            Err(())
                        }
                    }
                }
            })
            .collect();

        let results = futures::future::join_all(futures).await;
        let mut total = 0u64;
        let mut active = 0u64;
        let mut paused = 0u64;
        let mut failed = 0usize;
        for result in results {
            match result {
                Ok((t, a, p)) => {
                    total += t;
                    active += a;
                    paused += p;
                }
                Err(()) => failed += 1,
            }
        }
        (total, active, paused, failed)
    }

    /// Aggregate all PV names from all peers + dedup + sort.
    pub async fn aggregate_all_pvs(&self) -> Vec<String> {
        let futures: Vec<_> = self
            .peers
            .iter()
            .map(|peer| {
                let url = format!("{}/getAllPVs", peer.mgmt_url);
                let req = self.peer_get(&peer.mgmt_url, &url);
                async move {
                    match req.send().await {
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
    /// Java parity (26124b6): forward a multi-PV `getDataAtTime` request
    /// to a single peer's retrieval URL and return its JSON response.
    /// `at` is the ISO8601 timestamp the caller already parsed; passing
    /// it back as a string keeps this helper independent of the time
    /// representation used by handlers.
    pub async fn proxy_data_at_time(
        &self,
        peer_retrieval_url: &str,
        at: Option<&str>,
        pvs: &[String],
    ) -> Result<serde_json::Value, anyhow::Error> {
        // `peer_retrieval_url` is the configured base (e.g.
        // `http://host:17665/retrieval`); the relative path here is
        // just `data/getDataAtTime` to match the convention used by
        // `proxy_retrieval`. Prepending another `/retrieval` would
        // produce `/retrieval/retrieval/...` and silently 404.
        let mut url = format!(
            "{}/data/getDataAtTime",
            peer_retrieval_url.trim_end_matches('/')
        );
        if let Some(t) = at {
            url.push_str("?at=");
            url.push_str(&urlencoding::encode(t));
        }
        let req = self
            .http_client
            .post(&url)
            .header("X-Archiver-Proxied", "1")
            .json(pvs);
        let req = self.apply_auth(req, peer_retrieval_url);
        let resp = req.send().await?;
        if !resp.status().is_success() {
            anyhow::bail!("peer returned {}", resp.status());
        }
        // 64 MB cap on the peer body — accumulate via the byte stream
        // rather than `resp.bytes()` so a buggy / malicious peer
        // streaming gigabytes is rejected DURING transfer instead of
        // first being fully buffered into memory and then size-checked
        // post-hoc. Typical 1000-PV reply is ~1 MB; 64× headroom
        // covers worst-case waveform-heavy payloads.
        const PEER_BODY_MAX: usize = 64 * 1024 * 1024;
        let bytes = read_capped_body(resp, PEER_BODY_MAX).await?;
        let body: serde_json::Value = serde_json::from_slice(&bytes)?;
        Ok(body)
    }

    pub async fn aggregate_matching_pvs(&self, pattern: &str) -> Vec<String> {
        self.aggregate_matching_pvs_limited(pattern, None).await
    }

    /// `aggregate_all_pvs` with optional `&limit=` forwarding to peers
    /// (Java parity 9b78b21). `None` keeps peer defaults; positive caps;
    /// `Some(-1)` is explicit unlimited.
    pub async fn aggregate_all_pvs_limited(&self, limit: Option<i64>) -> Vec<String> {
        let futures: Vec<_> = self
            .peers
            .iter()
            .map(|peer| {
                let mut url = format!("{}/getAllPVs", peer.mgmt_url);
                if let Some(n) = limit {
                    url.push_str(&format!("?limit={n}"));
                }
                let req = self.peer_get(&peer.mgmt_url, &url);
                async move {
                    match req.send().await {
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
        let mut all = std::collections::BTreeSet::new();
        for pvs in results {
            all.extend(pvs);
        }
        all.into_iter().collect()
    }

    /// Java parity (9b78b21): forward `&limit=` to peers so a clustered
    /// `?limit=N` query doesn't pull `peer_default × num_peers` names off
    /// the wire only to truncate locally. `None` = preserve peer default;
    /// `Some(n)` (positive) = cap; `Some(-1)` = explicit unlimited.
    pub async fn aggregate_matching_pvs_limited(
        &self,
        pattern: &str,
        limit: Option<i64>,
    ) -> Vec<String> {
        let futures: Vec<_> = self
            .peers
            .iter()
            .map(|peer| {
                let mut url = format!(
                    "{}/getMatchingPVs?pv={}",
                    peer.mgmt_url,
                    urlencoding::encode(pattern)
                );
                if let Some(n) = limit {
                    url.push_str(&format!("&limit={n}"));
                }
                let req = self.peer_get(&peer.mgmt_url, &url);
                async move {
                    match req.send().await {
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
        match self.remote_pv_status_detailed(pv).await {
            RemoteStatusOutcome::Found(v) => Some(v),
            RemoteStatusOutcome::NotArchived | RemoteStatusOutcome::ApplianceDown => None,
        }
    }

    /// Like [`remote_pv_status`] but distinguishes a definitive cluster-wide
    /// negative ("no peer is archiving this PV") from the case where at
    /// least one peer was unreachable. Java parity (c2d9f9e).
    pub async fn remote_pv_status_detailed(&self, pv: &str) -> RemoteStatusOutcome {
        if self.peers.is_empty() {
            return RemoteStatusOutcome::NotArchived;
        }

        // Each peer query yields one of three outcomes:
        //   PeerProbe::Found(body)   — peer claims it archives this PV
        //   PeerProbe::NotArchived   — peer responded with negative
        //   PeerProbe::Unreachable   — HTTP / decode error
        enum PeerProbe {
            Found(serde_json::Value),
            NotArchived,
            Unreachable,
        }

        use futures::stream::{FuturesUnordered, StreamExt};

        let mut probes: FuturesUnordered<_> = self
            .peers
            .iter()
            .map(|peer| {
                let url = format!(
                    "{}/getPVStatus?pv={}",
                    peer.mgmt_url,
                    urlencoding::encode(pv)
                );
                let req = self.peer_get(&peer.mgmt_url, &url);
                async move {
                    let resp = match req.send().await {
                        Ok(r) => r,
                        Err(_) => return PeerProbe::Unreachable,
                    };
                    if !resp.status().is_success() {
                        return PeerProbe::Unreachable;
                    }
                    let body: serde_json::Value = match resp.json().await {
                        Ok(b) => b,
                        Err(_) => return PeerProbe::Unreachable,
                    };
                    match body.get("status").and_then(|s| s.as_str()) {
                        Some("Not being archived") => PeerProbe::NotArchived,
                        Some(_) => PeerProbe::Found(body),
                        // Malformed payload — treat as unreachable rather
                        // than authoritative-negative so we don't mask a
                        // sick peer.
                        None => PeerProbe::Unreachable,
                    }
                }
            })
            .collect();

        // Short-circuit on the first peer that claims the PV — without this
        // a single dead peer in a 10-node cluster forces every status query
        // to wait the full HTTP timeout for that peer, even when a healthy
        // carrier already answered.
        let mut any_unreachable = false;
        while let Some(r) = probes.next().await {
            match r {
                PeerProbe::Found(body) => return RemoteStatusOutcome::Found(body),
                PeerProbe::NotArchived => {}
                PeerProbe::Unreachable => any_unreachable = true,
            }
        }
        if any_unreachable {
            RemoteStatusOutcome::ApplianceDown
        } else {
            RemoteStatusOutcome::NotArchived
        }
    }
}

/// Stream `resp`'s body and bail when accumulated bytes exceed `cap`.
/// Unlike `resp.bytes()`, this never fully buffers a body that would
/// blow past the cap — the chunk loop drops accumulated bytes and
/// returns an error on the chunk that crosses the threshold.
async fn read_capped_body(resp: reqwest::Response, cap: usize) -> anyhow::Result<Vec<u8>> {
    use futures::StreamExt;
    let mut acc = Vec::with_capacity(8 * 1024);
    let mut stream = resp.bytes_stream();
    while let Some(chunk) = stream.next().await {
        let chunk = chunk?;
        if acc.len() + chunk.len() > cap {
            anyhow::bail!("peer response exceeded {cap} byte cap");
        }
        acc.extend_from_slice(&chunk);
    }
    Ok(acc)
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

    let id = cluster.identity();
    let mut appliances = vec![ApplianceInfoResponse {
        identity: id.name,
        mgmt_url: id.mgmt_url,
        retrieval_url: id.retrieval_url,
        engine_url: id.engine_url,
        etl_url: id.etl_url,
    }];

    for peer in cluster.peers() {
        appliances.push(ApplianceInfoResponse {
            identity: peer.name,
            mgmt_url: peer.mgmt_url,
            retrieval_url: peer.retrieval_url,
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

    let id = cluster.identity();
    let target_name = params.id.unwrap_or_else(|| id.name.clone());

    if target_name == id.name {
        let resp = ApplianceInfoResponse {
            identity: id.name,
            mgmt_url: id.mgmt_url,
            retrieval_url: id.retrieval_url,
            engine_url: id.engine_url,
            etl_url: id.etl_url,
        };
        return axum::Json(resp).into_response();
    }

    for peer in cluster.peers() {
        if peer.name == target_name {
            let resp = ApplianceInfoResponse {
                identity: peer.name,
                mgmt_url: peer.mgmt_url,
                retrieval_url: peer.retrieval_url,
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
                api_key: None,
            }],
            api_key: None,
            reassign_appliance_enabled: false,
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
                mgmt_url: "http://app1:17665/mgmt/bpl".to_string(),
                expires_at: Instant::now() + Duration::from_secs(60),
            },
        );

        // Should find it.
        let entry = client.pv_cache.get("TEST:PV").unwrap();
        assert_eq!(entry.retrieval_url, "http://app1:17665/retrieval");
        assert_eq!(entry.mgmt_url, "http://app1:17665/mgmt/bpl");
    }

    #[test]
    fn cache_expiry() {
        let client = ClusterClient::new(&test_config());

        // Insert an already-expired entry.
        client.pv_cache.insert(
            "TEST:EXPIRED".to_string(),
            CachedPeer {
                retrieval_url: "http://app1:17665/retrieval".to_string(),
                mgmt_url: "http://app1:17665/mgmt/bpl".to_string(),
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
