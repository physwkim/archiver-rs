use std::sync::Arc;

use async_trait::async_trait;

use crate::cluster::ClusterClient;
use crate::services::traits::{ApplianceIdentityDto, ClusterRouter, PeerDto, ResolvedPeerDto};

pub struct ClusterClientRouter {
    inner: Arc<ClusterClient>,
}

impl ClusterClientRouter {
    pub fn new(client: Arc<ClusterClient>) -> Self {
        Self { inner: client }
    }
}

#[async_trait]
impl ClusterRouter for ClusterClientRouter {
    async fn resolve_peer(&self, pv: &str) -> Option<ResolvedPeerDto> {
        self.inner.resolve_peer(pv).await.map(|r| ResolvedPeerDto {
            mgmt_url: r.mgmt_url,
            retrieval_url: r.retrieval_url,
        })
    }

    fn identity_name(&self) -> &str {
        &self.inner.identity().name
    }

    fn identity(&self) -> ApplianceIdentityDto {
        let id = self.inner.identity();
        ApplianceIdentityDto {
            name: id.name.clone(),
            mgmt_url: id.mgmt_url.clone(),
            retrieval_url: id.retrieval_url.clone(),
            engine_url: id.engine_url.clone(),
            etl_url: id.etl_url.clone(),
        }
    }

    fn peers(&self) -> Vec<PeerDto> {
        self.inner.peers().iter().map(|p| PeerDto {
            name: p.name.clone(),
            mgmt_url: p.mgmt_url.clone(),
            retrieval_url: p.retrieval_url.clone(),
        }).collect()
    }

    fn find_peer_by_name(&self, name: &str) -> Option<PeerDto> {
        self.inner.find_peer_by_name(name).map(|p| PeerDto {
            name: p.name.clone(),
            mgmt_url: p.mgmt_url.clone(),
            retrieval_url: p.retrieval_url.clone(),
        })
    }

    async fn proxy_mgmt_get(&self, mgmt_url: &str, endpoint: &str, qs: &str) -> anyhow::Result<axum::response::Response> {
        self.inner.proxy_mgmt_get(mgmt_url, endpoint, qs).await
    }

    async fn proxy_mgmt_post(&self, mgmt_url: &str, endpoint: &str, body: axum::body::Bytes) -> anyhow::Result<axum::response::Response> {
        self.inner.proxy_mgmt_post(mgmt_url, endpoint, body).await
    }

    async fn aggregate_all_pvs(&self) -> Vec<String> {
        self.inner.aggregate_all_pvs().await
    }

    async fn aggregate_matching_pvs(&self, pattern: &str) -> Vec<String> {
        self.inner.aggregate_matching_pvs(pattern).await
    }

    async fn aggregate_pv_count(&self) -> (u64, u64, u64) {
        self.inner.aggregate_pv_count().await
    }

    async fn remote_pv_status(&self, pv: &str) -> Option<serde_json::Value> {
        self.inner.remote_pv_status(pv).await
    }

    async fn proxy_retrieval(&self, peer_retrieval_url: &str, path: &str, query_string: &str) -> anyhow::Result<axum::response::Response> {
        self.inner.proxy_retrieval(peer_retrieval_url, path, query_string).await
    }
}
