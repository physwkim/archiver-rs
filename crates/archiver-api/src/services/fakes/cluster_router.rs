use std::collections::HashMap;

use async_trait::async_trait;
use axum::body::Bytes;
use axum::response::Response;

use crate::services::traits::{ApplianceIdentityDto, ClusterRouter, PeerDto, ResolvedPeerDto};

pub struct FakeClusterRouter {
    identity: ApplianceIdentityDto,
    peers: Vec<PeerDto>,
    peer_pvs: HashMap<String, ResolvedPeerDto>,
}

impl FakeClusterRouter {
    pub fn new(identity: ApplianceIdentityDto, peers: Vec<PeerDto>) -> Self {
        Self {
            identity,
            peers,
            peer_pvs: HashMap::new(),
        }
    }

    pub fn with_pv_routing(mut self, pv: &str, resolved: ResolvedPeerDto) -> Self {
        self.peer_pvs.insert(pv.to_string(), resolved);
        self
    }
}

#[async_trait]
impl ClusterRouter for FakeClusterRouter {
    async fn resolve_peer(&self, pv: &str) -> Option<ResolvedPeerDto> {
        self.peer_pvs.get(pv).cloned()
    }

    fn identity_name(&self) -> &str {
        &self.identity.name
    }

    fn identity(&self) -> ApplianceIdentityDto {
        self.identity.clone()
    }

    fn peers(&self) -> Vec<PeerDto> {
        self.peers.clone()
    }

    fn find_peer_by_name(&self, name: &str) -> Option<PeerDto> {
        self.peers.iter().find(|p| p.name == name).cloned()
    }

    async fn proxy_mgmt_get(
        &self,
        _mgmt_url: &str,
        _endpoint: &str,
        _qs: &str,
    ) -> anyhow::Result<Response> {
        Ok(axum::Json(serde_json::json!({"status": "proxied"})).into_response())
    }

    async fn proxy_mgmt_post(
        &self,
        _mgmt_url: &str,
        _endpoint: &str,
        _body: Bytes,
    ) -> anyhow::Result<Response> {
        Ok(axum::Json(serde_json::json!({"status": "proxied"})).into_response())
    }

    async fn aggregate_all_pvs(&self) -> Vec<String> {
        Vec::new()
    }

    async fn aggregate_matching_pvs(&self, _pattern: &str) -> Vec<String> {
        Vec::new()
    }

    async fn aggregate_pv_count(&self) -> (u64, u64, u64) {
        (0, 0, 0)
    }

    async fn remote_pv_status(&self, _pv: &str) -> Option<serde_json::Value> {
        None
    }

    async fn proxy_retrieval(
        &self,
        _peer_retrieval_url: &str,
        _path: &str,
        _query_string: &str,
    ) -> anyhow::Result<Response> {
        Ok(axum::Json(serde_json::json!([])).into_response())
    }
}

use axum::response::IntoResponse;
