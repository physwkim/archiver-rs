use std::path::PathBuf;

use serde::{Deserialize, Serialize};

use crate::storage::partition::PartitionGranularity;

/// Top-level archiver configuration (TOML-based).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ArchiverConfig {
    #[serde(default = "default_listen_addr")]
    pub listen_addr: String,
    #[serde(default = "default_listen_port")]
    pub listen_port: u16,
    pub storage: StorageConfig,
    #[serde(default)]
    pub engine: EngineConfig,

    #[serde(default)]
    pub cluster: Option<ClusterConfig>,
    /// Optional API keys for management endpoint authentication.
    /// If set, mgmt write endpoints require `Authorization: Bearer <key>` or `X-API-Key: <key>`.
    /// Retrieval GET endpoints remain open.
    #[serde(default)]
    pub api_keys: Option<Vec<String>>,
    /// Security settings (CORS, rate limiting, body limits).
    #[serde(default)]
    pub security: SecurityConfig,
    /// Optional TLS configuration for HTTPS.
    #[serde(default)]
    pub tls: Option<TlsConfig>,
}

fn default_listen_addr() -> String {
    "0.0.0.0".to_string()
}

fn default_listen_port() -> u16 {
    17665
}

/// 3-tier storage configuration.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StorageConfig {
    pub sts: TierConfig,
    pub mts: TierConfig,
    pub lts: TierConfig,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TierConfig {
    pub root_folder: PathBuf,
    pub partition_granularity: PartitionGranularity,
    /// Number of partitions to hold before ETL moves data out.
    #[serde(default = "default_hold")]
    pub hold: u32,
    /// Number of partitions to gather (move out) at once.
    #[serde(default = "default_gather")]
    pub gather: u32,
}

fn default_hold() -> u32 {
    5
}

fn default_gather() -> u32 {
    3
}

/// EPICS CA engine configuration.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct EngineConfig {
    /// Write period in seconds — how often buffered samples flush to storage.
    #[serde(default = "default_write_period")]
    pub write_period_secs: u64,
    /// Path to PV policy TOML file.
    pub policy_file: Option<PathBuf>,
}

fn default_write_period() -> u64 {
    10
}

/// Security configuration.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SecurityConfig {
    /// CORS allowed origins. Empty = same-origin only (strict).
    #[serde(default)]
    pub cors_origins: Vec<String>,
    /// Rate limit: requests per second per IP (0 = disabled).
    #[serde(default = "default_rate_limit_rps")]
    pub rate_limit_rps: u32,
    /// Rate limit burst size.
    #[serde(default = "default_rate_limit_burst")]
    pub rate_limit_burst: u32,
    /// Maximum request body size in bytes (default 10MB).
    #[serde(default = "default_max_body_size")]
    pub max_body_size: usize,
}

impl Default for SecurityConfig {
    fn default() -> Self {
        Self {
            cors_origins: Vec::new(),
            rate_limit_rps: default_rate_limit_rps(),
            rate_limit_burst: default_rate_limit_burst(),
            max_body_size: default_max_body_size(),
        }
    }
}

fn default_rate_limit_rps() -> u32 {
    100
}

fn default_rate_limit_burst() -> u32 {
    200
}

fn default_max_body_size() -> usize {
    10 * 1024 * 1024 // 10MB
}

/// TLS configuration for HTTPS support.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TlsConfig {
    pub cert_path: PathBuf,
    pub key_path: PathBuf,
}

/// Identity of this appliance in a cluster.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ApplianceIdentity {
    pub name: String,
    pub mgmt_url: String,
    pub retrieval_url: String,
    pub engine_url: String,
    pub etl_url: String,
}

/// A remote peer appliance.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PeerConfig {
    pub name: String,
    pub mgmt_url: String,
    pub retrieval_url: String,
}

/// Cluster configuration for multi-appliance mode.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ClusterConfig {
    pub identity: ApplianceIdentity,
    #[serde(default = "default_cache_ttl")]
    pub cache_ttl_secs: u64,
    #[serde(default = "default_peer_timeout")]
    pub peer_timeout_secs: u64,
    #[serde(default)]
    pub peers: Vec<PeerConfig>,
}

fn default_cache_ttl() -> u64 {
    300
}

fn default_peer_timeout() -> u64 {
    30
}

impl ArchiverConfig {
    pub fn from_toml(s: &str) -> Result<Self, toml::de::Error> {
        toml::from_str(s)
    }

    /// Validate configuration values that TOML deserialization alone cannot check.
    pub fn validate(&self) -> anyhow::Result<()> {
        for (name, tier) in [("sts", &self.storage.sts), ("mts", &self.storage.mts), ("lts", &self.storage.lts)] {
            if tier.gather >= tier.hold {
                anyhow::bail!(
                    "{name}: gather ({}) must be less than hold ({})",
                    tier.gather,
                    tier.hold,
                );
            }
        }
        if let Some(ref cluster) = self.cluster {
            for (i, peer) in cluster.peers.iter().enumerate() {
                if !peer.mgmt_url.starts_with("http://") && !peer.mgmt_url.starts_with("https://") {
                    anyhow::bail!("cluster.peers[{i}].mgmt_url must start with http:// or https://");
                }
                if !peer.retrieval_url.starts_with("http://") && !peer.retrieval_url.starts_with("https://") {
                    anyhow::bail!("cluster.peers[{i}].retrieval_url must start with http:// or https://");
                }
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_config_without_cluster() {
        let toml = r#"
[storage.sts]
root_folder = "/tmp/sts"
partition_granularity = "hour"

[storage.mts]
root_folder = "/tmp/mts"
partition_granularity = "day"

[storage.lts]
root_folder = "/tmp/lts"
partition_granularity = "year"
"#;
        let config = ArchiverConfig::from_toml(toml).unwrap();
        assert!(config.cluster.is_none());
    }

    #[test]
    fn parse_config_with_cluster() {
        let toml = r#"
[storage.sts]
root_folder = "/tmp/sts"
partition_granularity = "hour"

[storage.mts]
root_folder = "/tmp/mts"
partition_granularity = "day"

[storage.lts]
root_folder = "/tmp/lts"
partition_granularity = "year"

[cluster.identity]
name = "appliance0"
mgmt_url = "http://host0:17665/mgmt/bpl"
retrieval_url = "http://host0:17665/retrieval"
engine_url = "http://host0:17665"
etl_url = "http://host0:17665"

[[cluster.peers]]
name = "appliance1"
mgmt_url = "http://host1:17665/mgmt/bpl"
retrieval_url = "http://host1:17665/retrieval"
"#;
        let config = ArchiverConfig::from_toml(toml).unwrap();
        let cluster = config.cluster.unwrap();
        assert_eq!(cluster.identity.name, "appliance0");
        assert_eq!(cluster.peers.len(), 1);
        assert_eq!(cluster.peers[0].name, "appliance1");
        assert_eq!(cluster.cache_ttl_secs, 300);
        assert_eq!(cluster.peer_timeout_secs, 30);
    }
}
