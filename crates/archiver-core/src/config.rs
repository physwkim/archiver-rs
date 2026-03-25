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
    pub bluesky: Option<BlueskyConfig>,
    #[serde(default)]
    pub cluster: Option<ClusterConfig>,
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

/// Bluesky Kafka integration configuration.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BlueskyConfig {
    pub bootstrap_servers: String,
    pub topic: String,
    pub group_id: String,
    pub beamline: String,
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
