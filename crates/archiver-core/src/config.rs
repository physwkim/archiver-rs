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
    /// Optional list of external archivers used for failover-merged retrieval.
    /// When set, retrieval handlers fetch from each peer in addition to local
    /// data and merge by timestamp (with duplicate-timestamp drop).
    #[serde(default)]
    pub failover: Option<FailoverConfig>,
    /// PVA retrieval RPC server. When set, the archiver hosts
    /// `archappl/getData` and `archappl/getDataAtTime` PVA RPC PVs.
    #[serde(default)]
    pub pva: Option<PvaConfig>,
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
    /// Process-wide cap on open `BufWriter` file handles, shared
    /// across STS / MTS / LTS. When `Some`, every tier's
    /// PlainPbStoragePlugin draws from the SAME [`FdBudget`] —
    /// total open writers across all 3 tiers stays at-or-below
    /// this number, so the process cannot summed-overflow its
    /// `ulimit -n` even when each tier is busy. When `None`, each
    /// tier keeps its own per-tier cap (legacy behavior).
    #[serde(default)]
    pub max_open_writers_total: Option<usize>,
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
    /// Per-tier cap on open `BufWriter` file handles. Only
    /// consulted when [`StorageConfig::max_open_writers_total`] is
    /// `None`; otherwise all tiers share the global budget. `0`
    /// disables the cap (lifts to `usize::MAX`); `None` falls back
    /// to a tier-appropriate default (STS: 512, MTS/LTS: 64 — STS
    /// gets the bulk because it's the live ingest path).
    #[serde(default)]
    pub max_open_writers: Option<usize>,
}

fn default_hold() -> u32 {
    5
}

fn default_gather() -> u32 {
    3
}

/// EPICS CA engine configuration.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EngineConfig {
    /// Write period in seconds — how often buffered samples flush to storage.
    #[serde(default = "default_write_period")]
    pub write_period_secs: u64,
    /// Path to PV policy TOML file.
    pub policy_file: Option<PathBuf>,
    /// Maximum allowed drift between IOC-reported sample timestamps and
    /// the appliance's wall clock, in either direction (Java parity
    /// 6538631 — `org.epics.archiverappliance.engine.epics.SERVER_IOC_DRIFT_SECONDS`).
    /// Default 30 minutes; set higher for known-skewed sites without
    /// recompiling.
    #[serde(default = "default_server_ioc_drift_secs")]
    pub server_ioc_drift_secs: u64,
    /// Number of parallel write-loop shards. `1` (the default)
    /// keeps the legacy single-worker layout. Sites with many
    /// active PVs and a fast STS can raise this to e.g. 4–16; the
    /// engine spawns a dispatcher that hashes `pv_name` to a fixed
    /// shard so per-PV ordering is preserved while different PVs
    /// can append concurrently.
    #[serde(default = "default_write_shards")]
    pub write_shards: usize,
    /// Per-shard mpsc capacity. Only consulted when
    /// `write_shards > 1`. The dispatcher `try_send`s into each
    /// shard channel — when a shard is saturated its overflow is
    /// dropped and recorded on the per-PV `buffer_overflow_drops`
    /// counter, while OTHER shards keep flowing (per-shard
    /// isolation).
    #[serde(default = "default_per_shard_buffer")]
    pub per_shard_buffer: usize,
}

fn default_write_period() -> u64 {
    10
}

fn default_server_ioc_drift_secs() -> u64 {
    30 * 60
}

fn default_write_shards() -> usize {
    1
}

fn default_per_shard_buffer() -> usize {
    4096
}

impl Default for EngineConfig {
    // `#[serde(default)]` on the outer `engine` field falls back to
    // this when the TOML omits `[engine]` entirely. Without a manual
    // impl, the derived `Default` would zero every numeric field —
    // notably `write_period_secs = 0`, which then trips
    // `validate()` ("must be > 0"). Mirror the per-field
    // `#[serde(default = "...")]` defaults so the no-`[engine]`
    // and empty-`[engine]` cases behave identically.
    fn default() -> Self {
        Self {
            write_period_secs: default_write_period(),
            policy_file: None,
            server_ioc_drift_secs: default_server_ioc_drift_secs(),
            write_shards: default_write_shards(),
            per_shard_buffer: default_per_shard_buffer(),
        }
    }
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
    /// Trust X-Forwarded-For header for client IP detection (e.g., behind a reverse proxy).
    /// When false (default), only the direct connection IP is used for rate limiting.
    /// Enable only when the server is behind a trusted reverse proxy.
    #[serde(default)]
    pub trust_proxy_headers: bool,
}

impl Default for SecurityConfig {
    fn default() -> Self {
        Self {
            cors_origins: Vec::new(),
            rate_limit_rps: default_rate_limit_rps(),
            rate_limit_burst: default_rate_limit_burst(),
            max_body_size: default_max_body_size(),
            trust_proxy_headers: false,
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
    /// Per-peer outbound credential. When this appliance sends proxied requests
    /// to this peer, it uses this key instead of the cluster-level `api_key`.
    #[serde(default)]
    pub api_key: Option<String>,
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
    /// Shared secret for inter-peer authentication. Used as the outbound credential
    /// for any peer that does not have its own `api_key` in `[[cluster.peers]]`.
    /// Also serves as the inbound key this appliance accepts from peers.
    #[serde(default)]
    pub api_key: Option<String>,
    /// Java parity (59f0758): explicit opt-in for the destructive
    /// `reassignAppliance` live-migration endpoint. Default `false` so
    /// having cluster mode enabled doesn't on its own permit the
    /// migration — operators must validate destination data stores
    /// before flipping this on.
    #[serde(default)]
    pub reassign_appliance_enabled: bool,
}

fn default_cache_ttl() -> u64 {
    300
}

fn default_peer_timeout() -> u64 {
    30
}

/// Failover retrieval configuration.
///
/// `peers` is a list of external archiver URLs serving the same Java-style
/// retrieval endpoint (`/retrieval/data/getData.raw`). At query time, the
/// archiver fetches the same `pv` + time range from each peer and merges
/// the results with the local stream, dropping samples with duplicate
/// timestamps.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FailoverConfig {
    /// Per-peer retrieval base URLs (e.g. `https://archiver-b.example/retrieval`).
    /// `getData.raw` is appended automatically.
    pub peers: Vec<String>,
    /// HTTP timeout per peer fetch (seconds).
    #[serde(default = "default_failover_timeout")]
    pub timeout_secs: u64,
}

fn default_failover_timeout() -> u64 {
    30
}

/// PVA retrieval RPC server configuration.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PvaConfig {
    /// TCP port the PVA server listens on (default 5075).
    #[serde(default = "default_pva_tcp_port")]
    pub tcp_port: u16,
    /// UDP port for PVA search/beacon (default 5076).
    #[serde(default = "default_pva_udp_port")]
    pub udp_port: u16,
}

fn default_pva_tcp_port() -> u16 {
    5075
}

fn default_pva_udp_port() -> u16 {
    5076
}

impl Default for PvaConfig {
    fn default() -> Self {
        Self {
            tcp_port: default_pva_tcp_port(),
            udp_port: default_pva_udp_port(),
        }
    }
}

impl ArchiverConfig {
    pub fn from_toml(s: &str) -> Result<Self, toml::de::Error> {
        toml::from_str(s)
    }

    /// Validate configuration values that TOML deserialization alone cannot check.
    pub fn validate(&self) -> anyhow::Result<()> {
        if self.engine.write_period_secs == 0 {
            // The write_loop drives flushes via tokio::time::interval,
            // which panics on Duration::ZERO. The legacy elapsed-check
            // tolerated 0 but the ticker path doesn't, so reject it
            // at config load instead of letting the engine crash on
            // first start. 1s is the smallest sensible value (sub-
            // second flush rates serve no archiving purpose).
            anyhow::bail!("engine.write_period_secs must be > 0");
        }
        if self.engine.write_shards == 0 {
            anyhow::bail!(
                "engine.write_shards must be > 0 (use 1 for the legacy single-worker layout)"
            );
        }
        if self.engine.per_shard_buffer == 0 && self.engine.write_shards > 1 {
            anyhow::bail!(
                "engine.per_shard_buffer must be > 0 when write_shards > 1; \
                 a 0-capacity shard channel would drop every sample"
            );
        }
        for (name, tier) in [
            ("sts", &self.storage.sts),
            ("mts", &self.storage.mts),
            ("lts", &self.storage.lts),
        ] {
            if tier.gather >= tier.hold {
                anyhow::bail!(
                    "{name}: gather ({}) must be less than hold ({})",
                    tier.gather,
                    tier.hold,
                );
            }
        }
        if let Some(ref cluster) = self.cluster {
            if cluster.peer_timeout_secs == 0 {
                anyhow::bail!("cluster.peer_timeout_secs must be > 0");
            }
            if cluster.cache_ttl_secs == 0 {
                anyhow::bail!("cluster.cache_ttl_secs must be > 0");
            }
            // When external API keys are enabled, each peer must have an outbound
            // credential — either its own `api_key` or the cluster-level fallback.
            if self.api_keys.is_some() && !cluster.peers.is_empty() {
                let has_fallback = cluster.api_key.is_some();
                for (i, peer) in cluster.peers.iter().enumerate() {
                    if peer.api_key.is_none() && !has_fallback {
                        anyhow::bail!(
                            "cluster.peers[{i}] ({}) has no api_key and no cluster.api_key fallback; \
                             proxied write requests to this peer will be rejected",
                            peer.name
                        );
                    }
                }
            }
            for (i, peer) in cluster.peers.iter().enumerate() {
                if !peer.mgmt_url.starts_with("http://") && !peer.mgmt_url.starts_with("https://") {
                    anyhow::bail!(
                        "cluster.peers[{i}].mgmt_url must start with http:// or https://"
                    );
                }
                if !peer.retrieval_url.starts_with("http://")
                    && !peer.retrieval_url.starts_with("https://")
                {
                    anyhow::bail!(
                        "cluster.peers[{i}].retrieval_url must start with http:// or https://"
                    );
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

    #[test]
    fn validate_cluster_api_key_required_with_api_keys() {
        let toml = r#"
api_keys = ["secret"]

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
        let err = config.validate().unwrap_err();
        assert!(
            err.to_string()
                .contains("has no api_key and no cluster.api_key fallback")
        );
    }

    #[test]
    fn validate_cluster_api_key_not_required_without_api_keys() {
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
        config.validate().unwrap(); // No api_keys → no requirement for cluster.api_key
    }

    #[test]
    fn validate_per_peer_keys_without_fallback() {
        // Each peer has its own api_key → passes even without cluster.api_key.
        let toml = r#"
api_keys = ["secret"]

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
api_key = "peer1-key"
"#;
        let config = ArchiverConfig::from_toml(toml).unwrap();
        config.validate().unwrap();
    }

    #[test]
    fn validate_mixed_per_peer_and_fallback() {
        // One peer has its own key, another relies on the fallback → passes.
        let toml = r#"
api_keys = ["secret"]

[storage.sts]
root_folder = "/tmp/sts"
partition_granularity = "hour"

[storage.mts]
root_folder = "/tmp/mts"
partition_granularity = "day"

[storage.lts]
root_folder = "/tmp/lts"
partition_granularity = "year"

[cluster]
api_key = "shared-fallback"

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
api_key = "peer1-specific"

[[cluster.peers]]
name = "appliance2"
mgmt_url = "http://host2:17665/mgmt/bpl"
retrieval_url = "http://host2:17665/retrieval"
"#;
        let config = ArchiverConfig::from_toml(toml).unwrap();
        config.validate().unwrap();
    }

    #[test]
    fn parse_peer_api_key_from_toml() {
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
api_key = "peer1-secret"

[[cluster.peers]]
name = "appliance2"
mgmt_url = "http://host2:17665/mgmt/bpl"
retrieval_url = "http://host2:17665/retrieval"
"#;
        let config = ArchiverConfig::from_toml(toml).unwrap();
        let cluster = config.cluster.unwrap();
        assert_eq!(cluster.peers[0].api_key.as_deref(), Some("peer1-secret"));
        assert_eq!(cluster.peers[1].api_key, None);
    }
}
