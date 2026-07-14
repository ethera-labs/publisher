use std::time::Duration;

use serde::Deserialize;

use crate::ProvingMode;

#[derive(Debug, Clone, Default, Deserialize)]
#[serde(default)]
pub struct Config {
    pub server: ServerConfig,
    pub api: ApiConfig,
    pub consensus: ConsensusConfig,
    pub metrics: MetricsConfig,
    pub log: LogConfig,
    pub settlement: SettlementConfig,
    pub proofs: ProofsConfig,
}

#[derive(Debug, Clone, Default, Deserialize)]
#[serde(default)]
pub struct SettlementConfig {
    /// L1 JSON-RPC endpoint.
    pub l1_rpc_url: String,
    /// `DisputeGameFactory` a `ComposeDisputeGame` is created on per superblock.
    pub dispute_game_factory: String,
    /// `ComposeAnchorStateRegistry` the next superblock number resumes from.
    pub anchor_state_registry: String,
    /// Hex-encoded private key of the approved proposer.
    pub proposer_key: String,
    /// Factory game index of the immutable recovery checkpoint.
    pub recovery_checkpoint_game_index: Option<u64>,
    /// Superblock number stored at the immutable recovery checkpoint.
    pub recovery_checkpoint_number: Option<u64>,
    /// Hash of the checkpoint `SuperblockBatch`.
    pub recovery_checkpoint_hash: String,
    pub mock: bool,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(default)]
pub struct ProofsConfig {
    pub proving_mode: ProvingMode,
    /// Chains that must reconnect before protocol recovery completes.
    pub required_chain_ids: Vec<u64>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(default)]
pub struct ServerConfig {
    pub listen_addr: String,
    pub max_message_size: usize,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(default)]
pub struct ApiConfig {
    pub listen_addr: String,
    #[serde(with = "humantime_serde")]
    pub request_timeout: Duration,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(default)]
pub struct ConsensusConfig {
    #[serde(with = "humantime_serde")]
    pub timeout: Duration,
    #[serde(with = "humantime_serde")]
    pub period_duration: Duration,
    /// Unix timestamp at which protocol period 1 begins.
    pub genesis_unix_seconds: Option<u64>,
    #[serde(with = "humantime_serde")]
    pub proof_window: Duration,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(default)]
pub struct MetricsConfig {
    pub enabled: bool,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(default)]
pub struct LogConfig {
    pub level: String,
    pub pretty: bool,
}

impl Default for ServerConfig {
    fn default() -> Self {
        Self {
            listen_addr: ":8080".into(),
            max_message_size: 4 * 1024 * 1024,
        }
    }
}

impl Default for ApiConfig {
    fn default() -> Self {
        Self {
            listen_addr: ":8081".into(),
            request_timeout: Duration::from_secs(15),
        }
    }
}

impl Default for ConsensusConfig {
    fn default() -> Self {
        Self {
            timeout: Duration::from_secs(60),
            period_duration: ethera_spec::PERIOD_DURATION,
            genesis_unix_seconds: None,
            proof_window: Duration::from_secs(7200),
        }
    }
}

impl Default for MetricsConfig {
    fn default() -> Self {
        Self { enabled: true }
    }
}

impl Default for LogConfig {
    fn default() -> Self {
        Self {
            level: "info".into(),
            pretty: false,
        }
    }
}

impl Default for ProofsConfig {
    fn default() -> Self {
        Self {
            proving_mode: ProvingMode::Real,
            required_chain_ids: Vec::new(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn defaults() {
        let cfg = Config::default();
        assert_eq!(cfg.server.listen_addr, ":8080");
        assert_eq!(cfg.server.max_message_size, 4 * 1024 * 1024);
        assert_eq!(cfg.api.listen_addr, ":8081");
        assert_eq!(cfg.api.request_timeout, Duration::from_secs(15));
        assert_eq!(cfg.consensus.timeout, Duration::from_secs(60));
        assert_eq!(cfg.consensus.period_duration, Duration::from_secs(3840));
        assert_eq!(cfg.consensus.genesis_unix_seconds, None);
        assert_eq!(cfg.consensus.proof_window, Duration::from_secs(7200));
        assert!(cfg.metrics.enabled);
        assert_eq!(cfg.log.level, "info");
        assert!(!cfg.log.pretty);
        assert_eq!(cfg.proofs.proving_mode, ProvingMode::Real);
        assert!(cfg.proofs.required_chain_ids.is_empty());
    }

    #[test]
    fn deserialize_yaml() {
        let yaml = r#"
server:
  listen_addr: ":9090"
consensus:
  timeout: 3s
  period_duration: 30s
  genesis_unix_seconds: 1700000000
  proof_window: 5m
log:
  level: debug
  pretty: true
proofs:
  proving_mode: mock
"#;
        let cfg: Config = serde_yaml::from_str(yaml).unwrap();
        assert_eq!(cfg.server.listen_addr, ":9090");
        assert_eq!(cfg.consensus.timeout, Duration::from_secs(3));
        assert_eq!(cfg.consensus.period_duration, Duration::from_secs(30));
        assert_eq!(cfg.consensus.genesis_unix_seconds, Some(1_700_000_000));
        assert_eq!(cfg.consensus.proof_window, Duration::from_secs(300));
        assert_eq!(cfg.log.level, "debug");
        assert!(cfg.log.pretty);
        assert_eq!(cfg.api.listen_addr, ":8081");
        assert_eq!(cfg.proofs.proving_mode, ProvingMode::Mock);
    }
}
