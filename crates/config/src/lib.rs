//! Configuration loading for the shared publisher.
//!
//! Reads a YAML config file, then applies environment variable overrides.
//! Env vars follow the `SECTION_FIELD` convention (all uppercase, no prefix).

use std::path::PathBuf;
use std::time::Duration;

use anyhow::{Context, Result};
use clap::Parser;
use serde::{Deserialize, Deserializer};

#[derive(Debug, Parser)]
#[command(name = "publisher", about = "Ethera Shared Publisher")]
pub struct Cli {
    #[arg(long, short, default_value = "config.yaml")]
    pub config: PathBuf,
}

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
    pub mock: bool,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(default)]
pub struct ProofsConfig {
    pub proving_mode: ProvingMode,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum ProvingMode {
    Mock,
    #[default]
    Real,
}

impl ProvingMode {
    pub fn is_mock(self) -> bool {
        matches!(self, Self::Mock)
    }
}

impl<'de> Deserialize<'de> for ProvingMode {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let value = String::deserialize(deserializer)?;
        parse_proving_mode(&value).map_err(serde::de::Error::custom)
    }
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
        }
    }
}

impl Config {
    /// Loads config from a YAML file, applies env overrides, and validates.
    /// Falls back to defaults if the file does not exist.
    pub fn load(path: &std::path::Path) -> Result<Self> {
        let mut cfg = if path.exists() {
            let contents = std::fs::read_to_string(path)
                .with_context(|| format!("reading config file {}", path.display()))?;
            serde_yaml::from_str(&contents)
                .with_context(|| format!("parsing config file {}", path.display()))?
        } else {
            Self::default()
        };

        cfg.apply_env_overrides();
        cfg.normalize();
        cfg.validate()?;
        Ok(cfg)
    }

    fn normalize(&mut self) {
        normalize_addr(&mut self.server.listen_addr);
        normalize_addr(&mut self.api.listen_addr);
    }

    fn apply_env_overrides(&mut self) {
        env_str("SERVER_LISTEN_ADDR", &mut self.server.listen_addr);
        env_usize("SERVER_MAX_MESSAGE_SIZE", &mut self.server.max_message_size);

        env_str("API_LISTEN_ADDR", &mut self.api.listen_addr);
        env_duration("API_REQUEST_TIMEOUT", &mut self.api.request_timeout);

        env_duration("CONSENSUS_TIMEOUT", &mut self.consensus.timeout);
        env_duration(
            "CONSENSUS_PERIOD_DURATION",
            &mut self.consensus.period_duration,
        );
        env_duration("CONSENSUS_PROOF_WINDOW", &mut self.consensus.proof_window);

        env_bool("METRICS_ENABLED", &mut self.metrics.enabled);

        env_str("LOG_LEVEL", &mut self.log.level);
        env_bool("LOG_PRETTY", &mut self.log.pretty);

        env_str("SETTLEMENT_L1_RPC_URL", &mut self.settlement.l1_rpc_url);
        env_str(
            "SETTLEMENT_DISPUTE_GAME_FACTORY",
            &mut self.settlement.dispute_game_factory,
        );
        env_str(
            "SETTLEMENT_ANCHOR_STATE_REGISTRY",
            &mut self.settlement.anchor_state_registry,
        );
        env_str("SETTLEMENT_PROPOSER_KEY", &mut self.settlement.proposer_key);
        env_bool("SETTLEMENT_MOCK", &mut self.settlement.mock);

        let mut explicit_proving_mode =
            env_proving_mode("PROOFS_PROVING_MODE", &mut self.proofs.proving_mode);
        explicit_proving_mode |= env_proving_mode("PROVING_MODE", &mut self.proofs.proving_mode);
        explicit_proving_mode |= env_proving_mode("PROVE_MODE", &mut self.proofs.proving_mode);
        if let Some(bypass) = env_bool_value("PROOFS_BYPASS_PROVER") {
            self.proofs.proving_mode = if bypass {
                ProvingMode::Mock
            } else {
                ProvingMode::Real
            };
            explicit_proving_mode = true;
        }
        if self.settlement.mock && !explicit_proving_mode {
            self.proofs.proving_mode = ProvingMode::Mock;
        }
    }

    fn validate(&self) -> Result<()> {
        anyhow::ensure!(
            !self.server.listen_addr.is_empty(),
            "server.listen_addr must not be empty"
        );
        anyhow::ensure!(
            !self.consensus.timeout.is_zero(),
            "consensus.timeout must be positive"
        );
        anyhow::ensure!(
            !self.consensus.period_duration.is_zero(),
            "consensus.period_duration must be positive"
        );
        anyhow::ensure!(
            !self.consensus.proof_window.is_zero(),
            "consensus.proof_window must be positive"
        );
        Ok(())
    }
}

fn normalize_addr(addr: &mut String) {
    if addr.starts_with(':') {
        *addr = format!("0.0.0.0{addr}");
    }
}

fn env_str(key: &str, target: &mut String) {
    if let Ok(val) = std::env::var(key) {
        *target = val;
    }
}

fn env_bool(key: &str, target: &mut bool) {
    if let Some(val) = env_bool_value(key) {
        *target = val;
    }
}

fn env_bool_value(key: &str) -> Option<bool> {
    let val = std::env::var(key).ok()?;
    match val.to_lowercase().as_str() {
        "true" | "1" | "yes" => Some(true),
        "false" | "0" | "no" => Some(false),
        _ => None,
    }
}

fn env_proving_mode(key: &str, target: &mut ProvingMode) -> bool {
    let Ok(val) = std::env::var(key) else {
        return false;
    };
    if let Ok(mode) = parse_proving_mode(&val) {
        *target = mode;
        true
    } else {
        false
    }
}

fn parse_proving_mode(value: &str) -> Result<ProvingMode, String> {
    match value.trim().to_ascii_lowercase().as_str() {
        "mock" | "bypass" | "bypass_prover" => Ok(ProvingMode::Mock),
        "real" | "prover" => Ok(ProvingMode::Real),
        other => Err(format!(
            "invalid proving mode '{other}', expected 'mock' or 'real'"
        )),
    }
}

fn env_duration(key: &str, target: &mut Duration) {
    if let Ok(val) = std::env::var(key) {
        if let Ok(d) = humantime::parse_duration(&val) {
            *target = d;
        }
    }
}

fn env_usize(key: &str, target: &mut usize) {
    if let Ok(val) = std::env::var(key) {
        if let Ok(n) = val.parse() {
            *target = n;
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
        assert_eq!(cfg.consensus.proof_window, Duration::from_secs(7200));
        assert!(cfg.metrics.enabled);
        assert_eq!(cfg.log.level, "info");
        assert!(!cfg.log.pretty);
        assert_eq!(cfg.proofs.proving_mode, ProvingMode::Real);
    }

    #[test]
    fn deserialize_yaml() {
        let yaml = r#"
server:
  listen_addr: ":9090"
consensus:
  timeout: 3s
  period_duration: 30s
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
        assert_eq!(cfg.consensus.proof_window, Duration::from_secs(300));
        assert_eq!(cfg.log.level, "debug");
        assert!(cfg.log.pretty);
        assert_eq!(cfg.api.listen_addr, ":8081");
        assert_eq!(cfg.proofs.proving_mode, ProvingMode::Mock);
    }

    #[test]
    fn proving_mode_parses_supported_values() {
        assert_eq!(parse_proving_mode("mock").unwrap(), ProvingMode::Mock);
        assert_eq!(parse_proving_mode("bypass").unwrap(), ProvingMode::Mock);
        assert_eq!(
            parse_proving_mode("bypass_prover").unwrap(),
            ProvingMode::Mock
        );
        assert_eq!(parse_proving_mode("real").unwrap(), ProvingMode::Real);
        assert_eq!(parse_proving_mode("prover").unwrap(), ProvingMode::Real);
        assert!(parse_proving_mode("invalid").is_err());
    }

    #[test]
    fn env_override_string() {
        let mut val = "old".to_string();
        std::env::set_var("TEST_CFG_STR", "new");
        env_str("TEST_CFG_STR", &mut val);
        assert_eq!(val, "new");
        std::env::remove_var("TEST_CFG_STR");
    }

    #[test]
    fn env_override_duration() {
        let mut d = Duration::from_secs(10);
        std::env::set_var("TEST_CFG_DUR", "5s");
        env_duration("TEST_CFG_DUR", &mut d);
        assert_eq!(d, Duration::from_secs(5));
        std::env::remove_var("TEST_CFG_DUR");
    }

    #[test]
    fn env_override_bool() {
        let mut b = false;
        std::env::set_var("TEST_CFG_BOOL", "true");
        env_bool("TEST_CFG_BOOL", &mut b);
        assert!(b);
        std::env::remove_var("TEST_CFG_BOOL");
    }

    #[test]
    fn cli_default_config_path() {
        let cli = Cli::parse_from(["publisher"]);
        assert_eq!(cli.config, PathBuf::from("config.yaml"));
    }

    #[test]
    fn cli_custom_config_path() {
        let cli = Cli::parse_from(["publisher", "--config", "/etc/publisher/config.yaml"]);
        assert_eq!(cli.config, PathBuf::from("/etc/publisher/config.yaml"));
    }
}
