//! Configuration loading for the shared publisher.
//!
//! Reads a YAML config file, then applies environment variable overrides.
//! Env vars follow the `SECTION_FIELD` convention (all uppercase, no prefix).

use std::path::PathBuf;
use std::time::Duration;

use anyhow::{Context, Result};
use clap::Parser;
use serde::Deserialize;

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
}

#[derive(Debug, Clone, Default, Deserialize)]
#[serde(default)]
pub struct SettlementConfig {
    /// L1 JSON-RPC endpoint (e.g. `http://l1-rpc:8545`).
    pub l1_rpc_url: String,
    /// Deployed `ComposeL2OutputOracle` contract address.
    pub l2oo_address: String,
    /// Hex-encoded private key of the approved proposer account.
    pub proposer_key: String,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(default)]
pub struct ServerConfig {
    pub listen_addr: String,
    #[serde(with = "humantime_serde")]
    pub write_timeout: Duration,
    pub max_message_size: usize,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(default)]
pub struct ApiConfig {
    pub listen_addr: String,
    #[serde(with = "humantime_serde")]
    pub read_header_timeout: Duration,
    #[serde(with = "humantime_serde")]
    pub read_timeout: Duration,
    #[serde(with = "humantime_serde")]
    pub write_timeout: Duration,
    #[serde(with = "humantime_serde")]
    pub idle_timeout: Duration,
    pub max_header_bytes: usize,
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
    pub role: String,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(default)]
pub struct MetricsConfig {
    pub enabled: bool,
    pub port: u16,
    pub path: String,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(default)]
pub struct LogConfig {
    pub level: String,
    pub pretty: bool,
    pub output: String,
    pub file: String,
}

impl Default for ServerConfig {
    fn default() -> Self {
        Self {
            listen_addr: ":8080".into(),
            write_timeout: Duration::from_secs(30),
            max_message_size: 4 * 1024 * 1024,
        }
    }
}

impl Default for ApiConfig {
    fn default() -> Self {
        Self {
            listen_addr: ":8081".into(),
            read_header_timeout: Duration::from_secs(5),
            read_timeout: Duration::from_secs(15),
            write_timeout: Duration::from_secs(30),
            idle_timeout: Duration::from_secs(120),
            max_header_bytes: 1 << 20,
        }
    }
}

impl Default for ConsensusConfig {
    fn default() -> Self {
        Self {
            timeout: Duration::from_secs(60),
            period_duration: Duration::from_secs(3840),
            proof_window: Duration::from_secs(7200),
            role: "leader".into(),
        }
    }
}

impl Default for MetricsConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            port: 8081,
            path: "/metrics".into(),
        }
    }
}

impl Default for LogConfig {
    fn default() -> Self {
        Self {
            level: "info".into(),
            pretty: false,
            output: "stdout".into(),
            file: String::new(),
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
        env_duration("SERVER_WRITE_TIMEOUT", &mut self.server.write_timeout);
        env_usize("SERVER_MAX_MESSAGE_SIZE", &mut self.server.max_message_size);

        env_str("API_LISTEN_ADDR", &mut self.api.listen_addr);
        env_duration("API_READ_HEADER_TIMEOUT", &mut self.api.read_header_timeout);
        env_duration("API_READ_TIMEOUT", &mut self.api.read_timeout);
        env_duration("API_WRITE_TIMEOUT", &mut self.api.write_timeout);
        env_duration("API_IDLE_TIMEOUT", &mut self.api.idle_timeout);
        env_usize("API_MAX_HEADER_BYTES", &mut self.api.max_header_bytes);

        env_duration("CONSENSUS_TIMEOUT", &mut self.consensus.timeout);
        env_duration(
            "CONSENSUS_PERIOD_DURATION",
            &mut self.consensus.period_duration,
        );
        env_duration("CONSENSUS_PROOF_WINDOW", &mut self.consensus.proof_window);
        env_str("CONSENSUS_ROLE", &mut self.consensus.role);

        env_bool("METRICS_ENABLED", &mut self.metrics.enabled);
        env_u16("METRICS_PORT", &mut self.metrics.port);
        env_str("METRICS_PATH", &mut self.metrics.path);

        env_str("LOG_LEVEL", &mut self.log.level);
        env_bool("LOG_PRETTY", &mut self.log.pretty);
        env_str("LOG_OUTPUT", &mut self.log.output);
        env_str("LOG_FILE", &mut self.log.file);

        env_str("SETTLEMENT_L1_RPC_URL", &mut self.settlement.l1_rpc_url);
        env_str("SETTLEMENT_L2OO_ADDRESS", &mut self.settlement.l2oo_address);
        env_str("SETTLEMENT_PROPOSER_KEY", &mut self.settlement.proposer_key);
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
        if self.metrics.enabled {
            anyhow::ensure!(
                self.metrics.port > 0,
                "metrics.port must be > 0 when metrics enabled"
            );
        }
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
    if let Ok(val) = std::env::var(key) {
        match val.to_lowercase().as_str() {
            "true" | "1" | "yes" => *target = true,
            "false" | "0" | "no" => *target = false,
            _ => {}
        }
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

fn env_u16(key: &str, target: &mut u16) {
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
        assert_eq!(cfg.server.write_timeout, Duration::from_secs(30));
        assert_eq!(cfg.api.listen_addr, ":8081");
        assert_eq!(cfg.api.read_header_timeout, Duration::from_secs(5));
        assert_eq!(cfg.api.read_timeout, Duration::from_secs(15));
        assert_eq!(cfg.api.write_timeout, Duration::from_secs(30));
        assert_eq!(cfg.api.idle_timeout, Duration::from_secs(120));
        assert_eq!(cfg.api.max_header_bytes, 1 << 20);
        assert_eq!(cfg.consensus.timeout, Duration::from_secs(60));
        assert_eq!(cfg.consensus.period_duration, Duration::from_secs(3840));
        assert_eq!(cfg.consensus.proof_window, Duration::from_secs(7200));
        assert_eq!(cfg.consensus.role, "leader");
        assert!(cfg.metrics.enabled);
        assert_eq!(cfg.metrics.port, 8081);
        assert_eq!(cfg.metrics.path, "/metrics");
        assert_eq!(cfg.log.level, "info");
        assert!(!cfg.log.pretty);
        assert_eq!(cfg.log.output, "stdout");
    }

    #[test]
    fn deserialize_yaml() {
        let yaml = r#"
server:
  listen_addr: ":9090"
  write_timeout: 10s
consensus:
  timeout: 3s
  role: leader
log:
  level: debug
  pretty: true
"#;
        let cfg: Config = serde_yaml::from_str(yaml).unwrap();
        assert_eq!(cfg.server.listen_addr, ":9090");
        assert_eq!(cfg.server.write_timeout, Duration::from_secs(10));
        assert_eq!(cfg.consensus.timeout, Duration::from_secs(3));
        assert_eq!(cfg.log.level, "debug");
        assert!(cfg.log.pretty);
        assert_eq!(cfg.api.listen_addr, ":8081");
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
