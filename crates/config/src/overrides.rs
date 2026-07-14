use anyhow::Result;
use clap::{Args, Parser};

use crate::{
    ApiConfig, Config, ConsensusConfig, LogConfig, MetricsConfig, ProofsConfig, ProvingMode,
    ServerConfig, SettlementConfig,
};

#[derive(Debug, Clone, Default, Args)]
pub(crate) struct ConfigOverrides {
    #[command(flatten)]
    server: ServerOverrides,
    #[command(flatten)]
    api: ApiOverrides,
    #[command(flatten)]
    consensus: ConsensusOverrides,
    #[command(flatten)]
    metrics: MetricsOverrides,
    #[command(flatten)]
    log: LogOverrides,
    #[command(flatten)]
    settlement: SettlementOverrides,
    #[command(flatten)]
    proofs: ProofsOverrides,
}

#[derive(Debug, Clone, Default, Args)]
struct ServerOverrides {
    #[arg(
        id = "server_listen_addr",
        long = "server.listen-addr",
        env = "SERVER_LISTEN_ADDR"
    )]
    listen_addr: Option<String>,
    #[arg(
        id = "server_max_message_size",
        long = "server.max-message-size",
        env = "SERVER_MAX_MESSAGE_SIZE"
    )]
    max_message_size: Option<usize>,
}

#[derive(Debug, Clone, Default, Args)]
struct ApiOverrides {
    #[arg(
        id = "api_listen_addr",
        long = "api.listen-addr",
        env = "API_LISTEN_ADDR"
    )]
    listen_addr: Option<String>,
    #[arg(
        id = "api_request_timeout",
        long = "api.request-timeout",
        env = "API_REQUEST_TIMEOUT"
    )]
    request_timeout: Option<humantime::Duration>,
}

#[derive(Debug, Clone, Default, Args)]
struct ConsensusOverrides {
    #[arg(
        id = "consensus_timeout",
        long = "consensus.timeout",
        env = "CONSENSUS_TIMEOUT"
    )]
    timeout: Option<humantime::Duration>,
    #[arg(
        id = "consensus_period_duration",
        long = "consensus.period-duration",
        env = "CONSENSUS_PERIOD_DURATION"
    )]
    period_duration: Option<humantime::Duration>,
    #[arg(
        id = "consensus_genesis_unix_seconds",
        long = "consensus.genesis-unix-seconds",
        env = "CONSENSUS_GENESIS_UNIX_SECONDS"
    )]
    genesis_unix_seconds: Option<u64>,
    #[arg(
        id = "consensus_proof_window",
        long = "consensus.proof-window",
        env = "CONSENSUS_PROOF_WINDOW"
    )]
    proof_window: Option<humantime::Duration>,
}

#[derive(Debug, Clone, Default, Args)]
struct MetricsOverrides {
    #[arg(
        id = "metrics_enabled",
        long = "metrics.enabled",
        env = "METRICS_ENABLED",
        num_args = 0..=1,
        default_missing_value = "true",
        value_parser = clap::builder::BoolishValueParser::new(),
    )]
    enabled: Option<bool>,
}

#[derive(Debug, Clone, Default, Args)]
struct LogOverrides {
    #[arg(id = "log_level", long = "log.level", env = "LOG_LEVEL")]
    level: Option<String>,
    #[arg(
        id = "log_pretty",
        long = "log.pretty",
        env = "LOG_PRETTY",
        num_args = 0..=1,
        default_missing_value = "true",
        value_parser = clap::builder::BoolishValueParser::new(),
    )]
    pretty: Option<bool>,
}

#[derive(Debug, Clone, Default, Args)]
struct SettlementOverrides {
    #[arg(
        id = "settlement_l1_rpc_url",
        long = "settlement.l1-rpc-url",
        env = "SETTLEMENT_L1_RPC_URL"
    )]
    l1_rpc_url: Option<String>,
    #[arg(
        id = "settlement_dispute_game_factory",
        long = "settlement.dispute-game-factory",
        env = "SETTLEMENT_DISPUTE_GAME_FACTORY"
    )]
    dispute_game_factory: Option<String>,
    #[arg(
        id = "settlement_anchor_state_registry",
        long = "settlement.anchor-state-registry",
        env = "SETTLEMENT_ANCHOR_STATE_REGISTRY"
    )]
    anchor_state_registry: Option<String>,
    #[arg(
        id = "settlement_proposer_key",
        long = "settlement.proposer-key",
        env = "SETTLEMENT_PROPOSER_KEY"
    )]
    proposer_key: Option<String>,
    #[arg(
        id = "settlement_recovery_checkpoint_game_index",
        long = "settlement.recovery-checkpoint-game-index",
        env = "SETTLEMENT_RECOVERY_CHECKPOINT_GAME_INDEX"
    )]
    recovery_checkpoint_game_index: Option<u64>,
    #[arg(
        id = "settlement_recovery_checkpoint_number",
        long = "settlement.recovery-checkpoint-number",
        env = "SETTLEMENT_RECOVERY_CHECKPOINT_NUMBER"
    )]
    recovery_checkpoint_number: Option<u64>,
    #[arg(
        id = "settlement_recovery_checkpoint_hash",
        long = "settlement.recovery-checkpoint-hash",
        env = "SETTLEMENT_RECOVERY_CHECKPOINT_HASH"
    )]
    recovery_checkpoint_hash: Option<String>,
    #[arg(
        id = "settlement_mock",
        long = "settlement.mock",
        env = "SETTLEMENT_MOCK",
        num_args = 0..=1,
        default_missing_value = "true",
        value_parser = clap::builder::BoolishValueParser::new(),
    )]
    mock: Option<bool>,
}

#[derive(Debug, Clone, Default, Args)]
struct ProofsOverrides {
    #[arg(
        id = "proofs_proving_mode",
        long = "proofs.proving-mode",
        env = "PROOFS_PROVING_MODE"
    )]
    proving_mode: Option<ProvingMode>,
    #[arg(
        id = "proofs_required_chain_ids",
        long = "proofs.required-chain-ids",
        env = "PROOFS_COLLECTOR_REQUIRED_CHAIN_IDS",
        value_delimiter = ','
    )]
    required_chain_ids: Option<Vec<u64>>,
    #[arg(
        id = "proving_mode_alias",
        long = "proving-mode",
        env = "PROVING_MODE",
        hide = true
    )]
    proving_mode_alias: Option<ProvingMode>,
    #[arg(
        id = "prove_mode_alias",
        long = "prove-mode",
        env = "PROVE_MODE",
        hide = true
    )]
    prove_mode_alias: Option<ProvingMode>,
    #[arg(
        id = "proofs_bypass_prover",
        long = "proofs.bypass-prover",
        env = "PROOFS_BYPASS_PROVER",
        hide = true,
        num_args = 0..=1,
        default_missing_value = "true",
        value_parser = clap::builder::BoolishValueParser::new(),
    )]
    bypass_prover: Option<bool>,
}

#[derive(Debug, Parser)]
#[command(name = "publisher-env")]
struct EnvCli {
    #[command(flatten)]
    overrides: ConfigOverrides,
}

impl ConfigOverrides {
    pub(crate) fn from_env() -> Result<Self> {
        Ok(EnvCli::try_parse_from(["publisher-env"])
            .map_err(|e| anyhow::anyhow!(e.to_string()))?
            .overrides)
    }

    pub(crate) fn apply(self, cfg: &mut Config) {
        self.server.apply(&mut cfg.server);
        self.api.apply(&mut cfg.api);
        self.consensus.apply(&mut cfg.consensus);
        self.metrics.apply(&mut cfg.metrics);
        self.log.apply(&mut cfg.log);
        self.settlement.apply(&mut cfg.settlement);

        let explicit_proving_mode = self.proofs.apply(&mut cfg.proofs);
        if cfg.settlement.mock && !explicit_proving_mode {
            cfg.proofs.proving_mode = ProvingMode::Mock;
        }
    }
}

impl ServerOverrides {
    fn apply(self, cfg: &mut ServerConfig) {
        if let Some(listen_addr) = self.listen_addr {
            cfg.listen_addr = listen_addr;
        }
        if let Some(max_message_size) = self.max_message_size {
            cfg.max_message_size = max_message_size;
        }
    }
}

impl ApiOverrides {
    fn apply(self, cfg: &mut ApiConfig) {
        if let Some(listen_addr) = self.listen_addr {
            cfg.listen_addr = listen_addr;
        }
        if let Some(request_timeout) = self.request_timeout {
            cfg.request_timeout = request_timeout.into();
        }
    }
}

impl ConsensusOverrides {
    fn apply(self, cfg: &mut ConsensusConfig) {
        if let Some(timeout) = self.timeout {
            cfg.timeout = timeout.into();
        }
        if let Some(period_duration) = self.period_duration {
            cfg.period_duration = period_duration.into();
        }
        if let Some(genesis_unix_seconds) = self.genesis_unix_seconds {
            cfg.genesis_unix_seconds = Some(genesis_unix_seconds);
        }
        if let Some(proof_window) = self.proof_window {
            cfg.proof_window = proof_window.into();
        }
    }
}

impl MetricsOverrides {
    fn apply(self, cfg: &mut MetricsConfig) {
        if let Some(enabled) = self.enabled {
            cfg.enabled = enabled;
        }
    }
}

impl LogOverrides {
    fn apply(self, cfg: &mut LogConfig) {
        if let Some(level) = self.level {
            cfg.level = level;
        }
        if let Some(pretty) = self.pretty {
            cfg.pretty = pretty;
        }
    }
}

impl SettlementOverrides {
    fn apply(self, cfg: &mut SettlementConfig) {
        if let Some(l1_rpc_url) = self.l1_rpc_url {
            cfg.l1_rpc_url = l1_rpc_url;
        }
        if let Some(dispute_game_factory) = self.dispute_game_factory {
            cfg.dispute_game_factory = dispute_game_factory;
        }
        if let Some(anchor_state_registry) = self.anchor_state_registry {
            cfg.anchor_state_registry = anchor_state_registry;
        }
        if let Some(proposer_key) = self.proposer_key {
            cfg.proposer_key = proposer_key;
        }
        if let Some(game_index) = self.recovery_checkpoint_game_index {
            cfg.recovery_checkpoint_game_index = Some(game_index);
        }
        if let Some(number) = self.recovery_checkpoint_number {
            cfg.recovery_checkpoint_number = Some(number);
        }
        if let Some(hash) = self.recovery_checkpoint_hash {
            cfg.recovery_checkpoint_hash = hash;
        }
        if let Some(mock) = self.mock {
            cfg.mock = mock;
        }
    }
}

impl ProofsOverrides {
    fn apply(self, cfg: &mut ProofsConfig) -> bool {
        let mut explicit = false;
        if let Some(required_chain_ids) = self.required_chain_ids {
            cfg.required_chain_ids = required_chain_ids;
        }
        if let Some(proving_mode) = self.proving_mode {
            cfg.proving_mode = proving_mode;
            explicit = true;
        }
        if let Some(proving_mode) = self.proving_mode_alias {
            cfg.proving_mode = proving_mode;
            explicit = true;
        }
        if let Some(proving_mode) = self.prove_mode_alias {
            cfg.proving_mode = proving_mode;
            explicit = true;
        }
        if let Some(bypass_prover) = self.bypass_prover {
            cfg.proving_mode = if bypass_prover {
                ProvingMode::Mock
            } else {
                ProvingMode::Real
            };
            explicit = true;
        }
        explicit
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use clap::Parser;

    use super::*;

    #[test]
    fn cli_overrides_apply_to_config() {
        let cli = EnvCli::try_parse_from([
            "publisher-env",
            "--server.listen-addr",
            ":9090",
            "--server.max-message-size",
            "1024",
            "--api.request-timeout",
            "5s",
            "--consensus.genesis-unix-seconds",
            "1700000000",
            "--metrics.enabled",
            "false",
            "--settlement.dispute-game-factory",
            "0x1234",
            "--settlement.recovery-checkpoint-game-index",
            "12",
            "--settlement.recovery-checkpoint-number",
            "34",
            "--settlement.recovery-checkpoint-hash",
            "0xabcd",
            "--proofs.required-chain-ids",
            "100,200",
            "--proofs.proving-mode",
            "mock",
        ])
        .unwrap();

        let mut cfg = Config::default();
        cli.overrides.apply(&mut cfg);

        assert_eq!(cfg.server.listen_addr, ":9090");
        assert_eq!(cfg.server.max_message_size, 1024);
        assert_eq!(cfg.api.request_timeout, Duration::from_secs(5));
        assert_eq!(cfg.consensus.genesis_unix_seconds, Some(1_700_000_000));
        assert!(!cfg.metrics.enabled);
        assert_eq!(cfg.settlement.dispute_game_factory, "0x1234");
        assert_eq!(cfg.settlement.recovery_checkpoint_game_index, Some(12));
        assert_eq!(cfg.settlement.recovery_checkpoint_number, Some(34));
        assert_eq!(cfg.settlement.recovery_checkpoint_hash, "0xabcd");
        assert_eq!(cfg.proofs.required_chain_ids, vec![100, 200]);
        assert_eq!(cfg.proofs.proving_mode, ProvingMode::Mock);
    }

    #[test]
    fn settlement_mock_selects_mock_proving_mode() {
        let cli = EnvCli::try_parse_from(["publisher-env", "--settlement.mock", "true"]).unwrap();
        let mut cfg = Config::default();

        cli.overrides.apply(&mut cfg);

        assert!(cfg.settlement.mock);
        assert_eq!(cfg.proofs.proving_mode, ProvingMode::Mock);
    }

    #[test]
    fn explicit_proving_mode_overrides_settlement_mock() {
        let cli = EnvCli::try_parse_from([
            "publisher-env",
            "--settlement.mock",
            "true",
            "--proofs.bypass-prover",
            "false",
        ])
        .unwrap();
        let mut cfg = Config::default();

        cli.overrides.apply(&mut cfg);

        assert!(cfg.settlement.mock);
        assert_eq!(cfg.proofs.proving_mode, ProvingMode::Real);
    }
}
