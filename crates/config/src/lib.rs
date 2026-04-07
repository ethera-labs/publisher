//! CLI configuration for the shared publisher.

use clap::Parser;

/// Ethera Shared Publisher — cross-chain transaction coordinator.
#[derive(Debug, Clone, Parser)]
#[command(name = "publisher")]
pub struct PublisherArgs {
    #[command(flatten)]
    pub quic: QuicArgs,

    #[command(flatten)]
    pub api: ApiArgs,

    #[command(flatten)]
    pub consensus: ConsensusArgs,

    #[command(flatten)]
    pub log: LogArgs,
}

#[derive(Debug, Clone, clap::Args)]
pub struct QuicArgs {
    #[arg(
        id = "quic_listen_addr",
        long = "quic.listen-addr",
        env = "PUBLISHER_QUIC_LISTEN_ADDR",
        default_value = "0.0.0.0:8080"
    )]
    pub listen_addr: String,

    #[arg(
        long = "quic.max-message-size",
        env = "PUBLISHER_QUIC_MAX_MESSAGE_SIZE",
        default_value = "4194304"
    )]
    pub max_message_size: usize,
}

#[derive(Debug, Clone, clap::Args)]
pub struct ApiArgs {
    #[arg(
        id = "api_listen_addr",
        long = "api.listen-addr",
        env = "PUBLISHER_API_LISTEN_ADDR",
        default_value = "0.0.0.0:8081"
    )]
    pub listen_addr: String,
}

#[derive(Debug, Clone, clap::Args)]
pub struct ConsensusArgs {
    #[arg(
        long = "consensus.timeout-secs",
        env = "PUBLISHER_CONSENSUS_TIMEOUT_SECS",
        default_value = "60"
    )]
    pub timeout_secs: u64,
}

#[derive(Debug, Clone, clap::Args)]
pub struct LogArgs {
    #[arg(
        long = "log.level",
        env = "PUBLISHER_LOG_LEVEL",
        default_value = "info"
    )]
    pub level: String,

    #[arg(
        long = "log.format",
        env = "PUBLISHER_LOG_FORMAT",
        default_value = "json"
    )]
    pub format: String,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_args_are_valid() {
        let args = PublisherArgs::parse_from(["publisher"]);
        assert_eq!(args.quic.listen_addr, "0.0.0.0:8080");
        assert_eq!(args.api.listen_addr, "0.0.0.0:8081");
        assert_eq!(args.consensus.timeout_secs, 60);
        assert_eq!(args.log.level, "info");
        assert_eq!(args.log.format, "json");
    }

    #[test]
    fn cli_overrides() {
        let args = PublisherArgs::parse_from([
            "publisher",
            "--quic.listen-addr",
            "0.0.0.0:9090",
            "--consensus.timeout-secs",
            "3",
            "--log.level",
            "debug",
            "--log.format",
            "pretty",
        ]);
        assert_eq!(args.quic.listen_addr, "0.0.0.0:9090");
        assert_eq!(args.consensus.timeout_secs, 3);
        assert_eq!(args.log.level, "debug");
        assert_eq!(args.log.format, "pretty");
    }
}
