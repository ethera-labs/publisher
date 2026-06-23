use std::path::PathBuf;

use anyhow::Result;
use clap::Parser;

use crate::overrides::ConfigOverrides;
use crate::Config;

#[derive(Debug, Parser)]
#[command(name = "publisher", about = "Ethera Shared Publisher")]
pub struct Cli {
    #[arg(long, short, default_value = "config.yaml")]
    pub config: PathBuf,

    #[command(flatten)]
    overrides: ConfigOverrides,
}

impl Cli {
    pub fn load_config(&self) -> Result<Config> {
        Config::load_with_overrides(&self.config, self.overrides.clone())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_config_path() {
        let cli = Cli::parse_from(["publisher"]);
        assert_eq!(cli.config, PathBuf::from("config.yaml"));
    }

    #[test]
    fn custom_config_path() {
        let cli = Cli::parse_from(["publisher", "--config", "/etc/publisher/config.yaml"]);
        assert_eq!(cli.config, PathBuf::from("/etc/publisher/config.yaml"));
    }
}
