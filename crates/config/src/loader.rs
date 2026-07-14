use std::path::Path;

use anyhow::{Context, Result};

use crate::overrides::ConfigOverrides;
use crate::Config;

impl Config {
    pub fn load(path: &Path) -> Result<Self> {
        Self::load_with_overrides(path, ConfigOverrides::from_env()?)
    }

    pub(crate) fn load_with_overrides(path: &Path, overrides: ConfigOverrides) -> Result<Self> {
        let mut cfg = if path.exists() {
            let contents = std::fs::read_to_string(path)
                .with_context(|| format!("reading config file {}", path.display()))?;
            serde_yaml::from_str(&contents)
                .with_context(|| format!("parsing config file {}", path.display()))?
        } else {
            Self::default()
        };

        overrides.apply(&mut cfg);
        cfg.normalize();
        cfg.validate()?;
        Ok(cfg)
    }

    fn normalize(&mut self) {
        normalize_addr(&mut self.server.listen_addr);
        normalize_addr(&mut self.api.listen_addr);
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

        let checkpoint_fields = [
            self.settlement.recovery_checkpoint_game_index.is_some(),
            self.settlement.recovery_checkpoint_number.is_some(),
            !self.settlement.recovery_checkpoint_hash.is_empty(),
        ];
        anyhow::ensure!(
            checkpoint_fields.iter().all(|configured| *configured)
                || checkpoint_fields.iter().all(|configured| !*configured),
            "settlement recovery checkpoint requires game index, number, and hash"
        );
        Ok(())
    }
}

fn normalize_addr(addr: &mut String) {
    if addr.starts_with(':') {
        *addr = format!("0.0.0.0{addr}");
    }
}
