use std::str::FromStr;

use serde::{Deserialize, Deserializer};

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum ProvingMode {
    Mock,
    #[default]
    Real,
}

impl ProvingMode {
    pub const fn is_mock(self) -> bool {
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

impl FromStr for ProvingMode {
    type Err = String;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        parse_proving_mode(value)
    }
}

pub(crate) fn parse_proving_mode(value: &str) -> Result<ProvingMode, String> {
    match value.trim().to_ascii_lowercase().as_str() {
        "mock" | "bypass" | "bypass_prover" => Ok(ProvingMode::Mock),
        "real" | "prover" => Ok(ProvingMode::Real),
        other => Err(format!(
            "invalid proving mode '{other}', expected 'mock' or 'real'"
        )),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_supported_values() {
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
}
