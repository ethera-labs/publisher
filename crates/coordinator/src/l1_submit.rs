//! L1 settlement - builds and submits superblock proofs to `ComposeL2OutputOracle`.

use std::collections::HashMap;
use std::sync::Mutex;
use std::time::Duration;

use alloy::network::EthereumWallet;
use alloy::primitives::{keccak256, Address, Bytes, B256, U256};
use alloy::providers::ProviderBuilder;
use alloy::signers::local::PrivateKeySigner;
use alloy::sol_types::SolValue;
use anyhow::{Context, Result};
use sha2::{Digest, Sha256};
use tracing::{info, warn};

use crate::abi::{BootInfoStruct, IComposeL2OutputOracle, SuperblockAggregationOutputs};
use crate::proof_types::ProofData;

const MAX_RETRIES: u32 = 3;
const INITIAL_RETRY_DELAY: Duration = Duration::from_secs(2);

#[derive(Debug)]
pub struct L1Submitter {
    l2oo_address: Address,
    rpc_url: String,
    private_key: String,
    mock: bool,
    /// Tracks the hash of the last successfully submitted superblock so
    /// each new submission can reference its parent. Seeded from L1 on startup.
    parent_hash: Mutex<B256>,
}

impl L1Submitter {
    pub fn new(
        l2oo_address: &str,
        rpc_url: String,
        private_key: String,
        mock: bool,
    ) -> Result<Self> {
        let addr: Address = l2oo_address.parse().context("invalid l2oo_address")?;
        Ok(Self {
            l2oo_address: addr,
            rpc_url,
            private_key,
            mock,
            parent_hash: Mutex::new(B256::ZERO),
        })
    }

    fn build_provider(&self) -> Result<impl alloy::providers::Provider> {
        let signer: PrivateKeySigner = self.private_key.parse().context("invalid proposer_key")?;
        let wallet = EthereumWallet::from(signer);
        let url = self.rpc_url.parse().context("invalid l1_rpc_url")?;
        Ok(ProviderBuilder::new().wallet(wallet).connect_http(url))
    }

    /// Reads the latest superblock number and its hash from the L1 contract.
    /// Returns `None` only if the contract has no hash seeded for that number.
    pub async fn fetch_latest_superblock_state(&self) -> Result<Option<(u64, B256)>> {
        let provider = self.build_provider()?;
        let contract = IComposeL2OutputOracle::new(self.l2oo_address, provider);

        let result = contract
            .latestSuperblockNumber()
            .call()
            .await
            .context("latestSuperblockNumber call failed")?;
        let sb_num: u64 = result.try_into().unwrap_or(0);

        let sb_hash = contract
            .getSuperblockHash(U256::from(sb_num))
            .call()
            .await
            .context("getSuperblockHash call failed")?;

        let Some(state) = Self::validate_latest_superblock_state(sb_num, sb_hash) else {
            return Ok(None);
        };

        *self.parent_hash.lock().unwrap() = state.1;

        Ok(Some(state))
    }

    fn validate_latest_superblock_state(sb_num: u64, sb_hash: B256) -> Option<(u64, B256)> {
        if sb_hash == B256::ZERO {
            return None;
        }

        Some((sb_num, sb_hash))
    }

    pub async fn submit(
        &self,
        superblock_number: u64,
        proofs: &HashMap<u64, ProofData>,
    ) -> Result<()> {
        // Sort by rollup_config_hash so bootInfo and the proof payload
        // share one deterministic order (HashMap iteration order is not stable).
        let mut ordered: Vec<&ProofData> = proofs.values().collect();
        ordered.sort_by_key(|p| p.aggregation_outputs.rollup_config_hash);

        let boot_infos: Vec<BootInfoStruct> = ordered
            .iter()
            .map(|p| BootInfoStruct::from(&p.aggregation_outputs))
            .collect();

        let parent_hash = *self.parent_hash.lock().unwrap();
        let agg_outputs = SuperblockAggregationOutputs {
            superblockNumber: U256::from(superblock_number),
            parentSuperblockBatchHash: parent_hash,
            bootInfo: boot_infos,
        };

        let proof_bytes: Vec<u8> = if self.mock {
            Vec::new()
        } else {
            ordered
                .iter()
                .flat_map(|p| p.compressed_proof.iter().copied())
                .collect()
        };

        let extra_data: Bytes = (agg_outputs.clone(), Bytes::from(proof_bytes))
            .abi_encode_params()
            .into();

        let output_root = B256::from(sha256(agg_outputs.abi_encode().as_slice()));

        // Use the highest l1_head across all chains for deterministic selection.
        let l1_hash = proofs
            .values()
            .map(|p| p.aggregation_outputs.l1_head)
            .max()
            .unwrap_or(B256::ZERO);

        let provider = self.build_provider()?;
        let contract = IComposeL2OutputOracle::new(self.l2oo_address, provider);

        let mut last_err = None;
        for attempt in 0..MAX_RETRIES {
            if attempt > 0 {
                let delay = INITIAL_RETRY_DELAY * 2u32.pow(attempt - 1);
                warn!(
                    superblock_number,
                    attempt, "Retrying L1 submission after {:?}", delay
                );
                tokio::time::sleep(delay).await;
            }

            match contract
                .proposeL2Output(output_root, l1_hash, extra_data.clone())
                .send()
                .await
            {
                Ok(pending) => match pending.get_receipt().await {
                    Ok(receipt) => {
                        info!(
                            superblock_number,
                            tx_hash = %receipt.transaction_hash,
                            "Superblock submitted to L1"
                        );
                        *self.parent_hash.lock().unwrap() =
                            keccak256(agg_outputs.abi_encode().as_slice());
                        return Ok(());
                    }
                    Err(e) => {
                        last_err = Some(e.into());
                    }
                },
                Err(e) => {
                    last_err = Some(e.into());
                }
            }
        }

        Err(last_err.unwrap_or_else(|| {
            anyhow::anyhow!("L1 submission failed after {MAX_RETRIES} attempts")
        }))
    }
}

fn sha256(data: &[u8]) -> [u8; 32] {
    let mut h = Sha256::new();
    h.update(data);
    h.finalize().into()
}

#[cfg(test)]
mod tests {
    use alloy::primitives::{Bytes, B256, U256};
    use alloy::sol_types::SolValue;

    use super::L1Submitter;
    use crate::abi::{BootInfoStruct, SuperblockAggregationOutputs};

    const EXTRA_DATA_HEX: &str = "000000000000000000000000000000000000000000000000000000000000004000000000000000000000000000000000000000000000000000000000000001600000000000000000000000000000000000000000000000000000000000000007abababababababababababababababababababababababababababababababab00000000000000000000000000000000000000000000000000000000000000600000000000000000000000000000000000000000000000000000000000000001111111111111111111111111111111111111111111111111111111111111111122222222222222222222222222222222222222222222222222222222222222223333333333333333333333333333333333333333333333333333333333333333000000000000000000000000000000000000000000000000000000000000002a44444444444444444444444444444444444444444444444444444444444444440000000000000000000000000000000000000000000000000000000000000004deadbeef00000000000000000000000000000000000000000000000000000000";
    const OUTPUT_PREIMAGE_HEX: &str = "00000000000000000000000000000000000000000000000000000000000000200000000000000000000000000000000000000000000000000000000000000007abababababababababababababababababababababababababababababababab00000000000000000000000000000000000000000000000000000000000000600000000000000000000000000000000000000000000000000000000000000001111111111111111111111111111111111111111111111111111111111111111122222222222222222222222222222222222222222222222222222222222222223333333333333333333333333333333333333333333333333333333333333333000000000000000000000000000000000000000000000000000000000000002a4444444444444444444444444444444444444444444444444444444444444444";

    fn sample_outputs() -> SuperblockAggregationOutputs {
        SuperblockAggregationOutputs {
            superblockNumber: U256::from(7u64),
            parentSuperblockBatchHash: B256::repeat_byte(0xab),
            bootInfo: vec![BootInfoStruct {
                l1Head: B256::repeat_byte(0x11),
                l2PreRoot: B256::repeat_byte(0x22),
                l2PostRoot: B256::repeat_byte(0x33),
                l2BlockNumber: 42,
                rollupConfigHash: B256::repeat_byte(0x44),
            }],
        }
    }

    #[test]
    fn extra_data_encoding_is_stable() {
        let agg = sample_outputs();
        let extra = (agg.clone(), Bytes::from(vec![0xde, 0xad, 0xbe, 0xef])).abi_encode_params();
        assert_eq!(alloy::hex::encode(extra), EXTRA_DATA_HEX);
        assert_eq!(alloy::hex::encode(agg.abi_encode()), OUTPUT_PREIMAGE_HEX);
    }

    #[test]
    fn latest_superblock_state_accepts_seeded_genesis_hash() {
        let genesis_hash: B256 =
            "0xe7bac8efb0b12db59bbbe8667e31c486d1b6a9cc885edec48b834d943f3e2a46"
                .parse()
                .unwrap();

        assert_eq!(
            L1Submitter::validate_latest_superblock_state(0, genesis_hash),
            Some((0, genesis_hash))
        );
    }

    #[test]
    fn latest_superblock_state_rejects_empty_hash() {
        assert_eq!(
            L1Submitter::validate_latest_superblock_state(0, B256::ZERO),
            None
        );
    }
}
