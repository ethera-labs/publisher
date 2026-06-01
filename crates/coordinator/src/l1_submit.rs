//! L1 settlement — builds and submits superblock proofs to `ComposeL2OutputOracle`.

use std::collections::HashMap;
use std::sync::Mutex;
use std::time::Duration;

use alloy::network::EthereumWallet;
use alloy::primitives::{keccak256, Address, Bytes, B256, U256};
use alloy::providers::ProviderBuilder;
use alloy::signers::local::PrivateKeySigner;
use alloy::sol;
use alloy::sol_types::SolValue;
use anyhow::{Context, Result};
use sha2::{Digest, Sha256};
use tracing::{info, warn};

use crate::proof_types::ProofData;

const MAX_RETRIES: u32 = 3;
const INITIAL_RETRY_DELAY: Duration = Duration::from_secs(2);

sol! {
    struct SuperblockAggregationOutputs {
        uint256 superblockNumber;
        bytes32 parentSuperblockBatchHash;
        BootInfoStruct[] bootInfo;
    }

    struct BootInfoStruct {
        bytes32 l1Head;
        bytes32 l2PreRoot;
        bytes32 l2PostRoot;
        uint64 l2BlockNumber;
        bytes32 rollupConfigHash;
    }

    #[sol(rpc)]
    interface IComposeL2OutputOracle {
        function proposeL2Output(
            bytes32 _outputRoot,
            bytes32 _l1Hash,
            bytes memory _extraData
        ) external;

        function latestSuperblockNumber() external view returns (uint256);
        function getSuperblockHash(uint256 _superblockNumber) external view returns (bytes32);
    }
}

#[derive(Debug)]
pub struct L1Submitter {
    l2oo_address: Address,
    rpc_url: String,
    private_key: String,
    /// Tracks the hash of the last successfully submitted superblock so
    /// each new submission can reference its parent. Seeded from L1 on startup.
    parent_hash: Mutex<B256>,
}

impl L1Submitter {
    pub fn new(l2oo_address: &str, rpc_url: String, private_key: String) -> Result<Self> {
        let addr: Address = l2oo_address.parse().context("invalid l2oo_address")?;
        Ok(Self {
            l2oo_address: addr,
            rpc_url,
            private_key,
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
    /// Returns `None` if the contract has no superblocks yet (number == 0).
    pub async fn fetch_latest_superblock_state(&self) -> Result<Option<(u64, B256)>> {
        let provider = self.build_provider()?;
        let contract = IComposeL2OutputOracle::new(self.l2oo_address, provider);

        let result = contract
            .latestSuperblockNumber()
            .call()
            .await
            .context("latestSuperblockNumber call failed")?;
        let sb_num: u64 = result.try_into().unwrap_or(0);

        if sb_num == 0 {
            return Ok(None);
        }

        let sb_hash = contract
            .getSuperblockHash(U256::from(sb_num))
            .call()
            .await
            .context("getSuperblockHash call failed")?;

        *self.parent_hash.lock().unwrap() = sb_hash;

        Ok(Some((sb_num, sb_hash)))
    }

    pub async fn submit(
        &self,
        superblock_number: u64,
        proofs: &HashMap<u64, ProofData>,
    ) -> Result<()> {
        // Sort by rollup_config_hash so bootInfo and the concatenated proof bytes
        // share one deterministic order (HashMap iteration order is not stable).
        let mut ordered: Vec<&ProofData> = proofs.values().collect();
        ordered.sort_by_key(|p| p.aggregation_outputs.rollup_config_hash);

        let boot_infos: Vec<BootInfoStruct> = ordered
            .iter()
            .map(|p| {
                let o = &p.aggregation_outputs;
                BootInfoStruct {
                    l1Head: o.l1_head,
                    l2PreRoot: o.l2_pre_root,
                    l2PostRoot: o.l2_post_root,
                    l2BlockNumber: o.l2_block_number,
                    rollupConfigHash: o.rollup_config_hash,
                }
            })
            .collect();

        let parent_hash = *self.parent_hash.lock().unwrap();
        let agg_outputs = SuperblockAggregationOutputs {
            superblockNumber: U256::from(superblock_number),
            parentSuperblockBatchHash: parent_hash,
            bootInfo: boot_infos,
        };

        let proof_bytes: Vec<u8> = ordered
            .iter()
            .flat_map(|p| p.compressed_proof.iter().copied())
            .collect();

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
