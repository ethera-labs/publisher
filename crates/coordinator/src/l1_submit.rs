//! L1 settlement submission.

use std::collections::HashMap;
use std::sync::{Mutex, MutexGuard, PoisonError};
use std::time::Duration;

use alloy::eips::BlockNumberOrTag;
use alloy::network::EthereumWallet;
use alloy::primitives::{Address, Bytes, FixedBytes, B256};
use alloy::providers::{Provider, ProviderBuilder};
use alloy::signers::local::PrivateKeySigner;
use alloy::sol_types::SolValue;
use anyhow::{Context, Result};
use tracing::{info, warn};

use crate::abi::{
    IComposeAnchorStateRegistry, IDisputeGameFactory, SuperRootProof, COMPOSE_GAME_TYPE,
};
use crate::proof_types::ProofData;
use crate::settlement::{hash_super_root, mock_payload, SettlementPayload, SUPER_ROOT_VERSION};

const MAX_RETRIES: u32 = 3;
const INITIAL_RETRY_DELAY: Duration = Duration::from_secs(2);

#[derive(Debug)]
pub struct L1Submitter {
    factory: Address,
    anchor_state_registry: Option<Address>,
    rpc_url: String,
    private_key: String,
    /// Hash of the last submitted superblock, referenced as the parent of the
    /// next one. Seeded from the anchor registry on startup.
    parent_hash: Mutex<B256>,
}

impl L1Submitter {
    pub fn new(
        factory: &str,
        anchor_state_registry: &str,
        rpc_url: String,
        private_key: String,
    ) -> Result<Self> {
        let factory: Address = factory.parse().context("invalid dispute_game_factory")?;
        let anchor_state_registry = match anchor_state_registry.trim() {
            "" => None,
            a => Some(
                a.parse::<Address>()
                    .context("invalid anchor_state_registry")?,
            ),
        };
        Ok(Self {
            factory,
            anchor_state_registry,
            rpc_url,
            private_key,
            parent_hash: Mutex::new(B256::ZERO),
        })
    }

    /// Locks the cached parent hash, tolerating a poisoned mutex: the guarded
    /// `B256` has no broken invariant, so recovering the inner value is safe.
    fn lock_parent_hash(&self) -> MutexGuard<'_, B256> {
        self.parent_hash
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
    }

    fn build_provider(&self) -> Result<impl Provider> {
        let signer: PrivateKeySigner = self.private_key.parse().context("invalid proposer_key")?;
        let wallet = EthereumWallet::from(signer);
        let url = self.rpc_url.parse().context("invalid l1_rpc_url")?;
        Ok(ProviderBuilder::new().wallet(wallet).connect_http(url))
    }

    /// Reads the anchored superblock number and root from the
    /// `ComposeAnchorStateRegistry`. Returns `None` when no registry is
    /// configured or no anchor has been set yet.
    pub async fn fetch_latest_superblock_state(&self) -> Result<Option<(u64, B256)>> {
        let Some(asr) = self.anchor_state_registry else {
            return Ok(None);
        };

        let provider = self.build_provider()?;
        let registry = IComposeAnchorStateRegistry::new(asr, provider);
        let anchor = registry
            .getAnchorRoot()
            .call()
            .await
            .context("getAnchorRoot call failed")?;

        let sb_num: u64 = anchor.l2SequenceNumber_.try_into().unwrap_or(0);
        let Some(state) = Self::validate_anchor_state(sb_num, anchor.root_) else {
            return Ok(None);
        };

        *self.lock_parent_hash() = state.1;
        Ok(Some(state))
    }

    fn validate_anchor_state(sb_num: u64, root: B256) -> Option<(u64, B256)> {
        if root == B256::ZERO {
            return None;
        }

        Some((sb_num, root))
    }

    pub async fn submit_mock(
        &self,
        superblock_number: u64,
        proofs: &HashMap<u64, ProofData>,
    ) -> Result<()> {
        let parent_hash = *self.lock_parent_hash();
        let payload = mock_payload(superblock_number, parent_hash, proofs)?;
        self.submit(payload).await
    }

    async fn submit(&self, payload: SettlementPayload) -> Result<()> {
        let provider = self.build_provider()?;
        let factory = IDisputeGameFactory::new(self.factory, &provider);

        let block = provider
            .get_block_by_number(BlockNumberOrTag::Latest)
            .await
            .context("get_block_by_number failed")?
            .context("no latest L1 block")?;
        let super_root = SuperRootProof {
            version: FixedBytes::<1>::from([SUPER_ROOT_VERSION]),
            timestamp: block.header.timestamp,
            outputRoots: payload.output_roots,
        };
        let root_claim = hash_super_root(&super_root);

        let extra_data: Bytes = (
            payload.aggregation_outputs.clone(),
            super_root,
            Bytes::from(payload.proof.clone()),
        )
            .abi_encode_params()
            .into();

        let bond = factory
            .initBonds(COMPOSE_GAME_TYPE)
            .call()
            .await
            .context("initBonds call failed")?;

        let mut last_err = None;
        for attempt in 0..MAX_RETRIES {
            if attempt > 0 {
                let delay = INITIAL_RETRY_DELAY * 2u32.pow(attempt - 1);
                warn!(
                    superblock_number = payload.superblock_number,
                    attempt, "Retrying dispute game creation after {:?}", delay
                );
                tokio::time::sleep(delay).await;
            }

            match factory
                .create(COMPOSE_GAME_TYPE, root_claim, extra_data.clone())
                .value(bond)
                .send()
                .await
            {
                Ok(pending) => match pending.get_receipt().await {
                    Ok(receipt) => {
                        info!(
                            superblock_number = payload.superblock_number,
                            tx_hash = %receipt.transaction_hash,
                            "Dispute game created"
                        );
                        *self.lock_parent_hash() = payload.next_parent_hash;
                        return Ok(());
                    }
                    Err(e) => last_err = Some(e.into()),
                },
                Err(e) => last_err = Some(e.into()),
            }
        }

        Err(last_err.unwrap_or_else(|| {
            anyhow::anyhow!("dispute game creation failed after {MAX_RETRIES} attempts")
        }))
    }
}

#[cfg(test)]
mod tests {
    use alloy::primitives::B256;

    use super::L1Submitter;

    #[test]
    fn anchor_state_accepts_seeded_genesis_root() {
        let root = B256::repeat_byte(0x42);

        assert_eq!(L1Submitter::validate_anchor_state(0, root), Some((0, root)));
    }

    #[test]
    fn anchor_state_rejects_empty_root() {
        assert_eq!(L1Submitter::validate_anchor_state(0, B256::ZERO), None);
    }
}
