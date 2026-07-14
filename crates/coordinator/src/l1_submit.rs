//! L1 settlement submission.

use std::collections::HashMap;
use std::sync::{Mutex, MutexGuard, PoisonError};
use std::time::Duration;

use alloy::eips::BlockNumberOrTag;
use alloy::network::EthereumWallet;
use alloy::primitives::{keccak256, Address, Bytes, FixedBytes, B256, U256};
use alloy::providers::{Provider, ProviderBuilder};
use alloy::signers::local::PrivateKeySigner;
use alloy::sol_types::SolValue;
use anyhow::{Context, Result};
use futures::{stream, StreamExt, TryStreamExt};
use tracing::{info, warn};

use crate::abi::{
    IComposeAnchorStateRegistry, IDisputeGame, IDisputeGameFactory, SuperRootProof,
    SuperblockAggregationOutputs, COMPOSE_GAME_TYPE,
};
use crate::proof_types::ProofData;
use crate::settlement::{hash_super_root, mock_payload, SettlementPayload, SUPER_ROOT_VERSION};

const MAX_RETRIES: u32 = 3;
const INITIAL_RETRY_DELAY: Duration = Duration::from_secs(2);
const RECOVERY_READ_CONCURRENCY: usize = 8;
const RECOVERY_READ_RETRIES: u32 = 5;
const RECOVERY_RETRY_DELAY: Duration = Duration::from_millis(250);

#[derive(Debug)]
pub struct L1Submitter {
    factory: Address,
    anchor_state_registry: Option<Address>,
    rpc_url: String,
    private_key: String,
    recovery_checkpoint: Option<RecoveryCheckpoint>,
    /// Hash of the last submitted superblock, referenced as the parent of the
    /// next one. Seeded from the anchor registry on startup.
    parent_hash: Mutex<B256>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RecoveryCheckpoint {
    pub game_index: u64,
    pub superblock_number: u64,
    pub superblock_hash: B256,
}

impl L1Submitter {
    pub fn new(
        factory: &str,
        anchor_state_registry: &str,
        rpc_url: String,
        private_key: String,
        recovery_checkpoint: Option<RecoveryCheckpoint>,
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
            recovery_checkpoint,
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

    fn build_read_provider(&self) -> Result<impl Provider> {
        let url = self.rpc_url.parse().context("invalid l1_rpc_url")?;
        Ok(ProviderBuilder::new().connect_http(url))
    }

    /// Recovers the latest submitted superblock from canonical L1 data.
    ///
    /// A checkpoint selects the canonical branch after an incident that
    /// created duplicate superblock numbers. Descendants are accepted in L1
    /// factory order only when both number and parent hash extend the head.
    /// Without a checkpoint, the anchor registry remains the genesis fallback.
    pub async fn fetch_latest_superblock_state(&self) -> Result<Option<(u64, B256)>> {
        if let Some(checkpoint) = self.recovery_checkpoint {
            let state = self.recover_from_checkpoint(checkpoint).await?;
            *self.lock_parent_hash() = state.1;
            return Ok(Some(state));
        }

        let Some(asr) = self.anchor_state_registry else {
            return Ok(None);
        };

        let provider = self.build_read_provider()?;
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

    async fn recover_from_checkpoint(&self, checkpoint: RecoveryCheckpoint) -> Result<(u64, B256)> {
        let provider = self.build_read_provider()?;
        let factory = IDisputeGameFactory::new(self.factory, &provider);
        let game_count: u64 = factory
            .gameCount()
            .call()
            .await
            .context("gameCount call failed")?
            .try_into()
            .context("gameCount does not fit u64")?;

        anyhow::ensure!(
            checkpoint.game_index < game_count,
            "recovery checkpoint game index {} is outside factory game count {game_count}",
            checkpoint.game_index
        );

        let recovered_checkpoint =
            fetch_recovery_game(&provider, self.factory, checkpoint.game_index)
                .await?
                .context("recovery checkpoint is not a Compose dispute game")?;
        anyhow::ensure!(
            recovered_checkpoint.number == checkpoint.superblock_number,
            "recovery checkpoint game {} contains superblock {}, expected {}",
            checkpoint.game_index,
            recovered_checkpoint.number,
            checkpoint.superblock_number
        );
        anyhow::ensure!(
            recovered_checkpoint.hash == checkpoint.superblock_hash,
            "recovery checkpoint game {} has hash {}, expected {}",
            checkpoint.game_index,
            recovered_checkpoint.hash,
            checkpoint.superblock_hash
        );

        let mut candidates = stream::iter((checkpoint.game_index + 1)..game_count)
            .map(|index| {
                let provider = &provider;
                let factory_address = self.factory;
                async move {
                    fetch_recovery_game(provider, factory_address, index)
                        .await
                        .map(|candidate| candidate.map(|candidate| (index, candidate)))
                }
            })
            .buffer_unordered(RECOVERY_READ_CONCURRENCY)
            .try_collect::<Vec<_>>()
            .await?
            .into_iter()
            .flatten()
            .collect::<Vec<_>>();
        candidates.sort_unstable_by_key(|(index, _)| *index);

        let mut head = (checkpoint.superblock_number, checkpoint.superblock_hash);
        for (_, candidate) in candidates {
            if candidate.number == head.0 + 1 && candidate.parent_hash == head.1 {
                head = (candidate.number, candidate.hash);
            }
        }

        Ok(head)
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

async fn fetch_recovery_game(
    provider: &impl Provider,
    factory_address: Address,
    index: u64,
) -> Result<Option<RecoveredSuperblock>> {
    let mut last_error = None;
    for attempt in 0..RECOVERY_READ_RETRIES {
        let result = async {
            let factory = IDisputeGameFactory::new(factory_address, provider);
            let game = factory
                .gameAtIndex(U256::from(index))
                .call()
                .await
                .with_context(|| format!("gameAtIndex({index}) call failed"))?;
            if game.gameType_ != COMPOSE_GAME_TYPE {
                return Ok(None);
            }

            let proxy = IDisputeGame::new(game.proxy_, provider);
            let extra_data = proxy
                .extraData()
                .call()
                .await
                .with_context(|| format!("extraData call failed for game {index}"))?;
            Ok::<_, anyhow::Error>(decode_superblock_state(&extra_data))
        }
        .await;

        match result {
            Ok(candidate) => return Ok(candidate),
            Err(error) => {
                last_error = Some(error);
                if attempt + 1 < RECOVERY_READ_RETRIES {
                    tokio::time::sleep(RECOVERY_RETRY_DELAY * 2u32.pow(attempt)).await;
                }
            }
        }
    }

    Err(last_error.unwrap_or_else(|| anyhow::anyhow!("failed to read recovery game {index}")))
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct RecoveredSuperblock {
    number: u64,
    hash: B256,
    parent_hash: B256,
}

fn decode_superblock_state(extra_data: &[u8]) -> Option<RecoveredSuperblock> {
    let (outputs, _, _): (SuperblockAggregationOutputs, SuperRootProof, Bytes) =
        SolValue::abi_decode_params(extra_data).ok()?;
    let number = outputs.superblockNumber.try_into().ok()?;
    Some(RecoveredSuperblock {
        number,
        hash: keccak256(outputs.abi_encode()),
        parent_hash: outputs.parentSuperblockBatchHash,
    })
}

#[cfg(test)]
mod tests {
    use alloy::primitives::{keccak256, Bytes, FixedBytes, B256, U256};
    use alloy::sol_types::SolValue;

    use crate::abi::{SuperRootProof, SuperblockAggregationOutputs};

    use super::{decode_superblock_state, L1Submitter};

    #[test]
    fn anchor_state_accepts_seeded_genesis_root() {
        let root = B256::repeat_byte(0x42);

        assert_eq!(L1Submitter::validate_anchor_state(0, root), Some((0, root)));
    }

    #[test]
    fn anchor_state_rejects_empty_root() {
        assert_eq!(L1Submitter::validate_anchor_state(0, B256::ZERO), None);
    }

    #[test]
    fn decodes_recovery_state_from_game_extra_data() {
        let outputs = SuperblockAggregationOutputs {
            superblockNumber: U256::from(42),
            parentSuperblockBatchHash: B256::repeat_byte(0x11),
            bootInfo: Vec::new(),
        };
        let proof = SuperRootProof {
            version: FixedBytes::ZERO,
            timestamp: 1,
            outputRoots: Vec::new(),
        };
        let extra_data = (outputs.clone(), proof, Bytes::new()).abi_encode_params();

        let recovered = decode_superblock_state(&extra_data).unwrap();

        assert_eq!(recovered.number, 42);
        assert_eq!(recovered.parent_hash, B256::repeat_byte(0x11));
        assert_eq!(recovered.hash, keccak256(outputs.abi_encode()));
    }
}
