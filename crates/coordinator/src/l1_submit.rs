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
use tracing::{info, warn};

use crate::abi::{
    GameSearchResult, IComposeAnchorStateRegistry, IDisputeGameFactory, SuperRootProof,
    SuperblockAggregationOutputs, COMPOSE_GAME_TYPE,
};
use crate::proof_types::ProofData;
use crate::settlement::{hash_super_root, mock_payload, SettlementPayload, SUPER_ROOT_VERSION};

const MAX_RETRIES: u32 = 3;
const INITIAL_RETRY_DELAY: Duration = Duration::from_secs(2);
const RECOVERY_READ_RETRIES: u32 = 5;
const RECOVERY_RETRY_DELAY: Duration = Duration::from_millis(250);
const RECOVERY_PAGE_SIZE: u64 = 256;

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

    fn build_read_provider(&self) -> Result<impl Provider> {
        let url = self.rpc_url.parse().context("invalid l1_rpc_url")?;
        Ok(ProviderBuilder::new().connect_http(url))
    }

    /// Recovers the latest submitted superblock from canonical L1 data.
    ///
    /// Starting from the anchor registry, Compose games are replayed in L1
    /// factory order to build every sequential, parent-linked branch. Recovery
    /// selects the unique highest valid head and fails on an equal-height fork.
    pub async fn fetch_latest_superblock_state(&self) -> Result<Option<(u64, B256)>> {
        let Some(asr) = self.anchor_state_registry else {
            return Ok(None);
        };

        let provider = self.build_read_provider()?;
        let registry = IComposeAnchorStateRegistry::new(asr, &provider);
        let anchor = registry
            .getAnchorRoot()
            .call()
            .await
            .context("getAnchorRoot call failed")?;

        let sb_num: u64 = anchor
            .l2SequenceNumber_
            .try_into()
            .context("anchor sequence number does not fit u64")?;
        let Some(anchor_state) = Self::validate_anchor_state(sb_num, anchor.root_) else {
            return Ok(None);
        };

        let state = self.recover_from_factory(&provider, anchor_state).await?;
        *self.lock_parent_hash() = state.1;
        Ok(Some(state))
    }

    async fn recover_from_factory(
        &self,
        provider: &impl Provider,
        anchor: (u64, B256),
    ) -> Result<(u64, B256)> {
        let factory = IDisputeGameFactory::new(self.factory, provider);
        let game_count: u64 = factory
            .gameCount()
            .call()
            .await
            .context("gameCount call failed")?
            .try_into()
            .context("gameCount does not fit u64")?;
        if game_count == 0 {
            return Ok(anchor);
        }

        let mut candidates = Vec::new();
        let mut start = game_count - 1;
        loop {
            let games = fetch_recovery_page(provider, self.factory, start).await?;
            if games.is_empty() {
                break;
            }

            let page_len = games.len();
            let mut oldest_index = start;
            for game in games {
                let index: u64 = game
                    .index
                    .try_into()
                    .context("dispute game index does not fit u64")?;
                oldest_index = oldest_index.min(index);
                let candidate = decode_superblock_state(&game.extraData)
                    .with_context(|| format!("Compose game {index} has invalid extraData"))?;
                candidates.push((index, candidate));
            }

            if page_len < RECOVERY_PAGE_SIZE as usize || oldest_index == 0 {
                break;
            }
            start = oldest_index - 1;
        }
        candidates.sort_unstable_by_key(|(index, _)| *index);

        let head = select_canonical_superblock_head(anchor, &candidates)?;

        info!(
            anchor_number = anchor.0,
            recovered_number = head.0,
            game_count,
            "Reconstructed canonical superblock chain from L1"
        );

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

async fn fetch_recovery_page(
    provider: &impl Provider,
    factory_address: Address,
    start: u64,
) -> Result<Vec<GameSearchResult>> {
    let mut last_error = None;
    for attempt in 0..RECOVERY_READ_RETRIES {
        let factory = IDisputeGameFactory::new(factory_address, provider);
        let result = factory
            .findLatestGames(
                COMPOSE_GAME_TYPE,
                U256::from(start),
                U256::from(RECOVERY_PAGE_SIZE),
            )
            .call()
            .await
            .with_context(|| format!("findLatestGames call failed from index {start}"));

        match result {
            Ok(games) => return Ok(games),
            Err(error) => {
                last_error = Some(error);
                if attempt + 1 < RECOVERY_READ_RETRIES {
                    tokio::time::sleep(RECOVERY_RETRY_DELAY * 2u32.pow(attempt)).await;
                }
            }
        }
    }

    Err(last_error.unwrap_or_else(|| anyhow::anyhow!("failed to read recovery page at {start}")))
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct RecoveredSuperblock {
    number: u64,
    hash: B256,
    parent_hash: B256,
}

fn select_canonical_superblock_head(
    anchor: (u64, B256),
    candidates: &[(u64, RecoveredSuperblock)],
) -> Result<(u64, B256)> {
    let mut valid_states = HashMap::from([(anchor.1, anchor.0)]);
    let mut head = anchor;
    let mut head_is_ambiguous = false;

    for (_, candidate) in candidates {
        let Some(parent_number) = valid_states.get(&candidate.parent_hash) else {
            continue;
        };
        if candidate.number != parent_number + 1 {
            continue;
        }

        valid_states.insert(candidate.hash, candidate.number);
        if candidate.number > head.0 {
            head = (candidate.number, candidate.hash);
            head_is_ambiguous = false;
        } else if candidate.number == head.0 && candidate.hash != head.1 {
            head_is_ambiguous = true;
        }
    }

    anyhow::ensure!(
        !head_is_ambiguous,
        "multiple valid Compose chains have the same highest superblock number {}",
        head.0
    );
    Ok(head)
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

    use super::{
        decode_superblock_state, select_canonical_superblock_head, L1Submitter, RecoveredSuperblock,
    };

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

    #[test]
    fn recovery_selects_the_most_advanced_parent_linked_branch() {
        let genesis = B256::repeat_byte(0x01);
        let first = B256::repeat_byte(0x11);
        let second = B256::repeat_byte(0x12);
        let reset_first = B256::repeat_byte(0x21);
        let candidates = vec![
            recovered(1, first, genesis),
            recovered(2, second, first),
            recovered(1, reset_first, genesis),
            recovered(2, B256::repeat_byte(0x22), reset_first),
            recovered(3, B256::repeat_byte(0x13), second),
        ];

        assert_eq!(
            select_canonical_superblock_head((0, genesis), &candidates).unwrap(),
            (3, B256::repeat_byte(0x13))
        );
    }

    #[test]
    fn recovery_rejects_equal_height_heads() {
        let genesis = B256::repeat_byte(0x01);
        let candidates = vec![
            recovered(1, B256::repeat_byte(0x11), genesis),
            recovered(1, B256::repeat_byte(0x21), genesis),
        ];

        let error = select_canonical_superblock_head((0, genesis), &candidates).unwrap_err();
        assert!(error
            .to_string()
            .contains("same highest superblock number 1"));
    }

    fn recovered(number: u64, hash: B256, parent_hash: B256) -> (u64, RecoveredSuperblock) {
        (
            number,
            RecoveredSuperblock {
                number,
                hash,
                parent_hash,
            },
        )
    }
}
