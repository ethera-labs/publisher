//! Reconstruction of canonical publisher state from L1 settlement contracts.

use std::collections::HashMap;
use std::time::Duration;

use alloy::primitives::{keccak256, Address, Bytes, B256, U256};
use alloy::providers::Provider;
use alloy::sol_types::SolValue;
use anyhow::{Context, Result};
use tracing::info;

use crate::abi::{
    GameSearchResult, IComposeAnchorStateRegistry, IDisputeGame, IDisputeGameFactory,
    SuperRootProof, SuperblockAggregationOutputs, COMPOSE_GAME_TYPE,
};
use crate::settlement::hash_super_root;

const READ_RETRIES: u32 = 5;
const RETRY_DELAY: Duration = Duration::from_millis(250);
const PAGE_SIZE: u64 = 256;

/// Recovers the latest submitted superblock from canonical L1 data.
///
/// The anchor registry identifies the finalized base game. Compose games after
/// that anchor are replayed in factory order, preserving sequential numbers and
/// parent links. Equal-height competing heads are rejected as ambiguous.
pub(crate) async fn recover_latest_superblock(
    provider: &impl Provider,
    factory_address: Address,
    registry_address: Address,
) -> Result<Option<(u64, B256)>> {
    let anchor = resolve_anchor(provider, registry_address).await?;
    let Some(anchor) = anchor else {
        return Ok(None);
    };

    let factory = IDisputeGameFactory::new(factory_address, provider);
    let game_count: u64 = factory
        .gameCount()
        .call()
        .await
        .context("gameCount call failed")?
        .try_into()
        .context("gameCount does not fit u64")?;
    if game_count == 0 {
        return Ok(Some(anchor));
    }

    let candidates = fetch_candidates(provider, factory_address, game_count).await?;
    let head = select_canonical_head(anchor, &candidates)?;

    info!(
        anchor_number = anchor.0,
        recovered_number = head.0,
        game_count,
        "Reconstructed canonical superblock chain from L1"
    );
    Ok(Some(head))
}

async fn resolve_anchor(
    provider: &impl Provider,
    registry_address: Address,
) -> Result<Option<(u64, B256)>> {
    let registry = IComposeAnchorStateRegistry::new(registry_address, provider);
    let anchor = registry
        .getAnchorRoot()
        .call()
        .await
        .context("getAnchorRoot call failed")?;
    let anchor_number: u64 = anchor
        .l2SequenceNumber_
        .try_into()
        .context("anchor sequence number does not fit u64")?;
    let anchor_game = registry
        .anchorGame()
        .call()
        .await
        .context("anchorGame call failed")?;

    if anchor_game == Address::ZERO {
        return validate_starting_anchor(anchor_number, anchor.root_);
    }

    let extra_data = IDisputeGame::new(anchor_game, provider)
        .extraData()
        .call()
        .await
        .context("anchor game extraData call failed")?;
    decode_anchor_game(anchor_number, anchor.root_, &extra_data).map(Some)
}

async fn fetch_candidates(
    provider: &impl Provider,
    factory_address: Address,
    game_count: u64,
) -> Result<Vec<(u64, RecoveredSuperblock)>> {
    let mut candidates = Vec::new();
    let mut start = game_count - 1;
    loop {
        let games = fetch_page(provider, factory_address, start).await?;
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
            let candidate = decode_game_data(&game.extraData)
                .with_context(|| format!("Compose game {index} has invalid extraData"))?
                .superblock;
            candidates.push((index, candidate));
        }

        if page_len < PAGE_SIZE as usize || oldest_index == 0 {
            break;
        }
        start = oldest_index - 1;
    }
    candidates.sort_unstable_by_key(|(index, _)| *index);
    Ok(candidates)
}

async fn fetch_page(
    provider: &impl Provider,
    factory_address: Address,
    start: u64,
) -> Result<Vec<GameSearchResult>> {
    let mut last_error = None;
    for attempt in 0..READ_RETRIES {
        let factory = IDisputeGameFactory::new(factory_address, provider);
        let result = factory
            .findLatestGames(COMPOSE_GAME_TYPE, U256::from(start), U256::from(PAGE_SIZE))
            .call()
            .await
            .with_context(|| format!("findLatestGames call failed from index {start}"));

        match result {
            Ok(games) => return Ok(games),
            Err(error) => {
                last_error = Some(error);
                if attempt + 1 < READ_RETRIES {
                    tokio::time::sleep(RETRY_DELAY * 2u32.pow(attempt)).await;
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

#[derive(Debug)]
struct DecodedGameData {
    superblock: RecoveredSuperblock,
    super_root: SuperRootProof,
}

fn validate_starting_anchor(number: u64, root: B256) -> Result<Option<(u64, B256)>> {
    if root == B256::ZERO {
        return Ok(None);
    }

    anyhow::ensure!(
        number == 0,
        "anchor registry has no anchor game for non-genesis superblock {number}"
    );
    Ok(Some((number, root)))
}

fn decode_anchor_game(
    anchor_number: u64,
    anchor_root_claim: B256,
    extra_data: &[u8],
) -> Result<(u64, B256)> {
    let decoded = decode_game_data(extra_data).context("anchor game has invalid extraData")?;
    anyhow::ensure!(
        decoded.superblock.number == anchor_number,
        "anchor game superblock number {} does not match registry sequence number {anchor_number}",
        decoded.superblock.number
    );
    anyhow::ensure!(
        hash_super_root(&decoded.super_root) == anchor_root_claim,
        "anchor game root claim does not match anchor registry"
    );
    Ok((decoded.superblock.number, decoded.superblock.hash))
}

fn select_canonical_head(
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

fn decode_game_data(extra_data: &[u8]) -> Option<DecodedGameData> {
    let (outputs, super_root, _): (SuperblockAggregationOutputs, SuperRootProof, Bytes) =
        SolValue::abi_decode_params(extra_data).ok()?;
    let number = outputs.superblockNumber.try_into().ok()?;
    Some(DecodedGameData {
        superblock: RecoveredSuperblock {
            number,
            hash: keccak256(outputs.abi_encode()),
            parent_hash: outputs.parentSuperblockBatchHash,
        },
        super_root,
    })
}

#[cfg(test)]
mod tests {
    use alloy::primitives::{keccak256, Bytes, FixedBytes, B256, U256};
    use alloy::sol_types::SolValue;

    use crate::abi::{OutputRootWithChainId, SuperRootProof, SuperblockAggregationOutputs};
    use crate::settlement::hash_super_root;

    use super::{
        decode_anchor_game, decode_game_data, select_canonical_head, validate_starting_anchor,
        RecoveredSuperblock,
    };

    #[test]
    fn validates_genesis_anchor_without_a_game() {
        let root = B256::repeat_byte(0x42);
        assert_eq!(validate_starting_anchor(0, root).unwrap(), Some((0, root)));
        assert_eq!(validate_starting_anchor(0, B256::ZERO).unwrap(), None);
        assert!(validate_starting_anchor(1, root).is_err());
    }

    #[test]
    fn decodes_superblock_state_from_game_data() {
        let (outputs, _, extra_data) = game_data(42);
        let decoded = decode_game_data(&extra_data).unwrap();

        assert_eq!(decoded.superblock.number, 42);
        assert_eq!(decoded.superblock.parent_hash, B256::repeat_byte(0x11));
        assert_eq!(decoded.superblock.hash, keccak256(outputs.abi_encode()));
    }

    #[test]
    fn derives_and_validates_hash_from_anchor_game() {
        let (outputs, root_claim, extra_data) = game_data(42);

        assert_eq!(
            decode_anchor_game(42, root_claim, &extra_data).unwrap(),
            (42, keccak256(outputs.abi_encode()))
        );
        assert!(decode_anchor_game(41, root_claim, &extra_data).is_err());
        assert!(decode_anchor_game(42, B256::ZERO, &extra_data).is_err());
    }

    #[test]
    fn selects_the_most_advanced_parent_linked_branch() {
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
            select_canonical_head((0, genesis), &candidates).unwrap(),
            (3, B256::repeat_byte(0x13))
        );
    }

    #[test]
    fn rejects_equal_height_heads() {
        let genesis = B256::repeat_byte(0x01);
        let candidates = vec![
            recovered(1, B256::repeat_byte(0x11), genesis),
            recovered(1, B256::repeat_byte(0x21), genesis),
        ];

        let error = select_canonical_head((0, genesis), &candidates).unwrap_err();
        assert!(error
            .to_string()
            .contains("same highest superblock number 1"));
    }

    fn game_data(number: u64) -> (SuperblockAggregationOutputs, B256, Vec<u8>) {
        let outputs = SuperblockAggregationOutputs {
            superblockNumber: U256::from(number),
            parentSuperblockBatchHash: B256::repeat_byte(0x11),
            bootInfo: Vec::new(),
        };
        let super_root = SuperRootProof {
            version: FixedBytes::ZERO,
            timestamp: 1,
            outputRoots: vec![OutputRootWithChainId {
                chainId: U256::from(100_003),
                root: B256::repeat_byte(0x22),
            }],
        };
        let root_claim = hash_super_root(&super_root);
        let extra_data = (outputs.clone(), super_root, Bytes::new()).abi_encode_params();
        (outputs, root_claim, extra_data)
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
