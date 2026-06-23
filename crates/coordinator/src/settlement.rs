//! Settlement payload construction.

use std::collections::HashMap;

use alloy::primitives::{keccak256, B256, U256};
use alloy::sol_types::SolValue;
use anyhow::{ensure, Result};

use crate::abi::{
    BootInfoStruct, OutputRootWithChainId, SuperRootProof, SuperblockAggregationOutputs,
};
use crate::proof_types::ProofData;

pub(crate) const SUPER_ROOT_VERSION: u8 = 0x01;

const MOCK_PROOF: &[u8] = b"MOCK_SUPERBLOCK_PROOF";

#[derive(Debug)]
pub(crate) struct SettlementPayload {
    pub(crate) superblock_number: u64,
    pub(crate) aggregation_outputs: SuperblockAggregationOutputs,
    pub(crate) output_roots: Vec<OutputRootWithChainId>,
    pub(crate) proof: Vec<u8>,
    pub(crate) next_parent_hash: B256,
}

pub(crate) fn mock_payload(
    superblock_number: u64,
    parent_hash: B256,
    proofs: &HashMap<u64, ProofData>,
) -> Result<SettlementPayload> {
    ensure!(!proofs.is_empty(), "no proofs to submit");

    let mut ordered: Vec<(u64, &ProofData)> = proofs
        .iter()
        .map(|(chain_id, proof)| (*chain_id, proof))
        .collect();
    ordered.sort_by_key(|(_, proof)| proof.aggregation_outputs.rollup_config_hash);

    let mut boot_info = Vec::with_capacity(ordered.len());
    let mut output_roots = Vec::with_capacity(ordered.len());
    for (chain_id, proof) in ordered {
        ensure!(chain_id != 0, "chain id must not be zero");

        let boot = BootInfoStruct::from(&proof.aggregation_outputs);
        output_roots.push(OutputRootWithChainId {
            chainId: U256::from(chain_id),
            root: boot.l2PostRoot,
        });
        boot_info.push(boot);
    }

    let aggregation_outputs = SuperblockAggregationOutputs {
        superblockNumber: U256::from(superblock_number),
        parentSuperblockBatchHash: parent_hash,
        bootInfo: boot_info,
    };
    let next_parent_hash = keccak256(aggregation_outputs.abi_encode().as_slice());

    Ok(SettlementPayload {
        superblock_number,
        aggregation_outputs,
        output_roots,
        proof: MOCK_PROOF.to_vec(),
        next_parent_hash,
    })
}

pub(crate) fn hash_super_root(proof: &SuperRootProof) -> B256 {
    let mut buf = Vec::with_capacity(9 + proof.outputRoots.len() * 64);
    buf.push(proof.version.0[0]);
    buf.extend_from_slice(&proof.timestamp.to_be_bytes());
    for output in &proof.outputRoots {
        buf.extend_from_slice(&output.chainId.to_be_bytes::<32>());
        buf.extend_from_slice(output.root.as_slice());
    }
    keccak256(buf)
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use alloy::primitives::{FixedBytes, B256, U256};
    use alloy_primitives::Address;

    use super::{hash_super_root, mock_payload, SUPER_ROOT_VERSION};
    use crate::abi::{OutputRootWithChainId, SuperRootProof};
    use crate::proof_types::{AggregationOutputs, ProofData};

    #[test]
    fn super_root_hash_matches_encoding() {
        let proof = SuperRootProof {
            version: FixedBytes::<1>::from([SUPER_ROOT_VERSION]),
            timestamp: 42,
            outputRoots: vec![OutputRootWithChainId {
                chainId: U256::from(100003u64),
                root: B256::repeat_byte(0x33),
            }],
        };

        let expected: B256 = "0x7b3e608d2e5e47b1a427c9b6288aaec1884e2beadddd8f780b0c7e277f1bcc9d"
            .parse()
            .unwrap();
        assert_eq!(hash_super_root(&proof), expected);
    }

    #[test]
    fn mock_payload_orders_roots_by_rollup_config_hash() {
        let mut proofs = HashMap::new();
        proofs.insert(20, proof(B256::repeat_byte(0x22), B256::repeat_byte(0xbb)));
        proofs.insert(10, proof(B256::repeat_byte(0x11), B256::repeat_byte(0xaa)));

        let payload = mock_payload(7, B256::repeat_byte(0x01), &proofs).unwrap();

        assert_eq!(payload.superblock_number, 7);
        assert_eq!(payload.output_roots.len(), 2);
        assert_eq!(payload.output_roots[0].chainId, U256::from(10));
        assert_eq!(payload.output_roots[1].chainId, U256::from(20));
        assert_eq!(payload.proof, b"MOCK_SUPERBLOCK_PROOF");
        assert_ne!(payload.next_parent_hash, B256::ZERO);
    }

    #[test]
    fn mock_payload_rejects_zero_chain_id() {
        let mut proofs = HashMap::new();
        proofs.insert(0, proof(B256::repeat_byte(0x11), B256::repeat_byte(0xaa)));

        let err = mock_payload(7, B256::ZERO, &proofs).unwrap_err();
        assert!(err.to_string().contains("chain id must not be zero"));
    }

    fn proof(rollup_config_hash: B256, post_root: B256) -> ProofData {
        ProofData {
            aggregation_outputs: AggregationOutputs {
                l1_head: B256::repeat_byte(0x01),
                l2_pre_root: B256::repeat_byte(0x02),
                l2_post_root: post_root,
                l2_block_number: 42,
                rollup_config_hash,
                mailbox_root: B256::repeat_byte(0x03),
                multi_block_vkey: B256::repeat_byte(0x04),
                prover_address: Address::repeat_byte(0x05),
            },
            compressed_proof: Vec::new(),
            agg_vkey_hash: B256::ZERO,
        }
    }
}
