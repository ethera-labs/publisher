//! Proof data types for superblock settlement.

use alloy_primitives::{Address, B256};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct AggregationOutputs {
    pub l1_head: B256,
    pub l2_pre_root: B256,
    pub l2_post_root: B256,
    pub l2_block_number: u64,
    pub rollup_config_hash: B256,
    pub mailbox_root: B256,
    pub multi_block_vkey: B256,
    pub prover_address: Address,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct ProofData {
    pub aggregation_outputs: AggregationOutputs,
    pub compressed_proof: Vec<u8>,
    pub agg_vkey_hash: B256,
}
