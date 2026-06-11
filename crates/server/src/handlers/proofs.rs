//! Handler for op-succinct proof submissions.

use alloy_primitives::{Address, B256};
use axum::extract::State;
use axum::http::StatusCode;
use axum::Json;
use ethera_spec::ChainId;
use serde::Deserialize;
use tracing::warn;

use publisher_coordinator::proof_types::{AggregationOutputs, ProofData};

use crate::state::AppState;

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct IncomingAggregationOutputs {
    #[serde(rename = "l1Head")]
    l1_head: B256,
    #[serde(rename = "l2PreRoot")]
    l2_pre_root: B256,
    #[serde(rename = "l2PostRoot")]
    l2_post_root: B256,
    #[serde(rename = "l2BlockNumber")]
    l2_block_number: u64,
    #[serde(rename = "rollupConfigHash")]
    rollup_config_hash: B256,
    #[serde(rename = "mailboxRoot")]
    mailbox_root: B256,
    #[serde(rename = "multiBlockVKey")]
    multi_block_vkey: B256,
    #[serde(rename = "proverAddress")]
    prover_address: Address,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ProofSubmission {
    superblock_number: u64,
    chain_id: u64,
    aggregation_outputs: IncomingAggregationOutputs,
    agg_vkey_hash: B256,
    #[serde(default)]
    proof: Option<Vec<u8>>,
}

pub async fn handle_submit_proof(
    State(state): State<AppState>,
    Json(body): Json<ProofSubmission>,
) -> StatusCode {
    let o = body.aggregation_outputs;

    if o.l1_head == B256::ZERO {
        warn!(chain_id = body.chain_id, "Proof rejected: l1_head is zero");
        return StatusCode::BAD_REQUEST;
    }

    if !state
        .coordinator
        .is_chain_registered(ChainId::new(body.chain_id))
        .await
    {
        warn!(chain_id = body.chain_id, "Proof rejected: unknown chain");
        return StatusCode::BAD_REQUEST;
    }

    let data = ProofData {
        aggregation_outputs: AggregationOutputs {
            l1_head: o.l1_head,
            l2_pre_root: o.l2_pre_root,
            l2_post_root: o.l2_post_root,
            l2_block_number: o.l2_block_number,
            rollup_config_hash: o.rollup_config_hash,
            mailbox_root: o.mailbox_root,
            multi_block_vkey: o.multi_block_vkey,
            prover_address: o.prover_address,
        },
        compressed_proof: body.proof.unwrap_or_default(),
        agg_vkey_hash: body.agg_vkey_hash,
    };

    match state.coordinator.receive_chain_proof(body.chain_id, data) {
        Ok(_) => StatusCode::ACCEPTED,
        Err(e) => {
            warn!(
                chain_id = body.chain_id,
                superblock_number = body.superblock_number,
                "Proof rejected: {e}"
            );
            StatusCode::CONFLICT
        }
    }
}
