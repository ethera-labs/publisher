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

// TODO: Decide whether op-succinct metadata such as superblock_hash,
// l2_start_block, and mailbox_info should become part of the publisher API.
// For now the handler tolerates those extra top-level fields for client
// compatibility, but only validates and consumes aggregation_outputs.
#[derive(Debug, Deserialize)]
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

    state
        .coordinator
        .receive_proof(body.superblock_number, body.chain_id, data)
        .await;

    StatusCode::ACCEPTED
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::ProofSubmission;

    const B256_HEX: &str = "0x1111111111111111111111111111111111111111111111111111111111111111";
    const ADDRESS_HEX: &str = "0x2222222222222222222222222222222222222222";

    fn valid_aggregation_outputs() -> serde_json::Value {
        json!({
            "l1Head": B256_HEX,
            "l2PreRoot": B256_HEX,
            "l2PostRoot": B256_HEX,
            "l2BlockNumber": 1609810,
            "rollupConfigHash": B256_HEX,
            "mailboxRoot": B256_HEX,
            "multiBlockVKey": B256_HEX,
            "proverAddress": ADDRESS_HEX
        })
    }

    #[test]
    fn proof_submission_accepts_op_succinct_extra_metadata() {
        let body = json!({
            "superblock_number": 1609810,
            "superblock_hash": B256_HEX,
            "chain_id": 100003,
            "prover_address": ADDRESS_HEX,
            "l1_head": B256_HEX,
            "aggregation_outputs": valid_aggregation_outputs(),
            "l2_start_block": 1609660,
            "agg_vkey_hash": B256_HEX,
            "agg_vk": B256_HEX,
            "mailbox_info": {
                "inbox_chains": [],
                "outbox_chains": [],
                "inbox_roots": [],
                "outbox_roots": []
            }
        });

        serde_json::from_value::<ProofSubmission>(body).expect("op-succinct payload should parse");
    }

    #[test]
    fn aggregation_outputs_reject_unknown_fields() {
        let mut aggregation_outputs = valid_aggregation_outputs();
        aggregation_outputs["unexpectedNestedField"] = json!(true);

        let body = json!({
            "superblock_number": 1609810,
            "chain_id": 100003,
            "aggregation_outputs": aggregation_outputs,
            "agg_vkey_hash": B256_HEX
        });

        let err = serde_json::from_value::<ProofSubmission>(body).unwrap_err();
        assert!(
            err.to_string().contains("unexpectedNestedField"),
            "unexpected error: {err}"
        );
    }
}
