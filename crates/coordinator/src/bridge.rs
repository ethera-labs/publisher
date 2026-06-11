//! Non-blocking bridge between the synchronous spec state machines and the
//! publisher's async I/O. The spec invokes its effect traits while holding an
//! internal lock, so every implementation here only enqueues onto an mpsc
//! channel; a dedicated task performs the actual QUIC broadcast / L1 submit.

use std::collections::HashMap;

use ethera_spec::{ChainId, Instance, InstanceId, PeriodId, SuperblockHash, SuperblockNumber};
use prost::Message as _;
use tokio::sync::mpsc;
use tracing::warn;

use crate::proof_types::ProofData;

#[derive(Debug)]
pub(crate) enum Outbound {
    Broadcast(Vec<u8>),
    Rollback(Vec<u8>),
    SubmitProof {
        superblock_number: SuperblockNumber,
        proofs: Vec<ProofData>,
    },
}

#[derive(Debug, Clone)]
pub(crate) struct OutboundSink {
    tx: mpsc::UnboundedSender<Outbound>,
}

impl OutboundSink {
    pub(crate) fn channel() -> (Self, mpsc::UnboundedReceiver<Outbound>) {
        let (tx, rx) = mpsc::unbounded_channel();
        (Self { tx }, rx)
    }

    fn enqueue(&self, event: Outbound) {
        if self.tx.send(event).is_err() {
            warn!("Outbound channel closed, dropping event");
        }
    }

    fn enqueue_payload(&self, payload: ethera_spec_proto::Payload) {
        self.enqueue(Outbound::Broadcast(encode_message(payload)));
    }
}

pub(crate) fn encode_message(payload: ethera_spec_proto::Payload) -> Vec<u8> {
    ethera_spec_proto::Message {
        sender_id: "publisher".into(),
        payload: Some(payload),
    }
    .encode_to_vec()
}

impl ethera_spec_scp::PublisherNetwork for OutboundSink {
    fn send_start_instance(&self, instance: &Instance) {
        self.enqueue_payload(ethera_spec_proto::Payload::StartInstance(instance.into()));
    }

    fn send_decided(&self, instance_id: InstanceId, decided: bool) {
        self.enqueue_payload(ethera_spec_proto::Payload::Decided(
            ethera_spec_proto::Decided {
                instance_id: instance_id.as_bytes().to_vec(),
                decision: decided,
            },
        ));
    }
}

impl ethera_spec_sbcp::PublisherMessenger for OutboundSink {
    fn broadcast_start_period(
        &self,
        period_id: PeriodId,
        target_superblock_number: SuperblockNumber,
    ) {
        self.enqueue_payload(ethera_spec_proto::Payload::StartPeriod(
            ethera_spec_proto::StartPeriod {
                period_id: period_id.get(),
                superblock_number: target_superblock_number.get(),
            },
        ));
    }

    fn broadcast_rollback(
        &self,
        period_id: PeriodId,
        superblock_number: SuperblockNumber,
        superblock_hash: SuperblockHash,
    ) {
        // Rollback gets its own event so the coordinator can abandon
        // in-flight xTs before the broadcast goes out.
        self.enqueue(Outbound::Rollback(encode_message(
            ethera_spec_proto::Payload::Rollback(ethera_spec_proto::Rollback {
                period_id: period_id.get(),
                last_finalized_superblock_number: superblock_number.get(),
                last_finalized_superblock_hash: superblock_hash.as_bytes().to_vec(),
            }),
        )));
    }
}

impl ethera_spec_sbcp::PublisherProver for OutboundSink {
    type ChainProof = ProofData;
    type SuperblockProof = Vec<ProofData>;

    /// Per-chain aggregation proofs arrive already final (op-succinct); the
    /// superblock "network proof" is their bundle, assembled into L1 calldata
    /// by the submitter.
    fn request_superblock_proof(
        &self,
        _superblock_number: SuperblockNumber,
        _last_superblock_hash: SuperblockHash,
        proofs: HashMap<ChainId, ProofData>,
    ) -> Result<Vec<ProofData>, Box<dyn std::error::Error + Send + Sync>> {
        Ok(proofs.into_values().collect())
    }
}

impl ethera_spec_sbcp::L1Publisher for OutboundSink {
    type SuperblockProof = Vec<ProofData>;

    fn publish_proof(&self, superblock_number: SuperblockNumber, proof: Vec<ProofData>) {
        self.enqueue(Outbound::SubmitProof {
            superblock_number,
            proofs: proof,
        });
    }
}
