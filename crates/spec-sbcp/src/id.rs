use compose_spec::{InstanceId, PeriodId, SequenceNumber, XtRequest};
use sha2::{Digest, Sha256};

/// Generates a deterministic instance ID by hashing:
/// `SHA256(period_id || seq || chain_id || tx_count || tx_length || tx_data ...)`.
#[must_use]
pub fn generate_instance_id(
    period_id: PeriodId,
    seq: SequenceNumber,
    xt_request: &XtRequest,
) -> InstanceId {
    let mut hasher = Sha256::new();

    hasher.update(period_id.get().to_be_bytes());
    hasher.update(seq.get().to_be_bytes());

    for req in &xt_request.transactions {
        hasher.update(req.chain_id.get().to_be_bytes());
        hasher.update((req.transactions.len() as u64).to_be_bytes());

        for data in &req.transactions {
            if !data.is_empty() {
                hasher.update((data.len() as u64).to_be_bytes());
                hasher.update(data);
            }
        }
    }

    let hash: [u8; 32] = hasher.finalize().into();
    InstanceId::new(hash)
}

#[cfg(test)]
mod tests {
    use compose_spec::{ChainId, TransactionRequest};

    use super::*;

    fn chain_req(chain: u64, txs: &[&[u8]]) -> TransactionRequest {
        TransactionRequest {
            chain_id: ChainId(chain),
            transactions: txs.iter().map(|t| t.to_vec()).collect(),
        }
    }

    fn make_xt_request(entries: Vec<TransactionRequest>) -> XtRequest {
        XtRequest {
            transactions: entries,
        }
    }

    #[test]
    fn stability_and_sensitivity() {
        let req1 = make_xt_request(vec![
            chain_req(1, &[&[0x01, 0x02]]),
            chain_req(2, &[&[0x03]]),
        ]);
        let req1_copy = make_xt_request(vec![
            chain_req(1, &[&[0x01, 0x02]]),
            chain_req(2, &[&[0x03]]),
        ]);

        let id_a = generate_instance_id(PeriodId(10), SequenceNumber(1), &req1);
        let id_a2 = generate_instance_id(PeriodId(10), SequenceNumber(1), &req1_copy);
        assert_eq!(id_a, id_a2, "same inputs must yield same ID");

        // Period change
        let id_b = generate_instance_id(PeriodId(11), SequenceNumber(1), &req1);
        assert_ne!(id_a, id_b);

        // Sequence change
        let id_c = generate_instance_id(PeriodId(10), SequenceNumber(2), &req1);
        assert_ne!(id_a, id_c);

        // Tx bytes change (single byte)
        let req_mut = make_xt_request(vec![
            chain_req(1, &[&[0x01, 0x02]]),
            chain_req(2, &[&[0xFF]]),
        ]);
        let id_d = generate_instance_id(PeriodId(10), SequenceNumber(1), &req_mut);
        assert_ne!(id_a, id_d);

        // Order matters
        let req_reordered = make_xt_request(vec![
            chain_req(2, &[&[0x03]]),
            chain_req(1, &[&[0x01, 0x02]]),
        ]);
        let id_e = generate_instance_id(PeriodId(10), SequenceNumber(1), &req_reordered);
        assert_ne!(id_a, id_e);

        // Empty tx bytes is not ignored
        let req_with_empty =
            make_xt_request(vec![chain_req(1, &[&[0x01, 0x02]]), chain_req(2, &[&[]])]);
        let req_omit = make_xt_request(vec![chain_req(1, &[&[0x01, 0x02]])]);
        let id_f = generate_instance_id(PeriodId(10), SequenceNumber(3), &req_with_empty);
        let id_g = generate_instance_id(PeriodId(10), SequenceNumber(3), &req_omit);
        assert_ne!(id_f, id_g, "empty tx bytes are not ignored in ID");
    }
}
