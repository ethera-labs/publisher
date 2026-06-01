use compose_spec::{ChainId, InstanceId, PeriodId, SuperblockNumber};

use crate::messages::{StartInstance, StartPeriod, TransactionRequest, XtRequest};

impl From<&compose_spec::TransactionRequest> for TransactionRequest {
    fn from(r: &compose_spec::TransactionRequest) -> Self {
        Self {
            chain_id: r.chain_id.get(),
            transaction: r.transactions.clone(),
        }
    }
}

impl From<&TransactionRequest> for compose_spec::TransactionRequest {
    fn from(r: &TransactionRequest) -> Self {
        Self {
            chain_id: ChainId(r.chain_id),
            transactions: r.transaction.clone(),
        }
    }
}

impl From<&compose_spec::XtRequest> for XtRequest {
    fn from(r: &compose_spec::XtRequest) -> Self {
        Self {
            transaction_requests: r.transactions.iter().map(Into::into).collect(),
        }
    }
}

impl From<&XtRequest> for compose_spec::XtRequest {
    fn from(r: &XtRequest) -> Self {
        Self {
            transactions: r.transaction_requests.iter().map(Into::into).collect(),
        }
    }
}

impl From<&compose_spec::Instance> for StartInstance {
    fn from(i: &compose_spec::Instance) -> Self {
        Self {
            instance_id: i.id.as_bytes().to_vec(),
            period_id: i.period_id.get(),
            sequence_number: i.sequence_number.get(),
            xt_request: Some((&i.xt_request).into()),
        }
    }
}

impl TryFrom<&StartInstance> for compose_spec::Instance {
    type Error = &'static str;

    fn try_from(si: &StartInstance) -> Result<Self, Self::Error> {
        let id_bytes: [u8; 32] = si
            .instance_id
            .as_slice()
            .try_into()
            .map_err(|_| "instance_id must be 32 bytes")?;

        let xt_request = si.xt_request.as_ref().map(Into::into).unwrap_or_default();

        Ok(Self {
            id: InstanceId::new(id_bytes),
            period_id: PeriodId(si.period_id),
            sequence_number: compose_spec::SequenceNumber(si.sequence_number),
            xt_request,
        })
    }
}

impl From<&StartPeriod> for (PeriodId, SuperblockNumber) {
    fn from(sp: &StartPeriod) -> Self {
        (
            PeriodId(sp.period_id),
            SuperblockNumber(sp.superblock_number),
        )
    }
}

#[cfg(test)]
mod tests {
    use compose_spec::{ChainId, InstanceId, PeriodId};

    use crate::messages::*;

    #[test]
    fn xt_request_domain_roundtrip() {
        let domain = compose_spec::XtRequest {
            transactions: vec![
                compose_spec::TransactionRequest {
                    chain_id: ChainId(1),
                    transactions: vec![b"tx1".to_vec(), b"tx2".to_vec()],
                },
                compose_spec::TransactionRequest {
                    chain_id: ChainId(2),
                    transactions: vec![b"tx3".to_vec()],
                },
            ],
        };

        let proto: XtRequest = (&domain).into();
        let back: compose_spec::XtRequest = (&proto).into();
        assert_eq!(domain, back);
    }

    #[test]
    fn start_instance_domain_roundtrip() {
        let domain = compose_spec::Instance {
            id: InstanceId([42; 32]),
            period_id: PeriodId(7),
            sequence_number: compose_spec::SequenceNumber(3),
            xt_request: compose_spec::XtRequest {
                transactions: vec![compose_spec::TransactionRequest {
                    chain_id: ChainId(1),
                    transactions: vec![b"a".to_vec()],
                }],
            },
        };

        let proto: StartInstance = (&domain).into();
        let back: compose_spec::Instance = (&proto).try_into().unwrap();
        assert_eq!(domain, back);
    }
}
