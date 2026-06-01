use std::{collections::HashMap, sync::Mutex};

use compose_spec::{
    InstanceId, PeriodId, SequenceNumber, SuperblockHash, SuperblockNumber, XtRequest,
};
use thiserror::Error;
use tracing::{error, info};

use crate::block::{BlockHeader, BlockNumber, PendingBlock, SealedBlockHeader, SettledState};

/// Errors returned by [`Sequencer`] operations.
#[derive(Debug, Error)]
pub enum SequencerError {
    #[error("block number to be sealed does not match the current block number")]
    BlockSealMismatch,
    #[error("there is already an open block")]
    BlockAlreadyOpen,
    #[error("block number is not sequential")]
    BlockNotSequential,
    #[error("no pending block")]
    NoPendingBlock,
    #[error("there is already an active instance")]
    ActiveInstanceExists,
    #[error("no active instance")]
    NoActiveInstance,
    #[error("mismatched active instance ID")]
    ActiveInstanceMismatch,
    #[error("mismatched finalized state")]
    MismatchedFinalizedState,
    #[error("instance period ID does not match current block period ID")]
    PeriodIdMismatch,
    #[error("instance sequence number is not greater than last sequence number")]
    LowSequenceNumber,
}

/// Generates proofs for a rollup's contribution to a superblock.
pub trait SequencerProver: Send + Sync {
    /// If header is `None`, there is no sealed block for the period.
    fn request_proofs(
        &self,
        block_header: Option<&BlockHeader>,
        superblock_number: SuperblockNumber,
    ) -> Result<Vec<u8>, Box<dyn std::error::Error + Send + Sync>>;
}

/// Sends messages to the publisher (SP).
pub trait SequencerMessenger: Send + Sync {
    fn forward_request(
        &self,
        request: &XtRequest,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>>;
    fn send_proof(
        &self,
        period_id: PeriodId,
        superblock_number: SuperblockNumber,
        proof: Vec<u8>,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>>;
}

struct SequencerState {
    period_id: PeriodId,
    target_superblock_number: SuperblockNumber,
    pending_block: Option<PendingBlock>,
    active_instance_id: Option<InstanceId>,
    last_sequence_number: Option<SequenceNumber>,
    head: BlockNumber,
    sealed_block_head: HashMap<PeriodId, SealedBlockHeader>,
    settled_state: SettledState,
}

/// SBCP sequencer managing block building, instances, and settlement.
pub struct Sequencer<P: SequencerProver, M: SequencerMessenger> {
    inner: Mutex<SequencerState>,
    prover: P,
    messenger: M,
}

impl<P: SequencerProver, M: SequencerMessenger> std::fmt::Debug for Sequencer<P, M> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Sequencer").finish_non_exhaustive()
    }
}

impl<P: SequencerProver, M: SequencerMessenger> Sequencer<P, M> {
    pub fn new(
        prover: P,
        messenger: M,
        period_id: PeriodId,
        target_superblock: SuperblockNumber,
        settled_state: SettledState,
    ) -> Self {
        Self {
            inner: Mutex::new(SequencerState {
                period_id,
                target_superblock_number: target_superblock,
                pending_block: None,
                active_instance_id: None,
                last_sequence_number: None,
                head: settled_state.block_header.number,
                sealed_block_head: HashMap::new(),
                settled_state,
            }),
            prover,
            messenger,
        }
    }

    /// Forwards the request to the publisher.
    pub fn receive_xt_request(
        &self,
        request: &XtRequest,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        self.messenger.forward_request(request)
    }

    /// Starts a new period. Triggers settlement for the previous period if no block is pending.
    pub fn start_period(
        &self,
        period_id: PeriodId,
        target_superblock_number: SuperblockNumber,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let no_pending_block;
        {
            let mut state = self.inner.lock().unwrap();
            info!(
                new_period_id = period_id.get(),
                target_superblock_number = target_superblock_number.get(),
                "Starting new period"
            );
            state.period_id = period_id;
            state.target_superblock_number = target_superblock_number;
            state.last_sequence_number = None;
            no_pending_block = state.pending_block.is_none();
        }

        if no_pending_block {
            info!("No pending block, triggering settlement pipeline");
            return self.start_settlement(period_id - 1, target_superblock_number - 1);
        }

        info!("Started new period, but pending block exists, settlement pipeline will wait");
        Ok(())
    }

    fn start_settlement(
        &self,
        period_id: PeriodId,
        superblock_number: SuperblockNumber,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let header = {
            let state = self.inner.lock().unwrap();
            state
                .sealed_block_head
                .get(&period_id)
                .map(|s| s.block_header)
        };

        let proof = self
            .prover
            .request_proofs(header.as_ref(), superblock_number)?;

        self.messenger
            .send_proof(period_id, superblock_number, proof)
    }

    /// Begins a new L2 block.
    pub fn begin_block(&self, block_number: BlockNumber) -> Result<(), SequencerError> {
        let mut state = self.inner.lock().unwrap();

        if state.pending_block.is_some() {
            return Err(SequencerError::BlockAlreadyOpen);
        }

        if block_number != state.head + 1 {
            return Err(SequencerError::BlockNotSequential);
        }

        info!(new_block_number = block_number.get(), "Beginning block");

        state.pending_block = Some(PendingBlock {
            number: block_number,
            period_id: state.period_id,
            superblock_number: state.target_superblock_number,
        });
        Ok(())
    }

    /// Returns whether a local transaction is admissible right now.
    pub fn can_include_local_tx(&self) -> Result<bool, SequencerError> {
        let state = self.inner.lock().unwrap();
        if state.pending_block.is_none() {
            return Err(SequencerError::NoPendingBlock);
        }
        Ok(state.active_instance_id.is_none())
    }

    /// SCP start-up hook. Locks local tx inclusion.
    pub fn on_start_instance(
        &self,
        id: InstanceId,
        period_id: PeriodId,
        sequence_number: SequenceNumber,
    ) -> Result<(), SequencerError> {
        let mut state = self.inner.lock().unwrap();

        if state.pending_block.is_none() {
            return Err(SequencerError::NoPendingBlock);
        }
        if state.active_instance_id.is_some() {
            return Err(SequencerError::ActiveInstanceExists);
        }

        let pending = state.pending_block.as_ref().unwrap();
        if pending.period_id != period_id {
            return Err(SequencerError::PeriodIdMismatch);
        }

        if let Some(last_seq) = state.last_sequence_number {
            if sequence_number <= last_seq {
                return Err(SequencerError::LowSequenceNumber);
            }
        }

        state.last_sequence_number = Some(sequence_number);

        info!("Starting active instance, locking local tx inclusion");
        state.active_instance_id = Some(id);
        Ok(())
    }

    /// SCP decision hook. Unlocks local tx inclusion.
    pub fn on_decided_instance(&self, id: InstanceId) -> Result<(), SequencerError> {
        let mut state = self.inner.lock().unwrap();

        match state.active_instance_id {
            None => return Err(SequencerError::NoActiveInstance),
            Some(active_id) if active_id != id => {
                return Err(SequencerError::ActiveInstanceMismatch);
            }
            _ => {}
        }

        info!("Decided active instance, unlocking local tx inclusion");
        state.active_instance_id = None;
        Ok(())
    }

    /// Ends the current block. May trigger settlement for the previous period.
    pub fn end_block(
        &self,
        b: BlockHeader,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let should_start_settlement;
        let settlement_period;
        let settlement_superblock;

        {
            let mut state = self.inner.lock().unwrap();

            let Some(pending) = state.pending_block else {
                return Err(SequencerError::NoPendingBlock.into());
            };

            if pending.number != b.number {
                return Err(SequencerError::BlockSealMismatch.into());
            }

            if state.active_instance_id.is_some() {
                return Err(SequencerError::ActiveInstanceExists.into());
            }

            info!("Ending block");
            state.sealed_block_head.insert(
                pending.period_id,
                SealedBlockHeader {
                    block_header: b,
                    period_id: pending.period_id,
                    superblock_number: pending.superblock_number,
                },
            );

            should_start_settlement = pending.period_id < state.period_id;
            settlement_period = state.period_id - 1;
            settlement_superblock = state.target_superblock_number - 1;

            state.pending_block = None;
            state.head = b.number;
        }

        if should_start_settlement {
            info!("Period was ahead of sealed block, triggering settlement pipeline");
            return self.start_settlement(settlement_period, settlement_superblock);
        }
        Ok(())
    }

    /// Advances the settled state when L1 confirms a new superblock.
    pub fn advance_settled_state(&self, settled: SettledState) {
        let mut state = self.inner.lock().unwrap();
        if settled.superblock_number <= state.settled_state.superblock_number {
            return;
        }
        info!(
            new_settled_superblock_number = settled.superblock_number.get(),
            "Advancing settled state"
        );
        state.settled_state = settled;
    }

    /// Rolls back to the settled state, discarding blocks beyond the finalized superblock.
    pub fn rollback(
        &self,
        superblock_number: SuperblockNumber,
        superblock_hash: SuperblockHash,
        current_period_id: PeriodId,
    ) -> Result<BlockHeader, SequencerError> {
        let mut state = self.inner.lock().unwrap();

        if superblock_number != state.settled_state.superblock_number
            || superblock_hash != state.settled_state.superblock_hash
        {
            return Err(SequencerError::MismatchedFinalizedState);
        }

        info!(
            rollback_superblock_number = superblock_number.get(),
            "Rolling back to settled state"
        );

        // Discard blocks with superblock number greater than the finalized one
        let finalized_sb = state.settled_state.superblock_number;
        state
            .sealed_block_head
            .retain(|_, sb| sb.superblock_number <= finalized_sb);

        state.pending_block = None;
        state.active_instance_id = None;
        state.head = state.settled_state.block_header.number;

        state.period_id = current_period_id;
        state.target_superblock_number = state.settled_state.superblock_number + 1;

        Ok(state.settled_state.block_header)
    }

    // Test accessors
    #[cfg(test)]
    fn period_id(&self) -> PeriodId {
        self.inner.lock().unwrap().period_id
    }

    #[cfg(test)]
    fn target_superblock_number(&self) -> SuperblockNumber {
        self.inner.lock().unwrap().target_superblock_number
    }

    #[cfg(test)]
    fn head(&self) -> BlockNumber {
        self.inner.lock().unwrap().head
    }

    #[cfg(test)]
    fn pending_block(&self) -> Option<PendingBlock> {
        self.inner.lock().unwrap().pending_block
    }

    #[cfg(test)]
    fn active_instance_id(&self) -> Option<InstanceId> {
        self.inner.lock().unwrap().active_instance_id
    }

    #[cfg(test)]
    fn sealed_block_head(&self, period: PeriodId) -> Option<SealedBlockHeader> {
        self.inner
            .lock()
            .unwrap()
            .sealed_block_head
            .get(&period)
            .copied()
    }

    #[cfg(test)]
    fn settled_state(&self) -> SettledState {
        self.inner.lock().unwrap().settled_state
    }

    #[cfg(test)]
    fn insert_sealed_block(&self, period: PeriodId, sb: SealedBlockHeader) {
        self.inner
            .lock()
            .unwrap()
            .sealed_block_head
            .insert(period, sb);
    }

    #[cfg(test)]
    fn set_active_instance(&self, id: Option<InstanceId>) {
        self.inner.lock().unwrap().active_instance_id = id;
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use compose_spec::SuperblockHash;

    use super::*;

    fn mk_header(n: u64) -> BlockHeader {
        BlockHeader {
            number: BlockNumber(n),
            ..Default::default()
        }
    }

    fn mk_settled(sb: u64, head: u64) -> SettledState {
        SettledState {
            block_header: mk_header(head),
            superblock_number: SuperblockNumber(sb),
            superblock_hash: SuperblockHash([1; 32]),
        }
    }

    #[derive(Debug, Default)]
    struct FakeSeqProver {
        calls: Mutex<Vec<(Option<BlockHeader>, SuperblockNumber)>>,
        next_proof: Mutex<Vec<u8>>,
    }

    impl SequencerProver for Arc<FakeSeqProver> {
        fn request_proofs(
            &self,
            hdr: Option<&BlockHeader>,
            sb: SuperblockNumber,
        ) -> Result<Vec<u8>, Box<dyn std::error::Error + Send + Sync>> {
            self.calls.lock().unwrap().push((hdr.copied(), sb));
            Ok(self.next_proof.lock().unwrap().clone())
        }
    }

    #[derive(Debug, Default)]
    struct FakeSeqMessenger {
        requests: Mutex<Vec<XtRequest>>,
        proofs: Mutex<Vec<(PeriodId, SuperblockNumber, Vec<u8>)>>,
    }

    impl SequencerMessenger for Arc<FakeSeqMessenger> {
        fn forward_request(
            &self,
            request: &XtRequest,
        ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
            self.requests.lock().unwrap().push(request.clone());
            Ok(())
        }
        fn send_proof(
            &self,
            period_id: PeriodId,
            superblock_number: SuperblockNumber,
            proof: Vec<u8>,
        ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
            self.proofs
                .lock()
                .unwrap()
                .push((period_id, superblock_number, proof));
            Ok(())
        }
    }

    type TestSequencer = Sequencer<Arc<FakeSeqProver>, Arc<FakeSeqMessenger>>;

    fn new_seq_for_test(
        period: u64,
        target: u64,
        settled: SettledState,
    ) -> (TestSequencer, Arc<FakeSeqProver>, Arc<FakeSeqMessenger>) {
        let prover = Arc::new(FakeSeqProver::default());
        let messenger = Arc::new(FakeSeqMessenger::default());
        let seq = Sequencer::new(
            Arc::clone(&prover),
            Arc::clone(&messenger),
            PeriodId(period),
            SuperblockNumber(target),
            settled,
        );
        (seq, prover, messenger)
    }

    #[test]
    fn new_sequencer_initial_state() {
        let settled = mk_settled(4, 100);
        let (s, _, _) = new_seq_for_test(10, 11, settled);

        assert_eq!(s.period_id(), PeriodId(10));
        assert_eq!(s.target_superblock_number(), SuperblockNumber(11));
        assert_eq!(s.head(), BlockNumber(100));
        assert!(s.pending_block().is_none());
        assert!(s.active_instance_id().is_none());
    }

    #[test]
    fn begin_block_ok_and_errors() {
        let (s, _, _) = new_seq_for_test(5, 6, mk_settled(3, 10));

        // OK path
        s.begin_block(BlockNumber(11)).unwrap();
        let pending = s.pending_block().unwrap();
        assert_eq!(pending.number, BlockNumber(11));
        assert_eq!(pending.period_id, PeriodId(5));
        assert_eq!(pending.superblock_number, SuperblockNumber(6));

        // Already open
        let err = s.begin_block(BlockNumber(12)).unwrap_err();
        assert!(matches!(err, SequencerError::BlockAlreadyOpen));

        // Seal to clear pending
        s.end_block(mk_header(11)).unwrap();

        // Not sequential
        let err = s.begin_block(BlockNumber(13)).unwrap_err();
        assert!(matches!(err, SequencerError::BlockNotSequential));
    }

    #[test]
    fn can_include_local_tx_and_instance_hooks() {
        let (s, _, _) = new_seq_for_test(7, 8, mk_settled(2, 20));

        // No pending block -> error
        let err = s.can_include_local_tx().unwrap_err();
        assert!(matches!(err, SequencerError::NoPendingBlock));

        s.begin_block(BlockNumber(21)).unwrap();

        // No active instance -> can include
        assert!(s.can_include_local_tx().unwrap());

        // Start instance -> blocked
        let id = InstanceId([1; 32]);
        s.on_start_instance(id, PeriodId(7), SequenceNumber(1))
            .unwrap();
        assert!(!s.can_include_local_tx().unwrap());

        // Wrong decided id -> mismatch
        let err = s.on_decided_instance(InstanceId([2; 32])).unwrap_err();
        assert!(matches!(err, SequencerError::ActiveInstanceMismatch));

        // Correct decided id -> unblocks
        s.on_decided_instance(id).unwrap();
        assert!(s.can_include_local_tx().unwrap());

        // No active instance now
        let err = s.on_decided_instance(id).unwrap_err();
        assert!(matches!(err, SequencerError::NoActiveInstance));
    }

    #[test]
    fn end_block_seals_and_updates_head() {
        let (s, _, _) = new_seq_for_test(3, 4, mk_settled(1, 30));

        s.begin_block(BlockNumber(31)).unwrap();

        // Seal mismatch
        let err = s.end_block(mk_header(32));
        assert!(err.is_err());

        // Seal ok
        s.end_block(mk_header(31)).unwrap();
        assert!(s.pending_block().is_none());
        assert_eq!(s.head(), BlockNumber(31));

        let sb = s.sealed_block_head(PeriodId(3)).unwrap();
        assert_eq!(sb.block_header.number, BlockNumber(31));
    }

    #[test]
    fn end_block_rejects_active_instance() {
        let (s, _, _) = new_seq_for_test(3, 4, mk_settled(1, 30));

        s.begin_block(BlockNumber(31)).unwrap();
        let id = InstanceId([9; 32]);
        s.on_start_instance(id, PeriodId(3), SequenceNumber(1))
            .unwrap();

        let err = s.end_block(mk_header(31));
        assert!(err.is_err());

        s.on_decided_instance(id).unwrap();
        s.end_block(mk_header(31)).unwrap();
    }

    #[test]
    fn end_block_triggers_prev_period_settlement() {
        let (s, p, messenger) = new_seq_for_test(9, 10, mk_settled(5, 40));
        *p.next_proof.lock().unwrap() = b"seq-proof".to_vec();

        s.begin_block(BlockNumber(41)).unwrap();

        // Period rolls to 10 while block is open
        s.start_period(PeriodId(10), SuperblockNumber(11)).unwrap();
        assert!(p.calls.lock().unwrap().is_empty());

        // Seal the block -> triggers settlement for period 9
        s.end_block(mk_header(41)).unwrap();
        let calls = p.calls.lock().unwrap();
        assert_eq!(calls.len(), 1);
        assert!(calls[0].0.is_some());
        assert_eq!(calls[0].0.unwrap().number, BlockNumber(41));
        assert_eq!(calls[0].1, SuperblockNumber(10));
        drop(calls);

        let proofs = messenger.proofs.lock().unwrap();
        assert_eq!(proofs.len(), 1);
        assert_eq!(proofs[0].0, PeriodId(9));
        assert_eq!(proofs[0].1, SuperblockNumber(10));
        assert_eq!(proofs[0].2, b"seq-proof".to_vec());
    }

    #[test]
    fn start_period_triggers_immediate_settlement_when_no_pending() {
        let (s, p, messenger) = new_seq_for_test(10, 11, mk_settled(6, 50));
        *p.next_proof.lock().unwrap() = b"seq-proof".to_vec();

        // Case 1: no sealed block for previous period -> None header
        s.start_period(PeriodId(11), SuperblockNumber(12)).unwrap();
        let calls = p.calls.lock().unwrap();
        assert_eq!(calls.len(), 1);
        assert!(calls[0].0.is_none());
        assert_eq!(calls[0].1, SuperblockNumber(11));
        drop(calls);

        let proofs = messenger.proofs.lock().unwrap();
        assert_eq!(proofs.len(), 1);
        assert_eq!(proofs[0].0, PeriodId(10));
        assert_eq!(proofs[0].1, SuperblockNumber(11));
        assert_eq!(proofs[0].2, b"seq-proof".to_vec());
        drop(proofs);

        // Case 2: add sealed block for previous period, then start next period
        *p.calls.lock().unwrap() = vec![];
        *messenger.proofs.lock().unwrap() = vec![];

        s.insert_sealed_block(
            PeriodId(11),
            SealedBlockHeader {
                block_header: mk_header(51),
                period_id: PeriodId(11),
                superblock_number: SuperblockNumber(12),
            },
        );

        s.start_period(PeriodId(12), SuperblockNumber(13)).unwrap();
        let calls = p.calls.lock().unwrap();
        assert_eq!(calls.len(), 1);
        assert!(calls[0].0.is_some());
        assert_eq!(calls[0].0.unwrap().number, BlockNumber(51));
        assert_eq!(calls[0].1, SuperblockNumber(12));
        drop(calls);

        let proofs = messenger.proofs.lock().unwrap();
        assert_eq!(proofs.len(), 1);
        assert_eq!(proofs[0].0, PeriodId(11));
        assert_eq!(proofs[0].1, SuperblockNumber(12));
        assert_eq!(proofs[0].2, b"seq-proof".to_vec());
    }

    #[test]
    fn start_period_active_instance_does_not_defer() {
        let (s, p, _) = new_seq_for_test(2, 3, mk_settled(1, 10));
        s.set_active_instance(Some(InstanceId([1; 32])));

        s.start_period(PeriodId(3), SuperblockNumber(4)).unwrap();
        assert_eq!(p.calls.lock().unwrap().len(), 1);
    }

    #[test]
    fn receive_xt_request_forwards_to_publisher() {
        let (s, _, messenger) = new_seq_for_test(4, 5, mk_settled(2, 10));
        let req = XtRequest {
            transactions: vec![
                compose_spec::TransactionRequest {
                    chain_id: compose_spec::ChainId(1),
                    transactions: vec![b"a".to_vec()],
                },
                compose_spec::TransactionRequest {
                    chain_id: compose_spec::ChainId(2),
                    transactions: vec![b"b".to_vec()],
                },
            ],
        };

        s.receive_xt_request(&req).unwrap();
        let requests = messenger.requests.lock().unwrap();
        assert_eq!(requests.len(), 1);
        assert_eq!(requests[0], req);
    }

    #[test]
    fn advance_settled_state_monotonic() {
        let (s, _, _) = new_seq_for_test(1, 2, mk_settled(1, 5));

        // No update for same number
        s.advance_settled_state(mk_settled(1, 5));
        assert_eq!(s.settled_state().superblock_number, SuperblockNumber(1));

        // Advance forward
        s.advance_settled_state(mk_settled(2, 6));
        assert_eq!(s.settled_state().superblock_number, SuperblockNumber(2));
    }

    #[test]
    fn rollback_rejects_if_mismatch() {
        let settled = mk_settled(4, 100);
        let (s, _, _) = new_seq_for_test(9, 10, settled);

        let err = s
            .rollback(SuperblockNumber(5), SuperblockHash([9; 32]), PeriodId(9))
            .unwrap_err();
        assert!(matches!(err, SequencerError::MismatchedFinalizedState));
    }

    #[test]
    fn rollback_discards_newer_and_resets() {
        let settled = mk_settled(4, 100);
        let (s, _, _) = new_seq_for_test(9, 10, settled);

        // Seed sealed blocks
        s.insert_sealed_block(
            PeriodId(8),
            SealedBlockHeader {
                block_header: mk_header(90),
                period_id: PeriodId(8),
                superblock_number: SuperblockNumber(3),
            },
        );
        s.insert_sealed_block(
            PeriodId(9),
            SealedBlockHeader {
                block_header: mk_header(95),
                period_id: PeriodId(9),
                superblock_number: SuperblockNumber(4),
            },
        );
        s.insert_sealed_block(
            PeriodId(10),
            SealedBlockHeader {
                block_header: mk_header(99),
                period_id: PeriodId(10),
                superblock_number: SuperblockNumber(5),
            },
        );

        // Open block and set active instance
        s.begin_block(BlockNumber(101)).unwrap();
        let id = InstanceId([7; 32]);
        s.on_start_instance(id, PeriodId(9), SequenceNumber(1))
            .unwrap();

        let head = s
            .rollback(SuperblockNumber(4), settled.superblock_hash, PeriodId(12))
            .unwrap();

        assert_eq!(head.number, BlockNumber(100));
        assert!(s.pending_block().is_none());
        assert!(s.active_instance_id().is_none());

        // Blocks with SB > 4 removed
        assert!(s.sealed_block_head(PeriodId(10)).is_none());
        assert!(s.sealed_block_head(PeriodId(9)).is_some());

        // Period and target updated
        assert_eq!(s.period_id(), PeriodId(12));
        assert_eq!(s.target_superblock_number(), SuperblockNumber(5));
    }

    #[test]
    fn on_start_instance_validations() {
        // Rejects when no pending block
        {
            let (s, _, _) = new_seq_for_test(3, 4, mk_settled(1, 30));
            let err = s
                .on_start_instance(InstanceId([1; 32]), PeriodId(3), SequenceNumber(1))
                .unwrap_err();
            assert!(matches!(err, SequencerError::NoPendingBlock));
        }

        // Rejects when period mismatch (higher)
        {
            let (s, _, _) = new_seq_for_test(5, 6, mk_settled(2, 40));
            s.begin_block(BlockNumber(41)).unwrap();
            let err = s
                .on_start_instance(InstanceId([2; 32]), PeriodId(6), SequenceNumber(1))
                .unwrap_err();
            assert!(matches!(err, SequencerError::PeriodIdMismatch));
        }

        // Rejects when period mismatch (lower)
        {
            let (s, _, _) = new_seq_for_test(7, 8, mk_settled(3, 50));
            s.begin_block(BlockNumber(51)).unwrap();
            let err = s
                .on_start_instance(InstanceId([3; 32]), PeriodId(6), SequenceNumber(1))
                .unwrap_err();
            assert!(matches!(err, SequencerError::PeriodIdMismatch));
        }

        // Enforces strictly increasing sequence numbers
        {
            let (s, _, _) = new_seq_for_test(9, 10, mk_settled(4, 60));
            s.begin_block(BlockNumber(61)).unwrap();

            // First instance with seq=1
            s.on_start_instance(InstanceId([4; 32]), PeriodId(9), SequenceNumber(1))
                .unwrap();

            // Cannot start another while active
            let err = s
                .on_start_instance(InstanceId([5; 32]), PeriodId(9), SequenceNumber(1))
                .unwrap_err();
            assert!(matches!(err, SequencerError::ActiveInstanceExists));

            // Finish current
            s.on_decided_instance(InstanceId([4; 32])).unwrap();

            // Reject same sequence number
            let err = s
                .on_start_instance(InstanceId([6; 32]), PeriodId(9), SequenceNumber(1))
                .unwrap_err();
            assert!(matches!(err, SequencerError::LowSequenceNumber));

            // Accept higher
            s.on_start_instance(InstanceId([7; 32]), PeriodId(9), SequenceNumber(2))
                .unwrap();
            s.on_decided_instance(InstanceId([7; 32])).unwrap();

            // Reject lower
            let err = s
                .on_start_instance(InstanceId([6; 32]), PeriodId(9), SequenceNumber(1))
                .unwrap_err();
            assert!(matches!(err, SequencerError::LowSequenceNumber));
        }
    }
}
