use std::time::Duration;

mod primitives;
pub use primitives::{
    BlockHash, ChainId, EthAddress, InstanceId, PeriodId, SequenceNumber, SessionId, StateRoot,
    SuperblockHash, SuperblockNumber, TxHash,
};

mod instance;
pub use instance::{
    chains_from_request, clone_byte_slices, DecisionState, Instance, TransactionRequest, XtRequest,
};

/// Duration of a superblock period (10 Ethereum epochs = 10 * 32 * 12 seconds).
pub const PERIOD_DURATION: Duration = Duration::from_secs(10 * 32 * 12);

/// Allowed window (in number of periods) to submit a valid ZK proof for a superblock.
pub const PROOF_WINDOW: u64 = 24 * 7;
