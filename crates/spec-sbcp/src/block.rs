use std::{
    fmt,
    ops::{Add, Sub},
};

use compose_spec::{BlockHash, PeriodId, StateRoot, SuperblockHash, SuperblockNumber};

/// Block number within a rollup chain.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default, PartialOrd, Ord)]
pub struct BlockNumber(pub u64);

impl BlockNumber {
    #[must_use]
    pub const fn new(v: u64) -> Self {
        Self(v)
    }
    #[must_use]
    pub const fn get(self) -> u64 {
        self.0
    }
}

impl From<u64> for BlockNumber {
    fn from(v: u64) -> Self {
        Self(v)
    }
}

impl From<BlockNumber> for u64 {
    fn from(v: BlockNumber) -> Self {
        v.0
    }
}

impl fmt::Display for BlockNumber {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(f)
    }
}

impl Add<u64> for BlockNumber {
    type Output = Self;
    fn add(self, rhs: u64) -> Self {
        Self(self.0 + rhs)
    }
}

impl Sub<u64> for BlockNumber {
    type Output = Self;
    fn sub(self, rhs: u64) -> Self {
        Self(self.0 - rhs)
    }
}

/// A block that is currently being built.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PendingBlock {
    pub number: BlockNumber,
    pub period_id: PeriodId,
    pub superblock_number: SuperblockNumber,
}

/// Header of a finalized L2 block.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct BlockHeader {
    pub number: BlockNumber,
    pub block_hash: BlockHash,
    pub state_root: StateRoot,
}

/// A sealed block header associated with a specific period and superblock.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SealedBlockHeader {
    pub block_header: BlockHeader,
    pub period_id: PeriodId,
    pub superblock_number: SuperblockNumber,
}

/// State that has been settled on L1.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct SettledState {
    pub block_header: BlockHeader,
    pub superblock_number: SuperblockNumber,
    pub superblock_hash: SuperblockHash,
}
