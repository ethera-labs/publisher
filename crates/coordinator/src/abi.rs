//! Typed bindings for the `ComposeL2OutputOracle` L1 settlement contract.
//!
//! `proposeL2Output` carries the proof payload as opaque `bytes`, so the struct
//! layout below must match the contract's `_extraData` decoding exactly.

use alloy::sol;

use crate::proof_types::AggregationOutputs;

sol! {
    struct SuperblockAggregationOutputs {
        uint256 superblockNumber;
        bytes32 parentSuperblockBatchHash;
        BootInfoStruct[] bootInfo;
    }

    struct BootInfoStruct {
        bytes32 l1Head;
        bytes32 l2PreRoot;
        bytes32 l2PostRoot;
        uint64 l2BlockNumber;
        bytes32 rollupConfigHash;
    }

    #[sol(rpc)]
    interface IComposeL2OutputOracle {
        function proposeL2Output(
            bytes32 _outputRoot,
            bytes32 _l1Hash,
            bytes memory _extraData
        ) external;

        function latestSuperblockNumber() external view returns (uint256);
        function getSuperblockHash(uint256 _superblockNumber) external view returns (bytes32);
    }
}

impl From<&AggregationOutputs> for BootInfoStruct {
    fn from(o: &AggregationOutputs) -> Self {
        Self {
            l1Head: o.l1_head,
            l2PreRoot: o.l2_pre_root,
            l2PostRoot: o.l2_post_root,
            l2BlockNumber: o.l2_block_number,
            rollupConfigHash: o.rollup_config_hash,
        }
    }
}
