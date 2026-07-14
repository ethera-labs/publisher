//! Typed L1 settlement bindings.

use alloy::sol;

use crate::proof_types::AggregationOutputs;

pub const COMPOSE_GAME_TYPE: u32 = 5555;

sol! {
    #[derive(Debug)]
    struct SuperblockAggregationOutputs {
        uint256 superblockNumber;
        bytes32 parentSuperblockBatchHash;
        BootInfoStruct[] bootInfo;
    }

    #[derive(Debug)]
    struct BootInfoStruct {
        bytes32 l1Head;
        bytes32 l2PreRoot;
        bytes32 l2PostRoot;
        uint64 l2BlockNumber;
        bytes32 rollupConfigHash;
    }

    #[derive(Debug)]
    struct OutputRootWithChainId {
        uint256 chainId;
        bytes32 root;
    }

    #[derive(Debug)]
    struct SuperRootProof {
        bytes1 version;
        uint64 timestamp;
        OutputRootWithChainId[] outputRoots;
    }

    #[derive(Debug)]
    struct GameSearchResult {
        uint256 index;
        bytes32 metadata;
        uint64 timestamp;
        bytes32 rootClaim;
        bytes extraData;
    }

    #[sol(rpc)]
    interface IDisputeGameFactory {
        function gameCount() external view returns (uint256);

        function findLatestGames(uint32 _gameType, uint256 _start, uint256 _n)
            external view returns (GameSearchResult[] games_);

        function create(
            uint32 _gameType,
            bytes32 _rootClaim,
            bytes calldata _extraData
        ) external payable returns (address proxy_);

        function initBonds(uint32 _gameType) external view returns (uint256);
    }

    #[sol(rpc)]
    interface IComposeAnchorStateRegistry {
        function getAnchorRoot() external view returns (bytes32 root_, uint256 l2SequenceNumber_);

        function anchorGame() external view returns (address);
    }

    #[sol(rpc)]
    interface IDisputeGame {
        function extraData() external view returns (bytes);
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
