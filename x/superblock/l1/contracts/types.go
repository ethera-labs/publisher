package contracts

import (
	"math/big"
)

// superblockAggregationOutputs matches the Solidity struct in ComposeDisputeGame.sol
type superblockAggregationOutputs struct {
	SuperblockNumber          *big.Int         `abi:"superblockNumber"`
	ParentSuperblockBatchHash [32]byte         `abi:"parentSuperblockBatchHash"`
	BootInfo                  []bootInfoStruct `abi:"bootInfo"`
}

// bootInfoStruct matches the Solidity struct in ComposeDisputeGame.sol
type bootInfoStruct struct {
	L1Head           [32]byte `abi:"l1Head"`
	L2PreRoot        [32]byte `abi:"l2PreRoot"`
	L2PostRoot       [32]byte `abi:"l2PostRoot"`
	L2BlockNumber    uint64   `abi:"l2BlockNumber"`
	RollupConfigHash [32]byte `abi:"rollupConfigHash"`
}

// superRootProof matches Types.SuperRootProof in the OP Stack
type superRootProof struct {
	Version     [1]byte                 `abi:"version"`
	Timestamp   uint64                  `abi:"timestamp"`
	OutputRoots []outputRootWithChainId `abi:"outputRoots"`
}

// outputRootWithChainId matches Types.OutputRootWithChainId in the OP Stack
type outputRootWithChainId struct {
	ChainId *big.Int `abi:"chainId"`
	Root    [32]byte `abi:"root"`
}
