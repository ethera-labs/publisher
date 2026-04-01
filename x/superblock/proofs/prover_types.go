package proofs

import "context"

type ProverClient interface {
	RequestProof(ctx context.Context, job ProofJobInput) (jobID string, err error)
	GetStatus(ctx context.Context, jobID string) (ProofJobStatus, error)
}

// ProofJobInput bundles the prover payload and selected proof type.
type ProofJobInput struct {
	ProofType string                `json:"proof_type"`
	Input     SuperblockProverInput `json:"input"`
}

// BootInfo represents boot information for a rollup (mirrors Rust BootInfo).
type BootInfo struct {
	L1Head           string `json:"l1_head"`            // hex string
	L2PreRoot        string `json:"l2_pre_root"`        // hex string
	L2PostRoot       string `json:"l2_post_root"`       // hex string
	L2BlockNumber    uint64 `json:"l2_block_number"`
	RollupConfigHash string `json:"rollup_config_hash"` // hex string
	ChainId          uint64 `json:"chain_id,omitempty"` // populated by publisher from submissions
}

// SuperblockAggOutputs represents serializable superblock aggregation outputs (mirrors Rust SuperblockAggOutputs).
type SuperblockAggOutputs struct {
	SuperblockNumber          string     `json:"superblock_number"`            // U256 as hex string
	ParentSuperblockBatchHash string     `json:"parent_superblock_batch_hash"` // hex string
	BootInfo                  []BootInfo `json:"boot_info"`
}

// ProofJobStatus represents the prover's reported state.
type ProofJobStatus struct {
	Status               string `json:"status"`
	Proof                []byte `json:"proof,omitempty"`
	ProvingTimeMS        *uint64
	Cycles               *uint64
	SuperblockAggOutputs *SuperblockAggOutputs `json:"superblock_agg_outputs,omitempty"`
	Commitment           *string               `json:"commitment"`
}

// RollupStateTransition represents state transition information for a single rollup.
type RollupStateTransition struct {
	RollupConfigHash []int `json:"rollup_config_hash"` // bytes32 - Uniquely identifies a rollup
	L2PreRoot        []int `json:"l2_pre_root"`        // bytes32 - Pre-execution state root
	L2PostRoot       []int `json:"l2_post_root"`       // bytes32 - Post-execution state root
	L2BlockNumber    []int `json:"l2_block_number"`    // bytes32 - New L2 block number
}

// SuperblockBatch represents a superblock batch structure.
//
//nolint:lll // ok
type SuperblockBatch struct {
	SuperblockNumber          uint64                  `json:"superblock_number"`            // uint256 - Sequential superblock number
	ParentSuperblockBatchHash []int                   `json:"parent_superblock_batch_hash"` // bytes32 - Hash of the previous superblock
	RollupSt                  []RollupStateTransition `json:"rollup_st"`                    // RollupStateTransition[] - State transition information about each rollup
}

// SuperblockProverInput mirrors the Rust prover input schema.
type SuperblockProverInput struct {
	PreviousBatch     SuperblockBatch        `json:"previous_batch"`
	NewBatch          SuperblockBatch        `json:"new_batch"`
	AggregationProofs []AggregationProofData `json:"aggregation_proofs"`
}

// ProverSuperblock matches the prover's expected Superblock representation.
type ProverSuperblock struct {
	Number            uint64          `json:"number"`
	Slot              uint64          `json:"slot"`
	ParentHash        []byte          `json:"parent_hash"`
	Hash              []byte          `json:"hash"`
	MerkleRoot        []byte          `json:"merkle_root"`
	Timestamp         uint64          `json:"timestamp"`
	L2Blocks          []ProverL2Block `json:"l2_blocks"`
	IncludedXTs       [][]byte        `json:"included_xts"`
	L1TransactionHash []byte          `json:"l1_transaction_hash,omitempty"`
}

// ProverL2Block mirrors the prover-facing L2 block representation.
type ProverL2Block struct {
	Slot            uint64   `json:"slot"`
	ChainID         []byte   `json:"chain_id"`
	BlockNumber     uint64   `json:"block_number"`
	BlockHash       []byte   `json:"block_hash"`
	ParentBlockHash []byte   `json:"parent_block_hash"`
	IncludedXTs     [][]byte `json:"included_xts"`
	Block           []byte   `json:"block"`
}
