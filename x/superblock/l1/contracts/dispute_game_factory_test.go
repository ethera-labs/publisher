package contracts

import (
	"encoding/hex"
	"math/big"
	"testing"
	"time"

	"github.com/compose-network/publisher/x/superblock/proofs"
	"github.com/compose-network/publisher/x/superblock/store"
	"github.com/ethereum/go-ethereum/common"
)

func TestBuildPublishCalldataRootClaimIsSuperRootHash(t *testing.T) {
	binding, err := NewDisputeGameFactoryBinding("0x000000000000000000000000000000000000dEaD")
	if err != nil {
		t.Fatalf("binding error: %v", err)
	}

	parentBytes, _ := hex.DecodeString("0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef")

	sb := &store.Superblock{
		Number:     5,
		Slot:       7,
		ParentHash: common.BytesToHash(parentBytes),
		Timestamp:  time.Unix(1700000000, 0),
	}

	outputs := &proofs.SuperblockAggOutputs{
		SuperblockNumber:          "5",
		ParentSuperblockBatchHash: common.BytesToHash(parentBytes).Hex(),
		BootInfo: []proofs.BootInfo{
			{
				L1Head:           "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
				L2PreRoot:        "0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
				L2PostRoot:       "0xcccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc",
				L2BlockNumber:    100,
				RollupConfigHash: "0xdddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddd",
				ChainId:          11155111,
			},
		},
	}

	calldata, err := binding.BuildPublishWithProofCalldata(t.Context(), sb, []byte{0x01}, outputs)
	if err != nil {
		t.Fatalf("calldata error: %v", err)
	}

	method := binding.ABI().Methods["create"]
	unpacked, err := method.Inputs.Unpack(calldata[4:])
	if err != nil {
		t.Fatalf("unpack error: %v", err)
	}

	rootClaimBytes := unpacked[1].([32]byte)
	rootClaim := common.BytesToHash(rootClaimBytes[:])
	if (rootClaim == common.Hash{}) {
		t.Fatalf("expected non-zero root claim")
	}

	// rootClaim should be hashSuperRootProof, not sb.ParentHash
	expectedSRP := superRootProof{
		Version:   [1]byte{0x01},
		Timestamp: uint64(sb.Timestamp.Unix()),
		OutputRoots: []outputRootWithChainId{
			{
				ChainId: new(big.Int).SetUint64(11155111),
				Root:    common.HexToHash("0xcccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc"),
			},
		},
	}
	expected := hashSuperRootProof(expectedSRP)
	if rootClaim != expected {
		t.Fatalf("unexpected root claim: got %s want %s", rootClaim.Hex(), expected.Hex())
	}
}
