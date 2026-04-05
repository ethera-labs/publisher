package contracts

import (
	"context"
	_ "embed"
	"encoding/binary"
	"fmt"
	"math/big"
	"strings"

	"github.com/compose-network/publisher/x/superblock/proofs"
	"github.com/compose-network/publisher/x/superblock/store"
	"github.com/ethereum/go-ethereum/accounts/abi"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/crypto"
)

// DisputeGameFactory ABI JSON embedded at compile time
//
//go:embed abi/dispute_game_factory.json
var disputeGameFactoryABIJSON string

var (
	_ Binding = (*DisputeGameFactoryBinding)(nil)
)

const composeGameType uint32 = 5555

// DisputeGameFactoryBinding provides functionality to interact with DisputeGameFactory
// smart contracts for creating dispute games with superblock proofs.
type DisputeGameFactoryBinding struct {
	address common.Address
	abi     abi.ABI
}

// NewDisputeGameFactoryBinding creates a new DisputeGameFactoryBinding instance with
// the specified contract address. It parses the embedded ABI and validates
// the contract address.
func NewDisputeGameFactoryBinding(contractAddr string) (*DisputeGameFactoryBinding, error) {
	if strings.TrimSpace(contractAddr) == "" {
		return nil, fmt.Errorf("contract address cannot be empty")
	}

	parsedABI, err := abi.JSON(strings.NewReader(disputeGameFactoryABIJSON))
	if err != nil {
		return nil, fmt.Errorf("failed to parse DisputeGameFactory ABI: %w", err)
	}

	return &DisputeGameFactoryBinding{
		address: common.HexToAddress(contractAddr),
		abi:     parsedABI,
	}, nil
}

// Address returns the Ethereum address of the DisputeGameFactory contract.
func (b *DisputeGameFactoryBinding) Address() common.Address {
	return b.address
}

// ABI returns the parsed ABI of the DisputeGameFactory contract.
func (b *DisputeGameFactoryBinding) ABI() abi.ABI {
	return b.abi
}

// GameType returns the compose dispute game type identifier used when creating games.
func (b *DisputeGameFactoryBinding) GameType() uint32 {
	return composeGameType
}

// BuildPublishWithProofCalldata encodes a superblock and proof for DisputeGameFactory.create()
// according to the settlement layer specification.
func (b *DisputeGameFactoryBinding) BuildPublishWithProofCalldata(
	ctx context.Context,
	sb *store.Superblock,
	proof []byte,
	outputs *proofs.SuperblockAggOutputs,
) ([]byte, error) {
	if sb == nil {
		return nil, fmt.Errorf("superblock cannot be nil")
	}
	if len(proof) == 0 {
		return nil, fmt.Errorf("proof cannot be empty")
	}

	aggOutputs := b.toSuperblockAggregationOutputs(outputs)
	srp := b.buildSuperRootProof(sb, outputs)

	extraData, err := encodeExtraData(aggOutputs, srp, proof)
	if err != nil {
		return nil, fmt.Errorf("failed to encode extradata: %v", err)
	}

	rootClaim := hashSuperRootProof(srp)
	data, err := b.abi.Pack("create", composeGameType, rootClaim, extraData)
	if err != nil {
		return nil, fmt.Errorf("failed to pack DisputeGameFactory.create calldata: %w", err)
	}

	return data, nil
}

// BuildGameCountCalldata encodes the calldata to read gameCount from the contract.
func (b *DisputeGameFactoryBinding) BuildGameCountCalldata() ([]byte, error) {
	data, err := b.abi.Pack("gameCount")
	if err != nil {
		return nil, fmt.Errorf("failed to pack gameCount call: %w", err)
	}
	return data, nil
}

// BuildFindLatestGamesCalldata encodes the calldata to call findLatestGames.
func (b *DisputeGameFactoryBinding) BuildFindLatestGamesCalldata(start *big.Int, n *big.Int) ([]byte, error) {
	data, err := b.abi.Pack("findLatestGames", composeGameType, start, n)
	if err != nil {
		return nil, fmt.Errorf("failed to pack findLatestGames call: %w", err)
	}
	return data, nil
}

func encodeExtraData(aggOutputs superblockAggregationOutputs, srp superRootProof, proof []byte) ([]byte, error) {
	superblockType, _ := abi.NewType("tuple", "SuperblockAggregationOutputs", []abi.ArgumentMarshaling{
		{Name: "superblockNumber", Type: "uint256"},
		{Name: "parentSuperblockBatchHash", Type: "bytes32"},
		{Name: "bootInfo", Type: "tuple[]", Components: []abi.ArgumentMarshaling{
			{Name: "l1Head", Type: "bytes32"},
			{Name: "l2PreRoot", Type: "bytes32"},
			{Name: "l2PostRoot", Type: "bytes32"},
			{Name: "l2BlockNumber", Type: "uint64"},
			{Name: "rollupConfigHash", Type: "bytes32"},
		}},
	})

	superRootProofType, _ := abi.NewType("tuple", "SuperRootProof", []abi.ArgumentMarshaling{
		{Name: "version", Type: "bytes1"},
		{Name: "timestamp", Type: "uint64"},
		{Name: "outputRoots", Type: "tuple[]", Components: []abi.ArgumentMarshaling{
			{Name: "chainId", Type: "uint256"},
			{Name: "root", Type: "bytes32"},
		}},
	})

	bytesType, _ := abi.NewType("bytes", "", nil)

	arguments := abi.Arguments{
		{Type: superblockType},
		{Type: superRootProofType},
		{Type: bytesType},
	}

	packed, err := arguments.Pack(aggOutputs, srp, proof)
	if err != nil {
		return nil, err
	}

	return packed, nil
}

// buildSuperRootProof constructs a SuperRootProof from superblock and prover outputs.
func (b *DisputeGameFactoryBinding) buildSuperRootProof(
	sb *store.Superblock,
	outputs *proofs.SuperblockAggOutputs,
) superRootProof {
	var outputRoots []outputRootWithChainId
	if outputs != nil {
		for _, bi := range outputs.BootInfo {
			outputRoots = append(outputRoots, outputRootWithChainId{
				ChainId: new(big.Int).SetUint64(bi.ChainId),
				Root:    common.HexToHash(bi.L2PostRoot),
			})
		}
	}
	return superRootProof{
		Version:     [1]byte{0x01},
		Timestamp:   uint64(sb.Timestamp.Unix()),
		OutputRoots: outputRoots,
	}
}

// hashSuperRootProof mirrors Hashing.hashSuperRootProof from the OP Stack contracts.
// It computes keccak256(version || timestamp_be8 || chainId1_be32 || root1 || ...).
func hashSuperRootProof(srp superRootProof) common.Hash {
	buf := make([]byte, 0, 1+8+len(srp.OutputRoots)*64)
	buf = append(buf, srp.Version[0])

	ts := make([]byte, 8)
	binary.BigEndian.PutUint64(ts, srp.Timestamp)
	buf = append(buf, ts...)

	for _, or := range srp.OutputRoots {
		buf = append(buf, common.LeftPadBytes(or.ChainId.Bytes(), 32)...)
		buf = append(buf, or.Root[:]...)
	}

	return crypto.Keccak256Hash(buf)
}

// toSuperblockAggregationOutputs converts prover outputs to SuperblockAggregationOutputs
func (b *DisputeGameFactoryBinding) toSuperblockAggregationOutputs(
	outputs *proofs.SuperblockAggOutputs,
) superblockAggregationOutputs {
	var bootInfo []bootInfoStruct
	superblockNumber := new(big.Int)
	var parentSuperblockBatchHash common.Hash

	if outputs != nil {
		bootInfo = make([]bootInfoStruct, 0, len(outputs.BootInfo))
		for _, proverBootInfo := range outputs.BootInfo {
			bootInfo = append(bootInfo, bootInfoStruct{
				L1Head:           common.HexToHash(proverBootInfo.L1Head),
				L2PreRoot:        common.HexToHash(proverBootInfo.L2PreRoot),
				L2PostRoot:       common.HexToHash(proverBootInfo.L2PostRoot),
				L2BlockNumber:    proverBootInfo.L2BlockNumber,
				RollupConfigHash: common.HexToHash(proverBootInfo.RollupConfigHash),
			})
		}

		if outputs.SuperblockNumber != "" {
			superblockNumber.SetString(outputs.SuperblockNumber, 0)
		}

		parentSuperblockBatchHash = common.HexToHash(outputs.ParentSuperblockBatchHash)
	}

	return superblockAggregationOutputs{
		SuperblockNumber:          superblockNumber,
		ParentSuperblockBatchHash: parentSuperblockBatchHash,
		BootInfo:                  bootInfo,
	}
}
