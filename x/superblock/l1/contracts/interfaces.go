package contracts

import (
	"context"
	"math/big"

	"github.com/compose-network/publisher/x/superblock/proofs"

	"github.com/compose-network/publisher/x/superblock/store"
	"github.com/ethereum/go-ethereum/common"
)

// Binding defines how to encode a publish-superblock call to a specific contract.
// All modern superblock submissions require proof verification.
type Binding interface {
	// Address returns the L1 contract address for publishing.
	Address() common.Address

	// BuildPublishWithProofCalldata encodes the calldata to publish the given superblock with proof.
	BuildPublishWithProofCalldata(
		ctx context.Context,
		superblock *store.Superblock,
		proof []byte,
		outputs *proofs.SuperblockAggOutputs,
	) ([]byte, error)

	// BuildGameCountCalldata encodes the calldata to read gameCount from the contract.
	BuildGameCountCalldata() ([]byte, error)

	// BuildFindLatestGamesCalldata encodes the calldata to call findLatestGames.
	BuildFindLatestGamesCalldata(start *big.Int, n *big.Int) ([]byte, error)
}
