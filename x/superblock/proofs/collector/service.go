package collector

import (
	"context"

	"github.com/ethereum/go-ethereum/common"

	"github.com/compose-network/publisher/x/superblock/proofs"
)

// Service coordinates per-rollup proof submissions for a superblock hash.
type Service interface {
	SubmitOpSuccinct(ctx context.Context, s proofs.Submission) error
	GetStatus(ctx context.Context, sbHash common.Hash) (proofs.Status, error)
	ListSubmissions(ctx context.Context, sbHash common.Hash) ([]proofs.Submission, error)
	UpdateStatus(ctx context.Context, sbHash common.Hash, mutate func(*proofs.Status)) error
	GetStats() map[string]interface{}
	CountProvingJobs(ctx context.Context) (int, error)
	ListQueuedJobs(ctx context.Context) ([]proofs.Status, error)
	DeleteSubmissions(ctx context.Context, sbHash common.Hash) error
}
