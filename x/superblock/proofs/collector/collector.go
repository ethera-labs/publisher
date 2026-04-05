package collector

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/compose-network/publisher/x/superblock/proofs"
	"github.com/ethereum/go-ethereum/common"
	"github.com/rs/zerolog"
)

var _ Service = (*ProofCollector)(nil)

// ProofCollector implements an in-memory collector; suitable for tests and single-instance deployments.
type ProofCollector struct {
	mu       sync.RWMutex
	bySB     map[string]map[uint32]proofs.Submission
	statuses map[string]proofs.Status
	log      zerolog.Logger
	cancel   context.CancelFunc
}

// New returns a configured ProofCollector collector.
func New(ctx context.Context, log zerolog.Logger) *ProofCollector {
	logger := log.With().Str("component", "proof-collector").Logger()

	ctx, cancel := context.WithCancel(ctx)
	m := &ProofCollector{
		bySB:     make(map[string]map[uint32]proofs.Submission),
		statuses: make(map[string]proofs.Status),
		log:      logger,
		cancel:   cancel,
	}

	logger.Info().Msg("ProofCollector proof collector initialized")

	go m.statsLogger(ctx)

	return m
}

func (m *ProofCollector) SubmitOpSuccinct(_ context.Context, s proofs.Submission) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if s.SuperblockHash == (common.Hash{}) {
		m.log.Error().Msg("submission rejected: superblock hash is required")
		return fmt.Errorf("superblock hash is required")
	}
	if s.ReceivedAt.IsZero() {
		s.ReceivedAt = time.Now()
	}

	key := s.SuperblockHash.Hex()
	isNewSuperblock := m.bySB[key] == nil

	if isNewSuperblock {
		m.bySB[key] = make(map[uint32]proofs.Submission)
		m.log.Info().
			Str("superblock_hash", key).
			Uint64("superblock_number", s.SuperblockNumber).
			Msg("new superblock started collecting proofs")
	}

	m.bySB[key][s.ChainID] = s

	st := m.statuses[key]
	if st.SuperblockNumber == 0 {
		st.SuperblockNumber = s.SuperblockNumber
		st.SuperblockHash = s.SuperblockHash
		st.State = "collecting"
	}
	if st.Received == nil {
		st.Received = make(map[uint32]time.Time)
	}
	st.Received[s.ChainID] = s.ReceivedAt
	m.statuses[key] = st

	m.log.Info().
		Str("superblock_hash", key).
		Uint64("superblock_number", s.SuperblockNumber).
		Uint32("chain_id", s.ChainID).
		Int("total_submissions", len(m.bySB[key])).
		Str("mailbox_root", s.Aggregation.MailboxRoot.String()).
		Str("mailbox_info", s.MailboxInfoToString()).
		Msg("proof submission collected")

	return nil
}

func (m *ProofCollector) GetStatus(_ context.Context, sbHash common.Hash) (proofs.Status, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	key := sbHash.Hex()
	st, ok := m.statuses[key]
	if !ok {
		return proofs.Status{}, fmt.Errorf("unknown superblock")
	}
	return st, nil
}

func (m *ProofCollector) ListSubmissions(_ context.Context, sbHash common.Hash) ([]proofs.Submission, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	key := sbHash.Hex()
	subsMap, ok := m.bySB[key]
	if !ok {
		return nil, nil
	}
	out := make([]proofs.Submission, 0, len(subsMap))
	for _, sub := range subsMap {
		out = append(out, sub)
	}
	return out, nil
}

func (m *ProofCollector) UpdateStatus(_ context.Context, sbHash common.Hash, mutate func(*proofs.Status)) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	key := sbHash.Hex()
	st, ok := m.statuses[key]
	if !ok {
		// Initialize a new status entry if it doesn't exist
		st = proofs.Status{
			SuperblockHash: sbHash,
			State:          proofs.StateCollecting,
			Received:       make(map[uint32]time.Time),
		}
		m.log.Info().
			Str("superblock_hash", key).
			Msg("Initializing new superblock status")
	}
	mutate(&st)
	m.statuses[key] = st
	return nil
}

// GetStats returns collector statistics
func (m *ProofCollector) GetStats() map[string]interface{} {
	m.mu.RLock()
	defer m.mu.RUnlock()

	stats := map[string]interface{}{
		"total_superblocks":         len(m.bySB),
		"total_submissions":         0,
		"submissions_by_superblock": make(map[string]int),
		"statuses_by_state":         make(map[string]int),
	}

	for sbHash, submissions := range m.bySB {
		submissionCount := len(submissions)
		stats["total_submissions"] = stats["total_submissions"].(int) + submissionCount
		stats["submissions_by_superblock"].(map[string]int)[sbHash] = submissionCount
	}

	for _, status := range m.statuses {
		stateCounts := stats["statuses_by_state"].(map[string]int)
		stateCounts[status.State]++
	}

	return stats
}

// CountProvingJobs returns the number of jobs currently in StateProving
func (m *ProofCollector) CountProvingJobs(_ context.Context) (int, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	count := 0
	for _, status := range m.statuses {
		if status.State == proofs.StateProving {
			count++
		}
	}
	return count, nil
}

// ListQueuedJobs returns all jobs currently in StateQueued
func (m *ProofCollector) ListQueuedJobs(_ context.Context) ([]proofs.Status, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	var queuedJobs []proofs.Status
	for _, status := range m.statuses {
		if status.State == proofs.StateQueued {
			queuedJobs = append(queuedJobs, status)
		}
	}
	return queuedJobs, nil
}

// DeleteSubmissions removes all submissions and status for the given superblock hash.
func (m *ProofCollector) DeleteSubmissions(_ context.Context, sbHash common.Hash) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	key := sbHash.Hex()
	delete(m.bySB, key)
	delete(m.statuses, key)
	m.log.Info().Str("superblock_hash", key).Msg("Deleted submissions for superblock")
	return nil
}

// statsLogger periodically logs collector statistics
func (m *ProofCollector) statsLogger(ctx context.Context) {
	ticker := time.NewTicker(30 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			stats := m.GetStats()
			m.log.Info().
				Int("total_superblocks", stats["total_superblocks"].(int)).
				Int("total_submissions", stats["total_submissions"].(int)).
				Interface("statuses_by_state", stats["statuses_by_state"]).
				Msg("Proof Collector statistics")
		}
	}
}

// Close stops the collector and cleans up resources
func (m *ProofCollector) Close() {
	if m.cancel != nil {
		m.cancel()
	}
}
