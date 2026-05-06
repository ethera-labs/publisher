package superblock

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/ethereum/go-ethereum/common"
	"github.com/rs/zerolog"

	"github.com/compose-network/publisher/x/superblock/proofs"
	apicollector "github.com/compose-network/publisher/x/superblock/proofs/collector"
	"github.com/compose-network/publisher/x/superblock/store"
)

type proofPipeline struct {
	cfg       ProofsConfig
	collector apicollector.Service
	prover    proofs.ProverClient
	sbStore   store.SuperblockStore
	log       zerolog.Logger
	pollEvery time.Duration

	publishFn func(context.Context, *store.Superblock, []byte, *proofs.SuperblockAggOutputs) error

	mu   sync.Mutex
	jobs map[string]proofJob
	quit chan struct{}
	once sync.Once

	// pubMu protects lastPublishedL2BlockByChain, the per-chain high-water mark of
	// op-succinct's L2BlockNumber that we've already published. New submissions must
	// strictly exceed the high-water for their chain to be eligible for the next
	// publish — preventing the every-slot republish loop after RequireAllChains lets
	// us aggregate across multiple superblock-hash buckets.
	pubMu                       sync.Mutex
	lastPublishedL2BlockByChain map[uint32]uint64
}

type proofJob struct {
	hash      common.Hash
	number    uint64
	proofType string
}

func newProofPipeline(
	cfg ProofsConfig,
	collector apicollector.Service,
	prover proofs.ProverClient,
	sbStore store.SuperblockStore,
	publishFn func(context.Context, *store.Superblock, []byte, *proofs.SuperblockAggOutputs) error,
	log zerolog.Logger,
) *proofPipeline {
	if !cfg.Enabled || collector == nil {
		return nil
	}
	// When BypassProver is set, the superblock-prover HTTP client is not required;
	// outputs/proof are synthesized locally.
	if prover == nil && !cfg.BypassProver {
		return nil
	}
	poll := cfg.Prover.PollInterval
	if poll <= 0 {
		poll = 10 * time.Second
	}
	return &proofPipeline{
		cfg:                         cfg,
		collector:                   collector,
		prover:                      prover,
		sbStore:                     sbStore,
		publishFn:                   publishFn,
		log:                         log.With().Str("component", "proof-pipeline").Logger(),
		pollEvery:                   poll,
		jobs:                        make(map[string]proofJob),
		quit:                        make(chan struct{}),
		lastPublishedL2BlockByChain: make(map[uint32]uint64),
	}
}

func (p *proofPipeline) Start(ctx context.Context) {
	if p == nil {
		return
	}

	p.log.Info().
		Str("proof_type", p.cfg.Prover.ProofType).
		Dur("poll_interval", p.pollEvery).
		Bool("bypass_prover", p.cfg.BypassProver).
		Msg("Proof pipeline enabled")

	if p.cfg.BypassProver {
		p.log.Warn().
			Msg("BypassProver is enabled: superblock-prover will be skipped and a mock proof " +
				"will be published to L1. Do not use in production.")
	}

	// Polling the superblock-prover only makes sense when there is a real prover.
	if !p.cfg.BypassProver {
		go p.pollLoop(ctx)
	}
}

func (p *proofPipeline) Stop() {
	if p == nil {
		return
	}

	p.once.Do(func() { close(p.quit) })
}

// HandleSuperblock processes a given superblock by checking and handling proof submissions required for its processing.
// TODO: fix block numbers
//
//nolint:gocyclo // ok
func (p *proofPipeline) HandleSuperblock(ctx context.Context, sb *store.Superblock) error {
	if p == nil {
		return nil
	}

	p.log.Info().
		Uint64("superblock_number", sb.Number).
		Str("superblock_hash", sb.Hash.Hex()).
		Msg("HandleSuperblock called - checking for proofs")

	// Initialize superblock status if it doesn't exist (collector now handles this automatically)

	// TODO: For testing, can bypass missing proofs by creating dummy submissions
	proofSubs, err := p.collector.ListSubmissions(ctx, sb.Hash)
	if err != nil {
		p.log.Warn().Err(err).Uint64("superblock", sb.Number).Msg("No submissions yet for superblock")
		return err
	}

	p.log.Info().
		Uint64("superblock_number", sb.Number).
		Str("superblock_hash", sb.Hash.Hex()).
		Int("submissions_found", len(proofSubs)).
		Msg("Checking submissions for superblock")

	// TODO: Get ALL submissions from collector regardless of superblock hash
	// and then modify their superblock number/hash to match current superblock
	allSubs := p.collector.GetStats()
	totalSubmissions := allSubs["total_submissions"].(int)

	if len(proofSubs) == 0 && totalSubmissions > 0 {
		p.log.Info().
			Uint64("current_superblock", sb.Number).
			Int("total_submissions_in_collector", totalSubmissions).
			Msg("No submissions for current superblock, aggregating latest per chain across buckets")

		// op-succinct submits one bucket per chain (keyed by its own chain-local
		// superblock_hash), so a single bucket only ever holds one chain's submission.
		// To satisfy RequireAllChains we aggregate across every bucket, keeping the
		// latest submission per chain by L2BlockNumber, and skip submissions we've
		// already published (per-chain high-water).
		p.pubMu.Lock()
		highWater := make(map[uint32]uint64, len(p.lastPublishedL2BlockByChain))
		for k, v := range p.lastPublishedL2BlockByChain {
			highWater[k] = v
		}
		p.pubMu.Unlock()

		allSuperblocks := allSubs["submissions_by_superblock"].(map[string]int)
		latestByChain := make(map[uint32]proofs.Submission)
		for sbHash := range allSuperblocks {
			otherHash := common.HexToHash(sbHash)
			otherSubs, err := p.collector.ListSubmissions(ctx, otherHash)
			if err != nil || len(otherSubs) == 0 {
				continue
			}
			for _, s := range otherSubs {
				if s.Aggregation.L2BlockNumber <= highWater[s.ChainID] {
					continue
				}
				existing, ok := latestByChain[s.ChainID]
				if !ok || s.Aggregation.L2BlockNumber > existing.Aggregation.L2BlockNumber {
					latestByChain[s.ChainID] = s
				}
			}
		}

		if len(latestByChain) > 0 {
			proofSubs = make([]proofs.Submission, 0, len(latestByChain))
			for _, s := range latestByChain {
				// Stamp with the current slot's superblock identity (matches the
				// previous reuse semantics expected by downstream code).
				s.SuperblockNumber = sb.Number
				s.SuperblockHash = sb.Hash
				proofSubs = append(proofSubs, s)
			}
			p.log.Info().
				Int("submissions_count", len(proofSubs)).
				Int("buckets_scanned", len(allSuperblocks)).
				Msg("Aggregated latest submissions across buckets for current superblock")
		}
	}

	if len(proofSubs) == 0 {
		p.log.Info().Uint64("superblock", sb.Number).Msg("No proof submissions available")
		return nil
	}

	for i, sub := range proofSubs {
		p.log.Info().
			Int("submission_index", i).
			Uint64("submission_superblock_number", sub.SuperblockNumber).
			Str("submission_superblock_hash", sub.SuperblockHash.Hex()).
			Uint32("chain_id", sub.ChainID).
			Msg("Found proof submission")
	}

	required := p.requiredChainIDs(proofSubs)
	ready := p.isReady(required, proofSubs)

	p.log.Info().
		Uint64("superblock", sb.Number).
		Interface("required_chain_ids", required).
		Int("submissions_count", len(proofSubs)).
		Bool("ready_for_prover", ready).
		Bool("require_all_chains", p.cfg.Collector.RequireAllChains).
		Msg("Evaluated proof readiness")

	if !ready {
		missing := p.missingChains(required, proofSubs)
		p.log.Info().
			Uint64("superblock", sb.Number).
			Ints("missing_chains", missing).
			Int("received", len(proofSubs)).
			Interface("required_chain_ids", required).
			Msg("Not ready - waiting for remaining chain proofs")
		_ = p.collector.UpdateStatus(ctx, sb.Hash, func(st *proofs.Status) {
			st.Required = required
			st.SuperblockNumber = sb.Number
			st.SuperblockHash = sb.Hash
			if st.State == "" {
				st.State = proofs.StateCollecting
			}
		})
		return nil
	}

	if p.cfg.BypassProver {
		return p.handleBypass(ctx, sb, proofSubs, required)
	}

	// Rate limiter: Check if there's already a proof in StateProving
	provingCount, err := p.collector.CountProvingJobs(ctx)
	if err != nil {
		p.log.Error().Err(err).Msg("Failed to check proving job count")
		return fmt.Errorf("check proving jobs: %w", err)
	}

	if provingCount > 0 {
		p.log.Info().
			Uint64("superblock", sb.Number).
			Int("proving_count", provingCount).
			Msg("Rate limited: another proof is currently proving, queuing this one")
		_ = p.collector.UpdateStatus(ctx, sb.Hash, func(st *proofs.Status) {
			st.Required = required
			st.State = proofs.StateQueued
			st.SuperblockNumber = sb.Number
			st.SuperblockHash = sb.Hash
			st.Error = ""
		})
		return nil
	}

	job := p.buildProofJobInput(ctx, sb, proofSubs)

	jobID, err := p.prover.RequestProof(ctx, job)
	if err != nil {
		_ = p.collector.UpdateStatus(ctx, sb.Hash, func(st *proofs.Status) {
			st.SuperblockNumber = sb.Number
			st.SuperblockHash = sb.Hash
			st.State = proofs.StateFailed
			st.Error = err.Error()
		})
		return fmt.Errorf("request proof: %w", err)
	}

	if err := p.collector.UpdateStatus(ctx, sb.Hash, func(st *proofs.Status) {
		st.Required = required
		st.SuperblockNumber = sb.Number
		st.SuperblockHash = sb.Hash
		st.State = proofs.StateProving
		st.JobID = jobID
		st.Error = ""
	}); err != nil {
		p.log.Warn().Err(err).Uint64("superblock", sb.Number).Msg("Failed to update status post dispatch")
	}

	p.mu.Lock()
	p.jobs[jobID] = proofJob{hash: sb.Hash, number: sb.Number, proofType: job.ProofType}
	p.mu.Unlock()

	p.log.Info().Str("job_id", jobID).Uint64("superblock", sb.Number).Msg("Proof job dispatched")
	return nil
}

func (p *proofPipeline) requiredChainIDs(subs []proofs.Submission) []uint32 {
	// When operators have configured the expected chain IDs, honor them. This is what makes
	// `require_all_chains` meaningful — without an explicit list, "required" would otherwise
	// be derived from the submissions already in hand and `isReady` would be a tautology.
	if len(p.cfg.Collector.RequiredChainIDs) > 0 {
		return append([]uint32(nil), p.cfg.Collector.RequiredChainIDs...)
	}
	seen := make(map[uint32]struct{}, len(subs))
	for _, s := range subs {
		seen[s.ChainID] = struct{}{}
	}
	out := make([]uint32, 0, len(seen))
	for id := range seen {
		out = append(out, id)
	}
	return out
}

func (p *proofPipeline) isReady(required []uint32, subs []proofs.Submission) bool {
	if !p.cfg.Collector.RequireAllChains {
		return len(subs) > 0
	}
	have := make(map[uint32]struct{}, len(subs))
	for _, s := range subs {
		have[s.ChainID] = struct{}{}
	}
	for _, id := range required {
		if _, ok := have[id]; !ok {
			return false
		}
	}
	return true
}

func (p *proofPipeline) buildProofJobInput(
	ctx context.Context,
	sb *store.Superblock,
	proofSubs []proofs.Submission,
) proofs.ProofJobInput {
	rollupStateTransitions := make([]proofs.RollupStateTransition, 0, len(proofSubs))
	for _, ps := range proofSubs {
		l2BlockNumberBytes := make([]byte, 32)
		blockNumber := ps.Aggregation.L2BlockNumber
		for i := 0; i < 8; i++ {
			l2BlockNumberBytes[31-i] = byte(blockNumber)
			blockNumber >>= 8
		}

		rollupStateTransitions = append(rollupStateTransitions, proofs.RollupStateTransition{
			RollupConfigHash: bytesToInts(ps.Aggregation.RollupConfigHash.Bytes()),
			L2PreRoot:        bytesToInts(ps.Aggregation.L2PreRoot.Bytes()),
			L2PostRoot:       bytesToInts(ps.Aggregation.L2PostRoot.Bytes()),
			L2BlockNumber:    bytesToInts(l2BlockNumberBytes),
		})
	}

	var previousBatch proofs.SuperblockBatch
	if sb.Number > 0 {
		prev, err := p.sbStore.GetSuperblock(ctx, sb.Number-1)
		if err == nil {
			// TODO: Get actual parent superblock batch hash
			parentHashBytes := make([]byte, 32)
			copy(parentHashBytes, prev.Hash.Bytes())
			parentHashInts := bytesToInts(parentHashBytes)

			if len(parentHashInts) == 0 {
				parentHashInts = make([]int, 32)
			}

			previousBatch = proofs.SuperblockBatch{
				SuperblockNumber:          prev.Number,
				ParentSuperblockBatchHash: parentHashInts,
				// TODO: Get actual rollup state transitions for previous batch
				RollupSt: []proofs.RollupStateTransition{},
			}
		}
	}

	newBatch := proofs.SuperblockBatch{
		SuperblockNumber:          sb.Number,
		ParentSuperblockBatchHash: bytesToInts(sb.ParentHash.Bytes()),
		RollupSt:                  rollupStateTransitions,
	}

	aggProofs := make([]proofs.AggregationProofData, 0, len(proofSubs))
	for _, s := range proofSubs {
		proofBytes := make([]byte, len(s.Proof))
		copy(proofBytes, s.Proof)

		aggProofs = append(aggProofs, proofs.AggregationProofData{
			ChainID:            s.ChainID,
			AggregationOutputs: s.Aggregation,
			CompressedProof:    proofBytes,
			AggVKey:            [8]int{0, 0, 0, 0, 0, 0, 0, 0},
			MailboxInfo:        s.MailboxInfo,
		})
	}

	return proofs.ProofJobInput{
		ProofType: p.cfg.Prover.ProofType,
		Input: proofs.SuperblockProverInput{
			PreviousBatch:     previousBatch,
			NewBatch:          newBatch,
			AggregationProofs: aggProofs,
		},
	}
}

// func (p *proofPipeline) collectSuperblocks(
//	ctx context.Context,
//	current *store.Superblock,
// ) []proofs.ProverSuperblock {
//	result := []proofs.ProverSuperblock{convertSuperblock(current)}
//	if current.Number > 0 {
//		prev, err := p.sbStore.GetSuperblock(ctx, current.Number-1)
//		if err == nil {
//			result = append([]proofs.ProverSuperblock{convertSuperblock(prev)}, result...)
//		}
//	}
//
//	return result
// }
//
// func convertSuperblock(sb *store.Superblock) proofs.ProverSuperblock {
//	psb := proofs.ProverSuperblock{
//		Number:     sb.Number,
//		Slot:       sb.Slot,
//		ParentHash: sb.ParentHash.Bytes(),
//		Hash:       sb.Hash.Bytes(),
//		MerkleRoot: sb.MerkleRoot.Bytes(),
//		Timestamp:  uint64(sb.Timestamp.Unix()),
//	}
//
//	for _, blk := range sb.L2Blocks {
//		psb.L2Blocks = append(psb.L2Blocks, proofs.ProverL2Block{
//			Slot:            blk.GetSlot(),
//			ChainID:         append([]byte(nil), blk.GetChainId()...),
//			BlockNumber:     blk.GetBlockNumber(),
//			BlockHash:       append([]byte(nil), blk.GetBlockHash()...),
//			ParentBlockHash: append([]byte(nil), blk.GetParentBlockHash()...),
//			IncludedXTs:     cloneSlices(blk.GetIncludedXts()),
//			Block:           append([]byte(nil), blk.GetBlock()...),
//		})
//	}
//
//	for _, xt := range sb.IncludedXTs {
//		psb.IncludedXTs = append(psb.IncludedXTs, xt.Bytes())
//	}
//
//	if sb.L1TransactionHash != (common.Hash{}) {
//		psb.L1TransactionHash = sb.L1TransactionHash.Bytes()
//	}
//
//	return psb
// }
//
// func cloneSlices(src [][]byte) [][]byte {
//	out := make([][]byte, len(src))
//	for i, b := range src {
//		out[i] = append([]byte(nil), b...)
//	}
//
//	return out
// }

// processQueuedJobs attempts to process jobs that are in StateQueued
func (p *proofPipeline) processQueuedJobs(ctx context.Context) {
	if p == nil {
		return
	}

	// Check if we can process more jobs (should be 0 proving jobs now)
	provingCount, err := p.collector.CountProvingJobs(ctx)
	if err != nil {
		p.log.Error().Err(err).Msg("Failed to check proving job count while processing queue")
		return
	}

	if provingCount > 0 {
		p.log.Debug().Int("proving_count", provingCount).Msg("Still have proving jobs, not processing queue")
		return
	}

	// Get queued jobs
	queuedJobs, err := p.collector.ListQueuedJobs(ctx)
	if err != nil {
		p.log.Error().Err(err).Msg("Failed to list queued jobs")
		return
	}

	if len(queuedJobs) == 0 {
		p.log.Debug().Msg("No queued jobs to process")
		return
	}

	// Sort by superblock number to process in order (oldest first)
	// TODO: Add proper sorting if needed, for now just process the first one
	jobToProcess := queuedJobs[0]
	for _, job := range queuedJobs {
		if job.SuperblockNumber < jobToProcess.SuperblockNumber {
			jobToProcess = job
		}
	}

	p.log.Info().
		Uint64("superblock", jobToProcess.SuperblockNumber).
		Str("superblock_hash", jobToProcess.SuperblockHash.Hex()).
		Int("total_queued", len(queuedJobs)).
		Msg("Processing queued proof job")

	// Get the superblock for this job
	sb, err := p.sbStore.GetSuperblock(ctx, jobToProcess.SuperblockNumber)
	if err != nil {
		p.log.Error().
			Err(err).
			Uint64("superblock", jobToProcess.SuperblockNumber).
			Msg("Failed to load superblock for queued job")
		return
	}

	// Process this superblock (this will go through the normal flow but should now pass the rate limiter)
	if err := p.HandleSuperblock(ctx, sb); err != nil {
		p.log.Error().
			Err(err).
			Uint64("superblock", jobToProcess.SuperblockNumber).
			Msg("Failed to process queued superblock")
	}
}

// bytesToInts converts a byte slice to an int slice
func bytesToInts(src []byte) []int {
	out := make([]int, len(src))
	for i, b := range src {
		out[i] = int(b)
	}
	return out
}

func (p *proofPipeline) pollLoop(ctx context.Context) {
	if p == nil {
		return
	}

	ticker := time.NewTicker(p.pollEvery)
	defer ticker.Stop()

	statsTicker := time.NewTicker(5 * p.pollEvery)
	defer statsTicker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-p.quit:
			return
		case <-ticker.C:
			p.pollOnce(ctx)
		case <-statsTicker.C:
			p.logStats()
		}
	}
}

func (p *proofPipeline) pollOnce(ctx context.Context) {
	p.mu.Lock()
	jobs := make(map[string]proofJob, len(p.jobs))
	for id, job := range p.jobs {
		jobs[id] = job
	}
	p.mu.Unlock()

	for id, job := range jobs {
		status, err := p.prover.GetStatus(ctx, id)
		if err != nil {
			if errors.Is(err, proofs.ErrJobNotFound) {
				p.log.Warn().Str("job_id", id).Msg("Job lost on prover, marking failed")
				_ = p.collector.UpdateStatus(ctx, job.hash, func(st *proofs.Status) {
					st.State = proofs.StateFailed
					st.Error = "job lost on prover"
				})
				p.removeJob(id)
				go p.processQueuedJobs(ctx)
				continue
			}
			p.log.Warn().Err(err).Str("job_id", id).Msg("Failed to fetch proof status")
			continue
		}
		switch strings.ToLower(status.Status) {
		case "pending", "running", "proving":
			continue
		case "failed":
			_ = p.collector.UpdateStatus(ctx, job.hash, func(st *proofs.Status) {
				st.State = proofs.StateFailed
				st.Error = "prover reported failure"
			})
			p.removeJob(id)

			go p.processQueuedJobs(ctx)
		case "completed":
			p.handleCompleted(ctx, id, job, status)
		default:
			p.log.Warn().Str("job_id", id).Str("status", status.Status).Msg("Unknown proof job status")
		}
	}
}

func (p *proofPipeline) handleCompleted(ctx context.Context, jobID string, job proofJob, status proofs.ProofJobStatus) {
	p.log.Info().
		Str("job_id", jobID).
		Uint64("superblock", job.number).
		Str("proof_type", job.proofType).
		Int("proof_size_bytes", len(status.Proof)).
		Interface("proving_time_ms", status.ProvingTimeMS).
		Interface("cycles", status.Cycles).
		Interface("commitment", status.Commitment).
		Interface("superblock_agg_outputs", status.SuperblockAggOutputs).
		Msg("Proof job finished successfully")

	outputs := status.SuperblockAggOutputs
	p.enrichBootInfoChainIDs(ctx, job.hash, outputs)
	proofBytes := status.Proof
	if len(proofBytes) == 0 {
		p.log.Warn().Str("job_id", jobID).Msg("Completed proof job returned empty proof")
		_ = p.collector.UpdateStatus(ctx, job.hash, func(st *proofs.Status) {
			st.State = proofs.StateFailed
			st.Error = "empty proof from prover"
		})
		p.removeJob(jobID)
		return
	}

	sb, err := p.sbStore.GetSuperblock(ctx, job.number)
	if err != nil {
		p.log.Error().Err(err).Uint64("superblock", job.number).Msg("Failed to load superblock for proof completion")
		return
	}
	sb.Proof = append([]byte(nil), proofBytes...)

	if err := p.sbStore.StoreSuperblock(ctx, sb); err != nil {
		p.log.Error().Err(err).Uint64("superblock", job.number).Msg("Failed to persist superblock with proof")
		return
	}

	if p.publishFn != nil {
		if err := p.publishFn(ctx, sb, proofBytes, outputs); err != nil {
			p.log.Error().Err(err).Uint64("superblock", job.number).Msg("Failed to publish superblock with proof")
			_ = p.collector.UpdateStatus(ctx, job.hash, func(st *proofs.Status) {
				st.State = proofs.StateFailed
				st.Error = err.Error()
			})
			return
		}
	}

	_ = p.collector.UpdateStatus(ctx, job.hash, func(st *proofs.Status) {
		st.State = proofs.StateComplete
		st.Error = ""
	})
	p.removeJob(jobID)
	p.log.Info().Str("job_id", jobID).Uint64("superblock", job.number).Msg("Proof job completed and published")

	go p.processQueuedJobs(ctx)
}

// enrichBootInfoChainIDs populates ChainId on each BootInfo entry by matching
// rollupConfigHash against the original submissions which carry the chain ID.
func (p *proofPipeline) enrichBootInfoChainIDs(ctx context.Context, sbHash common.Hash, outputs *proofs.SuperblockAggOutputs) {
	if outputs == nil || len(outputs.BootInfo) == 0 {
		return
	}
	subs, err := p.collector.ListSubmissions(ctx, sbHash)
	if err != nil || len(subs) == 0 {
		p.log.Warn().Err(err).Msg("Could not retrieve submissions to enrich boot info chain IDs")
		return
	}
	configToChain := make(map[common.Hash]uint64, len(subs))
	for _, s := range subs {
		configToChain[s.Aggregation.RollupConfigHash] = uint64(s.ChainID)
	}
	for i, bi := range outputs.BootInfo {
		h := common.HexToHash(bi.RollupConfigHash)
		if chainId, ok := configToChain[h]; ok {
			outputs.BootInfo[i].ChainId = chainId
		}
	}
}

func (p *proofPipeline) removeJob(jobID string) {
	p.mu.Lock()
	delete(p.jobs, jobID)
	p.mu.Unlock()
}

func (p *proofPipeline) missingChains(required []uint32, subs []proofs.Submission) []int {
	have := make(map[uint32]struct{}, len(subs))
	for _, s := range subs {
		have[s.ChainID] = struct{}{}
	}
	var out []int
	for _, id := range required {
		if _, ok := have[id]; !ok {
			out = append(out, int(id))
		}
	}
	return out
}

// handleBypass synthesizes superblock aggregation outputs from the collected per-rollup
// submissions and publishes the superblock to L1 with a deterministic mock proof,
// skipping the superblock-prover entirely. Used only when cfg.BypassProver is set.
func (p *proofPipeline) handleBypass(
	ctx context.Context,
	sb *store.Superblock,
	proofSubs []proofs.Submission,
	required []uint32,
) error {
	outputs := buildMockAggOutputs(sb, proofSubs)
	proof := mockProofBytes()

	_ = p.collector.UpdateStatus(ctx, sb.Hash, func(st *proofs.Status) {
		st.Required = required
		st.SuperblockNumber = sb.Number
		st.SuperblockHash = sb.Hash
		st.State = proofs.StateProving
		st.JobID = "bypass"
		st.Error = ""
	})

	p.log.Info().
		Uint64("superblock", sb.Number).
		Str("superblock_hash", sb.Hash.Hex()).
		Int("boot_info_entries", len(outputs.BootInfo)).
		Int("mock_proof_bytes", len(proof)).
		Msg("BypassProver: publishing superblock with mock proof")

	if p.publishFn == nil {
		return fmt.Errorf("bypass: publishFn is not configured")
	}

	sb.Proof = append([]byte(nil), proof...)
	if err := p.sbStore.StoreSuperblock(ctx, sb); err != nil {
		p.log.Warn().Err(err).Uint64("superblock", sb.Number).Msg("Failed to persist superblock with mock proof")
	}

	if err := p.publishFn(ctx, sb, proof, outputs); err != nil {
		_ = p.collector.UpdateStatus(ctx, sb.Hash, func(st *proofs.Status) {
			st.State = proofs.StateFailed
			st.Error = err.Error()
		})
		return fmt.Errorf("bypass publish: %w", err)
	}

	// Advance per-chain high-water so we don't republish the same submissions next slot.
	p.pubMu.Lock()
	for _, s := range proofSubs {
		if s.Aggregation.L2BlockNumber > p.lastPublishedL2BlockByChain[s.ChainID] {
			p.lastPublishedL2BlockByChain[s.ChainID] = s.Aggregation.L2BlockNumber
		}
	}
	p.pubMu.Unlock()

	_ = p.collector.UpdateStatus(ctx, sb.Hash, func(st *proofs.Status) {
		st.State = proofs.StateComplete
		st.Error = ""
	})

	return nil
}

// buildMockAggOutputs maps each collected per-rollup AggregationOutputs into the
// SuperblockAggOutputs shape the L1 dispute-game factory binding expects. The prover
// normally returns this; here we construct it locally from the submissions.
func buildMockAggOutputs(sb *store.Superblock, subs []proofs.Submission) *proofs.SuperblockAggOutputs {
	bootInfo := make([]proofs.BootInfo, 0, len(subs))
	for _, sub := range subs {
		bootInfo = append(bootInfo, proofs.BootInfo{
			L1Head:           sub.Aggregation.L1Head.Hex(),
			L2PreRoot:        sub.Aggregation.L2PreRoot.Hex(),
			L2PostRoot:       sub.Aggregation.L2PostRoot.Hex(),
			L2BlockNumber:    sub.Aggregation.L2BlockNumber,
			RollupConfigHash: sub.Aggregation.RollupConfigHash.Hex(),
		})
	}
	return &proofs.SuperblockAggOutputs{
		SuperblockNumber:          fmt.Sprintf("%d", sb.Number),
		ParentSuperblockBatchHash: sb.ParentHash.Hex(),
		BootInfo:                  bootInfo,
	}
}

// mockProofBytes returns a fixed, non-empty byte blob used as a placeholder proof
// when BypassProver is enabled. Contents are intentionally recognizable so that
// anyone inspecting an on-chain tx can tell this was not a real SNARK.
func mockProofBytes() []byte {
	return []byte("MOCK_PROOF_BYPASS_PROVER_DEV_ONLY")
}

func (p *proofPipeline) logStats() {
	p.mu.Lock()
	queued := len(p.jobs)
	p.mu.Unlock()

	if queued == 0 {
		p.log.Debug().Msg("Proof pipeline idle")
		return
	}

	p.log.Info().
		Int("outstanding_jobs", queued).
		Msg("Active proof jobs awaiting completion")
}
