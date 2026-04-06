package superblock

import (
	"context"
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
}

type proofJob struct {
	hash         common.Hash
	number       uint64
	proofType    string
	consumedFrom []common.Hash        // source bucket hashes whose submissions were drawn into this proof
	subs         []proofs.Submission  // submissions dispatched with this job (used for boot-info enrichment)
}

func newProofPipeline(
	cfg ProofsConfig,
	collector apicollector.Service,
	prover proofs.ProverClient,
	sbStore store.SuperblockStore,
	publishFn func(context.Context, *store.Superblock, []byte, *proofs.SuperblockAggOutputs) error,
	log zerolog.Logger,
) *proofPipeline {
	if !cfg.Enabled || collector == nil || prover == nil {
		return nil
	}
	poll := cfg.Prover.PollInterval
	if poll <= 0 {
		poll = 10 * time.Second
	}
	return &proofPipeline{
		cfg:       cfg,
		collector: collector,
		prover:    prover,
		sbStore:   sbStore,
		publishFn: publishFn,
		log:       log.With().Str("component", "proof-pipeline").Logger(),
		pollEvery: poll,
		jobs:      make(map[string]proofJob),
		quit:      make(chan struct{}),
	}
}

func (p *proofPipeline) Start(ctx context.Context) {
	if p == nil {
		return
	}

	p.log.Info().
		Str("proof_type", p.cfg.Prover.ProofType).
		Dur("poll_interval", p.pollEvery).
		Msg("Proof pipeline enabled")

	go p.pollLoop(ctx)
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

	// Aggregate the latest submission per chain across every bucket currently
	// in the collector. We don't care which superblock hash each submission was
	// originally attached to — once we have one submission per required chain,
	// we dispatch them together and wipe the source buckets on completion.
	proofSubs, consumedFrom := p.collectLatestPerChain(ctx, sb)

	p.log.Info().
		Uint64("superblock_number", sb.Number).
		Str("superblock_hash", sb.Hash.Hex()).
		Int("submissions_found", len(proofSubs)).
		Int("source_buckets", len(consumedFrom)).
		Msg("Checking submissions for superblock")

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
			if st.State == "" {
				st.State = proofs.StateCollecting
			}
		})
		return nil
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
			st.Error = ""
		})
		return nil
	}

	job := p.buildProofJobInput(ctx, sb, proofSubs)

	jobID, err := p.prover.RequestProof(ctx, job)
	if err != nil {
		_ = p.collector.UpdateStatus(ctx, sb.Hash, func(st *proofs.Status) {
			st.State = proofs.StateFailed
			st.Error = err.Error()
		})
		return fmt.Errorf("request proof: %w", err)
	}

	if err := p.collector.UpdateStatus(ctx, sb.Hash, func(st *proofs.Status) {
		st.Required = required
		st.State = proofs.StateProving
		st.JobID = jobID
		st.Error = ""
	}); err != nil {
		p.log.Warn().Err(err).Uint64("superblock", sb.Number).Msg("Failed to update status post dispatch")
	}

	p.mu.Lock()
	p.jobs[jobID] = proofJob{
		hash:         sb.Hash,
		number:       sb.Number,
		proofType:    job.ProofType,
		consumedFrom: consumedFrom,
		subs:         proofSubs,
	}
	p.mu.Unlock()

	p.log.Info().Str("job_id", jobID).Uint64("superblock", sb.Number).Msg("Proof job dispatched")
	return nil
}

// collectLatestPerChain walks every bucket currently in the collector and
// returns the newest submission per chain ID (by ReceivedAt), rewritten to
// point at the given superblock. The second return value is the list of
// source bucket hashes visited — these are the buckets that should be wiped
// once the resulting proof job finishes, so stale submissions don't pile up.
func (p *proofPipeline) collectLatestPerChain(
	ctx context.Context,
	sb *store.Superblock,
) ([]proofs.Submission, []common.Hash) {
	stats := p.collector.GetStats()
	buckets, ok := stats["submissions_by_superblock"].(map[string]int)
	if !ok || len(buckets) == 0 {
		return nil, nil
	}

	latest := make(map[uint32]proofs.Submission)
	consumed := make([]common.Hash, 0, len(buckets))
	for sbHashHex := range buckets {
		bucketHash := common.HexToHash(sbHashHex)
		subs, err := p.collector.ListSubmissions(ctx, bucketHash)
		if err != nil {
			p.log.Warn().
				Err(err).
				Str("bucket", sbHashHex).
				Msg("Failed to list submissions while aggregating")
			continue
		}
		if len(subs) == 0 {
			continue
		}
		consumed = append(consumed, bucketHash)
		for _, s := range subs {
			existing, have := latest[s.ChainID]
			if !have || s.ReceivedAt.After(existing.ReceivedAt) {
				latest[s.ChainID] = s
			}
		}
	}

	if len(latest) == 0 {
		return nil, nil
	}

	out := make([]proofs.Submission, 0, len(latest))
	for _, s := range latest {
		s.SuperblockNumber = sb.Number
		s.SuperblockHash = sb.Hash
		out = append(out, s)
	}
	return out, consumed
}

func (p *proofPipeline) requiredChainIDs(subs []proofs.Submission) []uint32 {
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
	p.enrichBootInfoChainIDs(job.subs, outputs)
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

	// Wipe every bucket whose submissions were aggregated into this proof so
	// stale per-chain entries don't accumulate in the collector.
	for _, h := range job.consumedFrom {
		p.log.Info().
			Str("source_superblock_hash", h.Hex()).
			Uint64("published_superblock", job.number).
			Msg("Cleaning up consumed op-succinct submissions")
		_ = p.collector.DeleteSubmissions(ctx, h)
	}

	p.removeJob(jobID)
	p.log.Info().Str("job_id", jobID).Uint64("superblock", job.number).Msg("Proof job completed and published")

	go p.processQueuedJobs(ctx)
}

// enrichBootInfoChainIDs populates ChainId on each BootInfo entry by matching
// rollupConfigHash against the submissions the proof job was dispatched with.
func (p *proofPipeline) enrichBootInfoChainIDs(subs []proofs.Submission, outputs *proofs.SuperblockAggOutputs) {
	if outputs == nil || len(outputs.BootInfo) == 0 {
		return
	}
	if len(subs) == 0 {
		p.log.Warn().Msg("No submissions available to enrich boot info chain IDs")
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
