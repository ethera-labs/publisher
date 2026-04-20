package superblock

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"sort"
	"sync"
	"time"

	pb "github.com/compose-network/publisher/proto/rollup/v1"
	"github.com/compose-network/publisher/x/consensus"
	"github.com/compose-network/publisher/x/superblock/l1"
	l1events "github.com/compose-network/publisher/x/superblock/l1/events"
	"github.com/compose-network/publisher/x/superblock/l1/tx"
	"github.com/compose-network/publisher/x/superblock/proofs"
	apicollector "github.com/compose-network/publisher/x/superblock/proofs/collector"
	"github.com/compose-network/publisher/x/superblock/queue"
	"github.com/compose-network/publisher/x/superblock/registry"
	"github.com/compose-network/publisher/x/superblock/slot"
	"github.com/compose-network/publisher/x/superblock/store"
	"github.com/compose-network/publisher/x/superblock/wal"
	"github.com/compose-network/publisher/x/transport"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/crypto"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/rs/zerolog"
)

// Coordinator orchestrates the Superblock Construction Protocol (SBCP)
// by managing slot-based execution, cross-chain transactions, and L2 block assembly
type Coordinator struct {
	mu      sync.RWMutex
	config  Config
	log     zerolog.Logger
	metrics prometheus.Registerer

	slot            slot.Slot
	stateMachine    *slot.StateMachine
	registryService registry.Service
	l2BlockStore    store.L2BlockStore
	superblockStore store.SuperblockStore
	xtQueue         queue.XTRequestQueue
	l1Publisher     l1.Publisher
	walManager      wal.Manager

	consensusCoord consensus.Coordinator
	transport      transport.Server

	proofs *proofPipeline

	running  bool
	stopCh   chan struct{}
	workerWg sync.WaitGroup

	currentExecution *SlotExecution
	stats            map[string]interface{}

	l1TrackMu sync.Mutex
	l1Tracked map[uint64][]byte // superblock number -> tx hash
}

func NewCoordinator(
	config Config,
	log zerolog.Logger,
	metrics prometheus.Registerer,
	registryService registry.Service,
	l2BlockStore store.L2BlockStore,
	superblockStore store.SuperblockStore,
	xtQueue queue.XTRequestQueue,
	l1Publisher l1.Publisher,
	walManager wal.Manager,
	consensusCoord consensus.Coordinator,
	transport transport.Server,
	collector apicollector.Service,
	prover proofs.ProverClient,
) *Coordinator {
	slotImpl := slot.New(
		config.Slot.GenesisTime,
		config.Slot.Duration,
		config.Slot.SealCutover,
	)

	stateMachine := slot.NewStateMachine(slotImpl, log)

	c := &Coordinator{
		config:          config,
		log:             log.With().Str("component", "coordinator").Logger(),
		metrics:         metrics,
		slot:            slotImpl,
		stateMachine:    stateMachine,
		registryService: registryService,
		l2BlockStore:    l2BlockStore,
		superblockStore: superblockStore,
		xtQueue:         xtQueue,
		l1Publisher:     l1Publisher,
		walManager:      walManager,
		consensusCoord:  consensusCoord,
		transport:       transport,
		stopCh:          make(chan struct{}),
		stats:           make(map[string]interface{}),
		l1Tracked:       make(map[uint64][]byte),
	}

	c.setupStateCallbacks()
	c.setupConsensusCallbacks()

	if config.Proofs.Enabled && collector != nil && (prover != nil || config.Proofs.BypassProver) {
		c.proofs = newProofPipeline(
			config.Proofs,
			collector,
			prover,
			superblockStore,
			c.publishWithProof,
			c.log,
		)
	}
	return c
}

func (c *Coordinator) Start(ctx context.Context) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.running {
		return fmt.Errorf("coordinator already running")
	}

	c.log.Info().Msg("Starting superblock coordinator")

	if c.walManager != nil {
		if err := c.recoverFromWAL(ctx); err != nil {
			return fmt.Errorf("WAL recovery failed: %w", err)
		}
	}

	c.running = true

	c.workerWg.Add(5)
	go c.slotExecutionLoop(ctx)
	go c.queueProcessor(ctx)
	go c.metricsUpdater(ctx)
	go c.l1EventWatcher(ctx)
	go c.l1ReceiptPoller(ctx)
	if c.proofs != nil {
		c.proofs.Start(ctx)
	}

	return nil
}

func (c *Coordinator) Stop(ctx context.Context) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if !c.running {
		return nil
	}

	c.log.Info().Msg("Stopping superblock coordinator")
	c.running = false

	close(c.stopCh)
	c.workerWg.Wait()

	if c.walManager != nil {
		return c.walManager.Close()
	}

	if c.proofs != nil {
		c.proofs.Stop()
	}

	return nil
}

func (c *Coordinator) GetCurrentSlot() uint64 {
	return c.slot.GetCurrent()
}

func (c *Coordinator) GetSlotState() slot.State {
	return c.stateMachine.GetCurrentState()
}

// Logger exposes the coordinator's logger for external packages (e.g., handlers).
func (c *Coordinator) Logger() *zerolog.Logger { return &c.log }

// Consensus returns the underlying consensus coordinator.
func (c *Coordinator) Consensus() consensus.Coordinator { return c.consensusCoord }

// HandleL2Block is an exported wrapper to process incoming L2 block messages.
func (c *Coordinator) HandleL2Block(ctx context.Context, from string, l2Block *pb.L2Block) error {
	return c.handleL2Block(ctx, from, l2Block)
}

// StateMachine exposes the slot state machine for external observers.
func (c *Coordinator) StateMachine() *slot.StateMachine { return c.stateMachine }

// Transport exposes the transport server for broadcasting messages from adapters.
func (c *Coordinator) Transport() transport.Server { return c.transport }

// StartSCPForAdapter allows adapter layer to initiate SCP with a constructed request.
func (c *Coordinator) StartSCPForAdapter(ctx context.Context, xtReq *pb.XTRequest, xtID []byte, from string) error {
	queuedRequest := &queue.QueuedXTRequest{
		Request:     xtReq,
		XtID:        xtID,
		Priority:    time.Now().Unix(),
		SubmittedAt: time.Now(),
		ExpiresAt:   time.Now().Add(c.config.Queue.RequestExpiration),
		Attempts:    0,
		From:        from,
	}
	return c.startSCP(ctx, queuedRequest)
}

func (c *Coordinator) GetActiveTransactions() []*pb.XtID {
	instances := c.stateMachine.GetSCPInstances()
	var activeXTs []*pb.XtID

	for _, instance := range instances {
		if instance.Decision == nil {
			activeXTs = append(activeXTs, &pb.XtID{
				Hash: instance.XtID,
			})
		}
	}

	return activeXTs
}

func (c *Coordinator) SubmitXTRequest(ctx context.Context, from string, request *pb.XTRequest) error {
	queuedRequest := &queue.QueuedXTRequest{
		Request:     request,
		XtID:        c.calculateXtID(request),
		Priority:    time.Now().Unix(),
		SubmittedAt: time.Now(),
		ExpiresAt:   time.Now().Add(c.config.Queue.RequestExpiration),
		Attempts:    0,
		From:        from,
	}

	return c.xtQueue.Enqueue(ctx, queuedRequest)
}

func (c *Coordinator) GetStats() map[string]interface{} {
	c.mu.RLock()
	defer c.mu.RUnlock()

	result := make(map[string]interface{})
	for k, v := range c.stats {
		result[k] = v
	}

	result["current_slot"] = c.GetCurrentSlot()
	result["slot_state"] = c.GetSlotState().String()
	result["running"] = c.running

	return result
}

func (c *Coordinator) slotExecutionLoop(ctx context.Context) {
	defer c.workerWg.Done()

	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			c.log.Warn().Msg("Context done")
			return
		case <-c.stopCh:
			c.log.Warn().Msg("Channel stopped")
			return
		case <-ticker.C:
			if err := c.processSlotTick(ctx); err != nil {
				c.log.Error().Err(err).Msg("Error processing slot tick")
			}
		}
	}
}

func (c *Coordinator) processSlotTick(ctx context.Context) error {
	currentSlot := c.slot.GetCurrent()
	currentState := c.stateMachine.GetCurrentState()

	switch currentState {
	case slot.StateStarting:
		return c.handleStartingState(ctx, currentSlot)
	case slot.StateFree:
		return c.handleFreeState(ctx, currentSlot)
	case slot.StateLocked:
		return c.handleLockedState(ctx, currentSlot)
	case slot.StateSealing:
		return c.handleSealingState(ctx, currentSlot)
	}

	return nil
}

func (c *Coordinator) handleStartingState(ctx context.Context, currentSlot uint64) error {
	stateMachineSlot := c.stateMachine.GetCurrentSlot()

	if stateMachineSlot >= currentSlot {
		if stateMachineSlot > currentSlot {
			c.log.Warn().
				Uint64("current_slot", currentSlot).
				Uint64("state_machine_slot", stateMachineSlot).
				Msg("State machine has ahead slot; skipping start")
		}
		return nil
	}

	c.cleanupPreviousSlotConsensusState()

	activeRollups, err := c.registryService.GetActiveRollups(ctx)
	if err != nil {
		return fmt.Errorf("failed to get active rollups: %w", err)
	}

	lastSuperblock, err := c.superblockStore.GetLatestSuperblock(ctx)
	var nextNumber uint64 = 1
	var lastHash = make([]byte, 32)

	if err == nil && lastSuperblock != nil {
		nextNumber = lastSuperblock.Number + 1
		lastHash = lastSuperblock.Hash.Bytes()
	}

	// Seed state machine with last known L2 heads from store so that
	// L2BlocksRequest reflects actual chain heads (prevents block-number
	// mismatches on first/early slots or after restarts).
	for _, chainID := range activeRollups {
		latest, err := c.l2BlockStore.GetLatestL2Block(ctx, chainID)
		if err != nil {
			c.log.Warn().Err(err).
				Str("chain_id", consensus.ChainKeyBytes(chainID)).
				Msg("Failed to get latest L2 block for chain; skipping head seed")
		}
		if err == nil && latest != nil {
			c.stateMachine.SeedLastHead(latest)
		}
	}

	if err := c.stateMachine.BeginSlot(currentSlot, nextNumber, lastHash, activeRollups); err != nil {
		return fmt.Errorf("failed to begin slot: %w", err)
	}

	// Initialize current execution context
	c.currentExecution = &SlotExecution{
		Slot:                 currentSlot,
		State:                slot.StateFree,
		StartTime:            time.Now(),
		NextSuperblockNumber: nextNumber,
		LastSuperblockHash:   lastHash,
		ActiveRollups:        activeRollups,
		ReceivedL2Blocks:     make(map[string]*pb.L2Block),
		SCPInstances:         make(map[string]*slot.SCPInstance),
		L2BlockRequests:      make(map[string]*pb.L2BlockRequest),
		AttemptedRequests:    make(map[string]*queue.QueuedXTRequest),
	}

	c.sendStartSlotMessages(ctx, currentSlot, nextNumber, lastHash, activeRollups)

	return nil
}

// cleanupPreviousSlotConsensusState removes outdated consensus state
// associated with the previous slot from the coordinator.
func (c *Coordinator) cleanupPreviousSlotConsensusState() {
	instances := c.stateMachine.GetSCPInstances()
	if len(instances) == 0 {
		return
	}

	c.log.Info().
		Int("count", len(instances)).
		Uint64("slot", c.stateMachine.GetCurrentSlot()).
		Msg("Cleaning up consensus state from previous slot")

	for _, inst := range instances {
		if inst == nil || inst.Request == nil {
			continue
		}
		xtID, err := inst.Request.XtID()
		if err != nil {
			c.log.Warn().
				Err(err).
				Hex("xt_id", inst.XtID).
				Msg("Failed to compute XtID for cleanup")
			continue
		}
		c.consensusCoord.RemoveState(xtID)
	}
}

func (c *Coordinator) handleFreeState(ctx context.Context, currentSlot uint64) error {
	if c.slot.IsSealTime() {
		c.log.Info().Uint64("current_slot", currentSlot).
			Msg("Seal cutover reached (in free state); requesting seal")
		return c.requestSeal(ctx, currentSlot)
	}

	queuedRequest, err := c.xtQueue.Peek(ctx)
	if err != nil {
		return err
	}
	if queuedRequest == nil {
		return nil
	}

	if queuedRequest.ExpiresAt.Before(time.Now()) {
		c.xtQueue.Dequeue(ctx)
		c.log.Info().Hex("xt_id", queuedRequest.XtID).Msg("Expired XT request removed")
		return nil
	}

	return c.startSCP(ctx, queuedRequest)
}

func (c *Coordinator) handleLockedState(ctx context.Context, currentSlot uint64) error {
	if c.slot.IsSealTime() {
		c.log.Info().Uint64("current_slot", currentSlot).
			Msg("Seal cutover reached (in locked state); requesting seal")
		return c.requestSeal(ctx, currentSlot)
	}

	return nil
}

func (c *Coordinator) handleSealingState(ctx context.Context, currentSlot uint64) error {
	if c.stateMachine.CheckAllL2BlocksReceived() {
		return c.buildSuperblock(ctx, currentSlot)
	}

	// Check for slot timeout
	stateMachineSlot := c.stateMachine.GetCurrentSlot()
	managerCurrentSlot := c.slot.GetCurrent()

	if managerCurrentSlot > stateMachineSlot {
		c.log.Warn().Uint64("current_slot", currentSlot).
			Uint64("state_machine_slot", stateMachineSlot).
			Msg("Slot timeout reached during sealing; handling timeout")
		return c.handleSlotTimeout(ctx, stateMachineSlot)
	}

	return nil
}

func (c *Coordinator) startSCP(ctx context.Context, queuedRequest *queue.QueuedXTRequest) error {
	c.xtQueue.Dequeue(ctx)

	instances := c.stateMachine.GetSCPInstances()
	sequenceNumber := uint64(len(instances))

	participatingChains := c.extractParticipatingChains(queuedRequest.Request)

	attemptNumber := queuedRequest.Attempts + 1 // attempts is 0-based in queue, use 1-based for logs/visibility

	scpInstance := &slot.SCPInstance{
		XtID:                queuedRequest.XtID,
		Slot:                c.stateMachine.GetCurrentSlot(),
		SequenceNumber:      sequenceNumber,
		Attempt:             attemptNumber,
		Request:             queuedRequest.Request,
		ParticipatingChains: participatingChains,
		Votes:               make(map[string]bool),
		StartTime:           time.Now(),
	}

	if err := c.stateMachine.StartSCP(scpInstance); err != nil {
		return fmt.Errorf("failed to start SCP: %w", err)
	}

	// Track attempted request for potential requeue on failure path
	c.currentExecution.AttemptedRequests[fmt.Sprintf("%x", queuedRequest.XtID)] = queuedRequest

	startSCMsg := &pb.Message{
		SenderId: "publisher",
		Payload: &pb.Message_StartSc{
			StartSc: &pb.StartSC{
				Slot:             c.stateMachine.GetCurrentSlot(),
				XtSequenceNumber: sequenceNumber,
				XtRequest:        queuedRequest.Request,
				XtId:             queuedRequest.XtID,
			},
		},
	}

	if err := c.transport.Broadcast(ctx, startSCMsg, ""); err != nil {
		c.log.Error().Err(err).Msg("Failed to broadcast StartSC message")
	}

	c.sendStartSCMessages(ctx, scpInstance)

	c.log.Info().
		Hex("xt_id", scpInstance.XtID).
		Uint64("sequence", sequenceNumber).
		Int("attempt", attemptNumber).
		Int("participating_chains", len(participatingChains)).
		Msg("Started SCP instance")

	return nil
}

func (c *Coordinator) requestSeal(ctx context.Context, currentSlot uint64) error {
	// At seal cutover: abort any in-flight (undecided) xTs and broadcast Decided(false)
	if err := c.forceAbortUndecided(ctx); err != nil {
		return fmt.Errorf("failed to force abort undecided: %w", err)
	}

	includedXTs := c.stateMachine.GetIncludedXTs()

	if err := c.stateMachine.RequestSeal(includedXTs); err != nil {
		return fmt.Errorf("failed to request seal: %w", err)
	}

	c.sendRequestSealMessages(ctx, currentSlot, includedXTs)

	return nil
}

func (c *Coordinator) buildSuperblock(ctx context.Context, slotNumber uint64) error {
	l2Blocks := c.stateMachine.GetReceivedL2Blocks()
	includedXTs := c.stateMachine.GetIncludedXTs()

	if !c.validateL2Blocks(l2Blocks) {
		return c.failSlot(slotNumber, "invalid L2 blocks")
	}

	superblock := &store.Superblock{
		Number:     c.currentExecution.NextSuperblockNumber,
		Slot:       slotNumber,
		ParentHash: common.BytesToHash(c.currentExecution.LastSuperblockHash),
		Timestamp:  time.Now(),
		L2Blocks:   make([]*pb.L2Block, 0, len(l2Blocks)),
		Status:     store.SuperblockStatusPending,
	}

	for _, block := range l2Blocks {
		superblock.L2Blocks = append(superblock.L2Blocks, block)
	}

	superblock.MerkleRoot = common.BytesToHash(c.calculateMerkleRoot(superblock.L2Blocks))
	if len(includedXTs) > 0 {
		sbIDs := make([]common.Hash, 0, len(includedXTs))
		for _, id := range includedXTs {
			sbIDs = append(sbIDs, common.BytesToHash(id))
		}
		superblock.IncludedXTs = sbIDs
	}
	superblock.Hash = common.BytesToHash(c.calculateSuperblockHash(superblock))

	if err := c.superblockStore.StoreSuperblock(ctx, superblock); err != nil {
		return fmt.Errorf("failed to store superblock: %w", err)
	}

	if c.proofs != nil {
		if err := c.proofs.HandleSuperblock(ctx, superblock); err != nil {
			c.log.Warn().Err(err).Uint64("number", superblock.Number).Msg("Failed to enqueue superblock for proving")
		}
		if c.config.Proofs.RequireProof {
			c.log.Info().
				Uint64("number", superblock.Number).
				Uint64("slot", slotNumber).
				Int("l2_blocks", len(superblock.L2Blocks)).
				Int("included_xts", len(includedXTs)).
				Msg("Proof required; deferring L1 publish until proof ready")
			return c.stateMachine.TransitionTo(slot.StateStarting, "proof requested")
		}
	}

	if err := c.publishSuperblockTx(ctx, superblock, nil, nil); err != nil {
		c.log.Error().Err(err).Uint64("number", superblock.Number).Msg("Failed to publish superblock")
	} else {
		c.log.Info().
			Uint64("number", superblock.Number).
			Uint64("slot", slotNumber).
			Int("l2_blocks", len(superblock.L2Blocks)).
			Int("included_xts", len(includedXTs)).
			Msg("Built superblock")
	}

	return c.stateMachine.TransitionTo(slot.StateStarting, "superblock built")
}

// l1EventWatcher listens for L1 OutputProposed events and updates status/rollback.
func (c *Coordinator) l1EventWatcher(ctx context.Context) {
	defer c.workerWg.Done()
	if c.l1Publisher == nil {
		return
	}
	ch, err := c.l1Publisher.WatchSuperblocks(ctx)
	if err != nil {
		c.log.Warn().Err(err).Msg("L1 watcher unavailable")
		return
	}
	for {
		select {
		case <-ctx.Done():
			return
		case <-c.stopCh:
			return
		case ev, ok := <-ch:
			if !ok || ev == nil {
				return
			}
			// Update store for submitted or rollback
			sb, err := c.superblockStore.GetSuperblock(ctx, ev.SuperblockNumber)
			if err != nil {
				// Not ours or not stored; ignore
				continue
			}
			if ev.Type == l1events.SuperblockEventRolledBack {
				sb.Status = store.SuperblockStatusRolledBack
				// Remove from tracking
				c.l1TrackMu.Lock()
				delete(c.l1Tracked, ev.SuperblockNumber)
				c.l1TrackMu.Unlock()
			} else if sb.Status == store.SuperblockStatusPending {
				// Ensure at least Submitted
				sb.Status = store.SuperblockStatusSubmitted
			}
			_ = c.superblockStore.StoreSuperblock(ctx, sb)
		}
	}
}

// l1ReceiptPoller periodically checks transaction receipts for tracked superblocks
// and advances their status to Confirmed/Finalized.
func (c *Coordinator) l1ReceiptPoller(ctx context.Context) {
	defer c.workerWg.Done()
	if c.l1Publisher == nil {
		return
	}
	ticker := time.NewTicker(10 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-c.stopCh:
			return
		case <-ticker.C:
			c.l1TrackMu.Lock()
			for number, txHash := range c.l1Tracked {
				c.updateStatusFromReceipt(ctx, number, txHash)
			}
			c.l1TrackMu.Unlock()
		}
	}
}

func (c *Coordinator) updateStatusFromReceipt(ctx context.Context, number uint64, txHash []byte) {
	status, err := c.l1Publisher.GetPublishStatus(ctx, txHash)
	if err != nil || status == nil {
		return
	}
	sb, err := c.superblockStore.GetSuperblock(ctx, number)
	if err != nil || sb == nil {
		return
	}
	switch status.Status {
	case tx.TransactionStateFinalized:
		sb.Status = store.SuperblockStatusFinalized
		delete(c.l1Tracked, number)
	case tx.TransactionStateConfirmed:
		sb.Status = store.SuperblockStatusConfirmed
	case tx.TransactionStateIncluded:
		if sb.Status == store.SuperblockStatusPending {
			sb.Status = store.SuperblockStatusSubmitted
		}
	case tx.TransactionStateFailed:
		sb.Status = store.SuperblockStatusPending
		delete(c.l1Tracked, number)
	case tx.TransactionStatePending:
		// no-op
	}
	_ = c.superblockStore.StoreSuperblock(ctx, sb)
}

func (c *Coordinator) setupStateCallbacks() {
	c.stateMachine.RegisterStateChangeCallback(slot.StateFree, c.onStateFree)
	c.stateMachine.RegisterStateChangeCallback(slot.StateLocked, c.onStateLocked)
	c.stateMachine.RegisterStateChangeCallback(slot.StateSealing, c.onStateSealing)
}

func (c *Coordinator) setupConsensusCallbacks() {
	c.consensusCoord.SetStartCallback(c.handleConsensusStart)
	c.consensusCoord.SetVoteCallback(c.handleConsensusVote)
	c.consensusCoord.SetDecisionCallback(c.handleConsensusDecision)
}

func (c *Coordinator) onStateFree(from, to slot.State, slot uint64) {
}

func (c *Coordinator) onStateLocked(from, to slot.State, slot uint64) {
}

func (c *Coordinator) onStateSealing(from, to slot.State, slot uint64) {
}

func (c *Coordinator) handleConsensusStart(ctx context.Context, from string, xtReq *pb.XTRequest) error {
	c.log.Info().
		Str("from", from).
		Hex("xt_id", c.calculateXtID(xtReq)).
		Msg("Consensus start callback")
	return nil
}

func (c *Coordinator) handleConsensusVote(ctx context.Context, xtID *pb.XtID, vote bool) error {
	c.log.Info().Hex("xt_id", xtID.Hash).Bool("vote", vote).Msg("Broadcasting vote to sequencers")

	voteMsg := &pb.Message{
		SenderId: "publisher",
		Payload: &pb.Message_Vote{
			Vote: &pb.Vote{
				SenderChainId: []byte("publisher"),
				XtId:          xtID,
				Vote:          vote,
			},
		},
	}

	return c.transport.Broadcast(ctx, voteMsg, "")
}

func (c *Coordinator) handleConsensusDecision(ctx context.Context, xtID *pb.XtID, decision bool) error {
	c.log.Info().Hex("xt_id", xtID.Hash).Bool("decision", decision).Msg("Broadcasting decision to sequencers")

	decidedMsg := &pb.Message{
		SenderId: "publisher",
		Payload: &pb.Message_Decided{
			Decided: &pb.Decided{
				XtId:     xtID,
				Decision: decision,
			},
		},
	}

	err := c.transport.Broadcast(ctx, decidedMsg, "")
	if err != nil {
		return err
	}

	// Tag reason for diagnostics
	reason := "consensus_decision_commit"
	if !decision {
		reason = "consensus_decision_abort"
	}
	return c.stateMachine.ProcessSCPDecisionWithReason(xtID.Hash, decision, reason)
}

// forceAbortUndecided marks undecided SCP instances as decided=false and broadcasts Decided(false)
func (c *Coordinator) forceAbortUndecided(ctx context.Context) error {
	instances := c.stateMachine.GetSCPInstances()
	var errs []error
	for _, inst := range instances {
		if inst.Decision == nil {
			xtID := &pb.XtID{Hash: inst.XtID}
			votesRecorded := 0
			votesRequired := len(inst.ParticipatingChains)
			if st, ok := c.consensusCoord.GetState(xtID); ok {
				votesRecorded = st.GetVoteCount()
				votesRequired = st.GetParticipantCount()
			}

			// Only requeue if no progress was made in the slot. Partial votes indicate
			// the instance was already underway; abort those without retry to avoid
			// duplicating near-commit attempts.
			shouldRequeue := votesRecorded == 0
			reason := "forced_abort_at_seal"
			if !shouldRequeue {
				reason = "forced_abort_at_seal_partial_votes"
			}

			// Broadcast Decided(false)
			decidedMsg := &pb.Message{
				SenderId: "publisher",
				Payload: &pb.Message_Decided{
					Decided: &pb.Decided{XtId: xtID, Decision: false},
				},
			}
			if err := c.transport.Broadcast(ctx, decidedMsg, ""); err != nil {
				c.log.Error().
					Err(err).
					Hex("xt_id", inst.XtID).
					Msg("Failed to broadcast forced abort decision")
				errs = append(errs, fmt.Errorf("broadcast forced abort %x: %w", inst.XtID, err))
			}

			// Update state machine
			if err := c.stateMachine.ProcessSCPDecisionWithReason(inst.XtID, false, reason); err != nil {
				c.log.Error().
					Err(err).
					Hex("xt_id", inst.XtID).
					Msg("Failed to process forced abort decision")
				errs = append(errs, fmt.Errorf("update state forced abort %x: %w", inst.XtID, err))
			}

			// Clean consensus state so requeues start fresh
			c.consensusCoord.RemoveState(xtID)

			if shouldRequeue {
				err := c.requeueRequest(ctx, inst.XtID)
				if err != nil {
					errs = append(errs, err)
				}
			} else {
				c.log.Info().
					Hex("xt_id", inst.XtID).
					Int("votes_recorded", votesRecorded).
					Int("votes_required", votesRequired).
					Str("reason", reason).
					Msg("Forced abort at seal without requeue (partial progress)")
			}
		}
	}
	if len(errs) > 0 {
		return errors.Join(errs...)
	}
	return nil
}

func (c *Coordinator) queueProcessor(ctx context.Context) {
	defer c.workerWg.Done()

	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-c.stopCh:
			return
		case <-ticker.C:
			if _, err := c.xtQueue.RemoveExpired(ctx); err != nil {
				c.log.Error().Err(err).Msg("Failed to remove expired requests")
			}
		}
	}
}

func (c *Coordinator) metricsUpdater(ctx context.Context) {
	defer c.workerWg.Done()

	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-c.stopCh:
			return
		case <-ticker.C:
			c.updateMetrics(ctx)
		}
	}
}

func (c *Coordinator) updateMetrics(ctx context.Context) {
}

func (c *Coordinator) recoverFromWAL(ctx context.Context) error {
	return nil
}

func (c *Coordinator) sendStartSlotMessages(
	ctx context.Context,
	slot uint64,
	superblockNumber uint64,
	lastHash []byte,
	activeRollups [][]byte, //nolint:unparam // later
) {
	l2BlockRequests := c.stateMachine.GetL2BlockRequests()

	blockRequests := make([]*pb.L2BlockRequest, 0, len(l2BlockRequests))
	for _, req := range l2BlockRequests {
		blockRequests = append(blockRequests, req)
	}

	// Log which ChainIDs are being requested for this slot (diagnostics)
	if len(blockRequests) > 0 {
		ids := make([]string, 0, len(blockRequests))
		for _, r := range blockRequests {
			ids = append(ids, fmt.Sprintf("%x", r.ChainId))
		}
		c.log.Info().
			Uint64("slot", slot).
			Int("l2_requests", len(blockRequests)).
			Strs("chain_ids", ids).
			Msg("Broadcasting StartSlot with L2 block requests")
	} else {
		c.log.Info().
			Uint64("slot", slot).
			Msg("Broadcasting StartSlot with no L2 block requests")
	}

	startSlotMsg := &pb.Message{
		SenderId: "publisher",
		Payload: &pb.Message_StartSlot{
			StartSlot: &pb.StartSlot{
				Slot:                 slot,
				NextSuperblockNumber: superblockNumber,
				LastSuperblockHash:   lastHash,
				L2BlocksRequest:      blockRequests,
			},
		},
	}

	if err := c.transport.Broadcast(ctx, startSlotMsg, ""); err != nil {
		c.log.Error().Err(err).Msg("Failed to broadcast StartSlot message")
	}
}

func (c *Coordinator) sendStartSCMessages(ctx context.Context, instance *slot.SCPInstance) {
	xtID, err := instance.Request.XtID()
	if err != nil {
		c.log.Error().Err(err).Hex("xt_id", instance.XtID).Msg("Failed to compute XT ID for SCP start")
		return
	}

	if _, exists := c.consensusCoord.GetState(xtID); exists {
		c.log.Warn().Hex("xt_id", instance.XtID).Msg("SCP transaction already exists, skipping duplicate start")
		return
	}

	if err := c.consensusCoord.StartTransaction(ctx, "superblock-coordinator", instance.Request); err != nil {
		c.log.Error().Err(err).Hex("xt_id", instance.XtID).Msg("Failed to start SCP transaction")
	}
}

func (c *Coordinator) sendRequestSealMessages(ctx context.Context, slot uint64, includedXTs [][]byte) {

	// Prepare hex-encoded xT identifiers for visibility in logs
	xtIDs := make([]string, 0, len(includedXTs))
	for _, id := range includedXTs {
		if len(id) == 0 {
			continue
		}
		xtIDs = append(xtIDs, fmt.Sprintf("%x", id))
	}

	c.log.Info().Uint64("slot", slot).
		Int("included_xts_count", len(xtIDs)).
		Strs("included_xts", xtIDs).
		Msg("Broadcasting RequestSeal message")

	requestSealMsg := &pb.Message{
		SenderId: "publisher",
		Payload: &pb.Message_RequestSeal{
			RequestSeal: &pb.RequestSeal{
				Slot:        slot,
				IncludedXts: includedXTs,
			},
		},
	}

	if err := c.transport.Broadcast(ctx, requestSealMsg, ""); err != nil {
		c.log.Error().Err(err).Msg("Failed to broadcast RequestSeal message")
	}
}

func (c *Coordinator) extractParticipatingChains(request *pb.XTRequest) [][]byte {
	chains := make([][]byte, 0, len(request.Transactions))
	for _, tx := range request.Transactions {
		chains = append(chains, tx.ChainId)
	}
	return chains
}

func (c *Coordinator) validateL2Blocks(blocks map[string]*pb.L2Block) bool {
	slot := c.stateMachine.GetCurrentSlot()
	reqs := c.stateMachine.GetL2BlockRequests()

	for chainIDStr, blk := range blocks {
		if blk.Slot != slot {
			c.log.Error().Uint64("expected_slot", slot).Uint64("got", blk.Slot).Msg("L2 block with wrong slot")
			return false
		}
		req, ok := reqs[chainIDStr]
		if !ok {
			c.log.Error().Str("chain", fmt.Sprintf("%x", blk.ChainId)).Msg("unexpected chain in L2 blocks")
			return false
		}
		// accept blocks that are at or ahead of the requested number.
		// Reject only if the sequencer submitted an older block than requested.
		// TODO: rethink, parent hash validation too
		if blk.BlockNumber < req.BlockNumber {
			c.log.Error().
				Uint64("expected_min", req.BlockNumber).
				Uint64("got", blk.BlockNumber).
				Msg("L2 block number below requested minimum")
			return false
		}
	}

	return true
}

func (c *Coordinator) failSlot(slotNumber uint64, reason string) error {
	c.log.Error().Uint64("slot", slotNumber).Str("reason", reason).Msg("Slot failed")
	// Requeue attempted xTs for next slot
	if err := c.requeueAttemptedRequests(context.Background()); err != nil {
		c.log.Error().Err(err).Msg("Failed to requeue attempted requests")
	}
	return c.stateMachine.TransitionTo(slot.StateStarting, fmt.Sprintf("slot failed: %s", reason))
}

// requeueAttemptedRequests requeues all attempted cross-chain transactions for the next slot.
// This is called when a slot fails (e.g., validation errors, timeouts) to ensure that
// transactions that were started but not successfully included in a superblock are retried.
// The queued transactions will have their attempt count incremented and priority adjusted.
func (c *Coordinator) requeueAttemptedRequests(ctx context.Context) error {
	if c.currentExecution == nil || len(c.currentExecution.AttemptedRequests) == 0 {
		return nil
	}
	reqs := make([]*queue.QueuedXTRequest, 0, len(c.currentExecution.AttemptedRequests))
	for _, r := range c.currentExecution.AttemptedRequests {
		reqs = append(reqs, r)
	}
	// Clear current map to avoid double requeue
	c.currentExecution.AttemptedRequests = make(map[string]*queue.QueuedXTRequest)
	return c.xtQueue.RequeueForSlot(ctx, reqs)
}

func (c *Coordinator) requeueRequest(ctx context.Context, xtID []byte) error {
	if c.currentExecution == nil || len(c.currentExecution.AttemptedRequests) == 0 {
		return nil
	}

	var retryReq *queue.QueuedXTRequest
	for _, r := range c.currentExecution.AttemptedRequests {
		if bytes.Equal(r.XtID, xtID) {
			retryReq = r
			break
		}
	}

	if retryReq == nil {
		c.log.Warn().Hex("xt_id", xtID).Msg("Unable to find queued request")
		return nil
	}

	c.log.Info().Hex("xt_id", xtID).Msg("Requeueing XT request")

	return c.xtQueue.RequeueForSlot(ctx, []*queue.QueuedXTRequest{retryReq})
}

func (c *Coordinator) handleSlotTimeout(ctx context.Context, slotNumber uint64) error {
	c.log.Warn().Uint64("slot", slotNumber).Msg("Slot timeout")

	// attempt to build superblock with partial blocks
	return c.buildPartialSuperblock(ctx, slotNumber)
}

func (c *Coordinator) buildPartialSuperblock(ctx context.Context, slotNumber uint64) error {
	receivedL2Blocks := c.stateMachine.GetReceivedL2Blocks()
	scpInstances := c.stateMachine.GetSCPInstances()

	// check if we can build with partial blocks
	receivedChainIDs := make(map[string]bool)
	for chainID := range receivedL2Blocks {
		receivedChainIDs[chainID] = true
	}

	// Check if SCPs with decisions=1 have all their participating chains represented
	canBuildPartial := true
	for _, instance := range scpInstances {
		if instance.Decision != nil && *instance.Decision {
			// This SCP was included, check if all its chains sent blocks
			hasAtLeastOneChain := false
			for _, chainID := range instance.ParticipatingChains {
				if receivedChainIDs[string(chainID)] {
					hasAtLeastOneChain = true
					break
				}
			}
			if !hasAtLeastOneChain {
				canBuildPartial = false
				break
			}
		}
	}

	if canBuildPartial && c.validateL2Blocks(receivedL2Blocks) {
		c.log.Info().
			Uint64("slot", slotNumber).
			Int("received_blocks", len(receivedL2Blocks)).
			Int("total_rollups", len(c.stateMachine.GetActiveRollups())).
			Msg("Building partial superblock due to timeout")
		return c.buildSuperblock(ctx, slotNumber)
	} else {
		c.log.Warn().
			Uint64("slot", slotNumber).
			Int("received_blocks", len(receivedL2Blocks)).
			Bool("can_build_partial", canBuildPartial).
			Msg("Cannot build superblock, failing slot")
		return c.failSlot(slotNumber, "timeout with insufficient blocks")
	}
}

func (c *Coordinator) calculateXtID(request *pb.XTRequest) []byte {
	xtID, _ := request.XtID()
	return xtID.Hash
}

func (c *Coordinator) publishSuperblockTx(
	ctx context.Context,
	sb *store.Superblock,
	proof []byte,
	outputs *proofs.SuperblockAggOutputs,
) error {
	if len(proof) == 0 {
		return fmt.Errorf("proof is required for superblock submission")
	}

	if c.l1Publisher == nil {
		return fmt.Errorf("l1Publisher is not configured, cannot publish superblock %d", sb.Number)
	}

	recorded, err := c.l1Publisher.PublishSuperblockWithProof(ctx, sb, proof, outputs)
	if err != nil {
		return err
	}

	sb.Status = store.SuperblockStatusSubmitted
	sb.L1TransactionHash = common.BytesToHash(recorded.Hash)
	if err := c.superblockStore.StoreSuperblock(ctx, sb); err != nil {
		c.log.Warn().Err(err).Uint64("number", sb.Number).Msg("Failed to persist superblock post-publish")
	}

	c.l1TrackMu.Lock()
	c.l1Tracked[sb.Number] = append([]byte(nil), recorded.Hash...)
	c.l1TrackMu.Unlock()

	return nil
}

func (c *Coordinator) publishWithProof(
	ctx context.Context,
	sb *store.Superblock,
	proof []byte,
	outputs *proofs.SuperblockAggOutputs,
) error {
	return c.publishSuperblockTx(ctx, sb, proof, outputs)
}

func (c *Coordinator) calculateMerkleRoot(blocks []*pb.L2Block) []byte {
	if len(blocks) == 0 {
		return make([]byte, 32)
	}
	// Deterministic leaf order: sort by chainID (lexicographic)
	sort.Slice(blocks, func(i, j int) bool { return string(blocks[i].ChainId) < string(blocks[j].ChainId) })

	// Compute leaf hashes = keccak256(chainID || blockHash || blockNumberBE)
	leaves := make([][]byte, len(blocks))
	for i, b := range blocks {
		buf := make([]byte, 0, len(b.ChainId)+len(b.BlockHash)+8)
		buf = append(buf, b.ChainId...)
		buf = append(buf, b.BlockHash...)
		num := make([]byte, 8)
		binary.BigEndian.PutUint64(num, b.BlockNumber)
		buf = append(buf, num...)
		h := crypto.Keccak256(buf)
		leaves[i] = h
	}

	// Build Merkle tree with keccak256(left||right), duplicate last when odd
	level := leaves
	for len(level) > 1 {
		var next [][]byte
		for i := 0; i < len(level); i += 2 {
			left := level[i]
			right := left
			if i+1 < len(level) {
				right = level[i+1]
			}
			combined := append(append([]byte{}, left...), right...)
			next = append(next, crypto.Keccak256(combined))
		}
		level = next
	}
	return level[0]
}

func (c *Coordinator) calculateSuperblockHash(superblock *store.Superblock) []byte {
	// Header fields: Number || Slot || ParentHash || MerkleRoot
	header := make([]byte, 0, 8+8+common.HashLength+common.HashLength)
	nb := make([]byte, 8)
	sb := make([]byte, 8)
	binary.BigEndian.PutUint64(nb, superblock.Number)
	binary.BigEndian.PutUint64(sb, superblock.Slot)
	header = append(header, nb...)
	header = append(header, sb...)
	header = append(header, superblock.ParentHash.Bytes()...)
	header = append(header, superblock.MerkleRoot.Bytes()...)
	return crypto.Keccak256(header)
}

func (c *Coordinator) handleL2Block(ctx context.Context, from string, l2Block *pb.L2Block) error {
	c.log.Info().
		Str("from", from).
		Uint64("block_number", l2Block.BlockNumber).
		Str("chain_id", fmt.Sprintf("%x", l2Block.ChainId)).
		Msg("Received L2 block")

	if err := c.l2BlockStore.StoreL2Block(ctx, l2Block); err != nil {
		c.log.Error().Err(err).Msg("Failed to store L2 block")
		return err
	}

	return c.stateMachine.ReceiveL2Block(l2Block)
}
