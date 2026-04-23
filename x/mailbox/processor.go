package mailbox

import (
	"bytes"
	"context"
	"crypto/ecdsa"
	"fmt"
	"math/big"
	"strconv"
	"strings"
	"time"

	rollupv1 "github.com/compose-network/publisher/proto/rollup/v1"
	spconsensus "github.com/compose-network/publisher/x/consensus"
	"github.com/compose-network/publisher/x/superblock/sequencer"
	"github.com/compose-network/publisher/x/tracer"
	"github.com/compose-network/publisher/x/transport"
	"github.com/ethereum/go-ethereum/accounts/abi"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/core/vm"
	"github.com/ethereum/go-ethereum/log"
)

type Processor struct {
	chainID              uint64
	mailboxAddresses     []common.Address
	sequencerClients     map[string]transport.Client
	sequencerCoordinator sequencer.Coordinator
	coordinatorKey       *ecdsa.PrivateKey
	coordinatorAddr      common.Address
	mailboxSelector      func(chainID uint64) common.Address
}

func NewProcessor(cfg Config) *Processor {
	addresses := make([]common.Address, len(cfg.MailboxAddresses))
	copy(addresses, cfg.MailboxAddresses)

	clientCopy := make(map[string]transport.Client, len(cfg.SequencerClients))
	for k, v := range cfg.SequencerClients {
		clientCopy[k] = v
	}

	selector := cfg.MailboxSelector
	if selector == nil {
		selector = func(uint64) common.Address { return common.Address{} }
	}

	return &Processor{
		chainID:              cfg.ChainID,
		mailboxAddresses:     addresses,
		sequencerClients:     clientCopy,
		sequencerCoordinator: cfg.SequencerCoordinator,
		coordinatorKey:       cfg.CoordinatorKey,
		coordinatorAddr:      cfg.CoordinatorAddr,
		mailboxSelector:      selector,
	}
}

func (p *Processor) AnalyzeTransaction(
	traceResult *tracer.SSVTraceResult,
	sentOutboundMsgs []CrossRollupMessage,
	fullFilledDeps []CrossRollupDependency,
	tx *types.Transaction,
) (*SimulationState, error) {
	txHashHex := tx.Hash().Hex()
	simState, err := p.analyzeTransaction(traceResult, sentOutboundMsgs, fullFilledDeps, txHashHex)
	if err != nil {
		return nil, fmt.Errorf("failed to analyze transaction: %w", err)
	}

	simState.Tx = tx

	if !simState.RequiresCoordination() {
		log.Info("[SSV] Transaction requires no cross-rollup coordination", "txHash", txHashHex)
		return simState, nil
	}

	log.Info("[SSV] Transaction requires cross-rollup coordination",
		"txHash", txHashHex,
		"dependencies", len(simState.Dependencies),
		"outbound", len(simState.OutboundMessages))

	return simState, nil
}

func (p *Processor) analyzeTransaction(
	traceResult *tracer.SSVTraceResult,
	sentOutboundMsgs []CrossRollupMessage,
	fullfilledDeps []CrossRollupDependency,
	txHashHex string,
) (*SimulationState, error) {
	if traceResult == nil {
		return nil, fmt.Errorf("trace result is nil")
	}
	if traceResult.ExecutionResult == nil {
		return nil, fmt.Errorf("trace execution result missing")
	}

	simState := &SimulationState{
		Success:          traceResult.ExecutionResult.Err == nil,
		Dependencies:     make([]CrossRollupDependency, 0),
		OutboundMessages: make([]CrossRollupMessage, 0),
	}

	log.Info("[SSV] Analyzing transaction trace",
		"txHash", txHashHex,
		"success", simState.Success,
		"operations", len(traceResult.Operations))

	if traceResult.ExecutionResult.Err != nil {
		log.Warn("[SSV] Cross-chain transaction reverted during simulation",
			"txHash", txHashHex,
			"error", traceResult.ExecutionResult.Err,
			"revert", traceResult.ExecutionResult.Revert(),
			"continuing_analysis", true)
	}

	for i, op := range traceResult.Operations {
		p.handleMailboxOperation(op, sentOutboundMsgs, fullfilledDeps, simState, i)
	}

	p.logSimulationSummary(simState, txHashHex)

	return simState, nil
}

func (p *Processor) handleMailboxOperation(
	op tracer.SSVOperation,
	sentOutboundMsgs []CrossRollupMessage,
	fullfilledDeps []CrossRollupDependency,
	simState *SimulationState,
	opIndex int,
) {
	if !p.isMailboxAddress(op.Address) {
		return
	}

	log.Info("[SSV] Found mailbox operation",
		"index", opIndex,
		"type", op.Type.String(),
		"address", op.Address.Hex(),
		"from", op.From.Hex(),
		"callDataLen", len(op.CallData))

	if op.Type != vm.CALL && op.Type != vm.STATICCALL {
		log.Info(
			"[SSV] Ignoring non-CALL/STATICCALL operation to mailbox",
			"type",
			op.Type.String(),
			"address",
			op.Address.Hex(),
		)
		return
	}

	if len(op.CallData) < 4 {
		return
	}

	call, err := p.parseMailboxCall(op.CallData)
	if err != nil {
		log.Info("[SSV] Failed to parse mailbox call", "error", err)
		return
	}

	p.logParsedCall(call)

	if call.IsRead {
		p.processMailboxRead(call, op, fullfilledDeps, simState)
	}
	if call.IsWrite {
		p.processMailboxWrite(call, op, sentOutboundMsgs, simState)
	}
}

func (p *Processor) logParsedCall(call *MailboxCall) {
	if call.IsRead {
		log.Info("[SSV] Parsed mailbox read call",
			"chainMessageSender", call.ChainMessageSender,
			"sender", call.Sender.Hex(),
			"sessionId", call.SessionId,
			"label", string(call.Label))
		return
	}

	if call.IsWrite {
		log.Info("[SSV] Parsed mailbox write call",
			"chainMessageRecipient", call.ChainMessageRecipient,
			"receiver", call.Receiver.Hex(),
			"sessionId", call.SessionId,
			"label", string(call.Label),
			"dataLen", len(call.Data))
	}
}

func (p *Processor) processMailboxRead(
	call *MailboxCall,
	op tracer.SSVOperation,
	fullfilledDeps []CrossRollupDependency,
	simState *SimulationState,
) {
	if !awaitRead(call, p.chainID) {
		log.Info("[SSV] Ignore mailbox read call: chainDest is another chain",
			"chainSrc", call.ChainSrc.Uint64(),
			"chainDest", call.ChainDest.Uint64(),
			"localChain", p.chainID)
		return
	}

	// Receiver is sourced from the header so it matches what the cross-chain
	// writer populated (which in UniversalBridgeMailbox is the user for
	// SEND_TOKENS and the bridge for ACK). Using op.From would always be the
	// calling bridge and mis-match CIRC messages with receiver=user.
	dep := CrossRollupDependency{
		SourceChainID: call.ChainSrc.Uint64(),
		DestChainID:   call.ChainDest.Uint64(),
		Sender:        call.Sender,
		Receiver:      call.Receiver,
		SessionID:     call.SessionId,
		Label:         call.Label,
		RequiredData:  true,
		IsInboxRead:   true,
	}

	if containsDependency(fullfilledDeps, dep) {
		log.Info("[SSV] Ignore mailbox read call: already fulfilled",
			"chainSrc", call.ChainSrc.Uint64(),
			"chainDest", call.ChainDest.Uint64(),
			"localChain", p.chainID)
		return
	}

	simState.Dependencies = append(simState.Dependencies, dep)

	log.Info("[SSV] Detected new mailbox read call",
		"chainSrc", dep.SourceChainID,
		"chainDest", dep.DestChainID,
		"sender", dep.Sender.Hex(),
		"receiver", dep.Receiver.Hex(),
		"sessionId", dep.SessionID)
}

func (p *Processor) processMailboxWrite(
	call *MailboxCall,
	op tracer.SSVOperation,
	sentOutboundMsgs []CrossRollupMessage,
	simState *SimulationState,
) {
	if !mustWrite(call, p.chainID) {
		log.Info("[SSV] Ignore mailbox write call: chainSrc is another chain",
			"chainSrc", call.ChainSrc.Uint64(),
			"chainDest", call.ChainDest.Uint64(),
			"localChain", p.chainID)
		return
	}

	// Sender is taken from the ABI-decoded header, not op.From. The two match
	// for SEND_TOKENS (bridge writes itself as sender), but for ACK the bridge
	// constructs header.sender = the user on the remote chain so the receiving
	// bridge's checkAck can locate the inbox entry by (user, bridge) key.
	// UniversalBridgeMailbox.writeMessage currently overrides header.sender
	// with msg.sender on-chain — that drops this information, so the
	// coordinator reconstructs it from the calldata here.
	msg := CrossRollupMessage{
		SourceChainID: call.ChainSrc.Uint64(),
		DestChainID:   call.ChainDest.Uint64(),
		Sender:        call.Sender,
		Receiver:      call.Receiver,
		SessionID:     call.SessionId,
		Data:          call.Data,
		Label:         call.Label,
		MessageType:   "mailbox_write",
		IsOutboxWrite: true,
	}

	if alreadySent(sentOutboundMsgs, msg) {
		log.Info("[SSV] Ignore mailbox write call: already sent",
			"chainSrc", call.ChainSrc.Uint64(),
			"chainDest", call.ChainDest.Uint64(),
			"localChain", p.chainID)
		return
	}

	simState.OutboundMessages = append(simState.OutboundMessages, msg)

	log.Info("[SSV] Detected new mailbox write call",
		"chainSrc", msg.SourceChainID,
		"chainDest", msg.DestChainID,
		"sender", msg.Sender.Hex(),
		"receiver", msg.Receiver.Hex(),
		"sessionId", msg.SessionID,
		"dataLen", len(msg.Data))
}

func (p *Processor) logSimulationSummary(simState *SimulationState, txHashHex string) {
	log.Info("[SSV] Transaction analysis complete",
		"txHash", txHashHex,
		"requiresCoordination", simState.RequiresCoordination(),
		"dependencies", len(simState.Dependencies),
		"outboundMessages", len(simState.OutboundMessages))

	if !simState.RequiresCoordination() {
		return
	}

	depCount := len(simState.Dependencies)
	outCount := len(simState.OutboundMessages)
	depPreview := make([]string, 0, 2)
	for i := 0; i < depCount && i < 2; i++ {
		d := simState.Dependencies[i]
		depPreview = append(depPreview, fmt.Sprintf("%d:%s->%s", d.SourceChainID, d.Sender.Hex(), d.Receiver.Hex()))
	}
	outPreview := make([]string, 0, 2)
	for i := 0; i < outCount && i < 2; i++ {
		o := simState.OutboundMessages[i]
		outPreview = append(
			outPreview,
			fmt.Sprintf("%d:%s->%s:%s", o.DestChainID, o.Sender.Hex(), o.Receiver.Hex(), string(o.Label)),
		)
	}
	log.Info("[SSV] Coordination classification",
		"txHash", txHashHex,
		"deps", depCount,
		"deps_preview", depPreview,
		"outbound", outCount,
		"out_preview", outPreview,
	)
}

func (p *Processor) HandleCrossRollupCoordination(
	ctx context.Context,
	simState *SimulationState,
	xtID *rollupv1.XtID,
) ([]CrossRollupMessage, []CrossRollupDependency, error) {
	sentMsgs := make([]CrossRollupMessage, 0)
	for _, outMsg := range simState.OutboundMessages {
		if err := p.SendCIRCMessage(ctx, &outMsg, xtID); err != nil {
			return nil, nil, fmt.Errorf("failed to send CIRC message: %w", err)
		}
		sentMsgs = append(sentMsgs, outMsg)
	}

	circDeps := make([]CrossRollupDependency, 0)

	for _, dep := range simState.Dependencies {
		sourceBytes := new(big.Int).SetUint64(dep.SourceChainID).Bytes()
		sourceKey := spconsensus.ChainKeyBytes(sourceBytes)
		circMsg, err := p.waitForCIRCMessage(ctx, xtID, sourceKey, dep)
		if err != nil {
			return nil, nil, fmt.Errorf("failed to wait for CIRC message: %w", err)
		}

		if len(circMsg.Source) > 0 {
			dep.Sender = common.BytesToAddress(circMsg.Source[0])
		}
		if len(circMsg.Receiver) > 0 {
			dep.Receiver = common.BytesToAddress(circMsg.Receiver[0])
		}
		dep.Data = circMsg.Data[0]
		dep.SessionID = new(big.Int).SetBytes(circMsg.SessionId)
		circDeps = append(circDeps, dep)
	}

	log.Info(
		"[SSV] Cross-rollup coordination completed",
		"xtID",
		xtID.Hex(),
		"sent",
		len(sentMsgs),
		"received",
		len(circDeps),
	)
	return sentMsgs, circDeps, nil
}

func (p *Processor) SendCIRCMessage(ctx context.Context, msg *CrossRollupMessage, xtID *rollupv1.XtID) error {
	var sessionID []byte
	if msg.SessionID != nil {
		sessionID = common.LeftPadBytes(msg.SessionID.Bytes(), 32)
	}

	circMsg := &rollupv1.CIRCMessage{
		SourceChain:      new(big.Int).SetUint64(msg.SourceChainID).Bytes(),
		DestinationChain: new(big.Int).SetUint64(msg.DestChainID).Bytes(),
		Source:           [][]byte{msg.Sender.Bytes()},
		Receiver:         [][]byte{msg.Receiver.Bytes()},
		XtId:             xtID,
		Label:            string(msg.Label),
		Data:             [][]byte{msg.Data},
		SessionId:        sessionID,
	}

	spMsg := &rollupv1.Message{
		SenderId: strconv.FormatUint(p.chainID, 10),
		Payload: &rollupv1.Message_CircMessage{
			CircMessage: circMsg,
		},
	}

	destChainID := spconsensus.ChainKeyUint64(msg.DestChainID)
	sequencerClient := p.sequencerClients[destChainID]
	if sequencerClient == nil {
		keys := make([]string, 0, len(p.sequencerClients))
		for k := range p.sequencerClients {
			keys = append(keys, k)
		}
		log.Error("[SSV] Missing sequencer client for destination chain",
			"want", destChainID,
			"available", keys,
		)
		return fmt.Errorf("no client for destination chain %s", destChainID)
	}
	if err := sequencerClient.Send(ctx, spMsg); err != nil {
		log.Error("[SSV] Failed to send CIRC message",
			"xtID", xtID.Hex(),
			"destChain", spconsensus.ChainKeyUint64(msg.DestChainID),
			"err", err,
		)
		return err
	}
	return nil
}

func (p *Processor) CreatePutInboxTx(dep CrossRollupDependency, nonce uint64) (*types.Transaction, error) {
	parsedABI, err := abi.JSON(strings.NewReader(mailboxABI))
	if err != nil {
		return nil, err
	}

	callData, err := parsedABI.Pack("putInbox",
		new(big.Int).SetUint64(dep.SourceChainID),
		dep.Sender,
		dep.Receiver,
		dep.SessionID,
		string(dep.Label),
		dep.Data,
	)
	if err != nil {
		return nil, err
	}

	mailboxAddr := p.mailboxSelector(p.chainID)
	if (mailboxAddr == common.Address{}) {
		return nil, fmt.Errorf("unable to select mailbox addr. No address configured for chain %d", p.chainID)
	}

	txData := &types.DynamicFeeTx{
		ChainID:   new(big.Int).SetUint64(p.chainID),
		Nonce:     nonce,
		GasTipCap: big.NewInt(1_000_000_000),
		GasFeeCap: big.NewInt(20_000_000_000),
		// putInbox on UniversalBridgeMailbox writes one inbox slot, one key flag
		// and mutates the inbox-root hash. With a 320-byte payload (ERC-20 send
		// envelope: chainid + address + amount + name/symbol/decimals) the
		// observed cost is ~520k gas, so we budget 2M to leave headroom for
		// larger payloads.
		Gas:   2_000_000,
		To:    &mailboxAddr,
		Value: big.NewInt(0),
		Data:  callData,
	}

	tx := types.NewTx(txData)
	signedTx, err := types.SignTx(tx, types.NewLondonSigner(new(big.Int).SetUint64(p.chainID)), p.coordinatorKey)
	if err != nil {
		return nil, fmt.Errorf("failed to sign tx %v", err)
	}

	log.Info("[SSV] Created putInbox transaction",
		"txHash", signedTx.Hash().Hex(),
		"nonce", nonce,
		"sessionId", dep.SessionID,
		"mailbox", mailboxAddr.Hex(),
		"sourceChain", dep.SourceChainID,
		"sender", dep.Sender.Hex(),
		"receiver", dep.Receiver.Hex(),
		"label_len", len(dep.Label),
		"data_len", len(dep.Data),
		"gasTipCap", txData.GasTipCap,
		"gasFeeCap", txData.GasFeeCap,
	)

	return signedTx, nil
}

func (p *Processor) waitForCIRCMessage(
	ctx context.Context,
	xtID *rollupv1.XtID,
	sourceChainID string,
	expectedDep CrossRollupDependency,
) (*rollupv1.CIRCMessage, error) {
	if p.sequencerCoordinator == nil || p.sequencerCoordinator.Consensus() == nil {
		return nil, fmt.Errorf("sequencer coordinator unavailable for CIRC consumption")
	}

	timeoutMs := 12000
	timeout := time.NewTimer(time.Duration(timeoutMs) * time.Millisecond)
	defer timeout.Stop()

	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()

	tickCount := 0

	for {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-timeout.C:
			return nil, p.logCIRCTimeout(xtID, sourceChainID)
		case <-ticker.C:
			tickCount++
			circMsg, matched, err := p.consumeCIRCMessage(xtID, sourceChainID, expectedDep)
			if err != nil {
				p.logCIRCWait(xtID, sourceChainID, timeoutMs, tickCount, err)
				continue
			}
			if matched {
				return circMsg, nil
			}
		}
	}
}

func (p *Processor) consumeCIRCMessage(
	xtID *rollupv1.XtID,
	sourceChainID string,
	expectedDep CrossRollupDependency,
) (*rollupv1.CIRCMessage, bool, error) {
	circMsg, err := p.sequencerCoordinator.Consensus().ConsumeCIRCMessage(xtID, sourceChainID)
	if err != nil {
		return nil, false, err
	}

	if matchCIRCToDependency(expectedDep, circMsg) {
		log.Info("[SSV] Consumed matching CIRC message",
			"from", sourceChainID,
			"label", circMsg.GetLabel(),
			"dataLen", func() int {
				if len(circMsg.Data) == 0 {
					return 0
				}
				return len(circMsg.Data[0])
			}(),
		)
		return circMsg, true, nil
	}

	if err := p.sequencerCoordinator.Consensus().RecordCIRCMessage(circMsg); err != nil {
		log.Warn("[SSV] Failed to re-queue non-matching CIRC message", "err", err)
	} else {
		log.Info("[SSV] Deferred non-matching CIRC message",
			"from", sourceChainID,
			"label", circMsg.GetLabel(),
		)
	}

	return nil, false, nil
}

func (p *Processor) logCIRCWait(
	xtID *rollupv1.XtID,
	sourceChainID string,
	timeoutMs int,
	tickCount int,
	waitErr error,
) {
	if tickCount%10 != 0 {
		return
	}
	log.Info("[SSV] Still waiting for CIRC message",
		"xtID", xtID.Hex(),
		"from", sourceChainID,
		"wait_ms", timeoutMs-(tickCount*100),
		"err", waitErr.Error(),
	)
}

func (p *Processor) logCIRCTimeout(xtID *rollupv1.XtID, sourceChainID string) error {
	if p.sequencerCoordinator != nil && p.sequencerCoordinator.Consensus() != nil {
		if st, ok := p.sequencerCoordinator.Consensus().GetState(xtID); ok && st != nil {
			counts := make(map[string]int)
			for k, v := range st.CIRCMessages {
				counts[k] = len(v)
			}
			log.Warn("[SSV] Timeout waiting for CIRC message",
				"xtID", xtID.Hex(),
				"from", sourceChainID,
				"queues", counts,
			)
		}
	}
	return fmt.Errorf("timeout waiting for CIRC message from chain %s", sourceChainID)
}

// messageHeaderABI mirrors IUniversalBridgeMailbox.MessageHeader so the
// go-ethereum ABI decoder can unpack the tuple directly by field name.
type messageHeaderABI struct {
	ChainSrc  *big.Int
	ChainDest *big.Int
	Sender    common.Address
	Receiver  common.Address
	SessionId *big.Int
	Label     string
}

// messageABI mirrors IUniversalBridgeMailbox.Message = (MessageHeader, bytes).
type messageABI struct {
	Header  messageHeaderABI
	Payload []byte
}

func (p *Processor) parseMailboxCall(callData []byte) (*MailboxCall, error) {
	if len(callData) < 4 {
		return nil, fmt.Errorf("invalid call data length")
	}

	methodSig := callData[:4]
	parsedABI, err := abi.JSON(strings.NewReader(mailboxABI))
	if err != nil {
		return nil, err
	}

	if bytes.Equal(methodSig, parsedABI.Methods["readMessage"].ID) {
		call, err := p.parseReadCall(parsedABI, callData[4:])
		if err != nil {
			return nil, err
		}
		call.IsRead = true
		return call, nil
	}

	if bytes.Equal(methodSig, parsedABI.Methods["writeMessage"].ID) {
		call, err := p.parseWriteCall(parsedABI, callData[4:])
		if err != nil {
			return nil, err
		}
		call.IsWrite = true
		return call, nil
	}

	return nil, fmt.Errorf("unknown mailbox method")
}

func (p *Processor) parseReadCall(parsedABI abi.ABI, data []byte) (*MailboxCall, error) {
	inputs := parsedABI.Methods["readMessage"].Inputs
	values, err := inputs.Unpack(data)
	if err != nil {
		return nil, err
	}
	var decoded struct {
		Header messageHeaderABI
	}
	if err := inputs.Copy(&decoded, values); err != nil {
		return nil, err
	}

	h := decoded.Header
	return &MailboxCall{
		ChainMessageSender:    h.ChainSrc,
		ChainMessageRecipient: h.ChainDest,
		Sender:                h.Sender,
		Receiver:              h.Receiver,
		SessionId:             h.SessionId,
		Label:                 []byte(h.Label),
		ChainSrc:              h.ChainSrc,
		ChainDest:             new(big.Int).SetUint64(p.chainID),
	}, nil
}

func (p *Processor) parseWriteCall(parsedABI abi.ABI, data []byte) (*MailboxCall, error) {
	inputs := parsedABI.Methods["writeMessage"].Inputs
	values, err := inputs.Unpack(data)
	if err != nil {
		return nil, err
	}
	var decoded struct {
		Message messageABI
	}
	if err := inputs.Copy(&decoded, values); err != nil {
		return nil, err
	}

	h := decoded.Message.Header
	return &MailboxCall{
		ChainMessageSender:    h.ChainSrc,
		ChainMessageRecipient: h.ChainDest,
		Sender:                h.Sender,
		Receiver:              h.Receiver,
		SessionId:             h.SessionId,
		Label:                 []byte(h.Label),
		Data:                  decoded.Message.Payload,
		ChainSrc:              new(big.Int).SetUint64(p.chainID),
		ChainDest:             h.ChainDest,
	}, nil
}

func (p *Processor) isMailboxAddress(addr common.Address) bool {
	for _, mailboxAddr := range p.mailboxAddresses {
		if addr == mailboxAddr {
			return true
		}
	}
	return false
}
