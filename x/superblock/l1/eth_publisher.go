package l1

import (
	"context"
	"encoding/hex"
	"fmt"
	"math/big"
	"strings"
	"time"

	"github.com/compose-network/publisher/x/superblock/proofs"

	"github.com/compose-network/publisher/x/superblock/l1/contracts"
	"github.com/compose-network/publisher/x/superblock/l1/events"
	"github.com/compose-network/publisher/x/superblock/l1/tx"
	"github.com/compose-network/publisher/x/superblock/store"
	"github.com/ethereum/go-ethereum"
	"github.com/ethereum/go-ethereum/accounts/abi"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/crypto"
	"github.com/ethereum/go-ethereum/ethclient"
	"github.com/ethereum/go-ethereum/rpc"
	"github.com/rs/zerolog"
)

type abiProvider interface {
	ABI() abi.ABI
}

type gameTypeProvider interface {
	GameType() uint32
}

// EthPublisher publishes superblocks to Ethereum L1 using go-ethereum.
// It implements the Publisher interface and uses EIP-1559 by default.
type EthPublisher struct {
	cfg      Config
	client   ethClient
	signer   Signer
	contract contracts.Binding
	log      zerolog.Logger
}

// NewEthPublisher connects to the RPC endpoint and prepares an EthPublisher.
// The signer may be nil if Config.PrivateKeyHex is set.
func NewEthPublisher(
	ctx context.Context,
	cfg Config,
	contract contracts.Binding,
	signer Signer,
	log zerolog.Logger,
) (*EthPublisher, error) {
	if contract == nil {
		return nil, fmt.Errorf("contract binding must be provided")
	}
	if cfg.RPCEndpoint == "" {
		return nil, fmt.Errorf("rpc_endpoint must be provided")
	}

	// Dial with auto-protocol selection (http/ws)
	rpcClient, err := rpc.DialContext(ctx, cfg.RPCEndpoint)
	if err != nil {
		return nil, fmt.Errorf("failed to dial RPC: %w", err)
	}
	gethClient := ethclient.NewClient(rpcClient)

	// Resolve chainID
	rpcChainID, err := gethClient.ChainID(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to fetch chain id: %w", err)
	}
	if cfg.ChainID == 0 {
		cfg.ChainID = rpcChainID.Uint64()
		log.Info().Uint64("chain_id", cfg.ChainID).Msg("Auto-detected chain ID")
	} else if cfg.ChainID != rpcChainID.Uint64() {
		log.Warn().
			Uint64("config_chain_id", cfg.ChainID).
			Stringer("rpc_chain_id", rpcChainID).
			Msg("Configured chain ID differs from RPC endpoint; using configured value")
	}

	// Build local signer if not provided
	if signer == nil && cfg.SharedPublisherPkHex != "" {
		keyHex := strings.TrimPrefix(cfg.SharedPublisherPkHex, "0x")

		keyBytes, err := hex.DecodeString(keyHex)
		if err != nil {
			return nil, fmt.Errorf("invalid private key hex: %w", err)
		}
		privKey, err := crypto.ToECDSA(keyBytes)
		if err != nil {
			return nil, fmt.Errorf("failed to parse private key: %w", err)
		}

		signer = NewLocalECDSASigner(new(big.Int).SetUint64(cfg.ChainID), privKey)
	}
	if signer == nil {
		return nil, fmt.Errorf("no signer provided; set PrivateKeyHex or pass a Signer")
	}

	ep := &EthPublisher{
		cfg:      cfg,
		client:   gethClient,
		signer:   signer,
		contract: contract,
		log:      log.With().Str("component", "l1-eth-publisher").Logger(),
	}
	return ep, nil
}

// PublishSuperblockWithProof constructs, signs, and broadcasts a transaction
// that calls a proof-enabled contract method to publish the superblock + proof.
func (p *EthPublisher) PublishSuperblockWithProof(
	ctx context.Context,
	superblock *store.Superblock,
	proof []byte,
	outputs *proofs.SuperblockAggOutputs,
) (*tx.Transaction, error) {
	p.log.Info().
		Uint64("superblock_number", superblock.Number).
		Int("l2_block_count", len(superblock.L2Blocks)).
		Msg("Building superblock publish-with-proof transaction")
	for i, block := range superblock.L2Blocks {
		if block != nil {
			p.log.Info().
				Uint64("superblock_number", superblock.Number).
				Int("entry_index", i).
				Uint64("l2_block_number", block.BlockNumber).
				Str("parent_block_hash", fmt.Sprintf("%x", block.ParentBlockHash)).
				Str("block_hash", fmt.Sprintf("%x", block.BlockHash)).
				Str("chain_id", fmt.Sprintf("%x", block.ChainId)).
				Msg("Superblock entry to be published")
		}
	}

	calldata, err := p.contract.BuildPublishWithProofCalldata(ctx, superblock, proof, outputs)
	if err != nil {
		p.log.Error().
			Err(err).
			Uint64("superblock_number", superblock.Number).
			Msg("Failed to build calldata (with proof)")
		return nil, fmt.Errorf("build calldata: %w", err)
	}

	if ap, ok := p.contract.(abiProvider); ok {
		if method, exists := ap.ABI().Methods["create"]; exists {
			if decoded, decErr := method.Inputs.Unpack(calldata[4:]); decErr == nil {
				if len(decoded) >= 3 {
					if rc, ok := decoded[1].([32]byte); ok {
						root := common.BytesToHash(rc[:]).Hex()
						var extraLen int
						if extra, ok := decoded[2].([]byte); ok {
							extraLen = len(extra)
						}
						// TODO: drop once on-chain data is validated in staging.
						p.log.Info().
							Str("root_claim", root).
							Int("extra_data_len", extraLen).
							Msg("Decoded dispute game create call")
					}
				}
			} else {
				// TODO: remove once calldata validation is stable.
				p.log.Info().Err(decErr).Msg("Failed to decode create calldata")
			}
		}
	}

	from := p.signer.From()
	to := p.contract.Address()
	callValue := p.resolveCallValue(ctx, from, to)

	nonce, err := p.client.PendingNonceAt(ctx, from)
	if err != nil {
		p.log.Error().Err(err).Str("from", from.Hex()).Msg("Failed to fetch nonce")
		return nil, fmt.Errorf("fetch nonce: %w", err)
	}

	gasLimit := p.estimateGasLimit(ctx, from, to, callValue, calldata)
	tipCap, feeCap := p.suggestFees(ctx)

	txData := &types.DynamicFeeTx{
		ChainID:   big.NewInt(int64(p.cfg.ChainID)),
		Nonce:     nonce,
		To:        &to,
		Value:     callValue,
		Gas:       gasLimit,
		GasTipCap: tipCap,
		GasFeeCap: feeCap,
		Data:      calldata,
	}
	unsigned := types.NewTx(txData)

	signed, err := p.signer.SignTx(ctx, unsigned)
	if err != nil {
		return nil, fmt.Errorf("sign tx: %w", err)
	}

	if err := p.client.SendTransaction(ctx, signed); err != nil {
		p.log.Error().Err(err).
			Str("tx_hash", signed.Hash().Hex()).
			Uint64("superblock_number", superblock.Number).
			Msg("Failed to send transaction (with proof)")
		return nil, fmt.Errorf("send tx: %w", err)
	}

	p.log.Info().
		Str("tx_hash", signed.Hash().Hex()).
		Uint64("nonce", nonce).
		Uint64("gas_limit", gasLimit).
		Str("gas_tip_cap", tipCap.String()).
		Str("gas_fee_cap", feeCap.String()).
		Str("call_value", callValue.String()).
		Uint64("superblock_number", superblock.Number).
		Msg("Successfully submitted superblock transaction (with proof)")

	return &tx.Transaction{
		Hash:      signed.Hash().Bytes(),
		Nonce:     nonce,
		GasPrice:  0,
		GasLimit:  gasLimit,
		Data:      calldata,
		Timestamp: time.Now(),
	}, nil
}

// estimateGasLimit estimates gas and applies safety buffer
func (p *EthPublisher) estimateGasLimit(
	ctx context.Context,
	from, to common.Address,
	value *big.Int,
	calldata []byte,
) uint64 {
	msgValue := value
	if msgValue == nil {
		msgValue = big.NewInt(0)
	}
	gasMsg := ethereum.CallMsg{From: from, To: &to, Value: msgValue, Data: calldata}
	est, err := p.client.EstimateGas(ctx, gasMsg)
	if err == nil {
		buffer := est * p.cfg.GasLimitBufferPct / 100
		p.log.Debug().
			Uint64("estimated_gas", est).
			Uint64("gas_limit", est+buffer).
			Msg("Gas estimated")
		return est + buffer
	}
	p.log.Warn().
		Err(err).
		Uint64("fallback_gas_limit", 1_500_000).
		Msg("Gas estimation failed, using fallback")
	return 1_500_000
}

func (p *EthPublisher) resolveCallValue(ctx context.Context, from, to common.Address) *big.Int {
	bond, err := p.fetchInitBond(ctx, from, to)
	if err != nil {
		p.log.Warn().Err(err).Msg("Failed to resolve init bond; using zero call value")
		return big.NewInt(0)
	}
	if bond == nil {
		return big.NewInt(0)
	}
	if bond.Sign() > 0 {
		p.log.Debug().Str("init_bond_wei", bond.String()).Msg("Resolved init bond")
	}
	return bond
}

func (p *EthPublisher) fetchInitBond(ctx context.Context, from, to common.Address) (*big.Int, error) {
	ap, ok := p.contract.(abiProvider)
	if !ok {
		return nil, nil
	}
	gp, ok := p.contract.(gameTypeProvider)
	if !ok {
		return nil, nil
	}
	data, err := ap.ABI().Pack("initBonds", gp.GameType())
	if err != nil {
		return nil, fmt.Errorf("pack initBonds call: %w", err)
	}
	msg := ethereum.CallMsg{From: from, To: &to, Data: data}
	res, err := p.client.CallContract(ctx, msg, nil)
	if err != nil {
		return nil, fmt.Errorf("call initBonds: %w", err)
	}
	if len(res) == 0 {
		return big.NewInt(0), nil
	}
	return new(big.Int).SetBytes(res), nil
}

// suggestFees returns EIP-1559 tip and fee caps with config overrides
func (p *EthPublisher) suggestFees(ctx context.Context) (*big.Int, *big.Int) {
	head, _ := p.client.HeaderByNumber(ctx, nil)
	tipCap, err := p.client.SuggestGasTipCap(ctx)
	if err != nil || tipCap == nil {
		tipCap = big.NewInt(2_000_000_000)
	}
	var feeCap *big.Int
	if head != nil && head.BaseFee != nil {
		feeCap = new(big.Int).Add(new(big.Int).Mul(head.BaseFee, big.NewInt(2)), tipCap)
	} else if sp, err := p.client.SuggestGasPrice(ctx); err == nil && sp != nil {
		feeCap = sp
	} else {
		feeCap = new(big.Int).Add(big.NewInt(2_000_000_000), tipCap)
	}
	if p.cfg.MaxPriorityFeeWei != "" {
		if v, ok := new(big.Int).SetString(p.cfg.MaxPriorityFeeWei, 10); ok && v.Sign() > 0 && v.Cmp(tipCap) < 0 {
			tipCap = v
		}
	}
	if p.cfg.MaxFeePerGasWei != "" {
		if v, ok := new(big.Int).SetString(p.cfg.MaxFeePerGasWei, 10); ok && v.Sign() > 0 && v.Cmp(feeCap) < 0 {
			feeCap = v
		}
	}
	return tipCap, feeCap
}

// GetPublishStatus queries the transaction receipt and returns a normalized status.
func (p *EthPublisher) GetPublishStatus(ctx context.Context, txHash []byte) (*tx.TransactionStatus, error) {
	hash := common.BytesToHash(txHash)

	receipt, err := p.client.TransactionReceipt(ctx, hash)
	if err != nil {
		// In case of not found, consider pending
		if strings.Contains(strings.ToLower(err.Error()), "not found") {
			p.log.Debug().Str("tx_hash", hash.Hex()).Msg("Transaction not found, considering as pending")
			return &tx.TransactionStatus{Hash: txHash, Status: tx.TransactionStatePending}, nil
		}
		p.log.Error().Err(err).Str("tx_hash", hash.Hex()).Msg("Failed to get transaction receipt")
		return nil, fmt.Errorf("get receipt: %w", err)
	}

	status := &tx.TransactionStatus{
		Hash:        txHash,
		BlockNumber: receipt.BlockNumber.Uint64(),
		BlockHash:   receipt.BlockHash.Bytes(),
		GasUsed:     receipt.GasUsed,
	}

	if receipt.Status == types.ReceiptStatusFailed {
		status.Status = tx.TransactionStateFailed
		p.log.Warn().
			Str("tx_hash", hash.Hex()).
			Uint64("block_number", status.BlockNumber).
			Uint64("gas_used", status.GasUsed).
			Msg("Transaction failed")
		return status, nil
	}

	// included, compute confirmations
	head, err := p.client.HeaderByNumber(ctx, nil)
	if err != nil {
		p.log.Warn().Err(err).Str("tx_hash", hash.Hex()).Msg("Failed to get latest block for confirmation count")
		status.Status = tx.TransactionStateIncluded
		return status, nil
	}
	if head.Number.Uint64() <= status.BlockNumber {
		status.Status = tx.TransactionStateIncluded
		status.ConfirmationCount = 0
		return status, nil
	}
	confs := head.Number.Uint64() - status.BlockNumber
	status.ConfirmationCount = int(confs)

	switch {
	case confs >= p.cfg.FinalityDepth:
		status.Status = tx.TransactionStateFinalized
		p.log.Debug().
			Str("tx_hash", hash.Hex()).
			Int("confirmations", status.ConfirmationCount).
			Msg("Transaction finalized")
	case confs >= p.cfg.Confirmations:
		status.Status = tx.TransactionStateConfirmed
		p.log.Debug().
			Str("tx_hash", hash.Hex()).
			Int("confirmations", status.ConfirmationCount).
			Msg("Transaction confirmed")
	default:
		status.Status = tx.TransactionStateIncluded
		p.log.Debug().
			Str("tx_hash", hash.Hex()).
			Int("confirmations", status.ConfirmationCount).
			Msg("Transaction included")
	}
	return status, nil
}

// WatchSuperblocks subscribes to contract logs and maps them into SuperblockEvent.
// Without the ABI and concrete event signatures, this returns a closed channel for now.
func (p *EthPublisher) WatchSuperblocks(ctx context.Context) (<-chan *events.SuperblockEvent, error) {
	// Require contract to expose ABI for event decoding (L2OutputOracleBinding)
	ap, ok := p.contract.(abiProvider)
	if !ok {
		p.log.Error().Msg("Contract binding does not expose ABI for events")
		return nil, fmt.Errorf("contract binding does not expose ABI for events")
	}

	p.log.Info().Str("contract_address", p.contract.Address().Hex()).Msg("Starting superblock event watcher")
	evCh, err := events.WatchOutputProposed(ctx, p.client, p.contract.Address(), ap.ABI())
	if err != nil {
		p.log.Error().Err(err).Str("contract_address", p.contract.Address().Hex()).Msg("Failed to start event watcher")
		return nil, err
	}

	out := make(chan *events.SuperblockEvent, 128)
	go func() {
		defer close(out)
		for {
			select {
			case <-ctx.Done():
				return
			case e, ok := <-evCh:
				if !ok {
					return
				}
				p.log.Info().
					Str("event_type", string(e.Type)).
					Uint64("superblock_number", e.SuperblockNumber).
					Str("superblock_hash", common.BytesToHash(e.SuperblockHash).Hex()).
					Uint64("l1_block_number", e.L1BlockNumber).
					Str("l1_tx_hash", common.BytesToHash(e.L1TransactionHash).Hex()).
					Msg("Received superblock event")

				out <- e
			}
		}
	}()
	return out, nil
}

// GetLastSuperblockNumber reads the last published superblock number from the
// L1 contract by calling gameCount + findLatestGames and decoding the
// superblockNumber from the extraData of the most recent compose game.
func (p *EthPublisher) GetLastSuperblockNumber(ctx context.Context) (uint64, error) {
	to := p.contract.Address()

	// 1. Get total game count.
	countData, err := p.contract.BuildGameCountCalldata()
	if err != nil {
		return 0, fmt.Errorf("build gameCount calldata: %w", err)
	}
	countRes, err := p.client.CallContract(ctx, ethereum.CallMsg{To: &to, Data: countData}, nil)
	if err != nil {
		return 0, fmt.Errorf("call gameCount: %w", err)
	}
	if len(countRes) == 0 {
		return 0, nil
	}
	gameCount := new(big.Int).SetBytes(countRes)
	if gameCount.Sign() == 0 {
		return 0, nil
	}

	// 2. findLatestGames(composeGameType, gameCount-1, 1) to get the latest compose game.
	start := new(big.Int).Sub(gameCount, big.NewInt(1))
	findData, err := p.contract.BuildFindLatestGamesCalldata(start, big.NewInt(1))
	if err != nil {
		return 0, fmt.Errorf("build findLatestGames calldata: %w", err)
	}
	findRes, err := p.client.CallContract(ctx, ethereum.CallMsg{To: &to, Data: findData}, nil)
	if err != nil {
		return 0, fmt.Errorf("call findLatestGames: %w", err)
	}

	// 3. Decode the result to extract extraData, then extract superblockNumber.
	return p.decodeSuperblockNumberFromGames(findRes)
}

// decodeSuperblockNumberFromGames decodes the findLatestGames return value and
// extracts superblockNumber (first uint256) from the extraData of the first result.
func (p *EthPublisher) decodeSuperblockNumberFromGames(data []byte) (uint64, error) {
	ap, ok := p.contract.(abiProvider)
	if !ok {
		return 0, fmt.Errorf("contract binding does not expose ABI")
	}

	method, exists := ap.ABI().Methods["findLatestGames"]
	if !exists {
		return 0, fmt.Errorf("findLatestGames method not found in ABI")
	}

	decoded, err := method.Outputs.Unpack(data)
	if err != nil {
		return 0, fmt.Errorf("unpack findLatestGames output: %w", err)
	}

	// decoded[0] is a slice of structs (anonymous tuples).
	games, ok := decoded[0].([]struct {
		Index     *big.Int `json:"index"`
		Metadata  [32]byte `json:"metadata"`
		Timestamp uint64   `json:"timestamp"`
		RootClaim [32]byte `json:"rootClaim"`
		ExtraData []byte   `json:"extraData"`
	})
	if !ok {
		return 0, fmt.Errorf("unexpected findLatestGames result type: %T", decoded[0])
	}
	if len(games) == 0 {
		return 0, nil
	}

	extraData := games[0].ExtraData
	// extraData is ABI-encoded: (SuperblockAggregationOutputs, SuperRootProof, bytes proof)
	// SuperblockAggregationOutputs is a dynamic tuple whose first field is superblockNumber (uint256).
	// The first 32 bytes are the offset to SuperblockAggregationOutputs, then at that offset
	// the first 32 bytes are superblockNumber.
	if len(extraData) < 96 {
		return 0, fmt.Errorf("extraData too short: %d bytes", len(extraData))
	}
	// First word is offset to the first tuple element.
	offset := new(big.Int).SetBytes(extraData[:32]).Uint64()
	if offset+32 > uint64(len(extraData)) {
		return 0, fmt.Errorf("invalid offset in extraData: %d", offset)
	}
	sbNumber := new(big.Int).SetBytes(extraData[offset : offset+32])

	p.log.Info().
		Uint64("superblock_number", sbNumber.Uint64()).
		Uint64("game_index", games[0].Index.Uint64()).
		Msg("Decoded last superblock number from L1 contract")

	return sbNumber.Uint64(), nil
}

// GetLatestL1Block returns basic head info.
func (p *EthPublisher) GetLatestL1Block(ctx context.Context) (*BlockInfo, error) {
	head, err := p.client.HeaderByNumber(ctx, nil)
	if err != nil {
		p.log.Error().Err(err).Msg("Failed to get latest L1 block header")
		return nil, err
	}

	p.log.Debug().
		Uint64("block_number", head.Number.Uint64()).
		Str("block_hash", head.Hash().Hex()).
		Uint64("timestamp", head.Time).
		Msg("Retrieved latest L1 block")
	bi := &BlockInfo{
		Number:     head.Number.Uint64(),
		Hash:       head.Hash().Bytes(),
		ParentHash: head.ParentHash.Bytes(),
		Timestamp:  time.Unix(int64(head.Time), 0),
		GasLimit:   head.GasLimit,
		GasUsed:    head.GasUsed,
	}
	return bi, nil
}
