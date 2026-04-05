package l1

import (
	"context"
	"math/big"
	"testing"
	"time"

	"github.com/compose-network/publisher/x/superblock/proofs"

	"strings"

	pb "github.com/compose-network/publisher/proto/rollup/v1"
	"github.com/compose-network/publisher/x/superblock/l1/contracts"
	"github.com/compose-network/publisher/x/superblock/store"
	"github.com/ethereum/go-ethereum"
	"github.com/ethereum/go-ethereum/accounts/abi"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/crypto"
	"github.com/rs/zerolog"
)

type mockEthClient struct {
	sent             *types.Transaction
	initBond         *big.Int
	lastEstimateCall ethereum.CallMsg
	callContractCnt  int
}

func (m *mockEthClient) ChainID(ctx context.Context) (*big.Int, error) { return big.NewInt(1337), nil }
func (m *mockEthClient) PendingNonceAt(ctx context.Context, account common.Address) (uint64, error) {
	return 7, nil
}
func (m *mockEthClient) SuggestGasTipCap(ctx context.Context) (*big.Int, error) {
	return big.NewInt(2_000_000_000), nil
}
func (m *mockEthClient) SuggestGasPrice(ctx context.Context) (*big.Int, error) {
	return big.NewInt(3_000_000_000), nil
}
func (m *mockEthClient) EstimateGas(ctx context.Context, msg ethereum.CallMsg) (uint64, error) {
	m.lastEstimateCall = msg
	return 100_000, nil
}
func (m *mockEthClient) HeaderByNumber(ctx context.Context, number *big.Int) (*types.Header, error) {
	return &types.Header{
		Number:  big.NewInt(100),
		BaseFee: big.NewInt(10_000_000_000),
		Time:    uint64(time.Now().Unix()),
	}, nil
}
func (m *mockEthClient) CallContract(ctx context.Context, msg ethereum.CallMsg, blockNumber *big.Int) ([]byte, error) {
	m.callContractCnt++
	bond := m.initBond
	if bond == nil {
		bond = big.NewInt(0)
	}
	return common.LeftPadBytes(bond.Bytes(), 32), nil
}
func (m *mockEthClient) SendTransaction(ctx context.Context, tx *types.Transaction) error {
	m.sent = tx
	return nil
}
func (m *mockEthClient) TransactionReceipt(ctx context.Context, txHash common.Hash) (*types.Receipt, error) {
	return nil, ethereum.NotFound
}

func (m *mockEthClient) SubscribeFilterLogs(
	ctx context.Context,
	q ethereum.FilterQuery,
	ch chan<- types.Log,
) (ethereum.Subscription, error) {
	return nil, nil
}

type mockBinding struct {
	addr common.Address
	abi  abi.ABI
}

const mockInitBondsABI = `[{"inputs":[{"internalType":"uint32","name":"_gameType","type":"uint32"}],"name":"initBonds",
"outputs":[{"internalType":"uint256","name":"","type":"uint256"}],"stateMutability":"view","type":"function"}]`

func newMockBinding(addr common.Address) *mockBinding {
	parsed, err := abi.JSON(strings.NewReader(mockInitBondsABI))
	if err != nil {
		panic(err)
	}
	return &mockBinding{addr: addr, abi: parsed}
}

func (b *mockBinding) Address() common.Address { return b.addr }

func (b *mockBinding) BuildPublishWithProofCalldata(
	ctx context.Context,
	superblock *store.Superblock,
	proof []byte,
	outputs *proofs.SuperblockAggOutputs,
) ([]byte, error) {
	return []byte{0xde, 0xad, 0xbe, 0xef}, nil
}

func (b *mockBinding) ABI() abi.ABI { return b.abi }

func (b *mockBinding) GameType() uint32 { return 5555 }

func (b *mockBinding) BuildGameCountCalldata() ([]byte, error) {
	return b.abi.Pack("gameCount")
}

func (b *mockBinding) BuildFindLatestGamesCalldata(start *big.Int, n *big.Int) ([]byte, error) {
	return b.abi.Pack("findLatestGames", b.GameType(), start, n)
}

var _ contracts.Binding = (*mockBinding)(nil)

func TestPublishSuperblock_SignsAndSends(t *testing.T) {
	ctx := context.Background()
	key, _ := crypto.GenerateKey()
	signer := NewLocalECDSASigner(big.NewInt(1337), key)

	expectedBond := big.NewInt(1_234_567_890)
	client := &mockEthClient{initBond: expectedBond}
	binding := newMockBinding(common.HexToAddress("0x000000000000000000000000000000000000dead"))
	cfg := DefaultConfig()
	cfg.ChainID = 1337
	cfg.GasLimitBufferPct = 0

	pub := &EthPublisher{cfg: cfg, client: client, signer: signer, contract: binding, log: zerolog.Nop()}

	sb := &store.Superblock{
		Number:     1,
		Slot:       1,
		ParentHash: common.HexToHash(strings.Repeat("00", 32)),
		Timestamp:  time.Now(),
		L2Blocks:   []*pb.L2Block{},
	}
	tx, err := pub.PublishSuperblockWithProof(ctx, sb, []byte{0x01, 0x02, 0x03}, nil)
	if err != nil {
		t.Fatalf("PublishSuperblockWithProof error: %v", err)
	}
	if tx == nil || len(tx.Hash) == 0 {
		t.Fatalf("expected tx hash")
	}
	if client.sent == nil {
		t.Fatalf("expected transaction to be sent")
	}
	if *client.sent.To() != binding.addr {
		t.Fatalf("unexpected tx to: %s", client.sent.To().Hex())
	}
	if have := client.sent.Data(); len(have) != 4 || have[0] != 0xde {
		t.Fatalf("unexpected calldata")
	}
	if client.sent.Nonce() != 7 {
		t.Fatalf("unexpected nonce: %d", client.sent.Nonce())
	}
	if client.callContractCnt == 0 {
		t.Fatalf("expected init bond call")
	}
	if client.lastEstimateCall.Value == nil || client.lastEstimateCall.Value.Cmp(expectedBond) != 0 {
		t.Fatalf("expected estimate gas value %s, got %v", expectedBond, client.lastEstimateCall.Value)
	}
	if client.sent.Value().Cmp(expectedBond) != 0 {
		t.Fatalf("expected tx value %s, got %s", expectedBond, client.sent.Value())
	}
}
