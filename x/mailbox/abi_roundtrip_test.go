package mailbox

import (
	"bytes"
	"math/big"
	"strings"
	"testing"

	"github.com/ethereum/go-ethereum/accounts/abi"
	"github.com/ethereum/go-ethereum/common"
)

// Verifies that calldata packed against the UniversalBridgeMailbox ABI
// decodes back through the processor's parsers with identical fields.
func TestParseMailboxCall_RoundTrip(t *testing.T) {
	parsedABI, err := abi.JSON(strings.NewReader(mailboxABI))
	if err != nil {
		t.Fatalf("parse abi: %v", err)
	}

	wantHeader := messageHeaderABI{
		ChainSrc:  big.NewInt(11111),
		ChainDest: big.NewInt(22222),
		Sender:    common.HexToAddress("0x1111111111111111111111111111111111111111"),
		Receiver:  common.HexToAddress("0x2222222222222222222222222222222222222222"),
		SessionId: big.NewInt(0xABCD1234),
		Label:     "SEND_TOKENS",
	}
	wantPayload := []byte{0xde, 0xad, 0xbe, 0xef}

	p := &Processor{chainID: 22222}

	// readMessage round-trip.
	readData, err := parsedABI.Pack("readMessage", wantHeader)
	if err != nil {
		t.Fatalf("pack readMessage: %v", err)
	}
	readCall, err := p.parseMailboxCall(readData)
	if err != nil {
		t.Fatalf("parse readMessage: %v", err)
	}
	if !readCall.IsRead || readCall.IsWrite {
		t.Fatalf("expected IsRead, got IsRead=%v IsWrite=%v", readCall.IsRead, readCall.IsWrite)
	}
	if readCall.Sender != wantHeader.Sender || readCall.Receiver != wantHeader.Receiver {
		t.Fatalf("address fields mismatch: %+v", readCall)
	}
	if readCall.SessionId.Cmp(wantHeader.SessionId) != 0 {
		t.Fatalf("sessionId mismatch: got %s want %s", readCall.SessionId, wantHeader.SessionId)
	}
	if string(readCall.Label) != wantHeader.Label {
		t.Fatalf("label mismatch: got %q want %q", readCall.Label, wantHeader.Label)
	}
	if readCall.ChainSrc.Cmp(wantHeader.ChainSrc) != 0 {
		t.Fatalf("chainSrc mismatch: got %s want %s", readCall.ChainSrc, wantHeader.ChainSrc)
	}

	// writeMessage round-trip.
	writeData, err := parsedABI.Pack("writeMessage", messageABI{Header: wantHeader, Payload: wantPayload})
	if err != nil {
		t.Fatalf("pack writeMessage: %v", err)
	}
	writeCall, err := p.parseMailboxCall(writeData)
	if err != nil {
		t.Fatalf("parse writeMessage: %v", err)
	}
	if writeCall.IsRead || !writeCall.IsWrite {
		t.Fatalf("expected IsWrite, got IsRead=%v IsWrite=%v", writeCall.IsRead, writeCall.IsWrite)
	}
	if !bytes.Equal(writeCall.Data, wantPayload) {
		t.Fatalf("payload mismatch: got %x want %x", writeCall.Data, wantPayload)
	}
	if string(writeCall.Label) != wantHeader.Label {
		t.Fatalf("write label mismatch: got %q want %q", writeCall.Label, wantHeader.Label)
	}
	if writeCall.ChainDest.Cmp(wantHeader.ChainDest) != 0 {
		t.Fatalf("chainDest mismatch: got %s want %s", writeCall.ChainDest, wantHeader.ChainDest)
	}
}

// Verifies that CreatePutInboxTx packs a string label correctly.
func TestPutInboxTx_PacksStringLabel(t *testing.T) {
	parsedABI, err := abi.JSON(strings.NewReader(mailboxABI))
	if err != nil {
		t.Fatalf("parse abi: %v", err)
	}

	want := struct {
		ChainSrc  *big.Int
		Sender    common.Address
		Receiver  common.Address
		SessionID *big.Int
		Label     string
		Data      []byte
	}{
		ChainSrc:  big.NewInt(77777),
		Sender:    common.HexToAddress("0xAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA"),
		Receiver:  common.HexToAddress("0xBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBB"),
		SessionID: big.NewInt(0xDEAD),
		Label:     "ACK",
		Data:      []byte{0x01, 0x02, 0x03},
	}

	packed, err := parsedABI.Pack("putInbox", want.ChainSrc, want.Sender, want.Receiver, want.SessionID, want.Label, want.Data)
	if err != nil {
		t.Fatalf("pack putInbox: %v", err)
	}

	sig := parsedABI.Methods["putInbox"].ID
	if !bytes.Equal(packed[:4], sig) {
		t.Fatalf("unexpected selector: %x", packed[:4])
	}

	values, err := parsedABI.Methods["putInbox"].Inputs.Unpack(packed[4:])
	if err != nil {
		t.Fatalf("unpack: %v", err)
	}
	if got, ok := values[4].(string); !ok || got != want.Label {
		t.Fatalf("label round-trip failed: got %v (type %T) want %q", values[4], values[4], want.Label)
	}
}
