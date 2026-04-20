package superblock

import (
	"time"

	"github.com/compose-network/publisher/x/superblock/l1"
	"github.com/compose-network/publisher/x/superblock/queue"
	"github.com/compose-network/publisher/x/superblock/slot"
	"github.com/compose-network/publisher/x/superblock/store"
	"github.com/compose-network/publisher/x/superblock/wal"
)

// Config aggregates configuration for all SBCP components
type Config struct {
	Slot   slot.Config  `mapstructure:"slot"   yaml:"slot"`
	Queue  queue.Config `mapstructure:"queue"  yaml:"queue"`
	Store  store.Config `mapstructure:"store"  yaml:"store"`
	WAL    wal.Config   `mapstructure:"wal"    yaml:"wal"`
	L1     l1.Config    `mapstructure:"l1"     yaml:"l1"`
	Proofs ProofsConfig `mapstructure:"proofs" yaml:"proofs"`

	// Coordinator-level settings
	MaxConcurrentSlots     int           `mapstructure:"max_concurrent_slots"     yaml:"max_concurrent_slots"`
	BlockValidationTimeout time.Duration `mapstructure:"block_validation_timeout" yaml:"block_validation_timeout"`
}

// DefaultConfig returns sensible defaults for production deployment
func DefaultConfig() Config {
	return Config{
		Slot:   slot.DefaultConfig(),
		Queue:  queue.DefaultConfig(),
		Store:  store.DefaultConfig(),
		WAL:    wal.DefaultConfig(),
		L1:     l1.DefaultConfig(),
		Proofs: DefaultProofsConfig(),

		MaxConcurrentSlots:     2,
		BlockValidationTimeout: 5 * time.Second,
	}
}

// ProofsConfig controls the optional proof pipeline (collection → proving → L1 publish with proof).
type ProofsConfig struct {
	Enabled bool `mapstructure:"enabled" yaml:"enabled"`

	Collector struct {
		RequireAllChains bool          `mapstructure:"require_all_chains" yaml:"require_all_chains"`
		WaitTimeout      time.Duration `mapstructure:"wait_timeout" yaml:"wait_timeout"`
		RequiredChainIDs []uint32      `mapstructure:"required_chain_ids" yaml:"required_chain_ids"`
	} `mapstructure:"collector" yaml:"collector"`

	Prover struct {
		BaseURL      string        `mapstructure:"base_url" yaml:"base_url"`
		PollInterval time.Duration `mapstructure:"poll_interval" yaml:"poll_interval"`
		ProofType    string        `mapstructure:"proof_type" yaml:"proof_type"`
	} `mapstructure:"prover" yaml:"prover"`

	// If false, SP may fall back to publishing without a proof on prover failure.
	RequireProof bool `mapstructure:"require_proof" yaml:"require_proof"`

	// BypassProver skips the superblock-prover HTTP dispatch and instead synthesizes
	// aggregation outputs from collected per-rollup submissions plus a mock proof,
	// publishing directly to L1. Dev/testing use only.
	BypassProver bool `mapstructure:"bypass_prover" yaml:"bypass_prover"`
}

// DefaultProofsConfig returns sensible defaults.
func DefaultProofsConfig() ProofsConfig {
	return ProofsConfig{
		Enabled: true,
		Collector: struct {
			RequireAllChains bool          `mapstructure:"require_all_chains" yaml:"require_all_chains"`
			WaitTimeout      time.Duration `mapstructure:"wait_timeout" yaml:"wait_timeout"`
			RequiredChainIDs []uint32      `mapstructure:"required_chain_ids" yaml:"required_chain_ids"`
		}{RequireAllChains: true, WaitTimeout: 300 * time.Second, RequiredChainIDs: nil},
		Prover: struct {
			BaseURL      string        `mapstructure:"base_url" yaml:"base_url"`
			PollInterval time.Duration `mapstructure:"poll_interval" yaml:"poll_interval"`
			ProofType    string        `mapstructure:"proof_type" yaml:"proof_type"`
		}{BaseURL: "", PollInterval: 10 * time.Second, ProofType: "groth16"},
		RequireProof: true,
	}
}
