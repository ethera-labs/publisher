package config

import (
	"os"
	"path/filepath"
	"testing"
)

func TestLoadAcceptsDisputeGameFactoryLegacyEnvAliases(t *testing.T) {
	const contractAddr = "0x00000000000000000000000000000000000000aa"

	for _, tc := range []struct {
		name     string
		envVar   string
		envValue string
	}{
		{
			name:     "dispute_game_factory",
			envVar:   "L1_DISPUTE_GAME_FACTORY",
			envValue: contractAddr,
		},
		{
			name:     "superblock_contract",
			envVar:   "L1_SUPERBLOCK_CONTRACT",
			envValue: contractAddr,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			dir := t.TempDir()
			cfgPath := filepath.Join(dir, "config.yaml")
			if err := os.WriteFile(cfgPath, []byte(`
l1:
  enabled: true
  rpc_endpoint: "http://localhost:8545"
`), 0600); err != nil {
				t.Fatalf("WriteFile() error = %v", err)
			}

			t.Setenv("L1_DISPUTE_GAME_FACTORY", "")
			t.Setenv("L1_SUPERBLOCK_CONTRACT", "")
			t.Setenv(tc.envVar, tc.envValue)

			cfg, err := Load(cfgPath)
			if err != nil {
				t.Fatalf("Load() error = %v", err)
			}

			if got := cfg.L1.DisputeGameFactory; got != contractAddr {
				t.Fatalf("cfg.L1.DisputeGameFactory = %q, want %q", got, contractAddr)
			}
		})
	}
}
