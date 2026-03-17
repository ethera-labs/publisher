package registry

import (
	"context"
	"fmt"
	"math/big"
	"path"
	"time"

	compreg "github.com/ethera-labs/registry/registry"
	"github.com/rs/zerolog"
)

// composeService is a static, compose-registry-backed implementation of Service.
// It loads chains for the selected network (by L1 chain ID) from the registry
// (embedded or directory-based) and serves them via the Service interface.
type composeService struct {
	rollups      map[string]*RollupInfo
	log          zerolog.Logger
	l1PublicRPC  string
	publisherDGF string
}

// NewComposeService creates a compose-backed registry service.
// If registryPath is empty, the embedded registry is used.
func NewComposeService(registryPath string, composeNetworkName string, log zerolog.Logger) (*composeService, error) {
	var r compreg.Registry
	var err error
	if registryPath != "" {
		r, err = compreg.NewFromDir(registryPath)
		if err != nil {
			return nil, fmt.Errorf("open registry dir: %w", err)
		}
	} else {
		r = compreg.New()
	}
	if composeNetworkName == "" {
		return nil, fmt.Errorf("empty compose network name")
	}

	net, err := r.GetNetworkBySlug(composeNetworkName)
	if err != nil {
		return nil, fmt.Errorf("failed to get compose network '%s' from registry, %w", composeNetworkName, err)
	}
	log.Info().Str("network", net.Slug()).Msg("loaded compose network from registry")

	// Capture network-level config for later access (L1 RPC, SP contracts)
	ncfg, err := net.LoadConfig()
	if err != nil {
		return nil, fmt.Errorf("load network config: %w", err)
	}
	chains, err := net.ListChains()
	if err != nil {
		return nil, fmt.Errorf("list chains: %w", err)
	}

	rollups := make(map[string]*RollupInfo)
	now := time.Now()
	for _, ch := range chains {
		cfg, err := ch.LoadConfig()
		if err != nil {
			return nil, fmt.Errorf("load config for %s: %w", path.Join(net.Slug(), ch.Slug()), err)
		}
		log.Info().Str("chain", ch.Identifier()).Uint64("chainID", cfg.ChainID).Msg("loading chains from registry")

		// Encode chain id as big-endian bytes to match existing usage
		id := new(big.Int).SetUint64(cfg.ChainID).Bytes()

		// Build endpoint from [sequencer] host:port
		endpoint := cfg.Sequencer.Host
		if cfg.Sequencer.Port != 0 && cfg.Sequencer.Host != "" {
			endpoint = fmt.Sprintf("%s:%d", cfg.Sequencer.Host, cfg.Sequencer.Port)
		}

		ri := &RollupInfo{
			ChainID:      make([]byte, len(id)),
			Endpoint:     endpoint,
			PublicKey:    nil,
			StartingSlot: 1,
			IsActive:     true,
			UpdatedAt:    now,
		}
		copy(ri.ChainID, id)
		rollups[string(ri.ChainID)] = ri
	}

	return &composeService{
		rollups:      rollups,
		log:          log.With().Str("component", "registry.compose").Logger(),
		l1PublicRPC:  ncfg.L1.PublicRPC,
		publisherDGF: ncfg.Publisher.DisputeGameFactory,
	}, nil
}

func (c *composeService) Start(ctx context.Context) error { return nil }
func (c *composeService) Stop(ctx context.Context) error  { return nil }

// Service methods
func (c *composeService) GetActiveRollups(ctx context.Context) ([][]byte, error) {
	return c.active(), nil
}

func (c *composeService) GetRollupEndpoint(ctx context.Context, chainID []byte) (string, error) {
	if ri, ok := c.rollups[string(chainID)]; ok && ri.IsActive {
		return ri.Endpoint, nil
	}
	return "", fmt.Errorf("rollup not found or inactive")
}

func (c *composeService) GetRollupPublicKey(ctx context.Context, chainID []byte) ([]byte, error) {
	if ri, ok := c.rollups[string(chainID)]; ok && ri.IsActive {
		return ri.PublicKey, nil
	}
	return nil, fmt.Errorf("rollup not found or inactive")
}

func (c *composeService) IsRollupActive(ctx context.Context, chainID []byte) (bool, error) {
	if ri, ok := c.rollups[string(chainID)]; ok {
		return ri.IsActive, nil
	}
	return false, nil
}

func (c *composeService) WatchRegistry(ctx context.Context) (<-chan Event, error) {
	ch := make(chan Event)
	close(ch)
	return ch, nil
}

func (c *composeService) GetRollupInfo(chainID []byte) (*RollupInfo, error) {
	if ri, ok := c.rollups[string(chainID)]; ok {
		out := *ri
		out.ChainID = append([]byte(nil), ri.ChainID...)
		if ri.PublicKey != nil {
			out.PublicKey = append([]byte(nil), ri.PublicKey...)
		}
		return &out, nil
	}
	return nil, fmt.Errorf("rollup not found")
}

func (c *composeService) GetAllRollups() map[string]*RollupInfo {
	out := make(map[string]*RollupInfo, len(c.rollups))
	for k, v := range c.rollups {
		cp := *v
		cp.ChainID = append([]byte(nil), v.ChainID...)
		if v.PublicKey != nil {
			cp.PublicKey = append([]byte(nil), v.PublicKey...)
		}
		out[k] = &cp
	}
	return out
}

func (c *composeService) SetPollingInterval(_ time.Duration) {}

// helpers
func (c *composeService) active() [][]byte {
	res := make([][]byte, 0, len(c.rollups))
	for _, v := range c.rollups {
		if v.IsActive {
			b := make([]byte, len(v.ChainID))
			copy(b, v.ChainID)
			res = append(res, b)
		}
	}
	return res
}

// Optional network-level accessors (used by leader app for config hydration)
func (c *composeService) L1PublicRPC() string                 { return c.l1PublicRPC }
func (c *composeService) PublisherDisputeGameFactory() string { return c.publisherDGF }
