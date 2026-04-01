package config

import (
	"fmt"
	"os"
	"strings"
	"time"

	sbcfg "github.com/compose-network/publisher/x/superblock"
	l1cfg "github.com/compose-network/publisher/x/superblock/l1"
	"github.com/spf13/viper"
)

// Config holds the complete application configuration
type Config struct {
	Server    ServerConfig       `mapstructure:"server"    yaml:"server"`
	API       APIServerConfig    `mapstructure:"api"       yaml:"api"`
	Consensus ConsensusConfig    `mapstructure:"consensus" yaml:"consensus"`
	Metrics   MetricsConfig      `mapstructure:"metrics"   yaml:"metrics"`
	Log       LogConfig          `mapstructure:"log"       yaml:"log"`
	Auth      AuthConfig         `mapstructure:"auth"      yaml:"auth"`
	L1        l1cfg.Config       `mapstructure:"l1"        yaml:"l1"`
	Proofs    sbcfg.ProofsConfig `mapstructure:"proofs"    yaml:"proofs"`
	Registry  RegistryConfig     `mapstructure:"registry"  yaml:"registry"`
}

// RegistryConfig holds configuration for the embedded/disk registry
type RegistryConfig struct {
	// Path to a compose registry directory. If empty, embedded registry is used.
	Path string `mapstructure:"path" yaml:"path" env:"REGISTRY_PATH"`
}

// ServerConfig holds server configuration
type ServerConfig struct {
	ListenAddr     string        `mapstructure:"listen_addr"      yaml:"listen_addr"      env:"SERVER_LISTEN_ADDR"`
	ReadTimeout    time.Duration `mapstructure:"read_timeout"     yaml:"read_timeout"     env:"SERVER_READ_TIMEOUT"`
	WriteTimeout   time.Duration `mapstructure:"write_timeout"    yaml:"write_timeout"    env:"SERVER_WRITE_TIMEOUT"`
	MaxMessageSize int           `mapstructure:"max_message_size" yaml:"max_message_size" env:"SERVER_MAX_MESSAGE_SIZE"`
	MaxConnections int           `mapstructure:"max_connections"  yaml:"max_connections"  env:"SERVER_MAX_CONNECTIONS"`
}

// APIServerConfig holds HTTP API server configuration
type APIServerConfig struct {
	ListenAddr        string        `mapstructure:"listen_addr"         yaml:"listen_addr"`
	ReadHeaderTimeout time.Duration `mapstructure:"read_header_timeout" yaml:"read_header_timeout"`
	ReadTimeout       time.Duration `mapstructure:"read_timeout"        yaml:"read_timeout"`
	WriteTimeout      time.Duration `mapstructure:"write_timeout"       yaml:"write_timeout"`
	IdleTimeout       time.Duration `mapstructure:"idle_timeout"        yaml:"idle_timeout"`
	MaxHeaderBytes    int           `mapstructure:"max_header_bytes"    yaml:"max_header_bytes"`
}

// ConsensusConfig holds consensus configuration
type ConsensusConfig struct {
	Timeout time.Duration `mapstructure:"timeout" yaml:"timeout" env:"CONSENSUS_TIMEOUT"`
	// Optional: present for completeness; leader app always runs in leader mode
	Role string `mapstructure:"role"    yaml:"role"    env:"CONSENSUS_ROLE"`
}

// MetricsConfig holds metrics configuration
type MetricsConfig struct {
	Enabled bool   `mapstructure:"enabled" yaml:"enabled" env:"METRICS_ENABLED"`
	Port    int    `mapstructure:"port"    yaml:"port"    env:"METRICS_PORT"`
	Path    string `mapstructure:"path"    yaml:"path"    env:"METRICS_PATH"`
}

// LogConfig holds logging configuration
type LogConfig struct {
	Level  string `mapstructure:"level"  yaml:"level"  env:"LOG_LEVEL"`
	Pretty bool   `mapstructure:"pretty" yaml:"pretty" env:"LOG_PRETTY"`
	Output string `mapstructure:"output" yaml:"output" env:"LOG_OUTPUT"`
	File   string `mapstructure:"file"   yaml:"file"   env:"LOG_FILE"`
}

// AuthConfig holds authentication configuration for the TCP server
type AuthConfig struct {
	Enabled           bool               `mapstructure:"enabled"            yaml:"enabled"            env:"AUTH_ENABLED"`
	PrivateKey        string             `mapstructure:"private_key"        yaml:"private_key"        env:"AUTH_PRIVATE_KEY"` //nolint: lll // w
	TrustedSequencers []TrustedSequencer `mapstructure:"trusted_sequencers" yaml:"trusted_sequencers"`
}

// TrustedSequencer represents a known sequencer identity
type TrustedSequencer struct {
	ID        string `mapstructure:"id"         yaml:"id"`
	PublicKey string `mapstructure:"public_key" yaml:"public_key"`
}

type L2Config struct {
	NetworkName string `mapstructure:"network_name" yaml:"network_name"`
}

// Load loads configuration from file and environment
func Load(configPath string) (*Config, error) {
	v := viper.New()

	v.SetConfigFile(configPath)
	v.SetConfigType("yaml")
	v.AutomaticEnv()
	v.SetEnvKeyReplacer(strings.NewReplacer(".", "_"))

	setDefaults(v)

	if err := v.ReadInConfig(); err != nil {
		return nil, fmt.Errorf("failed to read config file %s: %w", configPath, err)
	}

	var cfg Config
	if err := v.Unmarshal(&cfg); err != nil {
		return nil, fmt.Errorf("failed to unmarshal config: %w", err)
	}

	// Fallback env aliases for L1
	if strings.TrimSpace(cfg.L1.RPCEndpoint) == "" {
		if v := strings.TrimSpace(os.Getenv("L1_RPC_ENDPOINT")); v != "" {
			cfg.L1.RPCEndpoint = v
		}
	}
	if strings.TrimSpace(cfg.L1.DisputeGameFactory) == "" {
		if v := strings.TrimSpace(os.Getenv("L1_DISPUTE_GAME_FACTORY")); v != "" {
			cfg.L1.DisputeGameFactory = v
		} else if v := strings.TrimSpace(os.Getenv("L1_SUPERBLOCK_CONTRACT")); v != "" {
			cfg.L1.DisputeGameFactory = v
		}
	}
	if strings.TrimSpace(cfg.L1.SharedPublisherPkHex) == "" {
		if v := strings.TrimSpace(os.Getenv("L1_SHARED_PUBLISHER_PK_HEX")); v != "" {
			cfg.L1.SharedPublisherPkHex = v
		}
	}

	if strings.TrimSpace(cfg.L1.ComposeNetworkName) == "" {
		if v := strings.TrimSpace(os.Getenv("L1_COMPOSE_NETWORK_NAME")); v != "" {
			cfg.L1.ComposeNetworkName = v
		}
	}

	if err := cfg.Validate(); err != nil {
		return nil, fmt.Errorf("config validation failed: %w", err)
	}

	return &cfg, nil
}

// setDefaults sets default configuration values
func setDefaults(v *viper.Viper) {
	v.SetDefault("server.listen_addr", ":8080")
	v.SetDefault("server.read_timeout", "30s")
	v.SetDefault("server.write_timeout", "30s")
	v.SetDefault("server.max_message_size", 10*1024*1024) // 10MB
	v.SetDefault("server.max_connections", 1000)

	// API defaults (separate HTTP API server)
	v.SetDefault("api.listen_addr", ":8081")
	v.SetDefault("api.read_header_timeout", "5s")
	v.SetDefault("api.read_timeout", "15s")
	v.SetDefault("api.write_timeout", "30s")
	v.SetDefault("api.idle_timeout", "120s")
	v.SetDefault("api.max_header_bytes", 1048576)

	v.SetDefault("consensus.timeout", "60s")

	v.SetDefault("metrics.enabled", true)
	v.SetDefault("metrics.port", 8081)
	v.SetDefault("metrics.path", "/metrics")

	v.SetDefault("log.level", "info")
	v.SetDefault("log.pretty", false)
	v.SetDefault("log.output", "stdout")

	v.SetDefault("auth.enabled", false)
	v.SetDefault("auth.private_key", "")
	v.SetDefault("auth.trusted_sequencers", []map[string]string{})
	v.SetDefault("auth.allow_untrusted", false)
	v.SetDefault("auth.require_auth", true)

	// Registry defaults
	v.SetDefault("registry.path", "")

	// L1 defaults
	v.SetDefault("l1.enabled", true)
	v.SetDefault("l1.rpc_endpoint", "")
	v.SetDefault("l1.dispute_game_factory", "")
	v.SetDefault("l1.chain_id", 0)
	v.SetDefault("l1.confirmations", 2)
	v.SetDefault("l1.finality_depth", 64)
	v.SetDefault("l1.use_eip1559", true)
	v.SetDefault("l1.max_fee_per_gas_wei", "0")
	v.SetDefault("l1.max_priority_fee_wei", "0")
	v.SetDefault("l1.gas_limit_buffer_pct", 15)
	v.SetDefault("l1.shared_publisher_pk_hex", "")
	v.SetDefault("l1.from_address", "")

	// Proofs defaults
	v.SetDefault("proofs.enabled", true)
	v.SetDefault("proofs.collector.require_all_chains", false) // TODO: testing
	v.SetDefault("proofs.collector.wait_timeout", "300s")
	v.SetDefault("proofs.collector.required_chain_ids", []uint32{})
	v.SetDefault("proofs.prover.base_url", "")
	v.SetDefault("proofs.prover.poll_interval", "5s") // TODO: testing
	v.SetDefault("proofs.prover.proof_type", "groth16")
	v.SetDefault("proofs.require_proof", true)
	v.SetDefault("proofs.bypass_prover", false)
}

// Validate validates the configuration
func (c *Config) Validate() error {
	if err := c.validateServer(); err != nil {
		return err
	}
	if err := c.validateConsensus(); err != nil {
		return err
	}
	if err := c.validateMetrics(); err != nil {
		return err
	}
	if err := c.validateAuth(); err != nil {
		return err
	}
	if err := c.validateL1(); err != nil {
		return err
	}
	return nil
}

func (c *Config) validateServer() error {
	if c.Server.MaxMessageSize <= 0 {
		return fmt.Errorf("server.max_message_size must be positive, got %d", c.Server.MaxMessageSize)
	}
	if c.Server.MaxConnections <= 0 {
		return fmt.Errorf("server.max_connections must be positive, got %d", c.Server.MaxConnections)
	}
	if c.Server.ReadTimeout <= 0 {
		return fmt.Errorf("server.read_timeout must be positive")
	}
	if c.Server.WriteTimeout <= 0 {
		return fmt.Errorf("server.write_timeout must be positive")
	}
	return nil
}

func (c *Config) validateConsensus() error {
	if c.Consensus.Timeout <= 0 {
		return fmt.Errorf("consensus.timeout must be positive")
	}
	return nil
}

func (c *Config) validateMetrics() error {
	if c.Metrics.Enabled && (c.Metrics.Port <= 0 || c.Metrics.Port > 65535) {
		return fmt.Errorf("metrics.port must be between 1-65535 when metrics enabled, got %d", c.Metrics.Port)
	}
	return nil
}

func (c *Config) validateAuth() error {
	if !c.Auth.Enabled {
		return nil
	}
	if strings.TrimSpace(c.Auth.PrivateKey) == "" {
		return fmt.Errorf("auth.enabled is true but auth.private_key is empty")
	}
	for _, ts := range c.Auth.TrustedSequencers {
		if strings.TrimSpace(ts.ID) == "" {
			return fmt.Errorf("auth.trusted_sequencers contains an entry with empty id")
		}
		if strings.TrimSpace(ts.PublicKey) == "" {
			return fmt.Errorf("auth.trusted_sequencers[%s] public_key is empty", ts.ID)
		}
	}
	return nil
}

func (c *Config) validateL1() error {
	// If L1 is disabled, skip validation
	if !c.L1.Enabled {
		return nil
	}
	if strings.TrimSpace(c.L1.RPCEndpoint) == "" {
		return fmt.Errorf("l1.rpc_endpoint is required when L1 is enabled")
	}
	if strings.TrimSpace(c.L1.DisputeGameFactory) == "" {
		return fmt.Errorf("l1.dispute_game_factory is required when L1 is enabled")
	}
	return nil
}

// Default returns default configuration
func Default() *Config {
	return &Config{
		Server: ServerConfig{
			ListenAddr:     ":8080",
			ReadTimeout:    30 * time.Second,
			WriteTimeout:   30 * time.Second,
			MaxMessageSize: 10 * 1024 * 1024,
			MaxConnections: 1000,
		},
		API: APIServerConfig{
			ListenAddr:        ":8081",
			ReadHeaderTimeout: 5 * time.Second,
			ReadTimeout:       15 * time.Second,
			WriteTimeout:      30 * time.Second,
			IdleTimeout:       120 * time.Second,
			MaxHeaderBytes:    1 << 20,
		},
		Consensus: ConsensusConfig{
			Timeout: 60 * time.Second,
			Role:    "leader",
		},
		Metrics: MetricsConfig{
			Enabled: true,
			Port:    8081,
			Path:    "/metrics",
		},
		Log: LogConfig{
			Level:  "info",
			Pretty: false,
			Output: "stdout",
		},
		Auth: AuthConfig{
			Enabled:           false,
			PrivateKey:        "",
			TrustedSequencers: []TrustedSequencer{},
		},
		L1: l1cfg.Config{},
	}
}
