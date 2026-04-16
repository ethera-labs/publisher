package publisher

import (
	"time"

	"github.com/compose-network/publisher/x/consensus"
	"github.com/compose-network/publisher/x/transport"
)

// Option configures the publisher
type Option func(*Config)

// Config holds publisher configuration
type Config struct {
	Transport      transport.Transport
	Consensus      consensus.Coordinator
	Timeout        time.Duration
	MetricsEnabled bool
}

// WithTransport sets the transport layer
func WithTransport(transport transport.Transport) Option {
	return func(c *Config) {
		c.Transport = transport
	}
}

// WithConsensus sets the consensus coordinator
func WithConsensus(consensus consensus.Coordinator) Option {
	return func(c *Config) {
		c.Consensus = consensus
	}
}

// WithTimeout sets the operation timeout
func WithTimeout(timeout time.Duration) Option {
	return func(c *Config) {
		c.Timeout = timeout
	}
}

// WithMetrics enables metrics collection
func WithMetrics(enabled bool) Option {
	return func(c *Config) {
		c.MetricsEnabled = enabled
	}

}
