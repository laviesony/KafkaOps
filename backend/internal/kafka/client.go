package kafka

import (
	"context"
	"crypto/tls"
	"fmt"

	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/sasl/plain"
	"github.com/twmb/franz-go/pkg/sasl/scram"
)

// Config holds Kafka client configuration.
type Config struct {
	Brokers  []string
	SASL     *SASLConfig
	TLS      bool
	ClientID string
}

// SASLConfig holds SASL authentication configuration.
type SASLConfig struct {
	Mechanism string // "PLAIN", "SCRAM-SHA-256", "SCRAM-SHA-512"
	Username  string
	Password  string
}

// Client wraps a franz-go Kafka client with centralized configuration.
type Client struct {
	client *kgo.Client
	config Config
}

// NewClient creates a new Kafka client with the given configuration.
func NewClient(cfg Config) (*Client, error) {
	opts := []kgo.Opt{
		kgo.SeedBrokers(cfg.Brokers...),
		kgo.ClientID(cfg.ClientID),
	}

	// Configure SASL authentication
	if cfg.SASL != nil {
		saslOpt, err := configureSASL(cfg.SASL)
		if err != nil {
			return nil, fmt.Errorf("failed to configure SASL: %w", err)
		}
		opts = append(opts, saslOpt)
	}

	// Configure TLS
	if cfg.TLS {
		opts = append(opts, kgo.DialTLSConfig(&tls.Config{
			MinVersion: tls.VersionTLS12,
		}))
	}

	client, err := kgo.NewClient(opts...)
	if err != nil {
		return nil, fmt.Errorf("failed to create kafka client: %w", err)
	}

	return &Client{
		client: client,
		config: cfg,
	}, nil
}

// configureSASL returns the appropriate SASL option based on mechanism.
func configureSASL(cfg *SASLConfig) (kgo.Opt, error) {
	switch cfg.Mechanism {
	case "PLAIN":
		return kgo.SASL(plain.Auth{
			User: cfg.Username,
			Pass: cfg.Password,
		}.AsMechanism()), nil

	case "SCRAM-SHA-256":
		return kgo.SASL(scram.Auth{
			User: cfg.Username,
			Pass: cfg.Password,
		}.AsSha256Mechanism()), nil

	case "SCRAM-SHA-512":
		return kgo.SASL(scram.Auth{
			User: cfg.Username,
			Pass: cfg.Password,
		}.AsSha512Mechanism()), nil

	default:
		return nil, fmt.Errorf("unsupported SASL mechanism: %s", cfg.Mechanism)
	}
}

// Ping verifies connectivity to the Kafka cluster.
func (c *Client) Ping(ctx context.Context) error {
	return c.client.Ping(ctx)
}

// KgoClient returns the underlying franz-go client for advanced operations.
func (c *Client) KgoClient() *kgo.Client {
	return c.client
}

// Close closes the Kafka client connection.
func (c *Client) Close() {
	c.client.Close()
}
