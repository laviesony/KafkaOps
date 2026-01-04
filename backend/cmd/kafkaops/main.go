/*
KafkaOps backend entrypoint.

Responsibilities:
- Initialize Kafka client using franz-go
- Initialize Schema Registry client
- Initialize embedded SQLite store
- Expose REST API on localhost
- No Kafka message payload may leave this process

This backend supports:
- Consuming DLQ topics
- Decoding Avro / JSON payloads
- Storing decoded messages locally
- Fix & Replay workflows
*/

package main

import (
	"context"
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/yourorg/kafkaops/internal/api"
	"github.com/yourorg/kafkaops/internal/decode"
	"github.com/yourorg/kafkaops/internal/kafka"
	"github.com/yourorg/kafkaops/internal/schema"
	"github.com/yourorg/kafkaops/internal/store"
)

func main() {
	// Configuration from environment
	cfg := loadConfig()

	// Initialize SQLite store
	db, err := store.OpenDB(cfg.SQLiteDSN)
	if err != nil {
		log.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	messageStore := store.NewMessageStore(db)

	// Initialize Schema Registry client
	registry := schema.NewRegistry(cfg.SchemaRegistryURL)

	// Initialize Decoder
	decoder := decode.NewDecoder(registry)

	// Initialize Kafka client
	kafkaClient, err := kafka.NewClient(kafka.Config{
		Brokers:  cfg.KafkaBrokers,
		SASL:     cfg.KafkaSASL,
		TLS:      cfg.KafkaTLS,
		ClientID: "kafkaops",
	})
	if err != nil {
		log.Fatalf("Failed to create Kafka client: %v", err)
	}
	defer kafkaClient.Close()

	// Initialize Consumer
	consumer := kafka.NewConsumer(kafkaClient, messageStore, decoder)

	// Initialize Producer
	producer := kafka.NewProducer(kafkaClient)

	// Initialize API server
	server := api.NewServer(api.ServerDeps{
		Store:    messageStore,
		Consumer: consumer,
		Producer: producer,
		Decoder:  decoder,
		Addr:     cfg.ServerAddr,
	})

	// Start consuming in background if topic is configured
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	if cfg.DLQTopic != "" {
		go func() {
			log.Printf("Starting to consume from topic: %s", cfg.DLQTopic)
			if err := consumer.Consume(ctx, cfg.DLQTopic); err != nil {
				log.Printf("Consumer error: %v", err)
			}
		}()
	}

	// Graceful shutdown
	go func() {
		sigCh := make(chan os.Signal, 1)
		signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
		<-sigCh
		log.Println("Shutting down...")
		cancel()
		server.Shutdown(context.Background())
	}()

	// Start HTTP server
	log.Printf("Starting KafkaOps server on %s", cfg.ServerAddr)
	if err := server.Start(); err != nil {
		log.Printf("Server stopped: %v", err)
	}
}

// Config holds all configuration for KafkaOps.
type Config struct {
	SQLiteDSN         string
	SchemaRegistryURL string
	KafkaBrokers      []string
	KafkaSASL         *kafka.SASLConfig
	KafkaTLS          bool
	DLQTopic          string
	ServerAddr        string
}

func loadConfig() Config {
	cfg := Config{
		SQLiteDSN:         getEnvOrDefault("KAFKAOPS_SQLITE_DSN", "kafkaops.db"),
		SchemaRegistryURL: getEnvOrDefault("KAFKAOPS_SCHEMA_REGISTRY_URL", ""),
		ServerAddr:        getEnvOrDefault("KAFKAOPS_SERVER_ADDR", ":8080"),
		DLQTopic:          os.Getenv("KAFKAOPS_DLQ_TOPIC"),
		KafkaTLS:          os.Getenv("KAFKAOPS_KAFKA_TLS") == "true",
	}

	// Parse Kafka brokers from comma-separated list
	brokers := os.Getenv("KAFKAOPS_KAFKA_BROKERS")
	if brokers != "" {
		cfg.KafkaBrokers = splitAndTrim(brokers, ",")
	} else {
		cfg.KafkaBrokers = []string{"localhost:9092"}
	}

	// SASL configuration
	saslMechanism := os.Getenv("KAFKAOPS_KAFKA_SASL_MECHANISM")
	if saslMechanism != "" {
		cfg.KafkaSASL = &kafka.SASLConfig{
			Mechanism: saslMechanism,
			Username:  os.Getenv("KAFKAOPS_KAFKA_SASL_USERNAME"),
			Password:  os.Getenv("KAFKAOPS_KAFKA_SASL_PASSWORD"),
		}
	}

	return cfg
}

func getEnvOrDefault(key, defaultVal string) string {
	if val := os.Getenv(key); val != "" {
		return val
	}
	return defaultVal
}

func splitAndTrim(s, sep string) []string {
	var result []string
	for _, part := range splitString(s, sep) {
		trimmed := trimSpace(part)
		if trimmed != "" {
			result = append(result, trimmed)
		}
	}
	return result
}

func splitString(s, sep string) []string {
	if s == "" {
		return nil
	}
	var result []string
	start := 0
	for i := 0; i <= len(s)-len(sep); i++ {
		if s[i:i+len(sep)] == sep {
			result = append(result, s[start:i])
			start = i + len(sep)
			i += len(sep) - 1
		}
	}
	result = append(result, s[start:])
	return result
}

func trimSpace(s string) string {
	start, end := 0, len(s)
	for start < end && (s[start] == ' ' || s[start] == '\t' || s[start] == '\n' || s[start] == '\r') {
		start++
	}
	for end > start && (s[end-1] == ' ' || s[end-1] == '\t' || s[end-1] == '\n' || s[end-1] == '\r') {
		end--
	}
	return s[start:end]
}
