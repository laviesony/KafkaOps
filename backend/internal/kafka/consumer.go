package kafka

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/yourorg/kafkaops/internal/decode"
	"github.com/yourorg/kafkaops/internal/store"
)

// Consumer handles DLQ topic consumption with batch writes to SQLite.
type Consumer struct {
	client       *Client
	messageStore *store.MessageStore
	decoder      *decode.Decoder
	batchSize    int
	batchTimeout time.Duration
}

// NewConsumer creates a new DLQ consumer.
func NewConsumer(client *Client, messageStore *store.MessageStore, decoder *decode.Decoder) *Consumer {
	return &Consumer{
		client:       client,
		messageStore: messageStore,
		decoder:      decoder,
		batchSize:    100,
		batchTimeout: 5 * time.Second,
	}
}

// Consume starts consuming messages from the given topic.
// It batches messages for efficient SQLite writes.
func (c *Consumer) Consume(ctx context.Context, topic string) error {
	// Create a consumer-specific client with topic assignment
	opts := []kgo.Opt{
		kgo.SeedBrokers(c.client.config.Brokers...),
		kgo.ConsumeTopics(topic),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
		kgo.ClientID(c.client.config.ClientID + "-consumer"),
	}

	consumer, err := kgo.NewClient(opts...)
	if err != nil {
		return fmt.Errorf("failed to create consumer client: %w", err)
	}
	defer consumer.Close()

	log.Printf("Started consuming from topic: %s", topic)

	batch := make([]*store.MessageIndex, 0, c.batchSize)
	ticker := time.NewTicker(c.batchTimeout)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			// Flush remaining batch before exit
			if len(batch) > 0 {
				if err := c.flushBatch(batch); err != nil {
					log.Printf("Failed to flush final batch: %v", err)
				}
			}
			return ctx.Err()

		case <-ticker.C:
			// Time-based flush
			if len(batch) > 0 {
				if err := c.flushBatch(batch); err != nil {
					log.Printf("Failed to flush batch: %v", err)
				}
				batch = batch[:0]
			}

		default:
			fetches := consumer.PollFetches(ctx)
			if errs := fetches.Errors(); len(errs) > 0 {
				for _, e := range errs {
					log.Printf("Fetch error topic=%s partition=%d: %v", e.Topic, e.Partition, e.Err)
				}
			}

			iter := fetches.RecordIter()
			for !iter.Done() {
				record := iter.Next()

				msg := c.recordToMessageIndex(record)
				batch = append(batch, msg)

				// Size-based flush
				if len(batch) >= c.batchSize {
					if err := c.flushBatch(batch); err != nil {
						log.Printf("Failed to flush batch: %v", err)
					}
					batch = batch[:0]
				}
			}
		}
	}
}

// recordToMessageIndex converts a Kafka record to a MessageIndex.
func (c *Consumer) recordToMessageIndex(record *kgo.Record) *store.MessageIndex {
	// Extract headers
	headers := make(map[string]string)
	for _, h := range record.Headers {
		headers[h.Key] = string(h.Value)
	}

	// Decode the payload
	decoded, decodeErr := c.decoder.Decode(record.Value)

	msg := &store.MessageIndex{
		Topic:          record.Topic,
		Partition:      int(record.Partition),
		Offset:         record.Offset,
		Key:            record.Key,
		Value:          record.Value,
		Headers:        headers,
		Timestamp:      record.Timestamp.UnixMilli(),
		DecodedPayload: decoded,
	}

	if decodeErr != nil {
		msg.DecodeError = decodeErr.Error()
	}

	return msg
}

// flushBatch writes a batch of messages to SQLite.
func (c *Consumer) flushBatch(batch []*store.MessageIndex) error {
	if len(batch) == 0 {
		return nil
	}

	if err := c.messageStore.InsertBatch(batch); err != nil {
		return fmt.Errorf("failed to insert batch: %w", err)
	}

	log.Printf("Flushed %d messages to store", len(batch))
	return nil
}
