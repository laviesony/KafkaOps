/*
Replay rules:

- Destination topic is read from X-Original-Topic header
- Strip all headers starting with X-Exception-
- Preserve correlation / tracing headers
- Validate JSON against Avro schema before producing
- Produce synchronously and return delivery status
*/

package kafka

import (
	"context"
	"fmt"
	"strings"

	"github.com/twmb/franz-go/pkg/kgo"
)

// Producer handles message replay to original topics.
type Producer struct {
	client *Client
}

// NewProducer creates a new replay producer.
func NewProducer(client *Client) *Producer {
	return &Producer{client: client}
}

// ReplayRequest contains the parameters for replaying a message.
type ReplayRequest struct {
	Topic     string            // Destination topic (falls back to X-Original-Topic header)
	Key       []byte            // Message key
	Value     []byte            // Modified message payload
	Headers   map[string]string // Original headers (will be filtered)
	Partition int32             // Target partition (-1 for auto)
}

// ReplayResult contains the result of a replay operation.
type ReplayResult struct {
	Topic     string
	Partition int32
	Offset    int64
}

// Replay sends a corrected message to the original topic.
// It strips X-Exception-* headers and preserves X-Original-* headers.
func (p *Producer) Replay(ctx context.Context, req ReplayRequest) (*ReplayResult, error) {
	// Determine destination topic
	topic := req.Topic
	if topic == "" {
		if originalTopic, ok := req.Headers["X-Original-Topic"]; ok {
			topic = originalTopic
		}
	}
	if topic == "" {
		return nil, fmt.Errorf("no destination topic specified and X-Original-Topic header not found")
	}

	// Filter headers: strip X-Exception-*, preserve X-Original-* and others
	filteredHeaders := filterHeaders(req.Headers)

	// Build Kafka record
	record := &kgo.Record{
		Topic:   topic,
		Key:     req.Key,
		Value:   req.Value,
		Headers: mapToKgoHeaders(filteredHeaders),
	}

	// If specific partition requested
	if req.Partition >= 0 {
		record.Partition = req.Partition
	}

	// Produce synchronously
	results := p.client.KgoClient().ProduceSync(ctx, record)
	if err := results.FirstErr(); err != nil {
		return nil, fmt.Errorf("failed to produce message: %w", err)
	}

	produced := results[0].Record
	return &ReplayResult{
		Topic:     produced.Topic,
		Partition: produced.Partition,
		Offset:    produced.Offset,
	}, nil
}

// ReplayBatch sends multiple corrected messages.
// Returns results for each message and any errors.
func (p *Producer) ReplayBatch(ctx context.Context, requests []ReplayRequest) ([]*ReplayResult, []error) {
	results := make([]*ReplayResult, len(requests))
	errors := make([]error, len(requests))

	records := make([]*kgo.Record, 0, len(requests))
	indices := make([]int, 0, len(requests))

	for i, req := range requests {
		topic := req.Topic
		if topic == "" {
			if originalTopic, ok := req.Headers["X-Original-Topic"]; ok {
				topic = originalTopic
			}
		}
		if topic == "" {
			errors[i] = fmt.Errorf("no destination topic for message %d", i)
			continue
		}

		record := &kgo.Record{
			Topic:   topic,
			Key:     req.Key,
			Value:   req.Value,
			Headers: mapToKgoHeaders(filterHeaders(req.Headers)),
		}
		if req.Partition >= 0 {
			record.Partition = req.Partition
		}

		records = append(records, record)
		indices = append(indices, i)
	}

	// Produce batch
	produceResults := p.client.KgoClient().ProduceSync(ctx, records...)

	for j, pr := range produceResults {
		idx := indices[j]
		if pr.Err != nil {
			errors[idx] = pr.Err
		} else {
			results[idx] = &ReplayResult{
				Topic:     pr.Record.Topic,
				Partition: pr.Record.Partition,
				Offset:    pr.Record.Offset,
			}
		}
	}

	return results, errors
}

// filterHeaders removes X-Exception-* headers while preserving X-Original-* and others.
func filterHeaders(headers map[string]string) map[string]string {
	filtered := make(map[string]string)
	for key, value := range headers {
		// Strip exception headers (these contain error info from DLQ infrastructure)
		if strings.HasPrefix(key, "X-Exception-") {
			continue
		}
		filtered[key] = value
	}
	return filtered
}

// mapToKgoHeaders converts a string map to kgo.RecordHeader slice.
func mapToKgoHeaders(m map[string]string) []kgo.RecordHeader {
	headers := make([]kgo.RecordHeader, 0, len(m))
	for k, v := range m {
		headers = append(headers, kgo.RecordHeader{
			Key:   k,
			Value: []byte(v),
		})
	}
	return headers
}
