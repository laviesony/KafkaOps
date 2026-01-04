/*
Decoder rules:

1. If payload[0] == 0x0:
   - Parse schema ID from payload[1:5]
   - Fetch schema from Schema Registry (cached)
   - Deserialize using generic Avro
2. Else:
   - Attempt JSON unmarshal
   - Else fallback to string

Return a map[string]interface{} suitable for JSON encoding.

Never panic on decode failure.
*/

package decode

import (
	"encoding/binary"
	"encoding/json"
	"fmt"

	"github.com/linkedin/goavro/v2"
	"github.com/yourorg/kafkaops/internal/schema"
)

// Decoder handles deserialization of Kafka message payloads.
type Decoder struct {
	registry *schema.Registry
	codecs   map[uint32]*goavro.Codec
}

// NewDecoder creates a new decoder with Schema Registry integration.
func NewDecoder(registry *schema.Registry) *Decoder {
	return &Decoder{
		registry: registry,
		codecs:   make(map[uint32]*goavro.Codec),
	}
}

// Decode deserializes a Kafka message payload.
// It follows the Confluent wire format and falls back to JSON/string.
func (d *Decoder) Decode(payload []byte) (any, error) {
	if len(payload) == 0 {
		return nil, nil
	}

	// Check for Confluent wire format (magic byte 0x0)
	if len(payload) >= 5 && payload[0] == 0x0 {
		return d.decodeAvro(payload)
	}

	// Try JSON
	if decoded, err := d.decodeJSON(payload); err == nil {
		return decoded, nil
	}

	// Fallback to string
	return string(payload), nil
}

// decodeAvro handles Confluent Schema Registry wire format.
func (d *Decoder) decodeAvro(payload []byte) (any, error) {
	// Extract schema ID from bytes 1-4 (big-endian uint32)
	schemaID := binary.BigEndian.Uint32(payload[1:5])

	// Get or create codec
	codec, err := d.getCodec(schemaID)
	if err != nil {
		// Fallback to raw data with error annotation
		return map[string]any{
			"_raw":         string(payload[5:]),
			"_schemaId":    schemaID,
			"_decodeError": err.Error(),
		}, fmt.Errorf("failed to get codec for schema %d: %w", schemaID, err)
	}

	// Decode Avro data (bytes after schema ID)
	native, _, err := codec.NativeFromBinary(payload[5:])
	if err != nil {
		return map[string]any{
			"_raw":         string(payload[5:]),
			"_schemaId":    schemaID,
			"_decodeError": err.Error(),
		}, fmt.Errorf("avro decode failed: %w", err)
	}

	return native, nil
}

// getCodec retrieves or creates an Avro codec for the given schema ID.
func (d *Decoder) getCodec(schemaID uint32) (*goavro.Codec, error) {
	// Check cache
	if codec, ok := d.codecs[schemaID]; ok {
		return codec, nil
	}

	// Fetch schema from registry
	schemaJSON, err := d.registry.FetchSchemaByID(schemaID)
	if err != nil {
		return nil, fmt.Errorf("failed to fetch schema: %w", err)
	}

	// Create codec
	codec, err := goavro.NewCodec(schemaJSON)
	if err != nil {
		return nil, fmt.Errorf("failed to create codec: %w", err)
	}

	// Cache codec
	d.codecs[schemaID] = codec

	return codec, nil
}

// decodeJSON attempts to decode payload as JSON.
func (d *Decoder) decodeJSON(payload []byte) (any, error) {
	var result any
	if err := json.Unmarshal(payload, &result); err != nil {
		return nil, err
	}
	return result, nil
}

// Encode serializes a payload back to Avro wire format.
// Used for replay validation.
func (d *Decoder) Encode(payload any, schemaID uint32) ([]byte, error) {
	codec, err := d.getCodec(schemaID)
	if err != nil {
		return nil, fmt.Errorf("failed to get codec: %w", err)
	}

	// Encode to Avro binary
	binary, err := codec.BinaryFromNative(nil, payload)
	if err != nil {
		return nil, fmt.Errorf("avro encode failed: %w", err)
	}

	// Prepend wire format header
	result := make([]byte, 5+len(binary))
	result[0] = 0x0 // magic byte
	putUint32BE(result[1:5], schemaID)
	copy(result[5:], binary)

	return result, nil
}

// putUint32BE writes a uint32 in big-endian format.
func putUint32BE(b []byte, v uint32) {
	b[0] = byte(v >> 24)
	b[1] = byte(v >> 16)
	b[2] = byte(v >> 8)
	b[3] = byte(v)
}

// ValidateJSON validates a JSON payload against an Avro schema.
func (d *Decoder) ValidateJSON(jsonPayload []byte, schemaID uint32) error {
	codec, err := d.getCodec(schemaID)
	if err != nil {
		return fmt.Errorf("failed to get codec: %w", err)
	}

	// Parse JSON
	var native any
	if err := json.Unmarshal(jsonPayload, &native); err != nil {
		return fmt.Errorf("invalid JSON: %w", err)
	}

	// Try to encode (validates against schema)
	_, err = codec.BinaryFromNative(nil, native)
	if err != nil {
		return fmt.Errorf("schema validation failed: %w", err)
	}

	return nil
}
