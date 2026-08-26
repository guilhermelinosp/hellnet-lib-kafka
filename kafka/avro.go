package kafka

import (
	"encoding/binary"
	"fmt"

	"github.com/hamba/avro/v2"
)

// AvroSerializer serializes messages with Avro backed by a Schema Registry
// (hellnet-lib-schema -> Apicurio Registry), using the Confluent wire format:
//
//	[magic 0x00][schema id: 4 bytes BE][avro payload]
//
// The subject follows the Confluent convention "{topic}-value"; the schema
// must be pre-registered (e.g. via hellnet-lib-schema scripts/register.sh).
// Structs map to the Avro schema via avro tags (see example/main.go).
type AvroSerializer struct {
	registry *registryClient
}

// NewAvroSerializer builds an Avro serializer bound to the registry URL.
func NewAvroSerializer(url, path string) (*AvroSerializer, error) {
	if url == "" {
		return nil, fmt.Errorf("kafka: schema registry URL is empty")
	}
	return &AvroSerializer{registry: newRegistryClient(url, path)}, nil
}

// Serialize encodes value with the latest schema for "{topic}-value" and
// prepends the Confluent wire header.
func (a *AvroSerializer) Serialize(topic string, value any) ([]byte, error) {
	subject := topic + "-value"
	schemaStr, id, err := a.registry.latestSchema(subject)
	if err != nil {
		return nil, fmt.Errorf("kafka: avro schema %s: %w", subject, err)
	}
	codec, err := avro.Parse(schemaStr)
	if err != nil {
		return nil, fmt.Errorf("kafka: avro parse: %w", err)
	}
	payload, err := avro.Marshal(codec, value)
	if err != nil {
		return nil, fmt.Errorf("kafka: avro encode: %w", err)
	}
	out := make([]byte, 5+len(payload))
	out[0] = 0
	binary.BigEndian.PutUint32(out[1:5], uint32(id)) //nolint:gosec // schema registry id is a small non-negative int
	copy(out[5:], payload)
	return out, nil
}

// Deserialize decodes a Confluent wire-format payload into out, fetching the
// schema referenced by the embedded schema id.
func (a *AvroSerializer) Deserialize(_ string, data []byte, out any) error {
	if len(data) < 5 || data[0] != 0 {
		return fmt.Errorf("kafka: invalid confluent avro wire format")
	}
	id := binary.BigEndian.Uint32(data[1:5])
	schemaStr, err := a.registry.schemaByID(int(id))
	if err != nil {
		return fmt.Errorf("kafka: avro schema id %d: %w", id, err)
	}
	codec, err := avro.Parse(schemaStr)
	if err != nil {
		return fmt.Errorf("kafka: avro parse: %w", err)
	}
	return avro.Unmarshal(codec, data[5:], out)
}
