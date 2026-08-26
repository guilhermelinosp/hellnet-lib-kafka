package kafka

import (
	"encoding/binary"
	"fmt"

	"google.golang.org/protobuf/proto"
)

// ProtobufSerializer serializes messages with Protobuf backed by a Schema
// Registry (hellnet-lib-schema -> Apicurio), using the Confluent wire format:
//
//	[magic 0x00][schema id: 4 bytes BE][protobuf payload]
//
// Values must implement proto.Message; the schema id is resolved from the
// subject "{topic}-value" (the .proto file is registered in the registry).
type ProtobufSerializer struct {
	registry *registryClient
}

// NewProtobufSerializer builds a Protobuf serializer bound to the registry URL.
func NewProtobufSerializer(url, path string) (*ProtobufSerializer, error) {
	if url == "" {
		return nil, fmt.Errorf("kafka: schema registry URL is empty")
	}
	return &ProtobufSerializer{registry: newRegistryClient(url, path)}, nil
}

// Serialize encodes a proto.Message and prepends the Confluent wire header.
func (p *ProtobufSerializer) Serialize(topic string, value any) ([]byte, error) {
	m, ok := value.(proto.Message)
	if !ok {
		return nil, fmt.Errorf("kafka: protobuf: value must implement proto.Message, got %T", value)
	}
	subject := topic + "-value"
	_, id, err := p.registry.latestSchema(subject)
	if err != nil {
		return nil, fmt.Errorf("kafka: protobuf schema %s: %w", subject, err)
	}
	payload, err := proto.Marshal(m)
	if err != nil {
		return nil, fmt.Errorf("kafka: protobuf encode: %w", err)
	}
	out := make([]byte, 5+len(payload))
	out[0] = 0
	binary.BigEndian.PutUint32(out[1:5], uint32(id)) //nolint:gosec // schema registry id is a small non-negative int
	copy(out[5:], payload)
	return out, nil
}

// Deserialize decodes a Confluent wire-format payload into out (a
// proto.Message), resolving the schema id through the registry.
func (p *ProtobufSerializer) Deserialize(_ string, data []byte, out any) error {
	if len(data) < 5 || data[0] != 0 {
		return fmt.Errorf("kafka: invalid confluent protobuf wire format")
	}
	m, ok := out.(proto.Message)
	if !ok {
		return fmt.Errorf("kafka: protobuf: out must implement proto.Message, got %T", out)
	}
	id := binary.BigEndian.Uint32(data[1:5])
	if _, err := p.registry.schemaByID(int(id)); err != nil {
		return fmt.Errorf("kafka: protobuf schema id %d: %w", id, err)
	}
	return proto.Unmarshal(data[5:], m)
}
