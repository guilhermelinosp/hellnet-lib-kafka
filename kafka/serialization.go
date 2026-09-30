package kafka

import (
	"context"
	"encoding/json"
)

// Serializer marshals/unmarshals message payloads. Topic is provided so
// schema-backed serializers can resolve the subject (Confluent convention
// "{topic}-value").
type Serializer interface {
	Serialize(topic string, value any) ([]byte, error)
	Deserialize(topic string, data []byte, out any) error
}

// ContextSerializer is an optional extension for serializers that perform
// context-aware work, such as fetching a schema over HTTP. Bus and Consumer
// use it when available; Serializer remains unchanged for compatibility with
// existing custom serializers.
type ContextSerializer interface {
	Serializer
	SerializeContext(ctx context.Context, topic string, value any) ([]byte, error)
	DeserializeContext(ctx context.Context, topic string, data []byte, out any) error
}

func serialize(ctx context.Context, serializer Serializer, topic string, value any) ([]byte, error) {
	if serializer, ok := serializer.(ContextSerializer); ok {
		return serializer.SerializeContext(ctx, topic, value)
	}
	return serializer.Serialize(topic, value)
}

func deserialize(ctx context.Context, serializer Serializer, topic string, data []byte, out any) error {
	if serializer, ok := serializer.(ContextSerializer); ok {
		return serializer.DeserializeContext(ctx, topic, data, out)
	}
	return serializer.Deserialize(topic, data, out)
}

// JSONSerializer is the default serializer: plain JSON of the message struct.
// The message type is carried by the topic, so no envelope is required.
type JSONSerializer struct{}

// Serialize marshals v to JSON (topic is ignored).
func (JSONSerializer) Serialize(_ string, v any) ([]byte, error) {
	return json.Marshal(v)
}

// Deserialize unmarshals data into out (topic is ignored).
func (JSONSerializer) Deserialize(_ string, data []byte, out any) error {
	return json.Unmarshal(data, out)
}
