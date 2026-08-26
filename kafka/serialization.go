package kafka

import "encoding/json"

// Serializer marshals/unmarshals message payloads. Topic is provided so
// schema-backed serializers can resolve the subject (Confluent convention
// "{topic}-value").
type Serializer interface {
	Serialize(topic string, value any) ([]byte, error)
	Deserialize(topic string, data []byte, out any) error
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
