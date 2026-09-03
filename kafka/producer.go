package kafka

import (
	"fmt"
)

// Producer is a type-safe producer bound to a single message type T — generics
// as the abstraction. The topic is derived from T.MessageType, and the
// serializer is selected from the options (json|avro|protobuf). NewProducer
// uses the Bus created by the zero-config New constructor.
type Producer[T Message] struct {
	bus *Bus
}

// NewProducer follows the zero-config New pattern: it creates the base context,
// loads .env, and resolves all options from HELLNET_KAFKA_*.
func NewProducer[T Message]() (*Producer[T], error) {
	bus, err := New()
	if err != nil {
		return nil, err
	}
	return &Producer[T]{bus: bus}, nil
}

// Publish produces msg to "{prefix}.{messageType}". The constructor context is
// used internally, with each attempt bounded by TimeoutProduce.
func (p *Producer[T]) Publish(msg T) error {
	return p.bus.Publish(msg)
}

// Close releases the underlying connection.
func (p *Producer[T]) Close() error {
	if p.bus == nil {
		return fmt.Errorf("kafka: producer already closed")
	}
	err := p.bus.Close()
	p.bus = nil
	return err
}
