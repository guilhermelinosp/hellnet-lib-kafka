package kafka

import (
	"context"
	"fmt"
)

// Producer is a type-safe producer bound to a single message type T — generics
// as the abstraction. The topic is derived from T.MessageType, and the
// serializer is selected from the options (json|avro|protobuf).
type Producer[T Message] struct {
	bus *Bus
}

// NewProducer loads env (HELLNET_KAFKA_* via .env) and builds a typed producer
// for T. Explicit opts override the environment when provided.
func NewProducer[T Message](opts ...Options) (*Producer[T], error) {
	o := LoadFromEnv()
	if len(opts) > 0 {
		o = opts[0]
	}
	if err := o.validate(); err != nil {
		return nil, err
	}
	s, err := o.buildSerializer()
	if err != nil {
		return nil, err
	}
	o.Serializer = s
	bus, err := newBus(o)
	if err != nil {
		return nil, err
	}
	return &Producer[T]{bus: bus}, nil
}

// Publish produces msg to "{prefix}.{messageType}" using a default (background)
// context — mirroring hellnet-lib-cache. Use PublishContext for
// cancellation/deadline control.
func (p *Producer[T]) Publish(msg T) error {
	return p.bus.Publish(msg)
}

// PublishContext produces msg with a caller-supplied context.
func (p *Producer[T]) PublishContext(ctx context.Context, msg T) error {
	return p.bus.PublishContext(ctx, msg)
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