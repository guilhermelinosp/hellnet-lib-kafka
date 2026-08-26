package kafka

import (
	"context"
	"fmt"
)

// Producer is a type-safe producer bound to a single message type T — generics
// as the abstraction. The topic is derived from T.MessageType, and the
// serializer is selected from the options (json|avro|protobuf). The context is
// captured once at NewProducer and propagated internally; applications never
// pass ctx to operations.
type Producer[T Message] struct {
	bus *Bus
}

// NewProducer loads env (HELLNET_KAFKA_* via .env) and builds a typed producer
// for T. The context is captured once here (stored as the Bus base context):
// every Publish derives its per-attempt produce timeout from it, so callers
// never pass ctx to operations. Explicit opts override the environment when
// provided.
func NewProducer[T Message](ctx context.Context, opts ...Options) (*Producer[T], error) {
	if ctx == nil {
		ctx = context.Background()
	}
	loadEnvFiles()
	o := LoadFromEnv()
	if len(opts) > 0 {
		o = opts[0]
	}
	if err := o.validate(); err != nil {
		return nil, err
	}
	s, err := o.buildSerializer(ctx)
	if err != nil {
		return nil, err
	}
	o.Serializer = s
	bus, err := newBus(ctx, o)
	if err != nil {
		return nil, err
	}
	return &Producer[T]{bus: bus}, nil
}

// Publish produces msg to "{prefix}.{messageType}". The context captured at
// NewProducer is used internally (each attempt bounded by TimeoutProduce);
// applications never pass ctx to operations.
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
