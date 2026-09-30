package kafka

import (
	"context"
	"fmt"

	"github.com/guilhermelinosp/hellnet-lib-telemetry/instrument"
)

// Producer is a type-safe producer bound to a single message type T — generics
// as the abstraction. The topic is derived from T.MessageType, and the
// serializer is selected from the options (json|avro|protobuf). NewProducer
// uses the Bus created by the zero-config New constructor.
type Producer[T Message] struct {
	bus *Bus
}

// NewProducer creates a producer from KAFKA_* environment variables. inst is the
// Hellnet observability contract (for example a *telemetry.Telemetry, or nil to
// emit no telemetry).
func NewProducer[T Message](ctx context.Context, inst instrument.Instrumentation, options ...Option) (*Producer[T], error) {
	bus, err := New(ctx, inst, options...)
	if err != nil {
		return nil, err
	}
	return &Producer[T]{bus: bus}, nil
}

// Publish produces msg to "{prefix}.{messageType}". The constructor context is
// used internally, with each attempt bounded by TimeoutProduce.
//
// Deprecated: use PublishContext with the caller's request context.
func (p *Producer[T]) Publish(msg T) error {
	return p.bus.Publish(msg)
}

// PublishContext produces msg to "{prefix}.{messageType}" using ctx, so the
// send span is a child of the caller's active span.
func (p *Producer[T]) PublishContext(ctx context.Context, msg T) error {
	if p.bus == nil {
		return fmt.Errorf("kafka: producer already closed")
	}
	return p.bus.PublishContext(ctx, msg)
}

// Shutdown releases the underlying connection unless ctx is already canceled.
func (p *Producer[T]) Shutdown(ctx context.Context) error {
	if p.bus == nil {
		return fmt.Errorf("kafka: producer already closed")
	}
	if err := p.bus.Shutdown(ctx); err != nil {
		return err
	}
	p.bus = nil
	return nil
}

// Close releases the underlying connection.
//
// Deprecated: use Shutdown with a caller-owned context.
func (p *Producer[T]) Close() error {
	if p.bus == nil {
		return fmt.Errorf("kafka: producer already closed")
	}
	err := p.bus.Close()
	p.bus = nil
	return err
}

// Ping checks if the Kafka broker is reachable.
func (p *Producer[T]) Ping(ctx context.Context) error {
	if p.bus == nil {
		return fmt.Errorf("kafka: producer already closed")
	}
	return p.bus.Ping(ctx)
}
