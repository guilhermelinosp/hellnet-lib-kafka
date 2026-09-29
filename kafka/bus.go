package kafka

import (
	"context"
	"fmt"

	"github.com/guilhermelinosp/hellnet-lib-telemetry/telemetry"
	"github.com/sony/gobreaker"
	"github.com/twmb/franz-go/pkg/kgo"
)

type recordProducer interface {
	ProduceSync(context.Context, ...*kgo.Record) kgo.ProduceResults
	Close()
}

// Bus is the message bus: an idempotent franz-go producer with a circuit
// breaker, plus DLQ publishing. New/MustNew use the constructor context.
type Bus struct {
	opts       Options
	client     *kgo.Client
	dlqWriter  recordProducer
	breaker    *gobreaker.CircuitBreaker
	serializer Serializer
	baseCtx    context.Context
	ops        telemetry.Client
}

func newBus(ctx context.Context, opts Options) (*Bus, error) {
	clientOpts, err := franzOptions(opts)
	if err != nil {
		return nil, err
	}
	client, err := kgo.NewClient(clientOpts...)
	if err != nil {
		return nil, fmt.Errorf("kafka: create franz client: %w", err)
	}
	b := &Bus{
		opts:       opts,
		client:     client,
		dlqWriter:  client,
		serializer: opts.Serializer,
		baseCtx:    ctx,
	}
	if b.serializer == nil {
		b.serializer = JSONSerializer{}
	}
	b.breaker = gobreaker.NewCircuitBreaker(gobreaker.Settings{
		Name: "kafka-produce",
		ReadyToTrip: func(c gobreaker.Counts) bool {
			return c.ConsecutiveFailures >= uint32(opts.CircuitBreakerCount) // #nosec G115 -- validate rejects values outside uint32.
		},
	})
	return b, nil
}

// Publish serializes msg and produces it to "{prefix}.{messageType}".
func (b *Bus) Publish(msg Message) error {
	topic := TopicName(b.opts, msg.MessageType())
	publish := func(ctx context.Context) error {
		payload, err := b.serializer.Serialize(topic, msg)
		if err != nil {
			return fmt.Errorf("kafka: serialize %s: %w", topic, err)
		}
		record := &kgo.Record{Topic: topic, Value: payload}
		injectTrace(ctx, record)
		_, err = b.breaker.Execute(func() (any, error) {
			results := b.client.ProduceSync(ctx, record)
			if err := results.FirstErr(); err != nil {
				return nil, err
			}
			return nil, nil
		})
		if err != nil {
			return fmt.Errorf("kafka: publish %s: %w", topic, err)
		}
		return nil
	}
	if b.ops != nil {
		return b.ops.Trace(b.baseCtx).Span("kafka.publish", publish)
	}
	return publish(b.baseCtx)
}

// Close releases the franz-go producer client.
func (b *Bus) Close() error {
	if b.client == nil {
		return nil
	}
	b.client.Close()
	return nil
}

// Ping opens metadata and authentication paths through franz-go by issuing a
// harmless metadata request for the configured broker set.
func (b *Bus) Ping(ctx context.Context) error {
	if err := b.client.Ping(ctx); err != nil {
		return fmt.Errorf("kafka: ping metadata/authentication: %w", err)
	}
	return nil
}
