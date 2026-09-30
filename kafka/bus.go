package kafka

import (
	"context"
	"fmt"
	"time"

	"github.com/guilhermelinosp/hellnet-lib-telemetry/messaging"
	"github.com/guilhermelinosp/hellnet-lib-telemetry/telemetry"
	"github.com/sony/gobreaker"
	"github.com/twmb/franz-go/pkg/kgo"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"
)

type recordProducer interface {
	ProduceSync(context.Context, ...*kgo.Record) kgo.ProduceResults
	Close()
}

type brokerPinger interface {
	Ping(context.Context) error
}

// Bus is the message bus: an idempotent franz-go producer with a circuit
// breaker, plus DLQ publishing. New/MustNew use the constructor context.
type Bus struct {
	opts          Options
	client        recordProducer
	dlqWriter     recordProducer
	breaker       *gobreaker.CircuitBreaker
	serializer    Serializer
	legacyContext func() context.Context
	ops           telemetry.Client
	obs           observability
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
		opts:          opts,
		client:        client,
		dlqWriter:     client,
		serializer:    opts.Serializer,
		legacyContext: func() context.Context { return ctx },
		obs:           newObservability(ctx, nil),
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

// Publish serializes msg and produces it using the construction context.
// Deprecated: use PublishContext with the caller's request context.
func (b *Bus) Publish(msg Message) error {
	return b.PublishContext(b.legacyContext(), msg)
}

// PublishContext serializes and produces msg as a child of ctx.
func (b *Bus) PublishContext(ctx context.Context, msg Message) error {
	if ctx == nil {
		return fmt.Errorf("kafka: publish context is nil")
	}
	return b.publishWithContext(ctx, msg)
}

func (b *Bus) publishWithContext(ctx context.Context, msg Message) (err error) {
	topic := TopicName(b.opts, msg.MessageType())
	ctx, span := b.obs.tracer.Start(ctx, messaging.SendSpanName(topic),
		trace.WithSpanKind(trace.SpanKindProducer),
		trace.WithAttributes(messaging.System("kafka"), messaging.DestinationName(topic), messaging.OperationType("send"), messaging.OperationName("publish")))
	defer span.End()
	started := time.Now()
	defer func() {
		result := "success"
		if err != nil {
			result = "error"
			span.RecordError(err)
			span.SetStatus(codes.Error, err.Error())
		}
		b.obs.observeSend(ctx, topic, result, started)
	}()
	publish := func(ctx context.Context) error {
		payload, err := serialize(ctx, b.serializer, topic, msg)
		if err != nil {
			return fmt.Errorf("kafka: serialize %s: %w", topic, err)
		}
		record := &kgo.Record{Topic: topic, Value: payload}
		injectTraceWith(ctx, record, b.obs.inst.Propagator())
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
	return publish(ctx)
}

func backgroundContext() context.Context { return context.Background() }

// PublishBatchContext publishes messages sequentially using ctx as the parent
// of each producer operation. It stops at the first error, preserving the
// existing at-least-once caller retry semantics without hiding partial work.
func (b *Bus) PublishBatchContext(ctx context.Context, messages ...Message) error {
	for _, msg := range messages {
		if msg == nil {
			return fmt.Errorf("kafka: batch contains nil message")
		}
		if err := b.PublishContext(ctx, msg); err != nil {
			return err
		}
	}
	return nil
}

// Close releases the franz-go producer client.
// Deprecated: use Shutdown with a caller-owned context.
func (b *Bus) Close() error {
	if b.client == nil {
		return nil
	}
	b.client.Close()
	return nil
}

// Shutdown releases the producer client unless ctx has already been canceled.
// franz-go's Close is synchronous and has no context-aware variant.
func (b *Bus) Shutdown(ctx context.Context) error {
	if err := ctxOrNil(ctx); err != nil {
		return err
	}
	if b.client == nil {
		return nil
	}
	b.client.Close()
	return nil
}

func ctxOrNil(ctx context.Context) error {
	if ctx != nil {
		return ctx.Err()
	}
	return nil
}

// Ping opens metadata and authentication paths through franz-go by issuing a
// harmless metadata request for the configured broker set.
func (b *Bus) Ping(ctx context.Context) error {
	pinger, ok := b.client.(brokerPinger)
	if !ok {
		return fmt.Errorf("kafka: client does not support ping")
	}
	if err := pinger.Ping(ctx); err != nil {
		return fmt.Errorf("kafka: ping metadata/authentication: %w", err)
	}
	return nil
}
