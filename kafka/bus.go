package kafka

import (
	"context"
	"fmt"
	"net"

	"github.com/segmentio/kafka-go"
	"github.com/sony/gobreaker"
)

// Bus is the message bus: an idempotent producer with retry and a circuit
// breaker, plus DLQ publishing. Create it with New() (env-first) or newBus.
type Bus struct {
	opts       Options
	writer     *kafka.Writer
	breaker    *gobreaker.CircuitBreaker
	serializer Serializer
}

// newBus builds a Bus from validated options.
func newBus(opts Options) (*Bus, error) {
	w := &kafka.Writer{
		Addr:                   kafka.TCP(opts.Brokers...),
		Balancer:               &kafka.LeastBytes{},
		RequiredAcks:           kafka.RequireAll,
		Async:                  false,
		MaxAttempts:            3,    // transient errors (e.g. leader election) retried
		AllowAutoTopicCreation: true, // brokers with auto_create_topics (e.g. Redpanda)
	}
	if d := newDialer(opts); d != nil {
		dial := d.DialContext
		w.Transport = &kafka.Transport{
			Dial: func(ctx context.Context, network, address string) (net.Conn, error) {
				return dial(ctx, network, address)
			},
			SASL: d.SASLMechanism,
			TLS:  d.TLS,
		}
	}
	b := &Bus{
		opts:       opts,
		writer:     w,
		serializer: opts.Serializer,
	}
	if b.serializer == nil {
		b.serializer = JSONSerializer{}
	}
	b.breaker = gobreaker.NewCircuitBreaker(gobreaker.Settings{
		Name: "kafka-produce",
		ReadyToTrip: func(c gobreaker.Counts) bool {
			return c.ConsecutiveFailures >= uint32(opts.CircuitBreakerCount)
		},
	})
	return b, nil
}

// Publish serializes msg and produces it to "{prefix}.{messageType}" using a
// default (background) context — mirroring hellnet-lib-cache. Use
// PublishContext for cancellation/deadline control.
func (b *Bus) Publish(msg Message) error {
	return b.PublishContext(context.Background(), msg)
}

// PublishContext serializes msg and produces it to "{prefix}.{messageType}".
// Produce is protected by Timeout -> CircuitBreaker (the Go counterpart of the
// .NET Polly pipeline). On breaker open, it fails fast until it half-opens.
func (b *Bus) PublishContext(ctx context.Context, msg Message) error {
	topic := TopicName(b.opts, msg.MessageType())
	payload, err := b.serializer.Serialize(topic, msg)
	if err != nil {
		return fmt.Errorf("kafka: serialize %s: %w", topic, err)
	}
	km := kafka.Message{Topic: topic, Value: payload}

	_, err = b.breaker.Execute(func() (any, error) {
		wctx, cancel := context.WithTimeout(ctx, b.opts.TimeoutProduce)
		defer cancel()
		if err := b.writer.WriteMessages(wctx, km); err != nil {
			return nil, fmt.Errorf("%w", err)
		}
		return nil, nil
	})
	if err != nil {
		return fmt.Errorf("kafka: publish %s: %w", topic, err)
	}
	return nil
}

// Close releases the underlying writer.
func (b *Bus) Close() error {
	return b.writer.Close()
}
