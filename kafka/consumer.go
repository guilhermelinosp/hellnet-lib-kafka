package kafka

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"time"

	"github.com/segmentio/kafka-go"
)

// Consumer runs a Handler[T] against a topic derived from T's MessageType
// (or overridden by HandlerSpec). Failed handlers are retried with exponential
// backoff and, once exhausted, the message goes to the Dead Letter Queue.
//
// The context is captured once at NewConsumer and propagated internally: Run
// derives its run context from it and Close cancels that derivation for
// cooperative shutdown. Applications never pass ctx to operations.
type Consumer[T Message] struct {
	opts       Options
	handler    Handler[T]
	spec       HandlerSpec
	reader     *kafka.Reader
	bus        *Bus
	serializer Serializer
	topic      string
	group      string
	maxRetries int

	// runCtx is derived once from the ctx captured at NewConsumer; cancelRun
	// (wired into Close) cancels it so FetchMessage, retry backoffs and DLQ
	// writes stop promptly on shutdown.
	runCtx    context.Context
	cancelRun context.CancelFunc
}

// NewConsumer builds a consumer for the given handler and per-handler spec.
// The context is captured once here and propagated internally: Run derives a
// cancelable run context from it (Close cancels it), handlers receive
// library-supplied contexts derived from it, and applications never pass ctx
// to operations. Options are optional: when omitted, the environment is loaded
// internally (HELLNET_KAFKA_* via .env), mirroring New(). Explicit opts
// override the env.
func NewConsumer[T Message](ctx context.Context, h Handler[T], spec HandlerSpec, opts ...Options) (*Consumer[T], error) {
	if h == nil {
		return nil, fmt.Errorf("kafka: handler is nil")
	}
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
	group := o.ConsumerGroup
	if spec.Group != "" {
		group = spec.Group
	}
	if group == "" {
		return nil, fmt.Errorf("kafka: consumer group required (HELLNET_KAFKA_CONSUMER_GROUP or HandlerSpec.Group)")
	}
	var zero T
	topic := spec.resolveTopic(o, zero.MessageType())

	bus, err := newBus(ctx, o)
	if err != nil {
		return nil, err
	}
	serializer, err := o.ensureSerializer()
	if err != nil {
		_ = bus.Close()
		return nil, err
	}

	r := kafka.NewReader(kafka.ReaderConfig{
		Brokers:        o.Brokers,
		GroupID:        group,
		Topic:          topic,
		MinBytes:       10e3,
		MaxBytes:       10e6,
		CommitInterval: time.Second,
		StartOffset:    kafka.FirstOffset, // new groups replay from the beginning
		Dialer:         newDialer(o),
	})

	maxRetries := o.MaxRetries
	if spec.MaxRetries > 0 {
		maxRetries = spec.MaxRetries
	}
	runCtx, cancelRun := context.WithCancel(ctx) //nolint:gosec // G118: cancelRun is stored and invoked by Close() for cooperative shutdown.
	return &Consumer[T]{
			opts:       o,
			handler:    h,
			spec:       spec,
			reader:     r,
			bus:        bus,
			serializer: serializer,
			topic:      topic,
			group:      group,
			maxRetries: maxRetries,
			runCtx:     runCtx,
			cancelRun:  cancelRun,
		},
		nil
}

// Run consumes messages until the internal run context is cancelled — by Close
// or by cancelling/closing the context captured at NewConsumer — an
// unrecoverable error occurs, or the reader fails. It commits offsets after
// each successful or DLQ'd batch.
//
// The run context is supplied by the library (derived once from the context
// given at NewConsumer); applications never pass ctx to operations.
func (c *Consumer[T]) Run() error {
	ctx := c.runCtx
	for {
		m, err := c.reader.FetchMessage(ctx)
		if err != nil {
			if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
				return nil
			}
			// Transient fetch errors (rebalance, leader changes, network):
			// back off briefly and keep consuming instead of dying silently.
			if err := sleepCtx(ctx, 200*time.Millisecond); err != nil {
				return nil
			}
			continue
		}

		var msg T
		out := any(&msg)
		// For pointer message types (the natural Go/protobuf style), allocate
		// and pass the pointer itself — serializers expect the message, not a
		// **T. Value types keep the addressable &msg.
		if t := reflect.TypeOf(msg); t != nil && t.Kind() == reflect.Pointer {
			msg = reflect.New(t.Elem()).Interface().(T)
			out = any(msg)
		}
		if err := c.serializer.Deserialize(m.Topic, m.Value, out); err != nil {
			_ = c.bus.publishDLQ(ctx, c.opts, m.Topic, m.Partition, m.Offset,
				"deserialize: "+err.Error(), m.Value)
			_ = c.reader.CommitMessages(ctx, m)
			continue
		}

		var lastErr error
		for attempt := 0; attempt < c.maxRetries; attempt++ {
			if attempt > 0 {
				if err := sleepCtx(ctx, backoff(c.opts.RetryDelay, attempt-1)); err != nil {
					return err
				}
			}
			lastErr = c.handler.Handle(ctx, msg, Ctx{
				Topic:     m.Topic,
				Partition: m.Partition,
				Offset:    m.Offset,
				Key:       m.Key,
			})
			if lastErr == nil {
				break
			}
		}
		if lastErr != nil {
			_ = c.bus.publishDLQ(ctx, c.opts, m.Topic, m.Partition, m.Offset, lastErr.Error(), m.Value)
		}
		if err := c.reader.CommitMessages(ctx, m); err != nil {
			return fmt.Errorf("kafka: commit: %w", err)
		}
	}
}

// Close cancels the internal run context first — so FetchMessage, retry
// backoffs and DLQ writes abort promptly instead of blocking until the next
// broker round-trip — and then releases the reader and the DLQ bus.
func (c *Consumer[T]) Close() error {
	c.cancelRun()
	cerr := c.reader.Close()
	berr := c.bus.Close()
	if cerr != nil {
		return cerr
	}
	return berr
}
