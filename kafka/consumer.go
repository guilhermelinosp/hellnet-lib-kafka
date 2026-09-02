package kafka

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"reflect"
	"time"

	"github.com/segmentio/kafka-go"
)

// Consumer runs a Handler[T] against a topic derived from T's MessageType
// (or overridden by HandlerSpec). Failed handlers are retried with exponential
// backoff and, once exhausted, the message goes to the Dead Letter Queue.
//
// NewConsumer uses an internal context. RunContext can add a per-run
// cancellation boundary, and Close always cancels consumption cooperatively.
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

	// runCtx is derived once from the constructor context; cancelRun
	// (wired into Close) cancels it so FetchMessage, retry backoffs and DLQ
	// writes stop promptly on shutdown.
	runCtx    context.Context
	cancelRun context.CancelFunc
}

// NewConsumer follows the zero-config New pattern: it creates the base context,
// loads .env, and resolves all options from HELLNET_KAFKA_*.
func NewConsumer[T Message](h Handler[T], spec HandlerSpec) (*Consumer[T], error) {
	if h == nil {
		return nil, fmt.Errorf("kafka: handler is nil")
	}
	bus, err := New()
	if err != nil {
		return nil, err
	}
	return newConsumerWithBus(h, spec, bus)
}

func newConsumerWithOptions[T Message](ctx context.Context, h Handler[T], spec HandlerSpec, o Options) (*Consumer[T], error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if h == nil {
		return nil, fmt.Errorf("kafka: handler is nil")
	}
	bus, err := newBusWithOptions(ctx, o)
	if err != nil {
		return nil, err
	}
	return newConsumerWithBus(h, spec, bus)
}

func newConsumerWithBus[T Message](h Handler[T], spec HandlerSpec, bus *Bus) (*Consumer[T], error) {
	o := bus.opts
	group := o.ConsumerGroup
	if spec.Group != "" {
		group = spec.Group
	}
	if group == "" {
		_ = bus.Close()
		return nil, fmt.Errorf("kafka: consumer group required (HELLNET_KAFKA_CONSUMER_GROUP or HandlerSpec.Group)")
	}
	var zero T
	topic := spec.resolveTopic(o, zero.MessageType())

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
	runCtx, cancelRun := context.WithCancel(bus.baseCtx) //nolint:gosec // G118: cancelRun is stored and invoked by Close() for cooperative shutdown.
	return &Consumer[T]{
			opts:       o,
			handler:    h,
			spec:       spec,
			reader:     r,
			bus:        bus,
			serializer: bus.serializer,
			topic:      topic,
			group:      group,
			maxRetries: maxRetries,
			runCtx:     runCtx,
			cancelRun:  cancelRun,
		},
		nil
}

// Run consumes messages until the internal run context is cancelled by Close
// (or by its internal context), an unrecoverable error occurs, or
// the reader fails. It commits offsets after each successful or DLQ'd batch.
//
// Shutdown contract: cancellation/shutdown paths always return nil — context
// cancellation during FetchMessage, during a backoff sleep, or a commit that
// races shutdown. Errors observed while still running (e.g. a real broker
// commit failure) are returned as errors.
//
// The run context is supplied by the library. Use RunContext when a caller
// needs a bounded run while using the zero-config constructor.
func (c *Consumer[T]) Run() error {
	return c.run(c.runCtx)
}

// RunContext consumes until ctx, Close, or the constructor context is
// cancelled. It is useful with the zero-config NewConsumer constructor.
func (c *Consumer[T]) RunContext(ctx context.Context) error {
	if ctx == nil {
		return c.Run()
	}
	runCtx, cancel := context.WithCancel(ctx)
	stop := context.AfterFunc(c.runCtx, cancel)
	defer func() {
		stop()
		cancel()
	}()
	return c.run(runCtx)
}

func (c *Consumer[T]) run(ctx context.Context) error {
	var fetchFails int // consecutive transient fetch failures for backoff
	for {
		m, err := c.reader.FetchMessage(ctx)
		if err == nil {
			fetchFails = 0 // healthy fetch: reset the escalating backoff
			if perr := c.processMessage(ctx, m); perr != nil {
				return perr
			}
			continue
		}
		if isCtxErr(err) {
			return nil
		}
		// Transient fetch errors (rebalance, leader changes, network): log,
		// escalate the capped backoff and keep consuming instead of dying
		// silently.
		fetchFails++
		slog.Warn("kafka: transient fetch error; retrying",
			slog.String("topic", c.topic),
			slog.String("group", c.group),
			slog.Int("consecutive_failures", fetchFails),
			slog.Any("error", err))
		if !c.sleepThroughShutdown(ctx, fetchBackoff(fetchFails-1)) {
			return nil // shutdown during backoff
		}
	}
}

// processMessage deserializes one fetched message, runs the handler with
// retries (exponential backoff), lands it in the DLQ when retries are
// exhausted, and commits the offset. The returned error is fatal to Run.
func (c *Consumer[T]) processMessage(ctx context.Context, m kafka.Message) error {
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
		return nil
	}

	var lastErr error
	for attempt := 0; attempt < c.maxRetries; attempt++ {
		if attempt > 0 && !c.sleepThroughShutdown(ctx, backoff(c.opts.RetryDelay, attempt-1)) {
			return nil // shutdown during handler-retry backoff
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
		if ctx.Err() != nil {
			return nil // commit raced shutdown: cooperative stop, not an error
		}
		return fmt.Errorf("kafka: commit: %w", err)
	}
	return nil
}

// sleepThroughShutdown sleeps for d; false means the run context ended while
// sleeping — the caller must treat it as cooperative shutdown.
func (c *Consumer[T]) sleepThroughShutdown(ctx context.Context, d time.Duration) bool {
	return sleepCtx(ctx, d) == nil
}

// isCtxErr reports whether err signals context cancellation/deadline.
func isCtxErr(err error) bool {
	return errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded)
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
