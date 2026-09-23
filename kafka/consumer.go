package kafka

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"reflect"
	"time"

	"github.com/guilhermelinosp/hellnet-lib-telemetry/telemetry"
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

// NewConsumer creates a consumer with the caller's context and telemetry.
// Configure must be called before Run to attach the handler and topic/group.
func NewConsumer[T Message](ctx context.Context, ops telemetry.Client) (*Consumer[T], error) {
	bus, err := New(ctx, ops)
	if err != nil {
		return nil, err
	}
	runCtx, cancelRun := context.WithCancel(bus.baseCtx)
	return &Consumer[T]{
		opts:       bus.opts,
		bus:        bus,
		serializer: bus.serializer,
		runCtx:     runCtx,
		cancelRun:  cancelRun,
	}, nil
}

// Configure attaches the handler and resolves the topic and consumer group.
// It must be called once before Run.
func (c *Consumer[T]) Configure(h Handler[T], spec ...HandlerSpec) error {
	if h == nil {
		return fmt.Errorf("kafka: handler is nil")
	}
	if c.reader != nil {
		return fmt.Errorf("kafka: consumer already configured")
	}
	s := HandlerSpec{}
	if len(spec) > 0 {
		s = spec[0]
	}
	group := c.opts.ConsumerGroup
	if s.Group != "" {
		group = s.Group
	}
	if group == "" {
		return fmt.Errorf("kafka: consumer group required (KAFKA_CONSUMER_GROUP or HandlerSpec.Group)")
	}
	var zero T
	topic := s.resolveTopic(c.opts, zero.MessageType())
	c.handler = h
	c.spec = s
	c.topic = topic
	c.group = group
	c.maxRetries = c.opts.MaxRetries
	if s.MaxRetries > 0 {
		c.maxRetries = s.MaxRetries
	}
	c.reader = kafka.NewReader(kafka.ReaderConfig{
		Brokers:        c.opts.Brokers,
		GroupID:        group,
		Topic:          topic,
		MinBytes:       10e3,
		MaxBytes:       10e6,
		CommitInterval: time.Second,
		StartOffset:    kafka.FirstOffset,
		Dialer:         newDialer(c.opts),
	})
	return nil
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
	if c.reader == nil {
		return fmt.Errorf("kafka: consumer is not configured; call Configure first")
	}
	return c.run(c.runCtx)
}

// RunContext consumes until ctx, Close, or the constructor context is
// cancelled. It is useful with the zero-config NewConsumer constructor.
func (c *Consumer[T]) RunContext(ctx context.Context) error {
	if c.reader == nil {
		return fmt.Errorf("kafka: consumer is not configured; call Configure first")
	}
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
	ctx = extractTrace(ctx, m)
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
		lastErr = c.handle(ctx, msg, m)
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
func (c *Consumer[T]) handle(ctx context.Context, msg T, m kafka.Message) error {
	fn := func(ctx context.Context) error {
		return c.handler.Handle(ctx, msg, Ctx{
			Topic:     m.Topic,
			Partition: m.Partition,
			Offset:    m.Offset,
			Key:       m.Key,
		})
	}
	if c.bus != nil && c.bus.ops != nil {
		return c.bus.ops.Span(ctx, "kafka.consume", func(ctx context.Context) error {
			return fn(ctx)
		})
	}
	return fn(ctx)
}

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
	if c.reader == nil {
		return c.bus.Close()
	}
	cerr := c.reader.Close()
	berr := c.bus.Close()
	if cerr != nil {
		return cerr
	}
	return berr
}
