package kafka

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"time"

	"github.com/guilhermelinosp/hellnet-lib-telemetry/instrument"
	"github.com/guilhermelinosp/hellnet-lib-telemetry/messaging"
	"github.com/twmb/franz-go/pkg/kgo"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"
)

type consumerClient interface {
	PollFetches(context.Context) kgo.Fetches
	CommitRecords(context.Context, ...*kgo.Record) error
	Close()
}

// Consumer runs a Handler[T] against a topic derived from T's MessageType.
// franz-go supplies cooperative-sticky consumer groups and manual commits.
type Consumer[T Message] struct {
	opts       Options
	handler    Handler[T]
	spec       HandlerSpec
	client     consumerClient
	bus        *Bus
	serializer Serializer
	topic      string
	group      string
	maxRetries int
}

// NewConsumer creates a consumer from KAFKA_* environment variables. inst is the
// Hellnet observability contract (for example a *telemetry.Telemetry, or nil to
// emit no telemetry). Configure must be called before Run to attach the handler
// and topic/group.
func NewConsumer[T Message](ctx context.Context, inst instrument.Instrumentation, options ...Option) (*Consumer[T], error) {
	bus, err := New(ctx, inst, options...)
	if err != nil {
		return nil, err
	}
	return newConsumer[T](bus), nil
}

func newConsumer[T Message](bus *Bus) *Consumer[T] {
	return &Consumer[T]{
		opts:       bus.opts,
		bus:        bus,
		serializer: bus.serializer,
	}
}

// Configure attaches the handler and resolves the topic and consumer group.
func (c *Consumer[T]) Configure(h Handler[T], spec ...HandlerSpec) error {
	if h == nil {
		return fmt.Errorf("kafka: handler is nil")
	}
	if c.client != nil {
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
	clientOpts, err := franzOptions(c.opts)
	if err != nil {
		return err
	}
	clientOpts = append(clientOpts,
		kgo.ConsumerGroup(group),
		kgo.ConsumeTopics(topic),
		kgo.Opt(kgo.Balancers(kgo.CooperativeStickyBalancer())),
		kgo.DisableAutoCommit(),
		kgo.FetchMinBytes(1),
		kgo.FetchMaxWait(500*time.Millisecond),
	)
	client, err := kgo.NewClient(clientOpts...)
	if err != nil {
		return fmt.Errorf("kafka: create franz consumer: %w", err)
	}
	c.handler = h
	c.spec = s
	c.topic = topic
	c.group = group
	c.maxRetries = c.opts.MaxRetries
	if s.MaxRetries > 0 {
		c.maxRetries = s.MaxRetries
	}
	c.client = client
	return nil
}

// Run consumes until cancellation or an unrecoverable error. Offsets commit
// only after successful handling or confirmed DLQ delivery.
func (c *Consumer[T]) Run() error {
	if c.client == nil {
		return fmt.Errorf("kafka: consumer is not configured; call Configure first")
	}
	return c.runWithoutContext()
}

func (c *Consumer[T]) runWithoutContext() error { return c.run(backgroundContext()) }

// RunContext consumes until the caller cancels ctx or Close closes the client.
func (c *Consumer[T]) RunContext(ctx context.Context) error {
	if c.client == nil {
		return fmt.Errorf("kafka: consumer is not configured; call Configure first")
	}
	if ctx == nil {
		return c.runWithoutContext()
	}
	return c.run(ctx)
}

func (c *Consumer[T]) run(ctx context.Context) error {
	var fetchFails int
	for {
		fetches := c.client.PollFetches(ctx)
		if err := fetches.Err(); err != nil {
			if isCtxErr(err) || fetches.IsClientClosed() {
				return nil
			}
			fetchFails++
			c.logger().Warn(ctx, "kafka: transient fetch error; retrying", "topic", c.topic, "group", c.group, "consecutive_failures", fetchFails, "error", err)
			if !c.sleepThroughShutdown(ctx, fetchBackoff(fetchFails-1)) {
				return nil
			}
			continue
		}
		if fetches.Empty() {
			continue
		}
		fetchFails = 0
		var processErr error
		fetches.EachRecord(func(record *kgo.Record) {
			if processErr == nil {
				processErr = c.processMessage(ctx, *record)
			}
		})
		if processErr != nil {
			return processErr
		}
	}
}

func (c *Consumer[T]) processMessage(ctx context.Context, m kgo.Record) (err error) {
	obs := newObservability(ctx, nil)
	if c.bus != nil && c.bus.obs.inst != nil {
		obs = c.bus.obs
	}
	ctx = extractTraceWith(ctx, m, obs.inst.Propagator())
	ctx, span := obs.tracer.Start(ctx, messaging.ProcessSpanName(m.Topic),
		trace.WithSpanKind(trace.SpanKindConsumer),
		trace.WithAttributes(messaging.System("kafka"), messaging.DestinationName(m.Topic), messaging.OperationType("process"), messaging.OperationName("process"), messaging.DestinationPartitionID(fmt.Sprintf("%d", m.Partition)), messaging.KafkaOffset(m.Offset), messaging.ConsumerGroupName(c.group)))
	defer span.End()
	started := time.Now()
	defer func() {
		result := "success"
		if err != nil {
			result = "error"
			span.RecordError(err)
			span.SetStatus(codes.Error, err.Error())
		}
		obs.observeProcess(ctx, m.Topic, result, started)
	}()
	return c.processMessageBody(ctx, m)
}

func (c *Consumer[T]) processMessageBody(ctx context.Context, m kgo.Record) error {
	var msg T
	out := any(&msg)
	if t := reflect.TypeOf(msg); t != nil && t.Kind() == reflect.Pointer {
		msg = reflect.New(t.Elem()).Interface().(T)
		out = any(msg)
	}
	if err := deserialize(ctx, c.serializer, m.Topic, m.Value, out); err != nil {
		if !c.publishDLQWithRetry(ctx, m, "deserialize: "+err.Error(), "deserialize") {
			return nil
		}
		return c.commit(ctx, m)
	}

	var lastErr error
	// MaxRetries is the number of retries after the initial handler attempt.
	// A value of zero still gives the message one processing attempt.
	for attempt := 0; attempt <= c.maxRetries; attempt++ {
		if attempt > 0 && !c.sleepThroughShutdown(ctx, backoff(c.opts.RetryDelay, attempt-1)) {
			return nil
		}
		lastErr = c.handle(ctx, msg, m)
		if lastErr == nil {
			break
		}
	}
	if lastErr != nil && !c.publishDLQWithRetry(ctx, m, lastErr.Error(), "handler") {
		return nil
	}
	return c.commit(ctx, m)
}

func (c *Consumer[T]) commit(ctx context.Context, m kgo.Record) error {
	if err := c.client.CommitRecords(ctx, &m); err != nil {
		if ctx.Err() != nil {
			return nil
		}
		return fmt.Errorf("kafka: commit: %w", err)
	}
	return nil
}

func (c *Consumer[T]) publishDLQWithRetry(ctx context.Context, m kgo.Record, reason, errorType string) bool {
	for attempt := 1; ; attempt++ {
		if ctx.Err() != nil {
			return false
		}
		if err := c.bus.publishDLQ(ctx, c.opts, m, reason, errorType, attempt); err == nil {
			return true
		} else {
			c.logger().Error(ctx, "kafka: failed to publish message to DLQ; retrying", "topic", m.Topic, "group", c.group, "partition", m.Partition, "offset", m.Offset, "attempt", attempt, "error", err)
		}
		if !c.sleepThroughShutdown(ctx, fetchBackoff(attempt-1)) {
			return false
		}
	}
}

func (c *Consumer[T]) logger() instrument.Logger {
	if c.bus != nil && c.bus.obs.logger != nil {
		return c.bus.obs.logger
	}
	return instrument.Noop().Logger(instrumentationScope)
}

func (c *Consumer[T]) handle(ctx context.Context, msg T, m kgo.Record) error {
	fn := func(ctx context.Context) error {
		return c.handler.Handle(ctx, msg, Ctx{
			Topic:     m.Topic,
			Partition: int(m.Partition),
			Offset:    m.Offset,
			Key:       m.Key,
			Headers:   recordHeaders(m),
		})
	}
	return fn(ctx)
}

func (c *Consumer[T]) sleepThroughShutdown(ctx context.Context, d time.Duration) bool {
	return sleepCtx(ctx, d) == nil
}

func isCtxErr(err error) bool {
	return errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded)
}

// Close cancels consumption and releases the franz-go clients.
func (c *Consumer[T]) Close() error {
	if c.client != nil {
		c.client.Close()
	}
	return c.bus.Close()
}
