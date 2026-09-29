//go:build integration

package kafka

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"os"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo"
)

// Integration tests run against a REAL broker (Redpanda/Kafka).
//
// Usage (tools namespace on kind, via kubectl port-forward):
//
//	kubectl port-forward -n tools svc/redpanda 19092:9092
//	export TEST_KAFKA_BROKERS=localhost:19092
//	go test -tags integration -count=1 -run TestIntegration ./kafka/

func integrationBrokers(t *testing.T) []string {
	t.Helper()
	b := os.Getenv("TEST_KAFKA_BROKERS")
	if b == "" {
		t.Skip("TEST_KAFKA_BROKERS not set")
	}
	return []string{b}
}

type evtTest struct {
	ID   string `json:"id"`
	Data string `json:"data"`
}

func (evtTest) MessageType() string { return "it.test.v1" }

func integrationBaseOpts(brokers []string) Options {
	o := testDefaultOptions()
	o.Brokers = brokers
	o.TopicPrefix = "hellnet"
	o.SecurityProtocol = "plaintext"
	return o
}

func ensureIntegrationTopic(t *testing.T, brokers []string, topic string) {
	t.Helper()
	opts, err := franzOptions(integrationBaseOpts(brokers))
	if err != nil {
		t.Fatalf("franz options: %v", err)
	}
	client, err := kgo.NewClient(opts...)
	if err != nil {
		t.Fatalf("create franz client: %v", err)
	}
	defer client.Close()
	responses, err := kadm.NewClient(client).CreateTopics(context.Background(), 1, 1, nil, topic)
	if err != nil {
		t.Fatalf("create topic %q: %v", topic, err)
	}
	if topicErr := responses[topic].Err; topicErr != nil && !errors.Is(topicErr, kerr.TopicAlreadyExists) {
		t.Fatalf("create topic %q: %v", topic, topicErr)
	}
}

func newProducerWithOptions[T Message](ctx context.Context, o Options) (*Producer[T], error) {
	if ctx == nil {
		ctx = context.Background()
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
	runCtx, cancelRun := context.WithCancel(bus.baseCtx)
	c := &Consumer[T]{opts: bus.opts, bus: bus, serializer: bus.serializer, runCtx: runCtx, cancelRun: cancelRun}
	if err := c.Configure(h, spec); err != nil {
		cancelRun()
		_ = bus.Close()
		return nil, err
	}
	return c, nil
}

// TestIntegrationPublishConsume covers the core loop: construct once with ctx,
// publish without ctx, consume via Run() into a handler that receives the
// lib-supplied ctx, then close and observe cooperative stop.
func TestIntegrationPublishConsume(t *testing.T) {
	brokers := integrationBrokers(t)
	ctx := context.Background()
	topic := "hellnet.it.test.v1"
	ensureIntegrationTopic(t, brokers, topic)

	prod, err := newProducerWithOptions[evtTest](ctx, integrationBaseOpts(brokers))
	if err != nil {
		t.Fatalf("NewProducer: %v", err)
	}
	defer func() { _ = prod.Close() }()

	const n = 3
	var seen sync.Map
	var gotCount atomic.Int32
	done := make(chan struct{})

	h := HandlerFunc[evtTest](func(ctx context.Context, msg evtTest, mc Ctx) error {
		if ctx == nil {
			t.Error("handler received nil ctx")
		}
		if mc.Topic == "" {
			t.Error("handler received empty mctx.Topic")
		}
		seen.Store(msg.ID, true)
		if gotCount.Add(1) == n {
			select {
			case <-done:
			default:
				close(done)
			}
		}
		return nil
	})

	spec := HandlerSpec{Topic: topic, Group: fmt.Sprintf("grp-%d", time.Now().UnixNano())}
	cons, err := newConsumerWithOptions[evtTest](ctx, h, spec, integrationBaseOpts(brokers))
	if err != nil {
		t.Fatalf("NewConsumer: %v", err)
	}

	runDone := make(chan error, 1)
	go func() { runDone <- cons.Run() }()

	time.Sleep(3 * time.Second) // consumer group join + assignment

	for i := 0; i < n; i++ {
		m := evtTest{ID: fmt.Sprintf("id-%d", i), Data: "payload"}
		if err := prod.Publish(m); err != nil {
			t.Fatalf("publish %d: %v", i, err)
		}
	}

	select {
	case <-done:
	case <-time.After(15 * time.Second):
		t.Fatalf("timeout waiting for %d messages (got %d)", n, gotCount.Load())
	}

	for i := 0; i < n; i++ {
		id := fmt.Sprintf("id-%d", i)
		if _, ok := seen.Load(id); !ok {
			t.Errorf("message %s not consumed", id)
		}
	}

	failErr := cons.Close()
	<-runDone // Run must return promptly after Close (cooperative shutdown)
	if failErr != nil {
		t.Logf("Close(): %v", failErr)
	}
}

// TestIntegrationHandlerRetryThenDLQ proves a permanently failing handler
// retries MaxRetries times and then lands in the DLQ topic.
func TestIntegrationHandlerRetryThenDLQ(t *testing.T) {
	brokers := integrationBrokers(t)
	ctx := context.Background()
	base := time.Now().UnixNano()
	topic := fmt.Sprintf("hellnet.it.test.v1.%d", base)
	ensureIntegrationTopic(t, brokers, topic)

	boom := errors.New("always fails")
	var attempts atomic.Int32
	h := HandlerFunc[evtTest](func(ctx context.Context, msg evtTest, mc Ctx) error {
		attempts.Add(1)
		return boom
	})

	o := integrationBaseOpts(brokers)
	o.MaxRetries = 2
	o.RetryDelay = 50 * time.Millisecond

	spec := HandlerSpec{Topic: topic, Group: fmt.Sprintf("grp-dlq-%d", base), MaxRetries: 2}
	cons, err := newConsumerWithOptions[evtTest](ctx, h, spec, o)
	if err != nil {
		t.Fatalf("NewConsumer: %v", err)
	}
	closed := false
	t.Cleanup(func() {
		if !closed {
			_ = cons.Close()
		}
	})
	go func() { _ = cons.Run() }()

	prod, err := newProducerWithOptions[evtTest](ctx, integrationBaseOpts(brokers))
	if err != nil {
		t.Fatalf("NewProducer: %v", err)
	}
	defer func() { _ = prod.Close() }()

	time.Sleep(3 * time.Second)
	if err := prod.Publish(evtTest{ID: "poison-1"}); err != nil {
		t.Fatalf("publish poison: %v", err)
	}

	deadline := time.After(20 * time.Second)
	ticker := time.NewTicker(200 * time.Millisecond)
	defer ticker.Stop()
	for {
		select {
		case <-deadline:
			t.Fatalf("expected >=3 attempts after poison message; got %d", attempts.Load())
		case <-ticker.C:
			if attempts.Load() >= int32(o.MaxRetries)+1 {
				fmt.Printf("DLQ path confirmed: handler attempted %d times\n", attempts.Load())
				goto dlq
			}
		}
	}

dlq:
	dlqOpts, err := franzOptions(integrationBaseOpts(brokers))
	if err != nil {
		t.Fatalf("DLQ client options: %v", err)
	}
	dlqOpts = append(dlqOpts,
		kgo.ConsumerGroup(fmt.Sprintf("grp-dlq-reader-%d", base)),
		kgo.ConsumeTopics(topic+".dlq"),
		kgo.DisableAutoCommit(),
		kgo.FetchMinBytes(1),
		kgo.FetchMaxWait(500*time.Millisecond),
	)
	dlqReader, err := kgo.NewClient(dlqOpts...)
	if err != nil {
		t.Fatalf("create DLQ client: %v", err)
	}
	defer dlqReader.Close()
	readCtx, cancelRead := context.WithTimeout(ctx, 10*time.Second)
	dlqFetches := dlqReader.PollFetches(readCtx)
	cancelRead()
	if err := dlqFetches.Err(); err != nil {
		t.Fatalf("read DLQ message: %v", err)
	}
	var dlqMessage *kgo.Record
	dlqFetches.EachRecord(func(record *kgo.Record) {
		if dlqMessage == nil {
			dlqMessage = record
		}
	})
	if dlqMessage == nil {
		t.Fatal("DLQ returned no message")
	}
	if !bytes.Contains(dlqMessage.Value, []byte("poison-1")) {
		t.Fatalf("DLQ value = %q, want poison message", dlqMessage.Value)
	}
	for _, header := range []string{"dlq.attempts", "dlq.timestamp", "dlq.error.type"} {
		if !hasHeader(dlqMessage.Headers, header) {
			t.Fatalf("DLQ headers missing %q: %#v", header, dlqMessage.Headers)
		}
	}

	if err := cons.Close(); err != nil {
		t.Fatalf("close source consumer: %v", err)
	}
	closed = true
	consAgain, err := newConsumerWithOptions[evtTest](ctx, h, spec, o)
	if err != nil {
		t.Fatalf("reopen source consumer: %v", err)
	}
	go func() { _ = consAgain.Run() }()
	time.Sleep(1 * time.Second)
	if err := consAgain.Close(); err != nil {
		t.Fatalf("close reopened source consumer: %v", err)
	}
	if got := attempts.Load(); got != int32(o.MaxRetries)+1 {
		t.Fatalf("handler attempts after committed DLQ = %d, want %d", got, o.MaxRetries+1)
	}
}

// TestIntegrationCloseCancelsRun proves Close() cooperatively cancels a
// blocked FetchMessage within a bounded time window.
func TestIntegrationCloseCancelsRun(t *testing.T) {
	brokers := integrationBrokers(t)
	ctx := context.Background()
	topic := "hellnet.it.test.v1"
	ensureIntegrationTopic(t, brokers, topic)

	h := HandlerFunc[evtTest](func(ctx context.Context, msg evtTest, mc Ctx) error { return nil })
	spec := HandlerSpec{Topic: topic, Group: fmt.Sprintf("grp-stop-%d", time.Now().UnixNano())}
	cons, err := newConsumerWithOptions[evtTest](ctx, h, spec, integrationBaseOpts(brokers))
	if err != nil {
		t.Fatalf("NewConsumer: %v", err)
	}

	runDone := make(chan error, 1)
	go func() { runDone <- cons.Run() }()
	time.Sleep(800 * time.Millisecond)

	start := time.Now()
	if err := cons.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	select {
	case err := <-runDone:
		if err != nil {
			t.Fatalf("Run = %v after Close; shutdown must return nil (Run contract)", err)
		}
		fmt.Printf("Run returned nil after %s (cooperative shutdown ok)\n", time.Since(start).Round(time.Millisecond))
	case <-time.After(10 * time.Second):
		t.Fatal("Run did not return within 10s after Close")
	}
}
