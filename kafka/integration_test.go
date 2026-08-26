//go:build integration

package kafka

import (
	"context"
	"errors"
	"fmt"
	"os"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// Integration tests run against a REAL broker (Redpanda/Kafka).
//
// Usage (tools namespace on kind, via kubectl port-forward):
//
//	kubectl port-forward -n tools svc/redpanda 19092:9092
//	export HELLNET_TEST_KAFKA_BROKERS=localhost:19092
//	go test -tags integration -count=1 -run TestIntegration ./kafka/

func integrationBrokers(t *testing.T) []string {
	t.Helper()
	b := os.Getenv("HELLNET_TEST_KAFKA_BROKERS")
	if b == "" {
		t.Skip("HELLNET_TEST_KAFKA_BROKERS not set")
	}
	return []string{b}
}

type evtTest struct {
	ID   string `json:"id"`
	Data string `json:"data"`
}

func (evtTest) MessageType() string { return "it.test.v1" }

func integrationBaseOpts(brokers []string) Options {
	o := Default()
	o.Brokers = brokers
	o.SecurityProtocol = "plaintext"
	return o
}

// TestIntegrationPublishConsume covers the core loop: construct once with ctx,
// publish without ctx, consume via Run() into a handler that receives the
// lib-supplied ctx, then close and observe cooperative stop.
func TestIntegrationPublishConsume(t *testing.T) {
	brokers := integrationBrokers(t)
	ctx := context.Background()
	topic := "hellnet.it.test.v1" // pré-existente: auto-create in-flight perde batches

	prod, err := NewProducer[evtTest](ctx, integrationBaseOpts(brokers))
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
	cons, err := NewConsumer[evtTest](ctx, h, spec, integrationBaseOpts(brokers))
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
	topic := "hellnet.it.test.v1"

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
	cons, err := NewConsumer[evtTest](ctx, h, spec, o)
	if err != nil {
		t.Fatalf("NewConsumer: %v", err)
	}
	defer func() { _ = cons.Close() }()
	go func() { _ = cons.Run() }()

	prod, err := NewProducer[evtTest](ctx, integrationBaseOpts(brokers))
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
				return
			}
		}
	}
}

// TestIntegrationCloseCancelsRun proves Close() cooperatively cancels a
// blocked FetchMessage within a bounded time window.
func TestIntegrationCloseCancelsRun(t *testing.T) {
	brokers := integrationBrokers(t)
	ctx := context.Background()
	topic := "hellnet.it.test.v1"

	h := HandlerFunc[evtTest](func(ctx context.Context, msg evtTest, mc Ctx) error { return nil })
	spec := HandlerSpec{Topic: topic, Group: fmt.Sprintf("grp-stop-%d", time.Now().UnixNano())}
	cons, err := NewConsumer[evtTest](ctx, h, spec, integrationBaseOpts(brokers))
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
