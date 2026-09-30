package kafka

import (
	"context"
	"errors"
	"testing"

	"github.com/guilhermelinosp/hellnet-lib-telemetry/telemetry"
	"github.com/sony/gobreaker"
)

func setOfflineEnv(t *testing.T) {
	t.Helper()
	t.Setenv("KAFKA_BROKERS", "127.0.0.1:19092")
	t.Setenv("KAFKA_SECURITY_PROTOCOL", "plaintext")
}

func TestNewWithOptionsUsesInstrumentation(t *testing.T) {
	setOfflineEnv(t)
	h := telemetry.NewHarness(t)
	bus, err := NewWithOptions(context.Background(), WithInstrumentation(h))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = bus.Shutdown(context.Background()) }()
	if bus.obs.inst != h {
		t.Fatal("NewWithOptions must use the supplied instrumentation")
	}
}

func TestNewWithOptionsWithoutInstrumentationIsNoop(t *testing.T) {
	setOfflineEnv(t)
	bus, err := NewWithOptions(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = bus.Shutdown(context.Background()) }()
	if bus.obs.tracer == nil {
		t.Fatal("observability must default to no-op, not nil")
	}
}

func TestProducerAndConsumerWithOptions(t *testing.T) {
	setOfflineEnv(t)
	ctx := context.Background()
	p, err := NewProducer[orderCreated](ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := p.Shutdown(ctx); err != nil {
		t.Fatal(err)
	}
	if err := p.Shutdown(ctx); err == nil {
		t.Fatal("second Shutdown must report the producer is closed")
	}
	if err := p.PublishContext(ctx, orderCreated{OrderID: "x"}); err == nil {
		t.Fatal("PublishContext after Shutdown must fail")
	}
	c, err := NewConsumer[orderCreated](ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	if c.bus == nil || c.serializer == nil {
		t.Fatal("consumer must carry its bus and serializer")
	}
	_ = c.Close()
}

func TestProducerPublishContextPropagatesParent(t *testing.T) {
	h := telemetry.NewHarness(t)
	bus := &Bus{
		opts:       testOfflineOptions(),
		client:     &capturingMessageWriter{},
		serializer: &contextSerializer{},
		breaker:    gobreaker.NewCircuitBreaker(gobreaker.Settings{}),
		obs:        newObservability(context.Background(), h),
	}
	p := &Producer[orderCreated]{bus: bus}
	ctx, parent := h.TracerProvider().Tracer("caller").Start(context.Background(), "caller")
	if err := p.PublishContext(ctx, orderCreated{OrderID: "order-1"}); err != nil {
		t.Fatal(err)
	}
	parent.End()
	spans := h.SpansByName("send order.created.v1")
	if len(spans) != 1 || !telemetry.ChildOf(h.SpansByName("caller")[0], spans[0]) {
		t.Fatalf("send span hierarchy = %#v", spans)
	}
	canceled, cancel := context.WithCancel(context.Background())
	cancel()
	if err := p.Shutdown(canceled); !errors.Is(err, context.Canceled) {
		t.Fatalf("Shutdown(canceled) = %v, want context canceled", err)
	}
}

func TestNewTakesInstrumentationDirectly(t *testing.T) {
	setOfflineEnv(t)
	h := telemetry.NewHarness(t)
	p, err := NewProducer[orderCreated](context.Background(), h)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = p.Shutdown(context.Background()) }()
	if p.bus.obs.inst != h {
		t.Fatal("NewProducer must use the supplied instrumentation")
	}
}
