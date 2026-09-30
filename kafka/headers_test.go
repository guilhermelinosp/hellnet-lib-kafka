package kafka

import (
	"context"
	"testing"

	"github.com/guilhermelinosp/hellnet-lib-telemetry/telemetry"
	"github.com/sony/gobreaker"
	"github.com/twmb/franz-go/pkg/kgo"
)

func TestPublishContextSendsCorrelationHeaders(t *testing.T) {
	h := telemetry.NewHarness(t)
	writer := &capturingMessageWriter{}
	bus := &Bus{
		opts:       testOfflineOptions(),
		client:     writer,
		serializer: &contextSerializer{},
		breaker:    gobreaker.NewCircuitBreaker(gobreaker.Settings{}),
		obs:        newObservability(context.Background(), h),
	}
	ctx, span := h.TracerProvider().Tracer("caller").Start(context.Background(), "caller")
	defer span.End()
	ctx = ContextWithHeaders(ctx, map[string]string{"event_id": "e1", "order_id": "o1", "traceparent": "spoofed"})
	ctx = ContextWithHeaders(ctx, map[string]string{"order_id": "o2"}) // merges, new value wins

	if err := bus.PublishContext(ctx, orderCreated{OrderID: "o2"}); err != nil {
		t.Fatal(err)
	}
	if len(writer.messages) != 1 {
		t.Fatalf("messages = %d, want 1", len(writer.messages))
	}
	got := recordHeaders(*writer.messages[0])
	if got["event_id"] != "e1" || got["order_id"] != "o2" {
		t.Fatalf("correlation headers = %v, want event_id=e1 order_id=o2", got)
	}
	if got["traceparent"] == "spoofed" || got["traceparent"] == "" {
		t.Fatalf("traceparent = %q: must come from the span context, not the caller", got["traceparent"])
	}
}

func TestHandlerReceivesRecordHeaders(t *testing.T) {
	h := telemetry.NewHarness(t)
	var seen Ctx
	bus := &Bus{opts: testOfflineOptions(), obs: newObservability(context.Background(), h)}
	consumer := &Consumer[orderCreated]{
		opts: bus.opts, bus: bus, client: &commitTrackingReader{}, serializer: JSONSerializer{}, group: "orders",
		handler: HandlerFunc[orderCreated](func(_ context.Context, _ orderCreated, kctx Ctx) error {
			seen = kctx
			return nil
		}),
	}
	record := kgo.Record{Topic: "orders", Value: []byte(`{}`), Headers: []kgo.RecordHeader{
		{Key: "event_id", Value: []byte("e1")}, {Key: "order_id", Value: []byte("o1")},
	}}
	if err := consumer.processMessage(context.Background(), record); err != nil {
		t.Fatal(err)
	}
	if seen.Headers["event_id"] != "e1" || seen.Headers["order_id"] != "o1" {
		t.Fatalf("handler headers = %v", seen.Headers)
	}
}
