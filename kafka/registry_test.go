package kafka

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/guilhermelinosp/hellnet-lib-telemetry/telemetry"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/trace"
)

// TestRegistryFetchDerivesOperationContext proves a cancelled active
// operation context aborts the schema lookup instead of using Background.
func TestRegistryFetchDerivesOperationContext(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	c := newRegistryClient("http://127.0.0.1:1", "")
	cancel()

	err := c.getContext(ctx, "/subjects/some-subject/versions/latest", &schemaResponse{})
	if err == nil || !errors.Is(err, context.Canceled) {
		t.Fatalf("get err = %v, want context.Canceled derived from the base ctx", err)
	}
}

// TestRegistryStandaloneDefaultsToBackground proves a standalone serializer
// can explicitly use Background with the registry timeout budget.
func TestRegistryStandaloneDefaultsToBackground(t *testing.T) {
	c := newRegistryClient("http://127.0.0.1:1", "")

	err := c.getContext(context.Background(), "/subjects/some-subject/versions/latest", &schemaResponse{})
	if err == nil {
		t.Fatal("expected connection error against closed port")
	}
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("standalone fetch failed with context error %v; want connection error", err)
	}
}

func TestRegistryRequestIsChildOfActiveOperation(t *testing.T) {
	h := telemetry.NewHarness(t)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/subjects/orders/versions/latest" {
			t.Fatalf("path = %q", r.URL.Path)
		}
		_, _ = w.Write([]byte(`{"schema":"{}","id":1}`))
	}))
	defer server.Close()

	ctx, parent := h.TracerProvider().Tracer("caller").Start(context.Background(), "caller")
	ctx, send := h.TracerProvider().Tracer("kafka").Start(ctx, "send orders")
	c := newRegistryClientWithInstrumentation(server.URL, "", h)
	if _, _, err := c.latestSchemaContext(ctx, "orders"); err != nil {
		t.Fatal(err)
	}
	send.End()
	parent.End()

	var requestSpan sdktrace.ReadOnlySpan
	for _, span := range h.Spans() {
		if span.SpanKind() == trace.SpanKindClient {
			requestSpan = span
			break
		}
	}
	if requestSpan == nil || !telemetry.ChildOf(h.SpansByName("send orders")[0], requestSpan) {
		t.Fatalf("schema registry span hierarchy = %#v", h.Spans())
	}
}
