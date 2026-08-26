package kafka

import (
	"context"
	"errors"
	"testing"
)

// TestRegistryFetchDerivesBaseContext proves the registry client derives its
// per-request timeout from the base context captured at construction (N4): a
// cancelled base ctx aborts the fetch instead of silently using Background.
func TestRegistryFetchDerivesBaseContext(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	c := newRegistryClient(ctx, "http://127.0.0.1:1", "")
	cancel()

	err := c.get("/subjects/some-subject/versions/latest", &schemaResponse{})
	if err == nil || !errors.Is(err, context.Canceled) {
		t.Fatalf("get err = %v, want context.Canceled derived from the base ctx", err)
	}
}

// TestRegistryStandaloneDefaultsToBackground proves standalone construction
// (nil base ctx, e.g. NewAvroSerializer/NewProtobufSerializer used directly)
// keeps working: the fetch is attempted against Background + timeout budget,
// failing with a connection error rather than a context error.
func TestRegistryStandaloneDefaultsToBackground(t *testing.T) {
	c := newRegistryClient(nil, "http://127.0.0.1:1", "")

	err := c.get("/subjects/some-subject/versions/latest", &schemaResponse{})
	if err == nil {
		t.Fatal("expected connection error against closed port")
	}
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("standalone fetch failed with context error %v; want connection error", err)
	}
}
