package kafka

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"
)

type orderCreated struct {
	OrderID string  `json:"orderId"`
	Total   float64 `json:"total"`
}

func (orderCreated) MessageType() string { return "order.created.v1" }

func testDefaultOptions() Options {
	return Options{
		SecurityProtocol:    "sasl_ssl",
		SASLMechanism:       "SCRAM-SHA-512",
		SASLUsername:        "hellnet-app",
		Idempotent:          true,
		MaxRetries:          3,
		RetryDelay:          200 * time.Millisecond,
		TimeoutProduce:      30 * time.Second,
		CircuitBreakerCount: 5,
		DefaultSerializer:   "json",
		SchemaRegistryPath:  "/apis/ccompat/v6",
	}
}

func TestTopicName(t *testing.T) {
	o := testDefaultOptions()
	if got := TopicName(o, "order.created.v1"); got != "order.created.v1" {
		t.Fatalf("TopicName = %q, want %q", got, "order.created.v1")
	}
	o.TopicPrefix = "hellnet"
	if got := TopicName(o, "order.created.v1"); got != "hellnet.order.created.v1" {
		t.Fatalf("TopicName with prefix = %q", got)
	}
}

func TestHandlerSpecResolve(t *testing.T) {
	o := testDefaultOptions()
	spec := HandlerSpec{}
	if got := spec.resolveTopic(o, "order.created.v1"); got != "order.created.v1" {
		t.Fatalf("resolveTopic = %q", got)
	}
	spec = HandlerSpec{Topic: "custom.topic.v1"}
	if got := spec.resolveTopic(o, "order.created.v1"); got != "custom.topic.v1" {
		t.Fatalf("resolveTopic override = %q", got)
	}
}

func TestDefaultsFromEnv(t *testing.T) {
	t.Setenv("HELLNET_KAFKA_BROKERS", "127.0.0.1:9092,127.0.0.1:9093")
	t.Setenv("HELLNET_KAFKA_SECURITY_PROTOCOL", "plaintext")
	t.Setenv("HELLNET_KAFKA_MAX_RETRIES", "7")

	t.Setenv("HELLNET_KAFKA_TOPIC_PREFIX", "")
	bus, err := New()
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = bus.Close() }()
	o := bus.opts
	if len(o.Brokers) != 2 || o.Brokers[0] != "127.0.0.1:9092" {
		t.Fatalf("Brokers = %v", o.Brokers)
	}
	if o.SecurityProtocol != "plaintext" {
		t.Fatalf("SecurityProtocol = %q", o.SecurityProtocol)
	}
	if o.MaxRetries != 7 {
		t.Fatalf("MaxRetries = %d", o.MaxRetries)
	}
	if o.TopicPrefix != "" {
		t.Fatalf("TopicPrefix = %q, want empty default", o.TopicPrefix)
	}
}

// TestEnvMillisKnobsParsePlainIntegers proves the millisecond-suffixed knobs
// land as plain integers ("30000"): ParseDuration rejects bare integers, so
// these are read through GetInt × time.Millisecond (hellnet-lib-cache style).
func TestEnvMillisKnobsParsePlainIntegers(t *testing.T) {
	t.Setenv("HELLNET_KAFKA_RETRY_DELAY_MS", "30000")
	t.Setenv("HELLNET_KAFKA_TIMEOUT_PRODUCE_MS", "45000")

	t.Setenv("HELLNET_KAFKA_BROKERS", "127.0.0.1:9092")
	t.Setenv("HELLNET_KAFKA_SECURITY_PROTOCOL", "plaintext")
	bus, err := New()
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = bus.Close() }()
	o := bus.opts
	if o.RetryDelay != 30*time.Second {
		t.Fatalf(`RetryDelay = %v, want 30s from plain integer "30000"`, o.RetryDelay)
	}
	if o.TimeoutProduce != 45*time.Second {
		t.Fatalf(`TimeoutProduce = %v, want 45s from plain integer "45000"`, o.TimeoutProduce)
	}
}

func TestBackoffGrows(t *testing.T) {
	a := backoff(200*time.Millisecond, 0)
	b := backoff(200*time.Millisecond, 1)
	c := backoff(200*time.Millisecond, 2)
	if !(a < b && b < c) {
		t.Fatalf("backoff not monotonic: %v %v %v", a, b, c)
	}
}

// TestFetchBackoffEscalatesAndCaps proves the consume-loop fetch retry delay
// starts around 200ms, escalates by doubling and stays capped at ~5s (±20%
// jitter) regardless of how long the failure streak lasts.
func TestFetchBackoffEscalatesAndCaps(t *testing.T) {
	for attempt := 0; attempt <= 12; attempt++ {
		wantBase := fetchBackoffBase
		for i := 0; i < attempt && wantBase < fetchBackoffMax; i++ {
			wantBase *= 2
		}
		if wantBase > fetchBackoffMax {
			wantBase = fetchBackoffMax
		}
		lo := wantBase - wantBase/5
		hi := wantBase + wantBase/5

		got := fetchBackoff(attempt)
		if got < lo || got > hi {
			t.Fatalf("attempt %d: fetchBackoff = %v, want within [%v, %v]", attempt, got, lo, hi)
		}
		if t.Failed() {
			return
		}
	}
	if got := fetchBackoff(100); got > fetchBackoffMax+fetchBackoffMax/5 {
		t.Fatalf("deep failure streak: fetchBackoff = %v exceeds capped ceiling", got)
	}
}

func TestDLQTopic(t *testing.T) {
	if got := dlqTopic("hellnet.order.created.v1"); got != "hellnet.order.created.v1.dlq" {
		t.Fatalf("dlqTopic = %q", got)
	}
}

func TestJSONSerializer(t *testing.T) {
	s := JSONSerializer{}
	msg := orderCreated{OrderID: "123", Total: 9.9}
	data, err := s.Serialize("hellnet.order.created.v1", msg)
	if err != nil {
		t.Fatal(err)
	}
	var out orderCreated
	if err := s.Deserialize("hellnet.order.created.v1", data, &out); err != nil {
		t.Fatal(err)
	}
	if out.OrderID != "123" || out.Total != 9.9 {
		t.Fatalf("roundtrip = %+v", out)
	}
}

// testOfflineOptions returns valid options that never touch the network.
func testOfflineOptions() Options {
	o := testDefaultOptions()
	o.Brokers = []string{"127.0.0.1:9092"}
	o.SecurityProtocol = "plaintext"
	return o
}

func TestNewLoadsEnvironmentWithInternalContext(t *testing.T) {
	t.Setenv("HELLNET_KAFKA_BROKERS", "127.0.0.1:19092")
	t.Setenv("HELLNET_KAFKA_SECURITY_PROTOCOL", "plaintext")
	t.Setenv("HELLNET_KAFKA_TOPIC_PREFIX", "from-env")

	bus, err := New()
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = bus.Close() }()

	if bus.baseCtx == nil {
		t.Fatal("New must create an internal base context")
	}
	if got := bus.opts.Brokers; len(got) != 1 || got[0] != "127.0.0.1:19092" {
		t.Fatalf("Brokers = %v, want environment value", got)
	}
	if bus.opts.TopicPrefix != "from-env" {
		t.Fatalf("TopicPrefix = %q, want environment value", bus.opts.TopicPrefix)
	}
}

func TestNewLoadsDotEnv(t *testing.T) {
	t.Setenv("HELLNET_ENVIRONMENT", "test")
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, ".env"), []byte(
		"HELLNET_KAFKA_BROKERS=127.0.0.1:29092\n"+
			"HELLNET_KAFKA_SECURITY_PROTOCOL=plaintext\n"+
			"HELLNET_KAFKA_TOPIC_PREFIX=from-dotenv\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	for _, key := range []string{
		"HELLNET_KAFKA_BROKERS", "HELLNET_BROKERS",
		"HELLNET_KAFKA_SECURITY_PROTOCOL", "HELLNET_SECURITY_PROTOCOL",
		"HELLNET_KAFKA_TOPIC_PREFIX", "HELLNET_TOPIC_PREFIX",
	} {
		t.Setenv(key, "")
		if err := os.Unsetenv(key); err != nil {
			t.Fatal(err)
		}
	}
	t.Chdir(dir)

	bus, err := New()
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = bus.Close() }()
	if got := bus.opts.Brokers; len(got) != 1 || got[0] != "127.0.0.1:29092" {
		t.Fatalf("Brokers = %v, want value loaded from .env", got)
	}
	if bus.opts.TopicPrefix != "from-dotenv" {
		t.Fatalf("TopicPrefix = %q, want value loaded from .env", bus.opts.TopicPrefix)
	}
}

func TestNewUsesHellnetFallback(t *testing.T) {
	t.Setenv("HELLNET_KAFKA_BROKERS", "")
	t.Setenv("HELLNET_BROKERS", "127.0.0.1:39092")
	t.Setenv("HELLNET_KAFKA_SECURITY_PROTOCOL", "")
	t.Setenv("HELLNET_SECURITY_PROTOCOL", "plaintext")

	bus, err := New()
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = bus.Close() }()
	if got := bus.opts.Brokers; len(got) != 1 || got[0] != "127.0.0.1:39092" {
		t.Fatalf("Brokers = %v, want HELLNET_BROKERS fallback", got)
	}
}

func TestPrivateConstructorCapturesContext(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	bus, err := newWithOptions(ctx, testOfflineOptions())
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = bus.Close() }()
	if bus.baseCtx == nil {
		t.Fatal("newWithOptions must capture the caller context as the Bus base context")
	}
	cancel()
	if bus.baseCtx.Err() == nil {
		t.Fatal("Bus base context must be derived from the ctx given at newWithOptions")
	}
}
