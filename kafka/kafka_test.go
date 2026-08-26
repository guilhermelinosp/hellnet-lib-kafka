package kafka

import (
	"testing"
	"time"
)

type orderCreated struct {
	OrderID string  `json:"orderId"`
	Total   float64 `json:"total"`
}

func (orderCreated) MessageType() string { return "order.created.v1" }

func TestTopicName(t *testing.T) {
	o := Default()
	if got := TopicName(o, "order.created.v1"); got != "hellnet.order.created.v1" {
		t.Fatalf("TopicName = %q, want %q", got, "hellnet.order.created.v1")
	}
	o.TopicPrefix = ""
	if got := TopicName(o, "order.created.v1"); got != "order.created.v1" {
		t.Fatalf("TopicName without prefix = %q", got)
	}
}

func TestHandlerSpecResolve(t *testing.T) {
	o := Default()
	spec := HandlerSpec{}
	if got := spec.resolveTopic(o, "order.created.v1"); got != "hellnet.order.created.v1" {
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

	var o Options
	o.fromEnv(Default())
	if len(o.Brokers) != 2 || o.Brokers[0] != "127.0.0.1:9092" {
		t.Fatalf("Brokers = %v", o.Brokers)
	}
	if o.SecurityProtocol != "plaintext" {
		t.Fatalf("SecurityProtocol = %q", o.SecurityProtocol)
	}
	if o.MaxRetries != 7 {
		t.Fatalf("MaxRetries = %d", o.MaxRetries)
	}
	if o.TopicPrefix != "hellnet" {
		t.Fatalf("TopicPrefix = %q (default lost)", o.TopicPrefix)
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
