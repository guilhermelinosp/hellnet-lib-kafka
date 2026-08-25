package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"os"
	"testing"
	"time"

	hellnetv1 "github.com/guilhermelinosp/hellnet-lib-kafka/example/proto/hellnet/events/v1"
	"github.com/guilhermelinosp/hellnet-lib-kafka/kafka"
	kafkago "github.com/segmentio/kafka-go"
	"google.golang.org/protobuf/proto"
)

// orderCreatedAvsc mirrors hellnet-lib-schema
// schemas/avro/hellnet-order-created/v1/schema.avsc so tests can auto-register
// the schema against a fresh (in-memory) registry.
const orderCreatedAvsc = `{
  "namespace": "hellnet.events",
  "type": "record",
  "name": "hellnet_order_created",
  "fields": [
    { "name": "orderId", "type": "string" },
    { "name": "customerId", "type": "string" },
    { "name": "amount", "type": "double" },
    { "name": "currency", "type": "string", "default": "BRL" },
    { "name": "items", "type": { "type": "array", "items": {
      "type": "record", "name": "OrderItem", "fields": [
        { "name": "productId", "type": "string" },
        { "name": "quantity", "type": "int" }
      ]
    }}}
  ]
}`

// registerSubject registers a schema in Apicurio (ccompat) idempotently.
func registerSubject(subject, schema, schemaType string) error {
	body, err := json.Marshal(map[string]string{"schema": schema, "schemaType": schemaType})
	if err != nil {
		return err
	}
	req, err := http.NewRequest(http.MethodPost,
		os.Getenv("HELLNET_TEST_SR")+"/apis/ccompat/v6/subjects/"+subject+"/versions", bytes.NewReader(body))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/vnd.schemaregistry.v1+json")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	// 200 (new) and 409 (already exists, identical) are both fine.
	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusConflict {
		return errors.New("register " + subject + ": HTTP " + resp.Status)
	}
	return nil
}

// ensureSchemas auto-registers the schemas used by the tests (avro + protobuf),
// so they work against a fresh in-memory Apicurio.
func ensureSchemas(t *testing.T) {
	if os.Getenv("HELLNET_TEST_SR") == "" {
		t.Skip("HELLNET_TEST_SR not set")
	}
	protoSchema := hellnetStockProto(t)
	if err := registerSubject("hellnet.order.created.v1-value", orderCreatedAvsc, "AVRO"); err != nil {
		t.Fatal(err)
	}
	if err := registerSubject("hellnet.stock.updated.v1-value", protoSchema, "PROTOBUF"); err != nil {
		t.Fatal(err)
	}
}

func hellnetStockProto(t *testing.T) string {
	b, err := os.ReadFile("proto/hellnet/events/v1/stock.proto")
	if err != nil {
		t.Fatal(err)
	}
	return string(b)
}

// Env gating:
//   - HELLNET_TEST_SR    -> runs Avro/Protobuf registry round-trips
//   - HELLNET_TEST_KAFKA -> runs the Kafka produce/consume smoke test
//
// Local:
//
//	HELLNET_TEST_SR=http://127.0.0.1:8085 \
//	HELLNET_TEST_KAFKA=1 \
//	HELLNET_KAFKA_BROKERS=127.0.0.1:9094 HELLNET_KAFKA_SECURITY_PROTOCOL=plaintext \
//	go test ./example -run "RoundTrip|E2E" -v

type jsonMessage struct {
	OrderID string `json:"orderId"`
	Total   float64 `json:"total"`
}

func (jsonMessage) MessageType() string { return "order.created.v1" }

func TestJSONRoundTrip(t *testing.T) {
	s := kafka.JSONSerializer{}
	msg := jsonMessage{OrderID: "J-1", Total: 12.5}
	data, err := s.Serialize("hellnet.order.created.v1", msg)
	if err != nil {
		t.Fatal(err)
	}
	var out jsonMessage
	if err := s.Deserialize("hellnet.order.created.v1", data, &out); err != nil {
		t.Fatal(err)
	}
	if out.OrderID != msg.OrderID || out.Total != msg.Total {
		t.Fatalf("roundtrip = %+v, want %+v", out, msg)
	}
}

func TestAvroRoundTrip(t *testing.T) {
	ensureSchemas(t)
	s, err := kafka.NewAvroSerializer(os.Getenv("HELLNET_TEST_SR"))
	if err != nil {
		t.Fatal(err)
	}
	msg := orderCreated{
		OrderID: "A-1", CustomerID: "CUST-1", Amount: 99.9, Currency: "BRL",
		Items: []orderItem{{ProductID: "P-1", Quantity: 2}},
	}
	data, err := s.Serialize("hellnet.order.created.v1", msg)
	if err != nil {
		t.Fatal(err)
	}
	var out orderCreated
	if err := s.Deserialize("hellnet.order.created.v1", data, &out); err != nil {
		t.Fatal(err)
	}
	if out.OrderID != msg.OrderID || out.Amount != msg.Amount || len(out.Items) != 1 || out.Items[0].ProductID != "P-1" {
		t.Fatalf("roundtrip = %+v, want %+v", out, msg)
	}
}

func TestProtobufRoundTrip(t *testing.T) {
	ensureSchemas(t)
	s, err := kafka.NewProtobufSerializer(os.Getenv("HELLNET_TEST_SR"))
	if err != nil {
		t.Fatal(err)
	}
	msg := &hellnetv1.StockUpdated{
		ProductId: "P-42", Quantity: 10, Reserved: 2, Available: 8,
		WarehouseId: "WH-1", Timestamp: 1720000000,
	}
	data, err := s.Serialize("hellnet.stock.updated.v1", msg)
	if err != nil {
		t.Fatal(err)
	}
	var out hellnetv1.StockUpdated
	if err := s.Deserialize("hellnet.stock.updated.v1", data, &out); err != nil {
		t.Fatal(err)
	}
	if !proto.Equal(msg, &out) {
		t.Fatalf("roundtrip = %+v, want %+v", &out, msg)
	}
}

func TestKafkaProtobufE2E(t *testing.T) {
	if os.Getenv("HELLNET_TEST_KAFKA") == "" {
		t.Skip("HELLNET_TEST_KAFKA not set")
	}
	opts := kafka.LoadFromEnv()
	opts.DefaultSerializer = "protobuf"

	prod, err := kafka.NewProducer[*stockMessage](opts)
	if err != nil {
		t.Fatal(err)
	}
	defer prod.Close()

	// 1) produce a StockUpdated (protobuf wire format) — built in place to
	// avoid copying the proto message (which holds a mutex).
	produced := &stockMessage{}
	produced.StockUpdated.ProductId = "P-E2E"
	produced.StockUpdated.Quantity = 5
	produced.StockUpdated.Available = 5
	produced.StockUpdated.WarehouseId = "WH-1"
	produced.StockUpdated.Timestamp = 1720000000
	want := &produced.StockUpdated
	if err := prod.Publish(produced); err != nil {
		t.Fatalf("publish: %v", err)
	}

	// 2) consume it back with a fresh group and a protobuf handler
	done := make(chan error, 1)
	h := handlerFunc(func(msg *stockMessage, mctx kafka.Ctx) error {
		if !proto.Equal(&msg.StockUpdated, want) {
			done <- errMismatch{got: &msg.StockUpdated, want: want}
			return nil
		}
		done <- nil
		return nil
	})
	opts.ConsumerGroup = "hellnet.test.protobuf." + time.Now().Format("150405")
	cons, err := kafka.NewConsumer[*stockMessage](h, kafka.HandlerSpec{}, opts)
	if err != nil {
		t.Fatal(err)
	}
	defer cons.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 25*time.Second)
	defer cancel()
	go func() { _ = cons.RunContext(ctx) }()

	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-ctx.Done():
		t.Fatal("timeout waiting for protobuf message")
	}
}

// failHandler always fails so the consumer retries and then DLQs.
type failHandler struct {
	t *testing.T
}

func (f failHandler) Handle(_ context.Context, msg orderCreated, mctx kafka.Ctx) error {
	if f.t != nil {
		f.t.Logf("failHandler called order=%q offset=%d", msg.OrderID, mctx.Offset)
	}
	return errors.New("always fails")
}

// TestKafkaRetryDLQ proves the retry -> Dead Letter Queue path end-to-end:
// a message whose handler keeps failing must land on "{topic}.dlq" with the
// dlq.* headers set.
func TestKafkaRetryDLQ(t *testing.T) {
	if os.Getenv("HELLNET_TEST_KAFKA") == "" {
		t.Skip("HELLNET_TEST_KAFKA not set")
	}
	ensureSchemas(t)

	opts := kafka.LoadFromEnv()
	opts.DefaultSerializer = "avro"
	opts.MaxRetries = 2
	opts.RetryDelay = 100 * time.Millisecond

	bus, err := kafka.New(opts)
	if err != nil {
		t.Fatal(err)
	}
	defer bus.Close()

	id := "DLQ-" + time.Now().Format("150405.000")
	if err := bus.Publish(orderCreated{OrderID: id, CustomerID: "CUST-1", Amount: 10, Currency: "BRL",
		Items: []orderItem{{ProductID: "P-1", Quantity: 1}}}); err != nil {
		t.Fatalf("publish: %v", err)
	}

	// consumer whose handler always fails -> retries -> DLQ
	opts.ConsumerGroup = "hellnet.test.dlq." + id
	cons, err := kafka.NewConsumer[orderCreated](failHandler{t: t}, kafka.HandlerSpec{}, opts)
	if err != nil {
		t.Fatal(err)
	}
	defer cons.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 25*time.Second)
	defer cancel()
	go func() {
		if err := cons.RunContext(ctx); err != nil {
			t.Logf("consumer RunContext ended: %v", err)
		}
	}()

	// read the .dlq topic raw until the message (with dlq.reason header) appears
	r := kafkago.NewReader(kafkago.ReaderConfig{
		Brokers:        opts.Brokers,
		GroupID:        "hellnet.test.dlqread." + id,
		Topic:          "hellnet.order.created.v1.dlq",
		StartOffset:    kafkago.FirstOffset,
		CommitInterval: time.Second,
	})
	defer r.Close()

	deadline := time.Now().Add(30 * time.Second)
	for time.Now().Before(deadline) {
		rctx, rcancel := context.WithTimeout(context.Background(), 3*time.Second)
		m, err := r.FetchMessage(rctx)
		rcancel()
		if err != nil {
			continue // timeout/no message yet — re-check the deadline
		}
		_ = r.CommitMessages(context.Background(), m)
		if !bytes.Contains(m.Value, []byte(id)) {
			continue
		}
		var hasReason bool
		for _, h := range m.Headers {
			if h.Key == "dlq.reason" && len(h.Value) > 0 {
				hasReason = true
			}
		}
		if !hasReason {
			t.Fatal("dlq.reason header missing on DLQ message")
		}
		return
	}
	t.Fatal("message not found on DLQ topic")
}

// helpers for the E2E test: stockMessage embeds the proto message and is used
// as a POINTER type (T = *stockMessage), so the consumer allocates and passes
// the pointer itself — no value copies of the proto message (no copylocks).
type stockMessage struct {
	hellnetv1.StockUpdated
}

func (*stockMessage) MessageType() string { return "stock.updated.v1" }

type handlerFunc func(msg *stockMessage, mctx kafka.Ctx) error

func (f handlerFunc) Handle(ctx context.Context, msg *stockMessage, mctx kafka.Ctx) error {
	return f(msg, mctx)
}

type errMismatch struct{ got, want *hellnetv1.StockUpdated }

func (e errMismatch) Error() string { return "mismatch" }