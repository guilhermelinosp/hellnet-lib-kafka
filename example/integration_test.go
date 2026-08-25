package main

import (
	"context"
	"os"
	"testing"
	"time"

	hellnetv1 "github.com/guilhermelinosp/hellnet-lib-kafka/example/proto/hellnet/events/v1"
	"github.com/guilhermelinosp/hellnet-lib-kafka/kafka"
	"google.golang.org/protobuf/proto"
)

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
	sr := os.Getenv("HELLNET_TEST_SR")
	if sr == "" {
		t.Skip("HELLNET_TEST_SR not set")
	}
	s, err := kafka.NewAvroSerializer(sr)
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
	sr := os.Getenv("HELLNET_TEST_SR")
	if sr == "" {
		t.Skip("HELLNET_TEST_SR not set")
	}
	s, err := kafka.NewProtobufSerializer(sr)
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

	bus, err := kafka.New(opts)
	if err != nil {
		t.Fatal(err)
	}
	defer bus.Close()

	// 1) produce a StockUpdated (protobuf wire format)
	want := &hellnetv1.StockUpdated{ProductId: "P-E2E", Quantity: 5, Available: 5, WarehouseId: "WH-1", Timestamp: 1720000000}
	if err := bus.Publish(&stockMessage{StockUpdated: *want}); err != nil {
		t.Fatalf("publish: %v", err)
	}

	// 2) consume it back with a fresh group and a protobuf handler
	done := make(chan error, 1)
	h := handlerFunc(func(msg stockMessage, mctx kafka.Ctx) error {
		if !proto.Equal(&msg.StockUpdated, want) {
			done <- errMismatch{got: &msg.StockUpdated, want: want}
			return nil
		}
		done <- nil
		return nil
	})
	opts.ConsumerGroup = "hellnet.test.protobuf." + time.Now().Format("150405")
	cons, err := kafka.NewConsumer[stockMessage](opts, h, kafka.HandlerSpec{})
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

// helpers for the E2E test: stockMessage embeds the proto message by VALUE so
// the consumer's &msg (*stockMessage) is both kafka.Message and proto.Message.
type stockMessage struct {
	hellnetv1.StockUpdated
}

func (stockMessage) MessageType() string { return "stock.updated.v1" }

type handlerFunc func(msg stockMessage, mctx kafka.Ctx) error

func (f handlerFunc) Handle(ctx context.Context, msg stockMessage, mctx kafka.Ctx) error {
	return f(msg, mctx)
}

type errMismatch struct{ got, want *hellnetv1.StockUpdated }

func (e errMismatch) Error() string { return "mismatch" }