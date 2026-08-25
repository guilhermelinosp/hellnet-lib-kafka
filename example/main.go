// Command hellnet-kafka-demo exercises hellnet-lib-kafka against a live Kafka:
// produce order.created.v1 messages, consume them with a handler, and (with
// FAIL_HANDLER=1) observe a message landing on the DLQ after retries.
//
// The message struct mirrors the Avro schema registered in hellnet-lib-schema
// (schemas/avro/hellnet-order-created/v1/schema.avsc). With
// HELLNET_KAFKA_DEFAULT_SERIALIZER=avro the payload is Confluent wire format
// (schema id header) resolved from the Schema Registry; with "json" it is
// plain JSON.
//
// Env (teste local):
//
//	HELLNET_KAFKA_BROKERS=127.0.0.1:9094
//	HELLNET_KAFKA_SECURITY_PROTOCOL=plaintext
//	HELLNET_KAFKA_CONSUMER_GROUP=hellnet.demo.orders
//	HELLNET_KAFKA_DEFAULT_SERIALIZER=avro
//	HELLNET_KAFKA_SCHEMA_REGISTRY_URL=http://127.0.0.1:8085
//	HELLNET_KAFKA_MAX_RETRIES=3
package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"time"

	"github.com/guilhermelinosp/hellnet-lib-kafka/kafka"
)

// orderItem matches the OrderItem Avro record.
type orderItem struct {
	ProductID string `avro:"productId"`
	Quantity  int32  `avro:"quantity"`
}

// orderCreated matches the hellnet_order_created Avro record.
type orderCreated struct {
	OrderID    string      `avro:"orderId"`
	CustomerID string      `avro:"customerId"`
	Amount     float64     `avro:"amount"`
	Currency   string      `avro:"currency"`
	Items      []orderItem `avro:"items"`
}

func (orderCreated) MessageType() string { return "order.created.v1" }

type orderHandler struct{}

func (orderHandler) Handle(_ context.Context, msg orderCreated, mctx kafka.Ctx) error {
	if os.Getenv("FAIL_HANDLER") == "1" {
		return fmt.Errorf("simulated failure for %s", msg.OrderID)
	}
	log.Printf("RECEIVED order %s customer=%s amount=%.2f %s items=%d (topic=%s partition=%d offset=%d)",
		msg.OrderID, msg.CustomerID, msg.Amount, msg.Currency, len(msg.Items), mctx.Topic, mctx.Partition, mctx.Offset)
	return nil
}

func main() {
	ctx := context.Background()

	bus, err := kafka.New()
	if err != nil {
		log.Fatalf("kafka.New: %v", err)
	}
	defer bus.Close()

	for i := 0; i < 3; i++ {
		msg := orderCreated{
			OrderID:    fmt.Sprintf("ORD-%d", i),
			CustomerID: "CUST-1",
			Amount:     99.90 + float64(i),
			Currency:   "BRL",
			Items: []orderItem{
				{ProductID: "P-1", Quantity: int32(i + 1)},
			},
		}
		if err := bus.Publish(msg); err != nil {
			log.Fatalf("publish: %v", err)
		}
		log.Printf("published %s -> hellnet.order.created.v1", msg.OrderID)
	}

	opts := kafka.LoadFromEnv()
	cons, err := kafka.NewConsumer(opts, orderHandler{}, kafka.HandlerSpec{})
	if err != nil {
		log.Fatalf("NewConsumer: %v", err)
	}
	defer cons.Close()

	rctx, cancel := context.WithTimeout(ctx, 20*time.Second)
	defer cancel()
	if err := cons.RunContext(rctx); err != nil {
		log.Printf("consumer run ended: %v", err)
	}
	log.Println("done")
}