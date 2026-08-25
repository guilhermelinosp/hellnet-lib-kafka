# hellnet-lib-kafka

> Biblioteca opinionada de integração Kafka para microsserviços Go — port fiel
> do .NET [Hellnet.Kafka](https://github.com/guilhermelinosp/hellnet-dep-kafka),
> no mesmo espírito de `hellnet-lib-cache` e `hellnet-lib-telemetry`.

## De-para (.NET → Go)

| Hellnet.Kafka (.NET) | hellnet-lib-kafka (Go) |
|---|---|
| `IMessage.MessageType` | `Message.MessageType() string` |
| `IMessageBus.PublishAsync` | `Bus.Publish(msg)` / `PublishContext(ctx, msg)` |
| `IMessageHandler<T>.HandleAsync` | `Handler[T].Handle(ctx, msg, Ctx)` |
| `IMessageContext` | `Ctx{Topic, Partition, Offset, Key}` |
| `MessageHandlerAttribute` | `HandlerSpec{Topic, Group, MaxRetries}` |
| `AddHellnetKafka()` (DI) | `kafka.New(opts ...Options)` / `kafka.NewConsumer(h, spec, opts...)` |
| Confluent.Kafka + Polly | `segmentio/kafka-go` + `sony/gobreaker` |
| `AvroMessageSerializer` | `kafka.AvroSerializer` (wire format Confluent) |
| `ProtobufMessageSerializer` | `kafka.ProtobufSerializer` (wire format Confluent) |
| RetryEngine + DeadLetter | retry exp + topic `{topic}.dlq` com headers `dlq.*` |

## Quick start (generics como abstração)

```go
package main

import (
	"context"
	"log"

	"github.com/guilhermelinosp/hellnet-lib-kafka/kafka"
)

type orderCreated struct {
	OrderID string `json:"orderId"`
	Total   float64 `json:"total"`
}

func (orderCreated) MessageType() string { return "order.created.v1" }

type orderHandler struct{}

func (orderHandler) Handle(ctx context.Context, msg orderCreated, mctx kafka.Ctx) error {
	log.Printf("order %s (partition %d offset %d)", msg.OrderID, mctx.Partition, mctx.Offset)
	return nil
}

func main() {
	// Producer tipado pelo tipo da mensagem (generics como abstração).
	prod, err := kafka.NewProducer[orderCreated]() // lê HELLNET_KAFKA_* (.env)
	if err != nil {
		log.Fatal(err)
	}
	defer prod.Close()

	if err := prod.Publish(orderCreated{OrderID: "123", Total: 99.90}); err != nil {
		log.Fatal(err)
	}

	// Consumer tipado pelo handler (env-first; opts opcionais).
	cons, err := kafka.NewConsumer(orderHandler{}, kafka.HandlerSpec{})
	if err != nil {
		log.Fatal(err)
	}
	defer cons.Close()
	_ = cons.RunContext(context.Background())
}
```

A abstração é o **tipo da mensagem `T`**: `Producer[T]` (publish tipado), `Consumer[T]`
e `Handler[T]` — tópico/grupo/serializer resolvidos a partir de `T.MessageType` e do env.

## Env vars

| Env | Default | Descrição |
|---|---|---|
| `HELLNET_KAFKA_BROKERS` | `kafka.hellnet.com.br:9094` | Lista de brokers (vírgula) |
| `HELLNET_KAFKA_SECURITY_PROTOCOL` | `sasl_ssl` | plaintext, ssl, sasl_plaintext, sasl_ssl |
| `HELLNET_KAFKA_SASL_MECHANISM` | `SCRAM-SHA-512` | PLAIN, SCRAM-SHA-256/512 |
| `HELLNET_KAFKA_SASL_USERNAME` | `hellnet-app` | Usuário SCRAM |
| `HELLNET_KAFKA_SASL_PASSWORD` | — | Obrigatório p/ sasl_* |
| `HELLNET_KAFKA_SSL_CA_LOCATION` | — | CA certificate |
| `HELLNET_KAFKA_CONSUMER_GROUP` | `""` | Obrigatório p/ consumers |
| `HELLNET_KAFKA_TOPIC_PREFIX` | `hellnet` | Prefixo dos topics (`{prefix}.{messageType}`) |
| `HELLNET_KAFKA_DEFAULT_SERIALIZER` | `json` | json, avro, protobuf |
| `HELLNET_KAFKA_SCHEMA_REGISTRY_URL` | — | Obrigatório p/ avro (Apicurio) |
| `HELLNET_KAFKA_IDEMPOTENT` | `true` | Producer idempotente |
| `HELLNET_KAFKA_MAX_RETRIES` | `3` | Total de attempts (handler) |
| `HELLNET_KAFKA_RETRY_DELAY_MS` | `200` | Backoff base (exponencial + jitter) |
| `HELLNET_KAFKA_TIMEOUT_PRODUCE_MS` | `30000` | Timeout de produce |
| `HELLNET_KAFKA_CIRCUIT_BREAKER_COUNT` | `5` | Falhas antes de abrir o circuit breaker |

## Avro + hellnet-lib-schema

Com `HELLNET_KAFKA_DEFAULT_SERIALIZER=avro` e `HELLNET_KAFKA_SCHEMA_REGISTRY_URL`,
a serialização usa o schema registrado no Apicurio (hellnet-lib-schema) e o wire
format Confluent (`0x00` + schema id + payload):

```go
type orderItem struct {
	ProductID string `avro:"productId"`
	Quantity  int32  `avro:"quantity"`
}

type orderCreated struct {
	OrderID    string      `avro:"orderId"`
	CustomerID string      `avro:"customerId"`
	Amount     float64     `avro:"amount"`
	Currency   string      `avro:"currency"`
	Items      []orderItem `avro:"items"`
}

func (orderCreated) MessageType() string { return "order.created.v1" }
```

Registre o schema (subject `{topic}-value`) com o `register.sh` do
[hellnet-lib-schema](https://github.com/guilhermelinosp/hellnet-lib-schema):
`./scripts/register.sh --registry http://localhost:8085 --schema schemas/avro/hellnet-order-created/v1`

Para protobuf, registre `schemas/protobuf/hellnet-stock-updated/v1` (subject `{topic}-value`) e use `HELLNET_KAFKA_DEFAULT_SERIALIZER=protobuf` com uma struct `proto.Message` (ex.: `example/proto/hellnet/events/v1/stock.pb.go`).

## Teste local (Kafka Strimzi + Apicurio)

```bash
kubectl -n kafka port-forward svc/kafka-kafka-external-bootstrap 9094:9094 &
kubectl -n schema port-forward svc/apicurio-registry 8085:8080 &

HELLNET_KAFKA_BROKERS=127.0.0.1:9094 \
HELLNET_KAFKA_SECURITY_PROTOCOL=plaintext \
HELLNET_KAFKA_DEFAULT_SERIALIZER=avro \
HELLNET_KAFKA_SCHEMA_REGISTRY_URL=http://127.0.0.1:8085 \
HELLNET_KAFKA_CONSUMER_GROUP=hellnet.demo.orders \
go run ./example
```

## Licença

Apache-2.0