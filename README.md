# hellnet-lib-kafka

> Biblioteca opinionada de integração Kafka para microsserviços Go — port fiel
> do .NET [Hellnet.Kafka](https://github.com/guilhermelinosp/hellnet-dep-kafka),
> no mesmo espírito de `hellnet-lib-cache` e `hellnet-lib-telemetry`.
> A abstração é o **tipo da mensagem `T`** (generics): `Producer[T]`, `Consumer[T]`,
> `Handler[T]`.

## De-para (.NET → Go)

| Hellnet.Kafka (.NET) | hellnet-lib-kafka (Go) |
|---|---|
| `IMessage.MessageType` | `Message.MessageType() string` |
| `IMessageBus.PublishAsync` | `Producer[T].Publish` / `PublishContext` |
| `IMessageHandler<T>.HandleAsync` | `Handler[T].Handle(ctx, msg, Ctx)` |
| `IMessageContext` | `Ctx{Topic, Partition, Offset, Key}` |
| `MessageHandlerAttribute` | `HandlerSpec{Topic, Group, MaxRetries}` |
| `AddHellnetKafka()` (DI) | `kafka.NewProducer[T]()` / `kafka.NewConsumer[T](h, spec, opts...)` |
| Confluent.Kafka + Polly | `segmentio/kafka-go` + `sony/gobreaker` |
| `AvroMessageSerializer` | `kafka.AvroSerializer` (wire format Confluent) |
| `ProtobufMessageSerializer` | `kafka.ProtobufSerializer` (wire format Confluent) |
| RetryEngine + Dead Letter | retry exp + topic `{topic}.dlq` com headers `dlq.*` |

## Features

- **Tipado por generics** — producer/consumer/handler presos ao tipo da mensagem
- **Env-first** — toda config via `HELLNET_KAFKA_*` (.env via `hellnet-lib-environments`)
- **3 serializers** — JSON, Avro e Protobuf (Schema Registry, wire format Confluent)
- **Resiliência** — timeout → retry exponencial → circuit breaker (produce)
- **Dead Letter Queue** — handler falhou após `MaxRetries` → `{topic}.dlq` com headers `dlq.*`
- **`ctx` default background** — métodos simples (`Publish`, `Run`) + variantes `*Context`
- **Apicurio e Redpanda** — SR com caminho configurável (`/apis/ccompat/v6` ou raiz)

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
	// Producer tipado pelo tipo da mensagem (env-first; lê HELLNET_KAFKA_* via .env).
	prod, err := kafka.NewProducer[orderCreated]()
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

## API

### Mensagem

```go
type Message interface {
	MessageType() string // ex.: "order.created.v1" — gera o topic "{prefix}.{messageType}"
}
```
`MessageType` deve estar no **value receiver** para o consumer derivar o topic do tipo.
Use `HandlerSpec.Topic` para sobrescrever.

### Producer[T]

```go
prod, _ := kafka.NewProducer[orderCreated](opts ...Options) // env-first; opts opcionais
prod.Publish(msg)                 // usa context.Background() internamente
prod.PublishContext(ctx, msg)     // com cancelamento/deadline
prod.Close()
```

### Consumer[T]

```go
cons, _ := kafka.NewConsumer[T](handler, HandlerSpec{}, opts ...Options) // env-first
cons.Run()                         // background; bloqueia até Close/erro
cons.RunContext(ctx)               // com cancelamento/deadline
cons.Close()
```

### Handler[T] e HandlerSpec

```go
type Handler[T Message] interface {
	Handle(ctx context.Context, msg T, mctx Ctx) error
}
type HandlerFunc[T Message] func(ctx context.Context, msg T, mctx Ctx) error

type HandlerSpec struct {
	Topic      string // sobrescreve "{prefix}.{messageType}"
	Group      string // sobrescreve HELLNET_KAFKA_CONSUMER_GROUP
	MaxRetries int    // sobrescreve HELLNET_KAFKA_MAX_RETRIES
}
```

### Ctx (contexto de consumo)

```go
type Ctx struct {
	Topic     string
	Partition int
	Offset    int64
	Key       []byte
}
```

### Bus (produtor compartilhado, nível baixo)

Para publicar **vários tipos** de mensagem pelo mesmo connection:
```go
bus, _ := kafka.New(opts ...Options) // env-first
bus.Publish(msg)                     // msg: Message
bus.PublishContext(ctx, msg)
bus.Close()
```

## Serialização

`HELLNET_KAFKA_DEFAULT_SERIALIZER` seleciona o formato. Para avro/protobuf, o
`HELLNET_KAFKA_SCHEMA_REGISTRY_URL` é obrigatório e o schema deve estar registrado
no subject `{topic}-value`.

### JSON (default)
```bash
HELLNET_KAFKA_DEFAULT_SERIALIZER=json
```
`encoding/json` do struct — sem Schema Registry, sem envelope.

### Avro
```go
type orderCreated struct {
	OrderID    string      `avro:"orderId"`
	CustomerID string      `avro:"customerId"`
	Amount     float64     `avro:"amount"`
	Currency   string      `avro:"currency"`
	Items      []orderItem `avro:"items"`
}
```
Wire format Confluent: `[0x00][schema id: 4B BE][avro payload]`. O schema é
resolvido do SR pelo subject `{topic}-value` (registrado via `hellnet-lib-schema`).

### Protobuf
```go
type stockMessage struct {
	hellnetv1.StockUpdated // proto.Message embutido (tipo por valor)
}
func (*stockMessage) MessageType() string { return "stock.updated.v1" }
```
O valor/out devem implementar `proto.Message`; use `T = *stockMessage` (ponteiro).
Wire format Confluent idêntico ao Avro.

## Resiliência e DLQ

- **Produce**: `Timeout (HELLNET_KAFKA_TIMEOUT_PRODUCE_MS)` → circuit breaker
  (`HELLNET_KAFKA_CIRCUIT_BREAKER_COUNT` falhas → OPEN → half-open → CLOSED).
- **Consumer**: handler com retry exponencial (`MaxRetries` + `HELLNET_KAFKA_RETRY_DELAY_MS`).
- **DLQ**: após esgotar, a mensagem vai para `{topic}.dlq` com headers:
  - `dlq.reason` · `dlq.original.topic` · `dlq.original.partition` · `dlq.original.offset`

## Schema Registry

| Registry | Caminho ccompat | `HELLNET_KAFKA_SCHEMA_REGISTRY_PATH` |
|---|---|---|
| Apicurio | `/apis/ccompat/v6/subjects/...` | `(default)` |
| Redpanda / Confluent | `/subjects/...` (raiz) | `none` |

O subject segue a convenção Confluente `{topic}-value`
(ex.: `hellnet.order.created.v1-value`).

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
| `HELLNET_KAFKA_SCHEMA_REGISTRY_URL` | — | Obrigatório p/ avro/protobuf |
| `HELLNET_KAFKA_SCHEMA_REGISTRY_PATH` | `/apis/ccompat/v6` | `none` = raiz (Redpanda/Confluent) |
| `HELLNET_KAFKA_IDEMPOTENT` | `true` | Producer idempotente |
| `HELLNET_KAFKA_MAX_RETRIES` | `3` | Total de attempts (handler) |
| `HELLNET_KAFKA_RETRY_DELAY_MS` | `200` | Backoff base (exponencial + jitter) |
| `HELLNET_KAFKA_TIMEOUT_PRODUCE_MS` | `30000` | Timeout de produce |
| `HELLNET_KAFKA_CIRCUIT_BREAKER_COUNT` | `5` | Falhas antes de abrir o circuit breaker |

`.env` local (Redpanda kind) em `.env.example` — copie para `.env` (gitignored).

## Testes

### Unitários
```bash
go test ./...
```

### Integração (Kafka + Schema Registry)
Gateados por env — sem as vars, os testes pulam:
```bash
HELLNET_TEST_SR=http://127.0.0.1:8081 \
HELLNET_TEST_KAFKA=1 \
HELLNET_KAFKA_BROKERS=127.0.0.1:9092 \
HELLNET_KAFKA_SECURITY_PROTOCOL=plaintext \
HELLNET_KAFKA_DEFAULT_SERIALIZER=avro \
HELLNET_KAFKA_SCHEMA_REGISTRY_URL=http://127.0.0.1:8081 \
HELLNET_KAFKA_SCHEMA_REGISTRY_PATH=none \
go test ./example -run "RoundTrip|E2E|RetryDLQ" -v
```

Cobertura: **RoundTrip/E2E/DLQ × JSON/Avro/Protobuf** (matriz completa).

## Gotchas / lições

- `New`/`NewProducer`/`NewConsumer` chamam `loadEnvFiles()` — sem isso o `.env` é
  ignorado e o default `sasl_ssl` falha com "SASL_PASSWORD required".
- `StartOffset: FirstOffset` — grupos novos fazem replay do início (testes determinísticos).
- Protobuf: use `T` ponteiro + mensagem embutida por valor (evita copylocks do `go vet`).
- SR **efêmero** (Redpanda sem PVC): schemas zeram no restart do pod — registre com
  `~/cluster/redpanda-register-schemas.sh` (ou adicione PVC).

## Licença

Apache-2.0