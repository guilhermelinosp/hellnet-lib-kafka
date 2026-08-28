# hellnet-lib-kafka

> Biblioteca opinionada de integração Kafka para microsserviços Go — port fiel
> do .NET [Hellnet.Kafka](https://github.com/guilhermelinosp/hellnet-dep-kafka),
> no mesmo espírito de `hellnet-lib-cache` e `hellnet-lib-telemetry`.
> A abstração é o **tipo da mensagem `T`** (generics): `Producer[T]`, `Consumer[T]`,
> `Handler[T]`.

## 🧒 Entenda com 15 anos

### A analogia

Imagine a **pa** (quadro de avisos) da escola: isso é o **Kafka**
(Redpanda é a mesma coisa, só muda a marca). Tem um recado pra dar pra outra turma?
Clipa o papel na pa e o monitor da outra sala pega quando pode. O quadro de avisos
em si é o que chamam de **broker**.

As peças da escola:

- **Tópico** = uma fileira da pa com etiqueta (ex.: `"pedidos"`).
- **Producer** = quem clipa o recado na fileira.
- **Consumer** = o monitor que retira o recado e age conforme ele.
- **Handler** = o manual que diz ao monitor o que fazer com cada tipo de recado.
- **DLQ** (*Dead Letter Queue*) = a gaveta "não entendi / deu erro", pra ninguém
  perder um recado problemático.
- **Consumer group** = vários monitores dividindo a mesma fileira — cada recado
  é lido por UM deles.

### O problema que resolve

- Apps conversam **sem saber um do outro** — não precisa correr até a outra sala:
  clipou na pa, fim de papo.
- Se o app da outra ponta estiver **dormindo**, o recado **espera** na fileira até
  alguém retirar.
- A lib cuida do trabalho chato: **reenviar** quando dá erro, **etiquetar** certinho
  e **transformar** mensagens (JSON/Avro/protobuf).

### Mini-dicionário

| Termo | Analogia |
|---|---|
| Tópico | Fileira da pa com etiqueta (`"pedidos"`, `"estoque"`…) |
| Producer | Quem clipa o recado |
| Consumer | O monitor que retira e age conforme o recado |
| Handler | O manual: o que fazer com cada tipo de recado |
| Consumer group | Vários monitores dividindo a mesma fileira; cada recado lido por UM |
| Broker | O quadro de avisos em si (servidor Kafka ou Redpanda) |
| At-least-once | Pode chegar repetido, mas **nunca se perde** — como enviar a foto duas vezes pro grupo pra garantir |
| Schema | Molde/recorte que garante que todo recado tem os campos certos |

### Primeiras linhas

```go
// interruptor geral da app: criado UMA vez, quem administra depois é a própria lib
ctx := context.Background()

prod, _ := kafka.NewProducer[MeuEvento](ctx) // clipa recados de MeuEvento; config vem das envs HELLNET_KAFKA_*
cons, _ := kafka.NewConsumer[MeuEvento](ctx, handler, spec) // monitor lê da fileira do spec e usa o manual (handler)
```

As próximas seções mostram o detalhe técnico completo de cada peça.

## De-para (.NET → Go)

| Hellnet.Kafka (.NET) | hellnet-lib-kafka (Go) |
|---|---|
| `IMessage.MessageType` | `Message.MessageType() string` |
| `IMessageBus.PublishAsync` | `Producer[T].Publish` |
| `IMessageHandler<T>.HandleAsync` | `Handler[T].Handle(ctx, msg, Ctx)` |
| `IMessageContext` | `Ctx{Topic, Partition, Offset, Key}` |
| `MessageHandlerAttribute` | `HandlerSpec{Topic, Group, MaxRetries}` |
| `AddHellnetKafka()` (DI) | `kafka.NewProducer[T](ctx)` / `kafka.NewConsumer[T](ctx, h, spec, opts...)` |
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
- **`ctx` uma vez na construção** — `New(ctx)`/`NewProducer(ctx)`/`NewConsumer(ctx, ...)` capturam o contexto e propagam internamente; apps **nunca** passam ctx às operações (`Publish`, `Run`, `Close`)
- **Apicurio e Redpanda** — SR com caminho configurável (`/apis/ccompat/v6` ou raiz)

## Quick start (generics como abstração)

> 🧒 Em termos de escola: vamos clipar nosso primeiro recado na pa e contratar o primeiro monitor.

```go
package main

import (
	"context"
	"log"
	"os"
	"os/signal"

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
	// Contexto passado UMA vez, na construção (Ctrl-C encerra cooperativamente).
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
	defer stop()

	// Producer tipado pelo tipo da mensagem (env-first; lê HELLNET_KAFKA_* via .env).
	prod, err := kafka.NewProducer[orderCreated](ctx)
	if err != nil {
		log.Fatal(err)
	}
	defer prod.Close()
	if err := prod.Publish(orderCreated{OrderID: "123", Total: 99.90}); err != nil {
		log.Fatal(err)
	}

	// Consumer tipado pelo handler (env-first; opts opcionais).
	cons, err := kafka.NewConsumer(ctx, orderHandler{}, kafka.HandlerSpec{})
	if err != nil {
		log.Fatal(err)
	}
	defer cons.Close()
	_ = cons.Run() // bloqueia até Close()/cancelamento do ctx capturado
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
prod, _ := kafka.NewProducer[orderCreated](ctx) // env-first; ctx capturado uma vez
prod.Publish(msg)                 // usa o ctx capturado (timeout por attempt internamente)
prod.Close()
```

### Consumer[T]

```go
cons, _ := kafka.NewConsumer[T](ctx, handler, HandlerSpec{}, opts ...Options) // env-first; ctx capturado uma vez
cons.Run()                         // bloqueia até Close()/cancelamento do ctx capturado
cons.Close()                       // cancela o run ctx interno e libera reader/bus
```

### Handler[T] e HandlerSpec

```go
type Handler[T Message] interface {
	Handle(ctx context.Context, msg T, mctx Ctx) error
}
type HandlerFunc[T Message] func(ctx context.Context, msg T, mctx Ctx) error

// O ctx de Handle é fornecido pela biblioteca: derivado do contexto passado
// em NewConsumer(...) + cancelamento do run loop. Usado para shutdown
// cooperativo — a aplicação nunca o passa manualmente.

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
bus, _ := kafka.New(ctx, opts ...Options) // env-first; ctx capturado uma vez
bus.Publish(msg)                          // msg: Message
bus.Close()
```

## Serialização

> 🧒 Serializer é o **molde de recados**: todo recado precisa sair com o formato certo (JSON, Avro ou Protobuf).

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

> 🧒 Quando um recado dá problema, tentamos de novo algumas vezes antes de arquivá-lo na **gaveta dos erros** (DLQ).

- **Produce**: `Timeout (HELLNET_KAFKA_TIMEOUT_PRODUCE_MS)` → circuit breaker
  (`HELLNET_KAFKA_CIRCUIT_BREAKER_COUNT` falhas → OPEN → half-open → CLOSED).
- **Consumer**: handler com retry exponencial (`MaxRetries` + `HELLNET_KAFKA_RETRY_DELAY_MS`).
- **DLQ**: após esgotar, a mensagem vai para `{topic}.dlq` com headers:
  - `dlq.reason` · `dlq.original.topic` · `dlq.original.partition` · `dlq.original.offset`

## Semântica de entrega (at-least-once)

> 🧒 At-least-once = pode chegar repetido, mas nunca se perde — como mandar a foto duas vezes no grupo pra garantir.

- **At-least-once**: o offset é commitado depois do sucesso (ou DLQ) da
  mensagem, e o grupo usa `CommitInterval` de 1s. Uma mensagem pode ser
  reprocessada se o consumidor cair entre o processamento e o commit —
  handlers devem ser idempotentes.
- **Rebalance**: joins/leaves do consumer group, troca de leader e falhas de
  rede **não derrubam o loop de consumo**. Cada erro transitório de fetch é
  logado via `log/slog` (WARN com contagem de falhas consecutivas) e o retry
  usa backoff crescente com teto (base 200ms dobrando até 5s, ±20% jitter);
  o primeiro fetch com sucesso reseta o contador.

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
| `HELLNET_KAFKA_RETRY_DELAY_MS` | `200` | Backoff base (exponencial + jitter), inteiro em ms |
| `HELLNET_KAFKA_TIMEOUT_PRODUCE_MS` | `30000` | Timeout de produce, inteiro em ms |
| `HELLNET_KAFKA_CIRCUIT_BREAKER_COUNT` | `5` | Falhas antes de abrir o circuit breaker |

`.env` local (Redpanda kind) em `.env.example` — copie para `.env` (gitignored).

> Os knobs sufixados `_MS` são lidos como **inteiros em milissegundos**
> (`GetInt` × `time.Millisecond`) — o mesmo formato do hellnet-lib-cache.

## Testes

### Unitários
```bash
go test ./kafka/ -count=1
```

### Integração (build tag `integration`)
Suite em `kafka/integration_test.go` contra um broker real (Redpanda),
compilada só com `-tags integration` e gateada por env — sem a variável,
os testes pulam. Para rodar local apontando para o Redpanda do cluster
(namespace `tools`, via port-forward):

```bash
kubectl port-forward -n tools svc/redpanda 19092:9092 &

HELLNET_TEST_KAFKA_BROKERS=localhost:19092 \
go test -tags integration -count=1 -run TestIntegration ./kafka/
```

Cobertura do suite:
`TestIntegrationPublishConsume` (publish/consume end-to-end com ctx capturado
uma vez), `TestIntegrationHandlerRetryThenDLQ` (retry esgota → DLQ) e
`TestIntegrationCloseCancelsRun` (`Close()` cancela `Run()` cooperativamente).
O mesmo suite roda no CI (`.github/workflows/integration.yml`) contra um
service container Redpanda (`localhost:9092`).

## Gotchas / lições

- `New`/`NewProducer`/`NewConsumer` chamam `loadEnvFiles()` — sem isso o `.env` é
  ignorado e o default `sasl_ssl` falha com "SASL_PASSWORD required".
- `StartOffset: FirstOffset` — grupos novos fazem replay do início (testes determinísticos).
- Protobuf: use `T` ponteiro + mensagem embutida por valor (evita copylocks do `go vet`).
- SR **efêmero** (Redpanda sem PVC): schemas zeram no restart do pod — registre com
  `~/cluster/redpanda-register-schemas.sh` (ou adicione PVC).

## Licença

Apache-2.0
<!-- release pipeline verification -->
