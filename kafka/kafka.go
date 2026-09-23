// Package kafka provides an opinionated Kafka integration library for Hellnet
// Go services, ported from the .NET Hellnet.Kafka library.
//
// The abstraction is the message type T (generics):
//
//   - Producer[T] publishes messages of type T ("{prefix}.{messageType}").
//   - Consumer[T] runs a Handler[T] with retry and a Dead Letter Queue.
//   - Handler[T]/HandlerFunc[T] process consumed messages.
//   - Bus is the low-level shared producer for multiple message types.
//
// It features env-first configuration (KAFKA_* via .env through
// hellnet-lib-environments), three serializers (JSON, Avro and Protobuf with
// Schema Registry and the Confluent wire format), timeout/retry/circuit
// breaker on produce, and graceful degradation. All public constructors are
// zero-config and env-first.
package kafka

import (
	"context"
	"fmt"
	"math"
	"strconv"
	"time"

	"github.com/guilhermelinosp/hellnet-lib-environments/environments"
	"github.com/guilhermelinosp/hellnet-lib-telemetry/telemetry"
)

// Options configures the Kafka bus. All values are env-first overridable.
type Options struct {
	// Brokers is the list of bootstrap servers.
	Brokers []string
	// ConsumerGroup is required for consumers (KAFKA_CONSUMER_GROUP).
	ConsumerGroup string
	// TopicPrefix optionally prefixes every topic (KAFKA_TOPIC_PREFIX).
	TopicPrefix string
	// SecurityProtocol: plaintext, ssl, sasl_plaintext, sasl_ssl.
	SecurityProtocol string
	// SASLMechanism: PLAIN, SCRAM-SHA-256, SCRAM-SHA-512.
	SASLMechanism string
	SASLUsername  string
	SASLPassword  string
	// SSLCA is the path to the CA certificate (optional).
	SSLCA string
	// SSLInsecureSkipVerify disables hostname/cert verification.
	SSLInsecureSkipVerify bool
	// Idempotent enables the idempotent producer.
	Idempotent bool
	// MaxRetries is the total handler attempts (default 3).
	MaxRetries int
	// RetryDelay is the base exponential backoff (default 200ms).
	RetryDelay time.Duration
	// TimeoutProduce bounds a single produce attempt (default 30s).
	TimeoutProduce time.Duration
	// CircuitBreakerCount is the number of failures before the breaker opens
	// for produce (default 5).
	CircuitBreakerCount int
	// Serializer defaults to JSON.
	Serializer Serializer
	// DefaultSerializer selects the serializer: "json" (default) or "avro".
	DefaultSerializer string
	// SchemaRegistryURL is required for avro/protobuf (hellnet-lib-schema registry).
	SchemaRegistryURL string
	// SchemaRegistryPath is the ccompat API base path: "/apis/ccompat/v6" for
	// Apicurio, "" (root) for Redpanda/Confluent.
	SchemaRegistryPath string
	// DeadLetterTopic overrides the default "{topic}.dlq".
	DeadLetterTopic string
}

func Default() Options {
	return Options{SecurityProtocol: "sasl_ssl", SASLMechanism: "SCRAM-SHA-512",
		SASLUsername: "hellnet-app", Idempotent: true, MaxRetries: 3,
		RetryDelay: 200 * time.Millisecond, TimeoutProduce: 30 * time.Second,
		CircuitBreakerCount: 5, DefaultSerializer: "json",
		SchemaRegistryPath: "/apis/ccompat/v6"}
}

func (o *Options) from(base Options) {
	o.Brokers = splitBrokers(environments.GetString("KAFKA_BROKERS"))
	o.ConsumerGroup = environments.GetString("KAFKA_CONSUMER_GROUP", base.ConsumerGroup)
	o.TopicPrefix = environments.GetString("KAFKA_TOPIC_PREFIX", base.TopicPrefix)
	o.SecurityProtocol = environments.GetString("KAFKA_SECURITY_PROTOCOL", base.SecurityProtocol)
	o.SASLMechanism = environments.GetString("KAFKA_SASL_MECHANISM", base.SASLMechanism)
	o.SASLUsername = environments.GetString("KAFKA_SASL_USERNAME", base.SASLUsername)
	o.SASLPassword = environments.GetString("KAFKA_SASL_PASSWORD", base.SASLPassword)
	o.SSLCA = environments.GetString("KAFKA_SSL_CA_LOCATION", base.SSLCA)
	o.SSLInsecureSkipVerify = environments.GetBool("KAFKA_SSL_INSECURE_SKIP_VERIFY", strconv.FormatBool(base.SSLInsecureSkipVerify))
	o.Idempotent = environments.GetBool("KAFKA_IDEMPOTENT", strconv.FormatBool(base.Idempotent))
	o.MaxRetries = environments.GetInt("KAFKA_MAX_RETRIES", strconv.Itoa(base.MaxRetries))
	o.RetryDelay = environments.GetDuration("KAFKA_RETRY_DELAY", base.RetryDelay.String())
	o.TimeoutProduce = environments.GetDuration("KAFKA_TIMEOUT_PRODUCE", base.TimeoutProduce.String())
	o.CircuitBreakerCount = environments.GetInt("KAFKA_CIRCUIT_BREAKER_COUNT", strconv.Itoa(base.CircuitBreakerCount))
	o.DeadLetterTopic = environments.GetString("KAFKA_DEAD_LETTER_TOPIC", base.DeadLetterTopic)
	o.DefaultSerializer = environments.GetString("KAFKA_DEFAULT_SERIALIZER", base.DefaultSerializer)
	o.SchemaRegistryURL = environments.GetString("KAFKA_SCHEMA_REGISTRY_URL", base.SchemaRegistryURL)
	o.SchemaRegistryPath = environments.GetString("KAFKA_SCHEMA_REGISTRY_PATH", base.SchemaRegistryPath)
}

// validate checks required and supported option values.
func (o *Options) validate() error {
	if len(o.Brokers) == 0 {
		return fmt.Errorf("kafka: KAFKA_BROKERS is empty")
	}
	if o.MaxRetries < 1 {
		return fmt.Errorf("kafka: KAFKA_MAX_RETRIES must be >= 1")
	}
	if o.CircuitBreakerCount < 1 {
		return fmt.Errorf("kafka: KAFKA_CIRCUIT_BREAKER_COUNT must be >= 1")
	}
	if uint64(o.CircuitBreakerCount) > uint64(math.MaxUint32) {
		return fmt.Errorf("kafka: KAFKA_CIRCUIT_BREAKER_COUNT must be <= %d", uint64(math.MaxUint32))
	}
	switch o.SecurityProtocol {
	case "plaintext", "ssl", "sasl_plaintext", "sasl_ssl":
	default:
		return fmt.Errorf("kafka: unsupported KAFKA_SECURITY_PROTOCOL %q", o.SecurityProtocol)
	}
	if (o.SecurityProtocol == "sasl_plaintext" || o.SecurityProtocol == "sasl_ssl") && o.SASLPassword == "" {
		return fmt.Errorf("kafka: KAFKA_SASL_PASSWORD is required for %s", o.SecurityProtocol)
	}
	return nil
}

// buildSerializer selects the serializer per DefaultSerializer ("json" or
// "avro"). Avro requires a Schema Registry URL (hellnet-lib-schema). The ctx
// becomes the registry client's base context: schema fetches derive their
// timeout budget from it, so cancelling the ctx captured at construction also
// aborts in-flight registry lookups.
func (o *Options) buildSerializer(baseCtx context.Context) (Serializer, error) {
	switch o.DefaultSerializer {
	case "", "json":
		return JSONSerializer{}, nil
	case "avro":
		if o.SchemaRegistryURL == "" {
			return nil, fmt.Errorf("kafka: KAFKA_SCHEMA_REGISTRY_URL required for avro serializer")
		}
		return &AvroSerializer{registry: newRegistryClient(baseCtx, o.SchemaRegistryURL, o.SchemaRegistryPath)}, nil
	case "protobuf":
		if o.SchemaRegistryURL == "" {
			return nil, fmt.Errorf("kafka: KAFKA_SCHEMA_REGISTRY_URL required for protobuf serializer")
		}
		return &ProtobufSerializer{registry: newRegistryClient(baseCtx, o.SchemaRegistryURL, o.SchemaRegistryPath)}, nil
	default:
		return nil, fmt.Errorf("kafka: unsupported KAFKA_DEFAULT_SERIALIZER %q", o.DefaultSerializer)
	}
}

// New follows the hellnet-lib-telemetry constructor pattern: it creates the
// base context, loads .env before reading configuration, and builds Options
// entirely from KAFKA_* variables and defaults. A consumer group is
// only required if a consumer will be started.
func New(ctx context.Context, ops telemetry.Client) (*Bus, error) {
	// Env-first: load .env before reading KAFKA_* variables. Best
	// effort: without a file (or with a parse error), process env still applies.
	_ = environments.LoadDotEnv()

	o := Default()
	o.from(o)
	if o.SchemaRegistryPath == "none" || o.SchemaRegistryPath == "/" {
		o.SchemaRegistryPath = ""
	}
	b, err := newBusWithOptions(ctx, o)
	if err != nil {
		return nil, err
	}
	b.ops = ops
	return b, nil
}

func newWithOptions(ctx context.Context, opts Options) (*Bus, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	return newBusWithOptions(ctx, opts)
}

func newBusWithOptions(ctx context.Context, o Options) (*Bus, error) {
	if err := o.validate(); err != nil {
		return nil, err
	}
	s, err := o.buildSerializer(ctx)
	if err != nil {
		return nil, err
	}
	o.Serializer = s
	return newBus(ctx, o)
}

// MustNew is like New but panics if construction fails.
func MustNew(ctx context.Context, ops telemetry.Client) *Bus {
	b, err := New(ctx, ops)
	if err != nil {
		panic(err)
	}
	return b
}

// TopicName resolves the topic for a message type: "{prefix}.{messageType}".
func TopicName(opts Options, messageType string) string {
	if opts.TopicPrefix == "" {
		return messageType
	}
	return opts.TopicPrefix + "." + messageType
}

// HandlerSpec declares per-handler overrides (the Go counterpart of the .NET
// MessageHandlerAttribute).
type HandlerSpec struct {
	// Topic overrides the derived "{prefix}.{messageType}" topic.
	Topic string
	// Group overrides the consumer group for this handler.
	Group string
	// MaxRetries overrides the global handler retry count.
	MaxRetries int
}

// resolveTopic returns the handler's topic, falling back to the derived name.
func (s HandlerSpec) resolveTopic(o Options, messageType string) string {
	if s.Topic != "" {
		return s.Topic
	}
	return TopicName(o, messageType)
}
