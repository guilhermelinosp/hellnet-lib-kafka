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
// godotenv), three serializers (JSON, Avro and Protobuf with
// Schema Registry and the Confluent wire format), timeout/retry/circuit
// breaker on produce, and graceful degradation. All public constructors are
// zero-config and env-first.
package kafka

import (
	"context"
	"fmt"
	"math"
	"time"

	"github.com/guilhermelinosp/hellnet-lib-kafka/internal/env"

	"github.com/guilhermelinosp/hellnet-lib-telemetry/instrument"
)

// kafkaEnv reads a KAFKA_<name> env var, defaulting to def.
func kafkaEnv(name, def string) string {
	return env.String("KAFKA_"+name, def)
}

// kafkaInt reads an int KAFKA_<name> env var.
func kafkaInt(name string, def int) int {
	return env.Int("KAFKA_"+name, def)
}

// kafkaBool reads a bool KAFKA_<name> env var.
func kafkaBool(name string, def bool) bool {
	return env.Bool("KAFKA_"+name, def)
}

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
	// SchemaSubjectStripPrefix is removed from the topic name to get the Avro
	// subject (empty: the subject is the topic name).
	SchemaSubjectStripPrefix string
	// DeadLetterTopic overrides the default "{topic}.dlq".
	DeadLetterTopic string
	instrumentation instrument.Instrumentation
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

// buildSerializer selects the serializer per DefaultSerializer ("json", "avro"
// or "protobuf"). Avro and Protobuf require a Schema Registry URL
// (hellnet-lib-schema). Active operation contexts are passed at serialization
// time through ContextSerializer rather than retained by the client.
func (o *Options) buildSerializer() (Serializer, error) {
	switch o.DefaultSerializer {
	case "", "json":
		return JSONSerializer{}, nil
	case "avro":
		if o.SchemaRegistryURL == "" {
			return nil, fmt.Errorf("kafka: KAFKA_SCHEMA_REGISTRY_URL required for avro serializer")
		}
		return &AvroSerializer{registry: newRegistryClientWithInstrumentation(o.SchemaRegistryURL, o.SchemaRegistryPath, o.instrumentation), subjectStripPrefix: o.SchemaSubjectStripPrefix}, nil
	case "protobuf":
		if o.SchemaRegistryURL == "" {
			return nil, fmt.Errorf("kafka: KAFKA_SCHEMA_REGISTRY_URL required for protobuf serializer")
		}
		return &ProtobufSerializer{registry: newRegistryClientWithInstrumentation(o.SchemaRegistryURL, o.SchemaRegistryPath, o.instrumentation)}, nil
	default:
		return nil, fmt.Errorf("kafka: unsupported KAFKA_DEFAULT_SERIALIZER %q", o.DefaultSerializer)
	}
}

// Option configures a Kafka constructor.
type Option func(*constructorOptions)

type constructorOptions struct {
	inst instrument.Instrumentation
}

// WithInstrumentation supplies the Hellnet observability contract.
func WithInstrumentation(inst instrument.Instrumentation) Option {
	return func(o *constructorOptions) { o.inst = inst }
}

// New creates a bus from KAFKA_* environment variables. inst is the Hellnet
// observability contract (for example a *telemetry.Telemetry, or nil to emit no
// telemetry; a nil pointer such as a nil *telemetry.Telemetry is treated the
// same way); options may refine it.
func New(ctx context.Context, inst instrument.Instrumentation, options ...Option) (*Bus, error) {
	return NewWithOptions(ctx, append([]Option{WithInstrumentation(inst)}, options...)...)
}

// NewWithOptions creates a bus from KAFKA_* environment variables. Supply the
// Hellnet observability contract with WithInstrumentation; without it the bus
// emits no telemetry.
func NewWithOptions(ctx context.Context, options ...Option) (*Bus, error) {
	// Env-first: load .env before reading KAFKA_* variables. Best
	// effort: without a file (or with a parse error), process env still applies.
	_ = env.Environment()

	o := Options{
		Brokers:                  splitBrokers(kafkaEnv("BROKERS", "")),
		ConsumerGroup:            kafkaEnv("CONSUMER_GROUP", ""),
		TopicPrefix:              kafkaEnv("TOPIC_PREFIX", ""),
		SecurityProtocol:         kafkaEnv("SECURITY_PROTOCOL", "sasl_ssl"),
		SASLMechanism:            kafkaEnv("SASL_MECHANISM", "SCRAM-SHA-512"),
		SASLUsername:             kafkaEnv("SASL_USERNAME", "hellnet-app"),
		SASLPassword:             kafkaEnv("SASL_PASSWORD", ""),
		SSLCA:                    kafkaEnv("SSL_CA_LOCATION", ""),
		SSLInsecureSkipVerify:    kafkaBool("SSL_INSECURE_SKIP_VERIFY", false),
		Idempotent:               kafkaBool("IDEMPOTENT", true),
		MaxRetries:               kafkaInt("MAX_RETRIES", 3),
		RetryDelay:               time.Duration(kafkaInt("RETRY_DELAY_MS", 200)) * time.Millisecond,
		TimeoutProduce:           time.Duration(kafkaInt("TIMEOUT_PRODUCE_MS", 30000)) * time.Millisecond,
		CircuitBreakerCount:      kafkaInt("CIRCUIT_BREAKER_COUNT", 5),
		DeadLetterTopic:          kafkaEnv("DEAD_LETTER_TOPIC", ""),
		DefaultSerializer:        kafkaEnv("DEFAULT_SERIALIZER", "json"),
		SchemaRegistryURL:        kafkaEnv("SCHEMA_REGISTRY_URL", ""),
		SchemaRegistryPath:       kafkaEnv("SCHEMA_REGISTRY_PATH", "/apis/ccompat/v6"),
		SchemaSubjectStripPrefix: kafkaEnv("SCHEMA_SUBJECT_STRIP_PREFIX", ""),
	}
	if o.SchemaRegistryPath == "none" || o.SchemaRegistryPath == "/" {
		o.SchemaRegistryPath = ""
	}
	var config constructorOptions
	for _, option := range options {
		if option != nil {
			option(&config)
		}
	}
	config.inst = instrument.Resolve(config.inst)
	o.instrumentation = config.inst
	b, err := newBusWithOptions(ctx, o)
	if err != nil {
		return nil, err
	}
	b.obs = newObservability(ctx, config.inst)
	return b, nil
}

func newBusWithOptions(ctx context.Context, o Options) (*Bus, error) {
	if err := o.validate(); err != nil {
		return nil, err
	}
	s, err := o.buildSerializer()
	if err != nil {
		return nil, err
	}
	o.Serializer = s
	return newBus(ctx, o)
}

// MustNew is like New but panics if construction fails.
func MustNew(ctx context.Context, inst instrument.Instrumentation, options ...Option) *Bus {
	b, err := New(ctx, inst, options...)
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
