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
// It features env-first configuration (HELLNET_KAFKA_* via .env through
// hellnet-lib-environments), three serializers (JSON, Avro and Protobuf with
// Schema Registry and the Confluent wire format), timeout/retry/circuit
// breaker on produce, and graceful degradation. All public constructors are
// zero-config and env-first.
package kafka

import (
	"context"
	"fmt"
	"time"

	"github.com/guilhermelinosp/hellnet-lib-environments/environments"
)

// Options configures the Kafka bus. All values are env-first overridable.
type Options struct {
	// Brokers is the list of bootstrap servers.
	Brokers []string
	// ConsumerGroup is required for consumers (HELLNET_KAFKA_CONSUMER_GROUP).
	ConsumerGroup string
	// TopicPrefix optionally prefixes every topic (HELLNET_KAFKA_TOPIC_PREFIX).
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

// validate checks required and supported option values.
func (o *Options) validate() error {
	if len(o.Brokers) == 0 {
		return fmt.Errorf("kafka: HELLNET_KAFKA_BROKERS is empty")
	}
	if o.MaxRetries < 1 {
		return fmt.Errorf("kafka: HELLNET_KAFKA_MAX_RETRIES must be >= 1")
	}
	if o.CircuitBreakerCount < 1 {
		return fmt.Errorf("kafka: HELLNET_KAFKA_CIRCUIT_BREAKER_COUNT must be >= 1")
	}
	switch o.SecurityProtocol {
	case "plaintext", "ssl", "sasl_plaintext", "sasl_ssl":
	default:
		return fmt.Errorf("kafka: unsupported HELLNET_KAFKA_SECURITY_PROTOCOL %q", o.SecurityProtocol)
	}
	if (o.SecurityProtocol == "sasl_plaintext" || o.SecurityProtocol == "sasl_ssl") && o.SASLPassword == "" {
		return fmt.Errorf("kafka: HELLNET_KAFKA_SASL_PASSWORD is required for %s", o.SecurityProtocol)
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
			return nil, fmt.Errorf("kafka: HELLNET_KAFKA_SCHEMA_REGISTRY_URL required for avro serializer")
		}
		return &AvroSerializer{registry: newRegistryClient(baseCtx, o.SchemaRegistryURL, o.SchemaRegistryPath)}, nil
	case "protobuf":
		if o.SchemaRegistryURL == "" {
			return nil, fmt.Errorf("kafka: HELLNET_KAFKA_SCHEMA_REGISTRY_URL required for protobuf serializer")
		}
		return &ProtobufSerializer{registry: newRegistryClient(baseCtx, o.SchemaRegistryURL, o.SchemaRegistryPath)}, nil
	default:
		return nil, fmt.Errorf("kafka: unsupported HELLNET_KAFKA_DEFAULT_SERIALIZER %q", o.DefaultSerializer)
	}
}

// New follows the hellnet-lib-telemetry constructor pattern: it creates the
// base context, loads .env before reading configuration, and builds Options
// entirely from HELLNET_KAFKA_* variables and defaults. A consumer group is
// only required if a consumer will be started.
func New() (*Bus, error) {
	ctx := context.Background()

	// Env-first: load .env before reading HELLNET_KAFKA_* variables. Best
	// effort: without a file (or with a parse error), process env still applies.
	_ = environments.LoadDotEnv()

	o := Options{
		Brokers:               splitBrokers(environments.GetString("HELLNET_KAFKA_", "HELLNET_", "BROKERS", "")),
		ConsumerGroup:         environments.GetString("HELLNET_KAFKA_", "HELLNET_", "CONSUMER_GROUP", ""),
		TopicPrefix:           environments.GetString("HELLNET_KAFKA_", "HELLNET_", "TOPIC_PREFIX", ""),
		SecurityProtocol:      environments.GetString("HELLNET_KAFKA_", "HELLNET_", "SECURITY_PROTOCOL", "sasl_ssl"),
		SASLMechanism:         environments.GetString("HELLNET_KAFKA_", "HELLNET_", "SASL_MECHANISM", "SCRAM-SHA-512"),
		SASLUsername:          environments.GetString("HELLNET_KAFKA_", "HELLNET_", "SASL_USERNAME", "hellnet-app"),
		SASLPassword:          environments.GetString("HELLNET_KAFKA_", "HELLNET_", "SASL_PASSWORD", ""),
		SSLCA:                 environments.GetString("HELLNET_KAFKA_", "HELLNET_", "SSL_CA_LOCATION", ""),
		SSLInsecureSkipVerify: environments.GetBool("HELLNET_KAFKA_", "HELLNET_", "SSL_INSECURE_SKIP_VERIFY", false),
		Idempotent:            environments.GetBool("HELLNET_KAFKA_", "HELLNET_", "IDEMPOTENT", true),
		MaxRetries:            environments.GetInt("HELLNET_KAFKA_", "HELLNET_", "MAX_RETRIES", 3),
		RetryDelay:            time.Duration(environments.GetInt("HELLNET_KAFKA_", "HELLNET_", "RETRY_DELAY_MS", 200)) * time.Millisecond,
		TimeoutProduce:        time.Duration(environments.GetInt("HELLNET_KAFKA_", "HELLNET_", "TIMEOUT_PRODUCE_MS", 30000)) * time.Millisecond,
		CircuitBreakerCount:   environments.GetInt("HELLNET_KAFKA_", "HELLNET_", "CIRCUIT_BREAKER_COUNT", 5),
		DeadLetterTopic:       environments.GetString("HELLNET_KAFKA_", "HELLNET_", "DEAD_LETTER_TOPIC", ""),
		DefaultSerializer:     environments.GetString("HELLNET_KAFKA_", "HELLNET_", "DEFAULT_SERIALIZER", "json"),
		SchemaRegistryURL:     environments.GetString("HELLNET_KAFKA_", "HELLNET_", "SCHEMA_REGISTRY_URL", ""),
		SchemaRegistryPath:    environments.GetString("HELLNET_KAFKA_", "HELLNET_", "SCHEMA_REGISTRY_PATH", "/apis/ccompat/v6"),
	}
	if o.SchemaRegistryPath == "none" || o.SchemaRegistryPath == "/" {
		o.SchemaRegistryPath = ""
	}
	return newBusWithOptions(ctx, o)
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
func MustNew() *Bus {
	b, err := New()
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
