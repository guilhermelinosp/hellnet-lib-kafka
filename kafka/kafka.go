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
// breaker on produce, and graceful degradation. Entry points: New, NewProducer
// and NewConsumer are env-first with optional explicit Options.
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
	// TopicPrefix prefixes every topic (HELLNET_KAFKA_TOPIC_PREFIX, default "hellnet").
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

// Default returns the Hellnet defaults for Options.
func Default() Options {
	return Options{
		Brokers:             []string{"kafka.hellnet.com.br:9094"},
		ConsumerGroup:       "",
		TopicPrefix:         "hellnet",
		SecurityProtocol:    "sasl_ssl",
		SASLMechanism:       "SCRAM-SHA-512",
		SASLUsername:        "hellnet-app",
		Idempotent:          true,
		MaxRetries:          3,
		RetryDelay:          200 * time.Millisecond,
		TimeoutProduce:      30 * time.Second,
		CircuitBreakerCount: 5,
		DeadLetterTopic:     "",
		DefaultSerializer:   "json",
		SchemaRegistryPath:  "/apis/ccompat/v6",
	}
}

// fromEnv overlays environment variables on top of the provided defaults,
// following the same precedence as hellnet-lib-cache.
func (o *Options) fromEnv(base Options) {
	o.Brokers = splitBrokers(environments.GetString("HELLNET_KAFKA_", "", "BROKERS", joinBrokers(base.Brokers)))
	o.ConsumerGroup = environments.GetString("HELLNET_KAFKA_", "", "CONSUMER_GROUP", base.ConsumerGroup)
	o.TopicPrefix = environments.GetString("HELLNET_KAFKA_", "", "TOPIC_PREFIX", base.TopicPrefix)
	o.SecurityProtocol = environments.GetString("HELLNET_KAFKA_", "", "SECURITY_PROTOCOL", base.SecurityProtocol)
	o.SASLMechanism = environments.GetString("HELLNET_KAFKA_", "", "SASL_MECHANISM", base.SASLMechanism)
	o.SASLUsername = environments.GetString("HELLNET_KAFKA_", "", "SASL_USERNAME", base.SASLUsername)
	o.SASLPassword = environments.GetString("HELLNET_KAFKA_", "", "SASL_PASSWORD", base.SASLPassword)
	o.SSLCA = environments.GetString("HELLNET_KAFKA_", "", "SSL_CA_LOCATION", base.SSLCA)
	o.SSLInsecureSkipVerify = environments.GetBool("HELLNET_KAFKA_", "", "SSL_INSECURE_SKIP_VERIFY", base.SSLInsecureSkipVerify)
	o.Idempotent = environments.GetBool("HELLNET_KAFKA_", "", "IDEMPOTENT", base.Idempotent)
	o.MaxRetries = environments.GetInt("HELLNET_KAFKA_", "", "MAX_RETRIES", base.MaxRetries)
	o.RetryDelay = environments.GetDuration("HELLNET_KAFKA_", "", "RETRY_DELAY_MS", base.RetryDelay)
	o.TimeoutProduce = environments.GetDuration("HELLNET_KAFKA_", "", "TIMEOUT_PRODUCE_MS", base.TimeoutProduce)
	o.CircuitBreakerCount = environments.GetInt("HELLNET_KAFKA_", "", "CIRCUIT_BREAKER_COUNT", base.CircuitBreakerCount)
	o.DeadLetterTopic = environments.GetString("HELLNET_KAFKA_", "", "DEAD_LETTER_TOPIC", base.DeadLetterTopic)
	o.DefaultSerializer = environments.GetString("HELLNET_KAFKA_", "", "DEFAULT_SERIALIZER", base.DefaultSerializer)
	o.SchemaRegistryURL = environments.GetString("HELLNET_KAFKA_", "", "SCHEMA_REGISTRY_URL", base.SchemaRegistryURL)
	o.SchemaRegistryPath = environments.GetString("HELLNET_KAFKA_", "", "SCHEMA_REGISTRY_PATH", base.SchemaRegistryPath)
	if o.SchemaRegistryPath == "none" || o.SchemaRegistryPath == "/" {
		o.SchemaRegistryPath = "" // raiz: Redpanda/Confluent
	}
	// Serializer is intentionally left nil here: New()/NewConsumer build it
	// from DefaultSerializer (json|avro).
}

// LoadFromEnv loads HELLNET_KAFKA_* env (plus .env via environments) into
// Options with Hellnet defaults — mirroring hellnet-lib-cache/telemetry.
func LoadFromEnv() Options {
	o := Options{}
	o.fromEnv(Default())
	return o
}

// loadEnvFiles loads .env files through hellnet-lib-environments (embedded
// .env + ./.env from disk), so callers only need New().
func loadEnvFiles() {
	_ = environments.LoadDotEnv("HELLNET_KAFKA_ENV_FILE", "HELLNET_ENV_FILE")
}

// validate checks required and supported option values.
func (o *Options) validate() error {
	if len(o.Brokers) == 0 {
		return fmt.Errorf("kafka: HELLNET_KAFKA_BROKERS is empty")
	}
	if o.MaxRetries < 1 {
		return fmt.Errorf("kafka: HELLNET_KAFKA_MAX_RETRIES must be >= 1")
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
// "avro"). Avro requires a Schema Registry URL (hellnet-lib-schema).
func (o *Options) buildSerializer() (Serializer, error) {
	switch o.DefaultSerializer {
	case "", "json":
		return JSONSerializer{}, nil
	case "avro":
		if o.SchemaRegistryURL == "" {
			return nil, fmt.Errorf("kafka: HELLNET_KAFKA_SCHEMA_REGISTRY_URL required for avro serializer")
		}
		return NewAvroSerializer(o.SchemaRegistryURL, o.SchemaRegistryPath)
	case "protobuf":
		if o.SchemaRegistryURL == "" {
			return nil, fmt.Errorf("kafka: HELLNET_KAFKA_SCHEMA_REGISTRY_URL required for protobuf serializer")
		}
		return NewProtobufSerializer(o.SchemaRegistryURL, o.SchemaRegistryPath)
	default:
		return nil, fmt.Errorf("kafka: unsupported HELLNET_KAFKA_DEFAULT_SERIALIZER %q", o.DefaultSerializer)
	}
}

// ensureSerializer returns the configured serializer, building it from
// DefaultSerializer when nil.
func (o *Options) ensureSerializer() (Serializer, error) {
	if o.Serializer != nil {
		return o.Serializer, nil
	}
	return o.buildSerializer()
}

// New loads env (.env + HELLNET_KAFKA_*) and builds a ready-to-use Bus.
// Explicit opts override the environment when provided (useful in tests and
// embedded contexts), mirroring hellnet-lib-cache/telemetry. A consumer group
// is only required if a consumer will be started.
func New(opts ...Options) (*Bus, error) {
	loadEnvFiles()
	o := LoadFromEnv()
	if len(opts) > 0 {
		o = opts[0]
	}
	if err := o.validate(); err != nil {
		return nil, err
	}
	s, err := o.buildSerializer()
	if err != nil {
		return nil, err
	}
	o.Serializer = s
	return newBus(o)
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

var _ = context.Background
