package kafka

import (
	"context"

	"github.com/segmentio/kafka-go"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/propagation"
)

// injectTrace carries the W3C trace context through Kafka headers. Kafka is
// an asynchronous boundary, so the consumer cannot inherit Go's context
// directly.
func injectTrace(ctx context.Context, message *kafka.Message) {
	carrier := propagation.HeaderCarrier{}
	otel.GetTextMapPropagator().Inject(ctx, carrier)
	for key, values := range carrier {
		for _, value := range values {
			message.Headers = append(message.Headers, kafka.Header{Key: key, Value: []byte(value)})
		}
	}
}

// extractTrace restores the producer's W3C trace context before the consumer
// handler creates its spans.
func extractTrace(ctx context.Context, message kafka.Message) context.Context {
	carrier := propagation.HeaderCarrier{}
	for _, header := range message.Headers {
		carrier[header.Key] = append(carrier[header.Key], string(header.Value))
	}
	return otel.GetTextMapPropagator().Extract(ctx, carrier)
}
