package kafka

import (
	"context"

	"github.com/guilhermelinosp/hellnet-lib-telemetry/messaging"
	"github.com/twmb/franz-go/pkg/kgo"
	"go.opentelemetry.io/otel/propagation"
)

// injectTrace carries the W3C trace context through Kafka headers. Kafka is
// an asynchronous boundary, so the consumer cannot inherit Go's context
// directly.
func injectTrace(ctx context.Context, message *kgo.Record) {
	injectTraceWith(ctx, message, propagation.TraceContext{})
}

func injectTraceWith(ctx context.Context, message *kgo.Record, propagator propagation.TextMapPropagator) {
	carrier := messaging.NewCarrier(nil)
	propagator.Inject(ctx, carrier)
	for _, header := range *carrier {
		message.Headers = append(message.Headers, kgo.RecordHeader{Key: header.Key, Value: header.Value})
	}
}

// extractTrace restores the producer's W3C trace context before the consumer
// handler creates its spans.
func extractTrace(ctx context.Context, message kgo.Record) context.Context {
	return extractTraceWith(ctx, message, propagation.TraceContext{})
}

func extractTraceWith(ctx context.Context, message kgo.Record, propagator propagation.TextMapPropagator) context.Context {
	headers := make([]messaging.Header, 0, len(message.Headers))
	for _, header := range message.Headers {
		headers = append(headers, messaging.Header{Key: header.Key, Value: header.Value})
	}
	return propagator.Extract(ctx, messaging.NewCarrier(headers))
}
