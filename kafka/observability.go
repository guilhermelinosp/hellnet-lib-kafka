package kafka

import (
	"context"
	"time"

	"github.com/guilhermelinosp/hellnet-lib-telemetry/instrument"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/trace"
)

const (
	instrumentationScope = "github.com/guilhermelinosp/hellnet-lib-kafka/kafka"
	modulePath           = "github.com/guilhermelinosp/hellnet-lib-kafka"
)

type observability struct {
	inst              instrument.Instrumentation
	tracer            trace.Tracer
	meter             metric.Meter
	logger            instrument.Logger
	sent              metric.Int64Counter
	consumed          metric.Int64Counter
	operationDuration metric.Float64Histogram
	processDuration   metric.Float64Histogram
}

func newObservability(_ context.Context, inst instrument.Instrumentation) observability {
	if inst == nil {
		inst = instrument.Noop()
	}
	s := instrument.NewScope(inst, instrumentationScope, modulePath)
	return observability{
		inst:              inst,
		tracer:            s.Tracer,
		meter:             s.Meter,
		logger:            s.Logger,
		sent:              s.Int64Counter("messaging.client.sent.messages"),
		consumed:          s.Int64Counter("messaging.client.consumed.messages"),
		operationDuration: s.Float64Histogram("messaging.client.operation.duration", metric.WithUnit("s")),
		processDuration:   s.Float64Histogram("messaging.process.duration", metric.WithUnit("s")),
	}
}

func (o observability) observeSend(ctx context.Context, destination, result string, started time.Time) {
	instrument.Observe(ctx, o.sent, o.operationDuration, started,
		attribute.String("messaging.destination.name", destination), attribute.String("result", result))
}

func (o observability) observeProcess(ctx context.Context, destination, result string, started time.Time) {
	instrument.Observe(ctx, o.consumed, o.processDuration, started,
		attribute.String("messaging.destination.name", destination), attribute.String("result", result))
}
