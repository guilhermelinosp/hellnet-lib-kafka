package kafka

import (
	"context"
	"runtime/debug"
	"time"

	"github.com/guilhermelinosp/hellnet-lib-telemetry/instrument"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
	metricnoop "go.opentelemetry.io/otel/metric/noop"
	"go.opentelemetry.io/otel/trace"
)

const instrumentationScope = "github.com/guilhermelinosp/hellnet-lib-kafka/kafka"

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

func newObservability(inst instrument.Instrumentation) observability {
	if inst == nil {
		inst = instrument.Noop()
	}
	version := moduleVersion()
	logger := inst.Logger(instrumentationScope)
	meter := inst.MeterProvider().Meter(instrumentationScope, metric.WithInstrumentationVersion(version))
	sent, err := meter.Int64Counter("messaging.client.sent.messages")
	if err != nil {
		logger.Error(context.TODO(), "kafka metric creation failed", "metric", "messaging.client.sent.messages", "error", err)
		sent, _ = metricnoop.NewMeterProvider().Meter(instrumentationScope).Int64Counter("messaging.client.sent.messages")
	}
	consumed, err := meter.Int64Counter("messaging.client.consumed.messages")
	if err != nil {
		logger.Error(context.TODO(), "kafka metric creation failed", "metric", "messaging.client.consumed.messages", "error", err)
		consumed, _ = metricnoop.NewMeterProvider().Meter(instrumentationScope).Int64Counter("messaging.client.consumed.messages")
	}
	operationDuration, err := meter.Float64Histogram("messaging.client.operation.duration", metric.WithUnit("s"))
	if err != nil {
		logger.Error(context.TODO(), "kafka metric creation failed", "metric", "messaging.client.operation.duration", "error", err)
		operationDuration, _ = metricnoop.NewMeterProvider().Meter(instrumentationScope).Float64Histogram("messaging.client.operation.duration")
	}
	processDuration, err := meter.Float64Histogram("messaging.process.duration", metric.WithUnit("s"))
	if err != nil {
		logger.Error(context.TODO(), "kafka metric creation failed", "metric", "messaging.process.duration", "error", err)
		processDuration, _ = metricnoop.NewMeterProvider().Meter(instrumentationScope).Float64Histogram("messaging.process.duration")
	}
	return observability{
		inst:   inst,
		tracer: inst.TracerProvider().Tracer(instrumentationScope, trace.WithInstrumentationVersion(version)),
		meter:  meter, logger: logger, sent: sent, consumed: consumed,
		operationDuration: operationDuration, processDuration: processDuration,
	}
}

func (o observability) observeSend(ctx context.Context, destination, result string, started time.Time) {
	attrs := metric.WithAttributes(attribute.String("messaging.destination.name", destination), attribute.String("result", result))
	o.sent.Add(ctx, 1, attrs)
	o.operationDuration.Record(ctx, time.Since(started).Seconds(), attrs)
}

func (o observability) observeProcess(ctx context.Context, destination, result string, started time.Time) {
	attrs := metric.WithAttributes(attribute.String("messaging.destination.name", destination), attribute.String("result", result))
	o.consumed.Add(ctx, 1, attrs)
	o.processDuration.Record(ctx, time.Since(started).Seconds(), attrs)
}

func moduleVersion() string {
	info, ok := debug.ReadBuildInfo()
	if !ok {
		return "unknown"
	}
	for _, dep := range info.Deps {
		if dep.Path == "github.com/guilhermelinosp/hellnet-lib-kafka" && dep.Version != "" {
			return dep.Version
		}
	}
	return "unknown"
}
