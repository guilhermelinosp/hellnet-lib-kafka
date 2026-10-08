# Changelog

## Unreleased

- Added `Options.SchemaSubjectStripPrefix` (`KAFKA_SCHEMA_SUBJECT_STRIP_PREFIX`): the Avro serializer removes this
  prefix from the topic to get the Schema Registry subject, so topic `br.com.hellnet.fast.order.accepted.v1` uses
  subject `fast.order.accepted.v1`. Empty keeps the previous behavior (subject = topic).

- A `nil` `context.Context` is no longer tolerated: `RunContext(nil)` no longer
  falls back to a background loop, `PublishContext(nil)` no longer returns an
  error and `Shutdown(nil)` no longer skips the cancelation check. Callers must
  pass a real context, as the Go convention requires.

- Added `ContextWithHeaders(ctx, headers)`: `PublishContext` sends the headers
  carried by the context as Kafka record headers (correlation data such as
  `event_id` or `order_id`); trace-context headers stay library-owned.
- Added `Ctx.Headers`: handlers receive the record headers.

- `New` resolves its instrumentation with `instrument.Resolve`: a nil value or a nil pointer
  (for example a nil `*telemetry.Telemetry`) disables telemetry instead of panicking.

- `New`, `MustNew`, `NewProducer` and `NewConsumer` now take an
  `instrument.Instrumentation` (for example a `*telemetry.Telemetry`, or nil)
  instead of the legacy `telemetry.Client` and are no longer deprecated.
  `NewProducerWithOptions` and `NewConsumerWithOptions` were removed in favor of
  them; `NewWithOptions`, `Producer.PublishContext` and `Producer.Shutdown` stay.
- Deprecated `Producer.Publish` and `Producer.Close` in favor of the
  context-first API.
- Corrige o teste de integração retry/DLQ para publicar no mesmo tópico isolado
  usado pelo consumer.
- Define `MaxRetries` como o número de retries após a tentativa inicial; por
  exemplo, `MaxRetries=2` executa até três tentativas do handler.
- Sincroniza `go.mod` com o resultado de `go mod tidy` usado pelo CI.
- Added `WithInstrumentation` and instrumentation scope/provider initialization using telemetry v1.9.1.
- Deprecated the telemetry-client constructor path in favor of the instrument contract.
- Added context-parented `send {topic}` and `process {topic}` spans with lower-case W3C Kafka propagation.
- Added messaging sent/consumed counters and operation/process duration histograms in seconds.
- Migrated consumer fetch and DLQ retry logs to the instrumentation contract logger.
- Added `PublishBatchContext`, preserving per-message caller-parented producer operations.
- Added `Shutdown(ctx)`; `Close()` remains compatible and is deprecated.
- Producer and consumer spans now record returned errors and set OTel error status.
- Schema Registry HTTP calls now use `otelhttp` with the contract tracer provider.
