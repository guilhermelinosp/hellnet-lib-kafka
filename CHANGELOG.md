# Changelog

## Unreleased

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
