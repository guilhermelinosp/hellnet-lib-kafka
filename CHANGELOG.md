# Changelog

## Unreleased

- Corrige o teste de integração retry/DLQ para publicar no mesmo tópico isolado
  usado pelo consumer.
- Define `MaxRetries` como o número de retries após a tentativa inicial; por
  exemplo, `MaxRetries=2` executa até três tentativas do handler.
- Sincroniza `go.mod` com o resultado de `go mod tidy` usado pelo CI.
