package kafka

import (
	"context"
	"sort"

	"github.com/twmb/franz-go/pkg/kgo"
)

type headersKey struct{}

// reservedHeaders are owned by trace propagation and cannot be set by callers.
var reservedHeaders = map[string]struct{}{"traceparent": {}, "tracestate": {}, "baggage": {}}

// ContextWithHeaders returns ctx carrying Kafka record headers that
// PublishContext adds to the messages it produces, typically correlation data
// such as event_id or order_id. Headers already in ctx are kept; on a repeated
// key the new value wins. Trace-context headers (traceparent, tracestate,
// baggage) are managed by the library and ignored here.
func ContextWithHeaders(ctx context.Context, headers map[string]string) context.Context {
	merged := make(map[string]string, len(headers))
	for k, v := range headersFromContext(ctx) {
		merged[k] = v
	}
	for k, v := range headers {
		merged[k] = v
	}
	return context.WithValue(ctx, headersKey{}, merged)
}

func headersFromContext(ctx context.Context) map[string]string {
	headers, _ := ctx.Value(headersKey{}).(map[string]string)
	return headers
}

// injectHeaders appends the headers carried by ctx to record, in key order.
func injectHeaders(ctx context.Context, record *kgo.Record) {
	headers := headersFromContext(ctx)
	keys := make([]string, 0, len(headers))
	for k := range headers {
		if _, reserved := reservedHeaders[k]; !reserved {
			keys = append(keys, k)
		}
	}
	sort.Strings(keys)
	for _, k := range keys {
		record.Headers = append(record.Headers, kgo.RecordHeader{Key: k, Value: []byte(headers[k])})
	}
}

// recordHeaders returns the record headers as a map (the last value wins).
func recordHeaders(record kgo.Record) map[string]string {
	if len(record.Headers) == 0 {
		return nil
	}
	headers := make(map[string]string, len(record.Headers))
	for _, h := range record.Headers {
		headers[h.Key] = string(h.Value)
	}
	return headers
}
