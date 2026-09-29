package kafka

import (
	"context"
	"fmt"
	"math/rand"
	"time"

	"github.com/segmentio/kafka-go"
)

type messageWriter interface {
	WriteMessages(context.Context, ...kafka.Message) error
}

// backoff returns the delay for attempt i (0-based): base * 2^i plus jitter.
func backoff(base time.Duration, attempt int) time.Duration {
	d := base
	for i := 0; i < attempt; i++ {
		d *= 2
	}
	if d <= 0 {
		d = 10 * time.Millisecond
	}
	// ±20% jitter to avoid thundering herds.
	jitter := time.Duration(rand.Int63n(int64(d/5)*2+1)) - d/5 // #nosec G404 -- scheduling jitter is non-cryptographic.
	return d + jitter
}

// Consume-loop fetch retry bounds: base 200ms doubling per consecutive failure,
// capped at 5s, both with ±20% jitter (see fetchBackoff).
const (
	fetchBackoffBase = 200 * time.Millisecond
	fetchBackoffMax  = 5 * time.Second
)

// fetchBackoff returns the delay before re-attempting FetchMessage after the
// attempt-th (0-based) consecutive transient failure: base 200ms doubling up
// to a 5s cap, with ±20% jitter.
func fetchBackoff(attempt int) time.Duration {
	d := fetchBackoffBase
	for i := 0; i < attempt && d < fetchBackoffMax; i++ {
		d *= 2
	}
	if d > fetchBackoffMax {
		d = fetchBackoffMax
	}
	// ±20% jitter to avoid thundering herds.
	jitter := time.Duration(rand.Int63n(int64(d/5)*2+1)) - d/5 // #nosec G404 -- scheduling jitter is non-cryptographic.
	return d + jitter
}

// sleepCtx sleeps for d or until ctx is done.
func sleepCtx(ctx context.Context, d time.Duration) error {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-t.C:
		return nil
	}
}

// dlqTopic returns the dead-letter topic for a source topic.
func dlqTopic(source string) string {
	return source + ".dlq"
}

// publishDLQ sends a message to the dead-letter topic with the standard Hellnet
// headers describing the original failure. It intentionally bypasses the
// normal produce circuit breaker: DLQ delivery must remain possible when the
// normal producer breaker is open.
func (b *Bus) publishDLQ(ctx context.Context, opts Options, original kafka.Message, reason, errorType string, attempts int) error {
	topic := opts.DeadLetterTopic
	if topic == "" {
		topic = dlqTopic(original.Topic)
	}
	if attempts < 1 {
		attempts = 1
	}
	km := kafka.Message{
		Topic: topic,
		Key:   append([]byte(nil), original.Key...),
		Value: append([]byte(nil), original.Value...),
		Headers: append(append([]kafka.Header(nil), original.Headers...),
			kafka.Header{Key: "dlq.attempts", Value: []byte(fmt.Sprintf("%d", attempts))},
			kafka.Header{Key: "dlq.timestamp", Value: []byte(time.Now().UTC().Format(time.RFC3339))},
			kafka.Header{Key: "dlq.error.type", Value: []byte(errorType)},
			kafka.Header{Key: "dlq.reason", Value: []byte(reason)},
			kafka.Header{Key: "dlq.original.topic", Value: []byte(original.Topic)},
			kafka.Header{Key: "dlq.original.partition", Value: []byte(fmt.Sprintf("%d", original.Partition))},
			kafka.Header{Key: "dlq.original.offset", Value: []byte(fmt.Sprintf("%d", original.Offset))},
		),
	}
	writer := b.dlqWriter
	if writer == nil {
		writer = b.writer
	}
	wctx, cancel := context.WithTimeout(ctx, opts.TimeoutProduce)
	defer cancel()
	err := writer.WriteMessages(wctx, km)
	if err != nil {
		return fmt.Errorf("kafka: dlq %s: %w", topic, err)
	}
	return nil
}
