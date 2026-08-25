package kafka

import (
	"context"
	"fmt"
	"math/rand"
	"time"

	"github.com/segmentio/kafka-go"
)

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
	jitter := time.Duration(rand.Int63n(int64(d/5)*2+1)) - d/5
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

// publishDLQ sends a payload to the dead-letter topic with the standard Hellnet
// headers describing the original failure.
func (b *Bus) publishDLQ(ctx context.Context, opts Options, originalTopic string, partition int, offset int64, reason string, payload []byte) error {
	topic := opts.DeadLetterTopic
	if topic == "" {
		topic = dlqTopic(originalTopic)
	}
	km := kafka.Message{
		Topic: topic,
		Value: payload,
		Headers: []kafka.Header{
			{Key: "dlq.reason", Value: []byte(reason)},
			{Key: "dlq.original.topic", Value: []byte(originalTopic)},
			{Key: "dlq.original.partition", Value: []byte(fmt.Sprintf("%d", partition))},
			{Key: "dlq.original.offset", Value: []byte(fmt.Sprintf("%d", offset))},
		},
	}
	_, err := b.breaker.Execute(func() (any, error) {
		wctx, cancel := context.WithTimeout(ctx, opts.TimeoutProduce)
		defer cancel()
		if err := b.writer.WriteMessages(wctx, km); err != nil {
			return nil, err
		}
		return nil, nil
	})
	if err != nil {
		return fmt.Errorf("kafka: dlq %s: %w", topic, err)
	}
	return nil
}