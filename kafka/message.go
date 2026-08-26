package kafka

import (
	"context"
	"strings"
)

// Message is the contract for messages flowing through the bus. MessageType
// identifies the event (e.g. "order.created.v1") and drives topic naming.
type Message interface {
	MessageType() string
}

// Ctx carries consumption context to a handler (the Go counterpart of the .NET
// IMessageContext).
type Ctx struct {
	Topic     string
	Partition int
	Offset    int64
	Key       []byte
}

// Handler processes a message of type T. Implementations must be safe for
// concurrent use (the consumer may invoke Handle from one goroutine at a time).
type Handler[T Message] interface {
	Handle(ctx context.Context, msg T, mctx Ctx) error
}

// HandlerFunc adapts a plain function to Handler.
type HandlerFunc[T Message] func(ctx context.Context, msg T, mctx Ctx) error

// Handle implements Handler.
func (f HandlerFunc[T]) Handle(ctx context.Context, msg T, mctx Ctx) error {
	return f(ctx, msg, mctx)
}

func splitBrokers(s string) []string {
	if strings.TrimSpace(s) == "" {
		return nil
	}
	var out []string
	for _, b := range strings.Split(s, ",") {
		if b = strings.TrimSpace(b); b != "" {
			out = append(out, b)
		}
	}
	return out
}

func joinBrokers(bs []string) string {
	return strings.Join(bs, ",")
}
