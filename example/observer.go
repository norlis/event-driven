package example

import (
	"context"
	"log/slog"
	"sync"
	"time"

	cloudevents "github.com/cloudevents/sdk-go/v2/event"

	"github.com/norlis/httpgate/logging"

	"github.com/norlis/event-driven/v2/pkg/event"
	"github.com/norlis/event-driven/v2/pkg/eventmux"
	"github.com/norlis/event-driven/v2/pkg/kit/logfields"
)

// keyOutcomeCount is the running count of one (type, outcome) pair.
const keyOutcomeCount = "example.outcome.count"

type outcomeKey struct {
	eventType string
	outcome   logfields.Outcome
}

// outcomeCounter is a minimal eventmux.Observer: it counts outcomes per event
// type and logs each one. A real deployment would feed a metrics SDK here.
type outcomeCounter struct {
	logger *slog.Logger

	mu     sync.Mutex
	counts map[outcomeKey]uint64
}

// NewObserver returns the example Observer bound to logger.
func NewObserver(logger *slog.Logger) eventmux.Observer {
	return &outcomeCounter{logger: logfields.Component(logger, "observer"), counts: make(map[outcomeKey]uint64)}
}

func (o *outcomeCounter) OnMessage(ctx context.Context, msg *event.Message, outcome logfields.Outcome, reason event.DeadLetterReason, elapsed time.Duration) {
	key := outcomeKey{eventType: msg.Type(), outcome: outcome}
	o.mu.Lock()
	o.counts[key]++
	n := o.counts[key]
	o.mu.Unlock()

	attrs := []slog.Attr{
		slog.String(logfields.KeyCloudEventsType, msg.Type()),
		outcome.Attr(),
	}
	if reason != "" {
		attrs = append(attrs, slog.String(logfields.KeyDeadLetterReason, string(reason)))
	}
	attrs = append(attrs, slog.Uint64(keyOutcomeCount, n), slog.Int64(logging.KeyEventDuration, elapsed.Nanoseconds()))
	o.logger.LogAttrs(ctx, slog.LevelDebug, "message observed", attrs...)
}

func (o *outcomeCounter) OnPublish(ctx context.Context, ce cloudevents.Event, err error) {
	if err == nil {
		return
	}
	o.logger.LogAttrs(ctx, slog.LevelWarn, "publish observed failing",
		slog.String(logfields.KeyCloudEventsType, ce.Type()),
		slog.String(logfields.KeyMessagingMessageID, ce.ID()),
		slog.String(logging.KeyErrorMessage, err.Error()),
	)
}
