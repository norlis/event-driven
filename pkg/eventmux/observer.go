package eventmux

import (
	"context"
	"fmt"
	"log/slog"
	"runtime/debug"
	"time"

	cloudevents "github.com/cloudevents/sdk-go/v2/event"

	"github.com/norlis/event-driven/v2/pkg/event"
	"github.com/norlis/event-driven/v2/pkg/kit/logfields"
)

// Observer receives the mux's per-message decisions and publish results, so a
// consumer can emit metrics or spans without the mux depending on a telemetry
// library. Methods run synchronously on the broker goroutine and must be fast.
type Observer interface {
	// OnMessage runs exactly once per message, after the mux decides its
	// outcome. reason is non-empty only when outcome is OutcomeDiscarded for a
	// classified cause; "no route matched" reports OutcomeDiscarded with an
	// empty reason. The outcome is the mux's decision even when the broker
	// refuses it (that failure has its own log record).
	OnMessage(ctx context.Context, msg *event.Message, outcome logfields.Outcome, reason event.DeadLetterReason, elapsed time.Duration)
	// OnPublish runs once per Publish issued by the mux, for result events and
	// dead-letter copies alike. err is the publisher's error, nil on success.
	// A result event carries the causationid extension; a dead-letter copy
	// carries deadletterreason. With Config.PublishAfterAck a result that
	// fails to publish is lost after OnMessage already reported OutcomeAcked:
	// OnPublish with err on an event with causationid is the only signal.
	OnPublish(ctx context.Context, ce cloudevents.Event, err error)
}

// noopObserver stands in for a nil Config.Observer so call sites never nil-check.
type noopObserver struct{}

func (noopObserver) OnMessage(context.Context, *event.Message, logfields.Outcome, event.DeadLetterReason, time.Duration) {
}

func (noopObserver) OnPublish(context.Context, cloudevents.Event, error) {}

// observedPublisher reports every Publish to the Observer. Start wraps the
// route and dead-letter publishers with it, so the mux has one publish path.
type observedPublisher struct {
	pub Publisher
	mux *Mux
}

func (p observedPublisher) Publish(ctx context.Context, ce cloudevents.Event) error {
	err := p.pub.Publish(ctx, ce)
	p.mux.observe(ctx, func() { p.mux.cfg.Observer.OnPublish(ctx, ce, err) })
	return err //nolint:wrapcheck // transparent decorator: the mux classifies the raw error
}

// observe runs an Observer callback and contains its panics: a telemetry bug
// must never change the decision about a message.
func (mux *Mux) observe(ctx context.Context, fn func()) {
	defer func() {
		if r := recover(); r != nil {
			mux.cfg.Logger.LogAttrs(ctx, slog.LevelError, "observer panicked",
				errorAttr(fmt.Errorf("%w: %v", event.ErrPanic, r), debug.Stack()))
		}
	}()
	fn()
}
