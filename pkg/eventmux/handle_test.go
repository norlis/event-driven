package eventmux

import (
	"bytes"
	"context"
	"encoding/json/jsontext"
	"errors"
	"log/slog"
	"testing"

	"github.com/norlis/event-driven/v2/pkg/event"
)

type order struct {
	ID string `json:"id"`
}

// newHandleHarness is a one-route mux without preflight, started, with a
// capturing dead-letter publisher.
func newHandleHarness[T any](t *testing.T, fn func(context.Context, T) (jsontext.Value, error), mws ...Middleware) (*Mux, *fakeSubscription, *capturePublisher, *bytes.Buffer) {
	t.Helper()
	sub, dlq, logs := newFakeSubscription(), &capturePublisher{}, &bytes.Buffer{}
	mux, err := New(Config{Name: "handle", Subscription: sub, DeadLetter: dlq, Logger: slog.New(slog.NewJSONHandler(logs, nil))})
	if err != nil {
		t.Fatal(err)
	}
	mux.Use(mws...)
	mux.Handle(nil, nil, fn)
	if err := mux.Start(t.Context(), nil); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = mux.Stop(context.Background()) })
	return mux, sub, dlq, logs
}

func TestHandleDecodesValueAndPointer(t *testing.T) {
	t.Parallel()

	t.Run("value", func(t *testing.T) {
		t.Parallel()
		var got order
		_, sub, _, _ := newHandleHarness(t, func(_ context.Context, o order) (jsontext.Value, error) { got = o; return nil, nil })
		msg := newIncoming(t, `{"id":"o-1"}`)
		sub.handler(msg)
		if got.ID != "o-1" {
			t.Errorf("handler got %+v", got)
		}
		if outcome, _ := msg.Outcome(); outcome != event.OutcomeAcked {
			t.Errorf("outcome = %v, want acked", outcome)
		}
	})
	t.Run("pointer", func(t *testing.T) {
		t.Parallel()
		var got *order
		_, sub, _, _ := newHandleHarness(t, func(_ context.Context, o *order) (jsontext.Value, error) { got = o; return nil, nil })
		sub.handler(newIncoming(t, `{"id":"o-2"}`))
		if got == nil || got.ID != "o-2" {
			t.Errorf("handler got %+v", got)
		}
	})
}

func TestHandleInvalidPayloadIsDecodeError(t *testing.T) {
	t.Parallel()

	_, sub, dlq, _ := newHandleHarness(t, func(context.Context, order) (jsontext.Value, error) { t.Fatal("handler must not run"); return nil, nil })
	msg := newIncoming(t, `{"id":"a","id":"b"}`)
	sub.handler(msg)
	outcome, cause := msg.Outcome()
	if outcome != event.OutcomeDiscarded || !errors.Is(cause, event.ErrDecode) {
		t.Fatalf("outcome = %v, %v; want discarded ErrDecode", outcome, cause)
	}
	if len(dlq.got) != 1 || dlq.got[0].Extensions()[event.ExtDeadLetterReason] != string(event.ReasonDecode) {
		t.Errorf("dead-letter = %+v, want one copy with reason decode", dlq.got)
	}
	if want := "unmarshal into eventmux.order"; !bytes.Contains([]byte(cause.Error()), []byte(want)) {
		t.Errorf("cause %q does not name the type (%s)", cause, want)
	}
}

// A middleware that hands next a different payload is a programming error:
// the route dead-letters instead of nacking forever.
func TestHandleRejectsSwappedPayload(t *testing.T) {
	t.Parallel()

	swap := func(next HandlerFunc) HandlerFunc {
		return func(ctx context.Context, _ any) (jsontext.Value, error) { return next(ctx, "not an order") }
	}
	_, sub, dlq, _ := newHandleHarness(t, func(context.Context, order) (jsontext.Value, error) { return nil, nil }, swap)
	msg := newIncoming(t, `{"id":"o-1"}`)
	sub.handler(msg)
	outcome, cause := msg.Outcome()
	if outcome != event.OutcomeDiscarded {
		t.Fatalf("outcome = %v, want discarded", outcome)
	}
	if _, ok := errors.AsType[*event.NonRetryableError](cause); !ok {
		t.Fatalf("cause %T is not NonRetryableError: %v", cause, cause)
	}
	if want := "eventmux: route expects eventmux.order, got string"; cause.Error() != want {
		t.Errorf("cause = %q, want %q", cause.Error(), want)
	}
	if len(dlq.got) != 1 {
		t.Errorf("dead-letter copies = %d, want 1", len(dlq.got))
	}
}
