package eventmux

import (
	"bytes"
	"context"
	"encoding/json/jsontext"
	"errors"
	"log/slog"
	"strings"
	"testing"

	cloudevents "github.com/cloudevents/sdk-go/v2/event"

	"github.com/norlis/httpgate/logging"

	"github.com/norlis/event-driven/v2/pkg/event"
	"github.com/norlis/event-driven/v2/pkg/kit/logfields"
)

// TestStopFailureKeepsMuxStarted: when the transport cannot stop (ctx expired),
// the mux is still delivering, so it must stay started and let a later Stop
// retry instead of answering nil.
func TestStopFailureKeepsMuxStarted(t *testing.T) {
	t.Parallel()

	h := newMuxHarness(t, func(context.Context, payload) (jsontext.Value, error) { return nil, nil })
	stopErr := errors.New("drain timed out")
	h.sub.stopErr = stopErr

	if err := h.mux.Stop(t.Context()); !errors.Is(err, stopErr) {
		t.Fatalf("Stop() = %v, want %v", err, stopErr)
	}
	if err := h.mux.Start(t.Context(), nil); !errors.Is(err, event.ErrAlreadyStarted) {
		t.Fatalf("Start after failed Stop = %v, want ErrAlreadyStarted", err)
	}
	h.sub.stopErr = nil
	if err := h.mux.Stop(t.Context()); err != nil || h.sub.stops != 2 {
		t.Fatalf("second Stop: err=%v stops=%d, want nil and 2", err, h.sub.stops)
	}
	if err := h.mux.Stop(t.Context()); err != nil || h.sub.stops != 2 {
		t.Fatalf("third Stop: err=%v stops=%d, want nil and 2 (already stopped)", err, h.sub.stops)
	}
}

type panickingFilter struct{}

func (panickingFilter) Match(*event.Message) bool { panic("nil map") }

// TestPanickingFilterIsDeadLettered: a Filter is user code outside the
// handler chain; a panic there must settle the message like a handler panic
// instead of unwinding into the broker SDK's goroutine.
func TestPanickingFilterIsDeadLettered(t *testing.T) {
	t.Parallel()

	h := newMuxHarness(t, func(context.Context, payload) (jsontext.Value, error) { return nil, nil })
	// A second route with a panicking filter is consulted first on a fresh mux.
	_ = h.mux.Stop(context.Background())
	h.sub = newFakeSubscription()
	h.logs.Reset()
	h.dlq.got = nil
	mux, err := New(Config{Name: "test", Subscription: h.sub, Logger: h.mux.cfg.Logger, DeadLetter: h.dlq})
	if err != nil {
		t.Fatal(err)
	}
	mux.Handle(nil, panickingFilter{}, func(context.Context, payload) (jsontext.Value, error) { return nil, nil })
	if err := mux.Start(t.Context(), nil); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = mux.Stop(context.Background()) })

	msg := newIncoming(t, `{"name":"a"}`)
	func() {
		defer func() {
			if r := recover(); r != nil {
				t.Fatalf("panic escaped the mux: %v", r)
			}
		}()
		h.sub.handler(msg)
	}()

	outcome, cause := msg.Outcome()
	if outcome != event.OutcomeDiscarded || !errors.Is(cause, event.ErrPanic) {
		t.Fatalf("Outcome() = %v, %v; want Discarded, ErrPanic", outcome, cause)
	}
	if len(h.dlq.got) != 1 || h.dlq.got[0].Extensions()[event.ExtDeadLetterReason] != string(event.ReasonPanic) {
		t.Fatalf("dead-letter copies = %d (%v), want 1 with reason panic", len(h.dlq.got), h.dlq.got)
	}
	line := logLine(t, h.logs.String(), "message dead-lettered")
	if n := strings.Count(line, `"`+logging.KeyErrorStackTrace+`":"`); n != 1 {
		t.Errorf("error.stack_trace appears %d times, want 1:\n%s", n, line)
	}
	if !strings.Contains(line, "runtime/panic.go") || !strings.Contains(line, "robustness_test.go") {
		t.Errorf("stack trace does not point at the filter's panic site:\n%s", line)
	}
	if !strings.Contains(line, `"`+logging.KeyErrorMessage+`":"handler panicked: nil map"`) {
		t.Errorf("error.message must be the bare panic value:\n%s", line)
	}
	if n := strings.Count(h.logs.String(), `"level":"ERROR"`); n != 1 {
		t.Fatalf("ERROR records = %d, want 1:\n%s", n, h.logs.String())
	}
}

// TestPanicAfterDecisionIsOnlyLogged: a Delivery callback that panics runs
// after decide recorded the outcome; the mux logs it and does not settle twice.
func TestPanicAfterDecisionIsOnlyLogged(t *testing.T) {
	t.Parallel()

	h := newMuxHarness(t, func(context.Context, payload) (jsontext.Value, error) { return nil, nil })
	ce := newIncoming(t, `{"name":"a"}`).Event
	msg := event.NewMessage(t.Context(), ce, event.Delivery{Ack: func() error { panic("broker client closed") }})
	h.sub.handler(msg)

	if outcome, err := msg.Outcome(); outcome != event.OutcomeAcked || err != nil {
		t.Fatalf("Outcome() = %v, %v; want Acked (decided before the callback panicked)", outcome, err)
	}
	line := logLine(t, h.logs.String(), "panic after message decision")
	if !strings.Contains(line, `"`+logging.KeyErrorMessage+`":"handler panicked: broker client closed"`) {
		t.Errorf("unexpected error.message:\n%s", line)
	}
	if len(h.dlq.got) != 0 {
		t.Errorf("dead-letter copies = %d, want 0", len(h.dlq.got))
	}
}

type panickingPublisher struct{}

func (panickingPublisher) Publish(context.Context, cloudevents.Event) error {
	panic("publisher closed")
}

// TestPanicWhileSettlingPanickedMessage: the dead-letter publisher panicking
// on a message that already panicked must still not escape the mux.
func TestPanicWhileSettlingPanickedMessage(t *testing.T) {
	t.Parallel()

	sub := newFakeSubscription()
	logs := &bytes.Buffer{}
	mux, err := New(Config{Name: "test", Subscription: sub, Logger: slog.New(slog.NewJSONHandler(logs, nil)), DeadLetter: panickingPublisher{}})
	if err != nil {
		t.Fatal(err)
	}
	mux.Handle(nil, panickingFilter{}, func(context.Context, payload) (jsontext.Value, error) { return nil, nil })
	if err := mux.Start(t.Context(), nil); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = mux.Stop(context.Background()) })

	msg := newIncoming(t, `{"name":"a"}`)
	sub.handler(msg) // must not panic
	if outcome, _ := msg.Outcome(); outcome != event.OutcomePending {
		t.Fatalf("Outcome() = %v, want Pending: nothing could settle it", outcome)
	}
	logLine(t, logs.String(), "panic while settling a panicked message")
}

// TestUndecodableMessageIsDeadLetteredBeforeRouting: a transport that could
// not decode the envelope hands the router a rescue event; the router applies
// the decode policy without consulting any route.
func TestUndecodableMessageIsDeadLetteredBeforeRouting(t *testing.T) {
	t.Parallel()

	routed := false
	h := newMuxHarness(t, func(context.Context, payload) (jsontext.Value, error) { routed = true; return nil, nil })
	rescue := cloudevents.New()
	rescue.SetID("broker-1")
	rescue.SetType("com.aws.sqs.message")
	rescue.SetSource("//sqs.amazonaws.com")
	_ = rescue.SetData(cloudevents.ApplicationJSON, []byte(`{"k":`))
	cause := errors.New("aws/sqs: unmarshal: ce-time \"x\": bad")
	msg := event.NewUndecodable(t.Context(), rescue, event.Delivery{}, cause)
	h.sub.handler(msg)

	if routed {
		t.Fatal("handler ran for an undecodable message")
	}
	if outcome, err := msg.Outcome(); outcome != event.OutcomeDiscarded || !errors.Is(err, cause) {
		t.Fatalf("Outcome() = %v, %v; want Discarded with the transport error", outcome, err)
	}
	if len(h.dlq.got) != 1 {
		t.Fatalf("dead-letter copies = %d, want 1", len(h.dlq.got))
	}
	copyExts := h.dlq.got[0].Extensions()
	if copyExts[event.ExtDeadLetterReason] != string(event.ReasonDecode) || string(h.dlq.got[0].Data()) != `{"k":` {
		t.Errorf("dead-letter copy = reason %v data %s; want decode with the raw body", copyExts[event.ExtDeadLetterReason], h.dlq.got[0].Data())
	}
	if copyExts[event.ExtCorrelationID] != "broker-1" {
		t.Errorf("copy correlationid = %v, want the rescue id", copyExts[event.ExtCorrelationID])
	}
	line := logLine(t, h.logs.String(), "message dead-lettered")
	if !strings.Contains(line, `"`+logfields.KeyDeadLetterReason+`":"decode"`) || !strings.Contains(line, `"messaging.message.id":"broker-1"`) {
		t.Errorf("unexpected record:\n%s", line)
	}
	if n := strings.Count(h.logs.String(), `"level":"ERROR"`); n != 1 {
		t.Errorf("ERROR records = %d, want 1:\n%s", n, h.logs.String())
	}
}
