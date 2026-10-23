package eventmux

import (
	"context"
	"encoding/json/jsontext"
	"errors"
	"log/slog"
	"strings"
	"sync"
	"testing"
	"time"

	cloudevents "github.com/cloudevents/sdk-go/v2/event"

	"github.com/norlis/httpgate/logging"

	"github.com/norlis/event-driven/v2/pkg/event"
	"github.com/norlis/event-driven/v2/pkg/kit/logfields"
)

type messageCall struct {
	id      string
	outcome logfields.Outcome
	reason  event.DeadLetterReason
	elapsed time.Duration
}

type publishCall struct {
	eventType   string
	err         error
	causationID string
}

// recordingObserver stores every callback; panicOn makes the named method panic.
type recordingObserver struct {
	mu        sync.Mutex
	messages  []messageCall
	publishes []publishCall
	panicOn   string
}

func (o *recordingObserver) OnMessage(_ context.Context, msg *event.Message, outcome logfields.Outcome, reason event.DeadLetterReason, elapsed time.Duration) {
	if o.panicOn == "OnMessage" {
		panic("observer bug")
	}
	o.mu.Lock()
	defer o.mu.Unlock()
	o.messages = append(o.messages, messageCall{id: msg.ID(), outcome: outcome, reason: reason, elapsed: elapsed})
}

func (o *recordingObserver) OnPublish(_ context.Context, ce cloudevents.Event, err error) {
	if o.panicOn == "OnPublish" {
		panic("observer bug")
	}
	o.mu.Lock()
	defer o.mu.Unlock()
	cid, _ := ce.Extensions()[event.ExtCausationID].(string)
	o.publishes = append(o.publishes, publishCall{eventType: ce.Type(), causationID: cid, err: err})
}

func withObserver(obs Observer) func(*Config) { return func(c *Config) { c.Observer = obs } }

// neverMatch is a Filter that rejects everything.
type neverMatch struct{}

func (neverMatch) Match(*event.Message) bool { return false }

func slogDiscard() *slog.Logger { return slog.New(slog.DiscardHandler) }

func TestObserverOnMessageOncePerOutcome(t *testing.T) {
	t.Parallel()

	handlerErr := errors.New("db down")
	tests := []struct {
		name        string
		data        string
		tune        func(*Config)
		handler     func(context.Context, payload) (jsontext.Value, error)
		wantOutcome logfields.Outcome
		wantReason  event.DeadLetterReason
	}{
		{name: "acked", data: `{"name":"a"}`, handler: func(context.Context, payload) (jsontext.Value, error) { return nil, nil }, wantOutcome: logfields.OutcomeAcked},
		{name: "nacked", data: `{"name":"a"}`, handler: func(context.Context, payload) (jsontext.Value, error) { return nil, handlerErr }, wantOutcome: logfields.OutcomeNacked},
		{name: "dead-lettered carries the reason", data: `{"name":"a"}`, handler: func(context.Context, payload) (jsontext.Value, error) {
			return nil, event.NewNonRetryableError(handlerErr)
		}, wantOutcome: logfields.OutcomeDiscarded, wantReason: event.ReasonNonRetryable},
		// No MaxRetries: a nack would be unbounded, so the copy is lost and the
		// decision (discard, reason validation) is what the Observer sees.
		{name: "dlq retryable failure without a bound reports the discard", data: `{}`, tune: func(c *Config) { c.DeadLetter = &capturePublisher{err: errors.New("broker down")} }, handler: func(context.Context, payload) (jsontext.Value, error) { return nil, nil }, wantOutcome: logfields.OutcomeDiscarded, wantReason: event.ReasonValidation},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			obs := &recordingObserver{}
			tune := []func(*Config){withObserver(obs)}
			if tt.tune != nil {
				tune = append(tune, tt.tune)
			}
			h := newMuxHarness(t, tt.handler, tune...)
			h.sub.handler(newIncoming(t, tt.data))

			if len(obs.messages) != 1 {
				t.Fatalf("OnMessage called %d times, want 1: %+v", len(obs.messages), obs.messages)
			}
			got := obs.messages[0]
			if got.id != "in-1" || got.outcome != tt.wantOutcome || got.reason != tt.wantReason {
				t.Errorf("OnMessage = %+v, want id=in-1 outcome=%s reason=%q", got, tt.wantOutcome, tt.wantReason)
			}
			if got.elapsed <= 0 {
				t.Errorf("elapsed = %s, want > 0", got.elapsed)
			}
		})
	}
}

func TestObserverOnMessageForNoRoute(t *testing.T) {
	t.Parallel()

	obs := &recordingObserver{}
	sub := newFakeSubscription()
	mux, err := New(Config{Name: "noroute", Subscription: sub, Logger: slogDiscard(), Observer: obs})
	if err != nil {
		t.Fatal(err)
	}
	mux.Handle(nil, neverMatch{}, func(context.Context, payload) (jsontext.Value, error) { return nil, nil })
	if err := mux.Start(t.Context(), nil); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = mux.Stop(context.Background()) })
	sub.handler(newIncoming(t, `{"name":"a"}`))

	if len(obs.messages) != 1 || obs.messages[0].outcome != logfields.OutcomeDiscarded || obs.messages[0].reason != "" {
		t.Errorf("OnMessage = %+v, want one discarded call with empty reason", obs.messages)
	}
}

func TestObserverOnPublishForResultAndDeadLetter(t *testing.T) {
	t.Parallel()

	t.Run("result success", func(t *testing.T) {
		t.Parallel()
		obs := &recordingObserver{}
		h := newMuxHarness(t, func(context.Context, payload) (jsontext.Value, error) { return []byte(`{}`), nil }, withObserver(obs))
		h.sub.handler(newIncoming(t, `{"name":"a"}`))
		if len(obs.publishes) != 1 || obs.publishes[0].eventType != "test.event.result" || obs.publishes[0].err != nil {
			t.Errorf("OnPublish = %+v, want one test.event.result with nil error", obs.publishes)
		}
	})
	t.Run("result failure", func(t *testing.T) {
		t.Parallel()
		obs := &recordingObserver{}
		errBroker := errors.New("broker down")
		h := newMuxHarness(t, func(context.Context, payload) (jsontext.Value, error) { return []byte(`{}`), nil }, withObserver(obs))
		h.pub.err = errBroker
		h.sub.handler(newIncoming(t, `{"name":"a"}`))
		if len(obs.publishes) != 1 || !errors.Is(obs.publishes[0].err, errBroker) {
			t.Errorf("OnPublish = %+v, want one call with the broker error", obs.publishes)
		}
	})
	t.Run("dead-letter copy", func(t *testing.T) {
		t.Parallel()
		obs := &recordingObserver{}
		h := newMuxHarness(t, func(context.Context, payload) (jsontext.Value, error) { return nil, nil }, withObserver(obs))
		h.sub.handler(newIncoming(t, `not json`))
		if len(obs.publishes) != 1 || obs.publishes[0].eventType != "test.event" || obs.publishes[0].err != nil {
			t.Errorf("OnPublish = %+v, want one test.event (the copy) with nil error", obs.publishes)
		}
	})
}

func TestObserverPanicDoesNotChangeOutcome(t *testing.T) {
	t.Parallel()

	for _, method := range []string{"OnMessage", "OnPublish"} {
		t.Run(method, func(t *testing.T) {
			t.Parallel()
			obs := &recordingObserver{panicOn: method}
			h := newMuxHarness(t, func(context.Context, payload) (jsontext.Value, error) { return []byte(`{}`), nil }, withObserver(obs))
			msg := newIncoming(t, `{"name":"a"}`)
			h.sub.handler(msg)

			if outcome, _ := msg.Outcome(); outcome != event.OutcomeAcked {
				t.Errorf("Outcome() = %v, want Acked", outcome)
			}
			if len(h.pub.got) != 1 {
				t.Errorf("published %d, want 1", len(h.pub.got))
			}
			logs := h.logs.String()
			if n := strings.Count(logs, `"msg":"observer panicked"`); n != 1 {
				t.Fatalf("observer panicked logged %d times, want 1:\n%s", n, logs)
			}
			line := logLine(t, logs, "observer panicked")
			for _, want := range []string{`"level":"ERROR"`, `"` + logging.KeyErrorMessage + `":"handler panicked: observer bug"`, `"` + logging.KeyErrorStackTrace + `":"`} {
				if !strings.Contains(line, want) {
					t.Errorf("observer panicked lacks %s:\n%s", want, line)
				}
			}
		})
	}
}

func TestObserverReportsDecidedOutcomeWhenBrokerRejectsIt(t *testing.T) {
	t.Parallel()

	obs := &recordingObserver{}
	h := newMuxHarness(t, func(context.Context, payload) (jsontext.Value, error) { return []byte(`{}`), nil }, withObserver(obs))
	ce := cloudevents.New()
	ce.SetID("ack-fail")
	ce.SetSource("/test")
	ce.SetType("test.event")
	_ = ce.SetData(cloudevents.ApplicationJSON, []byte(`{"name":"a"}`))
	fail := func() error { return errors.New("broker down") }
	h.sub.handler(event.NewMessage(t.Context(), ce, event.Delivery{Ack: fail, Nack: fail, Discard: fail}))

	if len(h.pub.got) != 1 {
		t.Errorf("published %d, want 1 (the result went out before the Ack)", len(h.pub.got))
	}
	if len(obs.messages) != 1 || obs.messages[0].outcome != logfields.OutcomeAcked {
		t.Errorf("OnMessage = %+v, want one acked call", obs.messages)
	}
}

func TestStartWrapsOnlyNonNilPublishers(t *testing.T) {
	t.Parallel()

	sub := newFakeSubscription()
	mux, err := New(Config{Name: "wrap", Subscription: sub, Logger: slogDiscard(), Observer: &recordingObserver{}})
	if err != nil {
		t.Fatal(err)
	}
	mux.Handle(nil, nil, func(context.Context, payload) (jsontext.Value, error) { return nil, nil })
	if err := mux.Start(t.Context(), nil); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = mux.Stop(context.Background()) })
	if mux.routes[0].pub != nil {
		t.Errorf("route publisher = %T, want nil", mux.routes[0].pub)
	}
	if mux.cfg.DeadLetter != nil {
		t.Errorf("DeadLetter = %T, want nil", mux.cfg.DeadLetter)
	}
}

func TestNilObserverLeavesPublishersBare(t *testing.T) {
	t.Parallel()

	h := newMuxHarness(t, func(context.Context, payload) (jsontext.Value, error) { return nil, nil })
	if _, ok := h.mux.routes[0].pub.(*capturePublisher); !ok {
		t.Errorf("route publisher = %T, want *capturePublisher", h.mux.routes[0].pub)
	}
}
