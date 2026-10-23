package eventmux

import (
	"context"
	"encoding/json/jsontext"
	"errors"
	"strings"
	"testing"

	cloudevents "github.com/cloudevents/sdk-go/v2/event"

	"github.com/norlis/event-driven/v2/pkg/event"
	"github.com/norlis/event-driven/v2/pkg/kit/logfields"
)

// newNoNackIncoming models a transport without redelivery (Core NATS).
func newNoNackIncoming(t *testing.T, data string) *event.Message {
	t.Helper()
	ce := cloudevents.New()
	ce.SetID("in-1")
	ce.SetSource("/test")
	ce.SetType("test.event")
	if err := ce.SetData(cloudevents.ApplicationJSON, []byte(data)); err != nil {
		t.Fatal(err)
	}
	return event.NewMessage(t.Context(), ce, event.Delivery{})
}

// A retryable failure on a transport that cannot redeliver is definitive: the
// message is dead-lettered with reason noredelivery and discarded, so logs and
// the Observer never report a redelivery that will not happen.
func TestRetryableFailureWithoutNackIsDeadLettered(t *testing.T) {
	t.Parallel()

	boom := errors.New("db down")
	h := newMuxHarness(t, func(context.Context, payload) (jsontext.Value, error) { return nil, boom })
	msg := newNoNackIncoming(t, `{"name":"a"}`)
	h.sub.handler(msg)

	if outcome, err := msg.Outcome(); outcome != event.OutcomeDiscarded || !errors.Is(err, boom) {
		t.Fatalf("Outcome() = %v, %v; want Discarded, db down", outcome, err)
	}
	if len(h.dlq.got) != 1 || h.dlq.got[0].Extensions()[event.ExtDeadLetterReason] != string(event.ReasonNoRedelivery) {
		t.Fatalf("dead-letter copies = %v, want 1 with reason noredelivery", h.dlq.got)
	}
	line := logLine(t, h.logs.String(), "message dead-lettered")
	if !strings.Contains(line, `"`+logfields.KeyDeadLetterReason+`":"noredelivery"`) || strings.Contains(h.logs.String(), `"nacked"`) {
		t.Errorf("unexpected records:\n%s", h.logs.String())
	}
}

// The same transport with a result publish that fails retryably: the handler
// already ran and nothing can retry, so the publish failure is definitive.
func TestResultPublishFailureWithoutNackIsDeadLettered(t *testing.T) {
	t.Parallel()

	h := newMuxHarness(t, func(context.Context, payload) (jsontext.Value, error) { return []byte(`{}`), nil })
	h.pub.err = errors.New("broker down")
	msg := newNoNackIncoming(t, `{"name":"a"}`)
	h.sub.handler(msg)

	if outcome, err := msg.Outcome(); outcome != event.OutcomeDiscarded || !errors.Is(err, event.ErrPublicationFailed) {
		t.Fatalf("Outcome() = %v, %v; want Discarded, ErrPublicationFailed", outcome, err)
	}
	if len(h.dlq.got) != 1 || h.dlq.got[0].Extensions()[event.ExtDeadLetterReason] != string(event.ReasonPublish) {
		t.Fatalf("dead-letter copies = %v, want 1 with reason publish", h.dlq.got)
	}
}

// Without a Nack the dead-letter copy failing retryably cannot be retried
// either: the copy is lost and the discard stands.
func TestDeadLetterFailureWithoutNackDiscards(t *testing.T) {
	t.Parallel()

	boom := errors.New("db down")
	h := newMuxHarness(t, func(context.Context, payload) (jsontext.Value, error) { return nil, boom },
		func(c *Config) { c.DeadLetter = &capturePublisher{err: errors.New("dlq down")} })
	msg := newNoNackIncoming(t, `{"name":"a"}`)
	h.sub.handler(msg)

	if outcome, err := msg.Outcome(); outcome != event.OutcomeDiscarded || !errors.Is(err, boom) {
		t.Fatalf("Outcome() = %v, %v; want Discarded, db down", outcome, err)
	}
	line := logLine(t, h.logs.String(), "message discarded")
	if !strings.Contains(line, `"`+logfields.KeyDeadLetterPublishError+`":"dlq down"`) {
		t.Errorf("record lacks the dead-letter publish error:\n%s", line)
	}
}
