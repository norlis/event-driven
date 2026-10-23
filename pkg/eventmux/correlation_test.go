package eventmux

import (
	"context"
	"encoding/json/jsontext"
	"strings"
	"testing"

	cloudevents "github.com/cloudevents/sdk-go/v2/event"

	"github.com/norlis/event-driven/v2/pkg/event"
	"github.com/norlis/event-driven/v2/pkg/eventmux/metadata"
	"github.com/norlis/event-driven/v2/pkg/kit/logfields"
)

// newUncorrelated is newIncoming without the correlationid extension.
func newUncorrelated(t *testing.T, id, data string) *event.Message {
	t.Helper()
	ce := cloudevents.New()
	ce.SetID(id)
	ce.SetSource("/test")
	ce.SetType("test.event")
	if err := ce.SetData(cloudevents.ApplicationJSON, []byte(data)); err != nil {
		t.Fatal(err)
	}
	return event.NewMessage(t.Context(), ce, event.Delivery{})
}

func TestMissingCorrelationIDIsTheEventID(t *testing.T) {
	t.Parallel()

	h := newMuxHarness(t, func(context.Context, payload) (jsontext.Value, error) { return []byte(`{}`), nil })
	h.sub.handler(newUncorrelated(t, "in-9", `{"name":"a"}`))

	if len(h.pub.got) != 1 {
		t.Fatalf("published %d, want 1", len(h.pub.got))
	}
	if got := event.CorrelationID(h.pub.got[0]); got != "in-9" {
		t.Errorf("result correlationid = %q, want in-9", got)
	}
	line := logLine(t, h.logs.String(), "message processed")
	if !strings.Contains(line, `"`+logfields.KeyCorrelationID+`":"in-9"`) {
		t.Errorf("processed log lacks correlation id = in-9:\n%s", line)
	}
}

func TestMissingCorrelationIDReachesDeadLetterCopy(t *testing.T) {
	t.Parallel()

	h := newMuxHarness(t, func(context.Context, payload) (jsontext.Value, error) { return nil, nil })
	h.sub.handler(newUncorrelated(t, "in-9", `not json`))

	if len(h.dlq.got) != 1 {
		t.Fatalf("dead-lettered %d, want 1", len(h.dlq.got))
	}
	if got := event.CorrelationID(h.dlq.got[0]); got != "in-9" {
		t.Errorf("dead-letter correlationid = %q, want in-9", got)
	}
	if _, ok := h.dlq.got[0].Extensions()[event.ExtCausationID]; ok {
		t.Errorf("dead-letter copy must not carry causationid: %v", h.dlq.got[0].Extensions())
	}
}

func TestPresentCorrelationIDIsKept(t *testing.T) {
	t.Parallel()

	h := newMuxHarness(t, func(context.Context, payload) (jsontext.Value, error) { return []byte(`{}`), nil })
	h.sub.handler(newIncoming(t, `{"name":"a"}`)) // carries corr-1

	if got := event.CorrelationID(h.pub.got[0]); got != "corr-1" {
		t.Errorf("result correlationid = %q, want corr-1", got)
	}
}

func TestEmptyIDLeavesCorrelationIDAbsent(t *testing.T) {
	t.Parallel()

	h := newMuxHarness(t, func(context.Context, payload) (jsontext.Value, error) { return []byte(`{}`), nil })
	h.sub.handler(newUncorrelated(t, "", `{"name":"a"}`))

	if len(h.pub.got) != 1 {
		t.Fatalf("published %d, want 1", len(h.pub.got))
	}
	exts := h.pub.got[0].Extensions()
	if _, ok := exts[event.ExtCorrelationID]; ok {
		t.Errorf("result carries an empty correlationid: %v", exts)
	}
	if _, ok := exts[event.ExtCausationID]; ok {
		t.Errorf("result carries an empty causationid: %v", exts)
	}
}

func TestResultCausationIDIsTheIncomingID(t *testing.T) {
	t.Parallel()

	h := newMuxHarness(t, func(ctx context.Context, p payload) (jsontext.Value, error) {
		if store, ok := metadata.FromContext(ctx); ok {
			store.Set(event.ExtCausationID, "hijacked")
		}
		return []byte(`{}`), nil
	})
	msg := newIncoming(t, `{"name":"a"}`)          // id in-1
	msg.SetExtension(event.ExtCausationID, "in-0") // the event that caused in-1
	h.sub.handler(msg)

	if len(h.pub.got) != 1 {
		t.Fatalf("published %d, want 1", len(h.pub.got))
	}
	if got := h.pub.got[0].Extensions()[event.ExtCausationID]; got != "in-1" {
		t.Errorf("result causationid = %v, want in-1", got)
	}
	if got := strings.Count(h.logs.String(), `"msg":"reserved extension ignored"`); got != 1 {
		t.Errorf("WARN logged %d times, want 1:\n%s", got, h.logs.String())
	}
}
