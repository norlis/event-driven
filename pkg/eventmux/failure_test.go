package eventmux

import (
	"context"
	"encoding/json/jsontext"
	"errors"
	"strings"
	"testing"
	"unicode/utf8"

	cloudevents "github.com/cloudevents/sdk-go/v2/event"

	"github.com/norlis/event-driven/v2/pkg/event"
)

func TestDeadLetterEventKeepsOriginalAndSetsExtensions(t *testing.T) {
	t.Parallel()

	src := cloudevents.New()
	src.SetID("orig-1")
	src.SetSource("/orders")
	src.SetType("order.created")
	src.SetExtension(event.ExtCorrelationID, "corr-9")
	src.SetExtension("traceparent", "00-aa-bb-01")
	src.SetExtension(event.ExtDeadLetterReason, "stale") // re-injected copy: must be overwritten
	_ = src.SetData(cloudevents.ApplicationJSON, []byte(`{"k":1}`))

	dl := deadLetterEvent(src, event.ReasonDecode, errors.New("bad json"))

	if dl.ID() != "orig-1" || dl.Source() != "/orders" || dl.Type() != "order.created" || string(dl.Data()) != `{"k":1}` {
		t.Errorf("original attributes changed: id=%q source=%q type=%q data=%s", dl.ID(), dl.Source(), dl.Type(), dl.Data())
	}
	exts := dl.Extensions()
	if exts[event.ExtCorrelationID] != "corr-9" || exts["traceparent"] != "00-aa-bb-01" {
		t.Errorf("original extensions lost: %v", exts)
	}
	if exts[event.ExtDeadLetterReason] != "decode" || exts[event.ExtDeadLetterError] != "bad json" {
		t.Errorf("dead-letter extensions = %v", exts)
	}
	if src.Extensions()[event.ExtDeadLetterReason] != "stale" {
		t.Error("deadLetterEvent mutated the source event")
	}
}

func TestTruncateRuneBoundary(t *testing.T) {
	t.Parallel()

	// 255 ASCII bytes followed by "é" (bytes 255-256) puts the cut inside the rune.
	s := strings.Repeat("a", 255) + "éZ"
	got := truncate(s, event.MaxDeadLetterErrorLen)
	if len(got) != 255 || !utf8.ValidString(got) {
		t.Errorf("truncate() len=%d valid=%v, want 255 and valid", len(got), utf8.ValidString(got))
	}
	if short := truncate("short", event.MaxDeadLetterErrorLen); short != "short" {
		t.Errorf("truncate(short) = %q", short)
	}
}

func TestDeadLetterErrorIsSanitized(t *testing.T) {
	t.Parallel()

	raw := errors.New("bad\x00byte \xffinvalid\ttab\nnewline")
	dl := deadLetterEvent(cloudevents.New(), event.ReasonDecode, raw)
	got, _ := dl.Extensions()[event.ExtDeadLetterError].(string)
	if got != "badbyte \uFFFDinvalid\ttab\nnewline" {
		t.Errorf("deadlettererror = %q", got)
	}
	if !utf8.ValidString(got) {
		t.Error("deadlettererror is not valid UTF-8")
	}
}

func TestInvokeRecoversPanicWithStack(t *testing.T) {
	t.Parallel()

	mux, err := New(Config{Subscription: newFakeSubscription()})
	if err != nil {
		t.Fatal(err)
	}
	mux.preflight = noopHandler
	rt := &route{chain: func(context.Context, any) (jsontext.Value, error) { panic("boom") }}

	_, err = mux.invoke(t.Context(), rt, nil)

	stack, cause := unwrapFailure(err)
	if !errors.Is(cause, event.ErrPanic) || cause.Error() != "handler panicked: boom" {
		t.Errorf("cause = %v, want ErrPanic wrapping boom", cause)
	}
	if !strings.Contains(string(stack), "goroutine") {
		t.Errorf("stack = %q, want a goroutine dump", stack)
	}
}
