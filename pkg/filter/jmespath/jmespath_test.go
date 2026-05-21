package jmespath

import (
	"log/slog"
	"testing"

	cloudevents "github.com/cloudevents/sdk-go/v2/event"

	"github.com/norlis/event-driven/pkg/event"
)

func newMessage(t *testing.T, body []byte) *event.Message {
	t.Helper()
	ce := cloudevents.New()
	ce.SetID("test-id")
	ce.SetType("test.event")
	ce.SetSource("test://src")
	if body != nil {
		if err := ce.SetData(cloudevents.ApplicationJSON, body); err != nil {
			t.Fatalf("set data: %v", err)
		}
	}
	return event.NewMessageWithoutAck(ce)
}

func discardLogger() *slog.Logger {
	return slog.New(slog.DiscardHandler)
}

func TestJMESPath_Match(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		expr string
		body []byte
		want bool
	}{
		{
			name: "boolean true expression matches",
			expr: "status == 'paid'",
			body: []byte(`{"status":"paid"}`),
			want: true,
		},
		{
			name: "boolean false expression does not match",
			expr: "status == 'paid'",
			body: []byte(`{"status":"pending"}`),
			want: false,
		},
		{
			name: "nested field comparison",
			expr: "user.role == 'admin'",
			body: []byte(`{"user":{"role":"admin"}}`),
			want: true,
		},
		{
			name: "missing field evaluates falsy",
			expr: "user.role == 'admin'",
			body: []byte(`{"other":"value"}`),
			want: false,
		},
		{
			name: "invalid JSON body returns false",
			expr: "status == 'paid'",
			body: []byte(`{not-json`),
			want: false,
		},
		{
			name: "non-boolean result returns false",
			expr: "status", // returns the string, not a bool
			body: []byte(`{"status":"paid"}`),
			want: false,
		},
		{
			name: "numeric comparison true",
			expr: "amount > `100`",
			body: []byte(`{"amount":500}`),
			want: true,
		},
		{
			name: "numeric comparison false",
			expr: "amount > `100`",
			body: []byte(`{"amount":50}`),
			want: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			f := New(tt.expr, discardLogger())
			got := f.Match(newMessage(t, tt.body))
			if got != tt.want {
				t.Errorf("Match() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestJMESPath_InvalidExpressionReturnsFalse(t *testing.T) {
	t.Parallel()

	f := New("???invalid???", discardLogger())
	got := f.Match(newMessage(t, []byte(`{"a":1}`)))
	if got {
		t.Error("expected false for invalid expression, got true")
	}
}

func TestJMESPath_NilLoggerDoesNotCrash(t *testing.T) {
	t.Parallel()

	f := New("status == 'paid'", nil)
	got := f.Match(newMessage(t, []byte(`{"status":"paid"}`)))
	if !got {
		t.Error("expected true match with nil logger")
	}

	// Also exercise the error paths with a nil logger (they should fall back
	// to the discard logger internally).
	got = f.Match(newMessage(t, []byte(`not-json`)))
	if got {
		t.Error("expected false on invalid JSON with nil logger")
	}
}
