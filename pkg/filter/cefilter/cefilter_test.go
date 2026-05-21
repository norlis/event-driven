package cefilter

import (
	"testing"

	cloudevents "github.com/cloudevents/sdk-go/v2/event"

	"github.com/norlis/event-driven/pkg/event"
	"github.com/norlis/event-driven/pkg/eventmux"
)

func newMessage(t *testing.T, evType, source string) *event.Message {
	t.Helper()
	ce := cloudevents.New()
	ce.SetID("test-id")
	ce.SetType(evType)
	ce.SetSource(source)
	return event.NewMessageWithoutAck(ce)
}

// stubFilter always returns its preset match value. Used to test All().
type stubFilter struct{ result bool }

func (s stubFilter) Match(*event.Message) bool { return s.result }

func TestByType(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		registered []string
		incoming   string
		want       bool
	}{
		{"single type match", []string{"order.created"}, "order.created", true},
		{"single type no-match", []string{"order.created"}, "order.deleted", false},
		{"multi type, first matches", []string{"order.created", "order.deleted"}, "order.created", true},
		{"multi type, second matches", []string{"order.created", "order.deleted"}, "order.deleted", true},
		{"multi type, none matches", []string{"order.created", "order.deleted"}, "user.signup", false},
		{"empty registered list never matches", nil, "order.created", false},
		{"case sensitive", []string{"Order.Created"}, "order.created", false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			f := ByType(tt.registered...)
			msg := newMessage(t, tt.incoming, "src")
			if got := f.Match(msg); got != tt.want {
				t.Errorf("Match() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestBySource(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		prefix   string
		incoming string
		want     bool
	}{
		{"exact match", "//orders/svc", "//orders/svc", true},
		{"prefix match", "//orders", "//orders/svc/v1", true},
		{"no match", "//orders", "//users/svc", false},
		{"empty prefix matches any source", "", "//anything", true},
		{"empty prefix matches empty source", "", "", true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			f := BySource(tt.prefix)
			msg := newMessage(t, "x", tt.incoming)
			if got := f.Match(msg); got != tt.want {
				t.Errorf("Match() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestAll(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		filters []eventmux.Filter
		want    bool
	}{
		{"all true → match", []eventmux.Filter{stubFilter{true}, stubFilter{true}, stubFilter{true}}, true},
		{"one false → no match", []eventmux.Filter{stubFilter{true}, stubFilter{false}, stubFilter{true}}, false},
		{"all false → no match", []eventmux.Filter{stubFilter{false}, stubFilter{false}}, false},
		{"empty composite → vacuous true", nil, true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			composite := All(tt.filters...)
			msg := newMessage(t, "x", "src")
			if got := composite.Match(msg); got != tt.want {
				t.Errorf("Match() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestAll_CombinedWithRealFilters(t *testing.T) {
	t.Parallel()

	f := All(
		ByType("order.created"),
		BySource("//orders"),
	)

	msg := newMessage(t, "order.created", "//orders/svc/v1")
	if !f.Match(msg) {
		t.Error("expected match (type ok, source ok)")
	}

	msg2 := newMessage(t, "order.created", "//users/svc")
	if f.Match(msg2) {
		t.Error("expected no match (source mismatch)")
	}

	msg3 := newMessage(t, "user.signup", "//orders/svc")
	if f.Match(msg3) {
		t.Error("expected no match (type mismatch)")
	}
}
