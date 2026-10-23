package event

import (
	"errors"
	"iter"
	"maps"
	"slices"
	"strings"
	"testing"

	cloudevents "github.com/cloudevents/sdk-go/v2/event"
)

func TestValidateCorrelationID(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name  string
		value string
		ok    bool
	}{
		{name: "uuid", value: "0199c2f1-4e8b-7d44-a1b2-c3d4e5f60718", ok: true},
		{name: "max length", value: strings.Repeat("a", MaxCorrelationIDLen), ok: true},
		{name: "all printable ascii edges", value: "!~", ok: true},
		{name: "empty", value: "", ok: false},
		{name: "too long", value: strings.Repeat("a", MaxCorrelationIDLen+1), ok: false},
		{name: "space", value: "a b", ok: false},
		{name: "control char", value: "a\tb", ok: false},
		{name: "non ascii", value: "ñ", ok: false},
		{name: "invalid utf8", value: "ab\xffcd", ok: false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			err := ValidateCorrelationID(tt.value)
			if tt.ok && err != nil {
				t.Fatalf("ValidateCorrelationID(%q) = %v, want nil", tt.value, err)
			}
			if !tt.ok && !errors.Is(err, ErrInvalidCorrelationID) {
				t.Fatalf("ValidateCorrelationID(%q) = %v, want ErrInvalidCorrelationID", tt.value, err)
			}
		})
	}
}

func TestCorrelationID(t *testing.T) {
	t.Parallel()

	ce := cloudevents.New()
	if got := CorrelationID(ce); got != "" {
		t.Errorf("CorrelationID(no ext) = %q, want empty", got)
	}
	ce.SetExtension(ExtCorrelationID, "c-1")
	if got := CorrelationID(ce); got != "c-1" {
		t.Errorf("CorrelationID() = %q, want %q", got, "c-1")
	}
	ce.SetExtension(ExtCorrelationID, 42)
	if got := CorrelationID(ce); got != "" {
		t.Errorf("CorrelationID(non-string ext) = %q, want empty", got)
	}
}

// TestCorrelationIDDoesNotAllocate: the hot path reads the id on every log
// record, so it must not copy the extension map.
//
//nolint:paralleltest // AllocsPerRun panics inside a parallel test
func TestCorrelationIDDoesNotAllocate(t *testing.T) {
	ce := cloudevents.New()
	ce.SetExtension(ExtCorrelationID, "c-1")
	ce.SetExtension("other", "x")
	if allocs := testing.AllocsPerRun(100, func() { _ = CorrelationID(ce) }); allocs != 0 {
		t.Fatalf("CorrelationID allocates %v times per call, want 0", allocs)
	}
}

//nolint:paralleltest // AllocsPerRun panics inside a parallel test
func TestExtensionsIteratesWithoutCopy(t *testing.T) {
	ce := cloudevents.New()
	if n := len(slices.Collect(keysOf(Extensions(ce)))); n != 0 {
		t.Fatalf("Extensions(no ext) yielded %d, want 0", n)
	}
	ce.SetExtension("a", "1")
	ce.SetExtension("b", 2)
	got := maps.Collect(Extensions(ce))
	if len(got) != 2 || got["a"] != "1" || got["b"] != int32(2) {
		t.Fatalf("Extensions() = %v, want a=1 b=2", got)
	}
	if allocs := testing.AllocsPerRun(100, func() {
		for range Extensions(ce) {
		}
	}); allocs != 0 {
		t.Fatalf("Extensions allocates %v times per iteration, want 0", allocs)
	}
}

func keysOf(seq iter.Seq2[string, any]) iter.Seq[string] {
	return func(yield func(string) bool) {
		for k := range seq {
			if !yield(k) {
				return
			}
		}
	}
}
