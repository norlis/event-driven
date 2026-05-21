package eventmux

import (
	"encoding/json"
	"fmt"
	"reflect"
	"strings"
	"testing"
)

// benchPayload is the canonical struct used across decode benchmarks.
type benchPayload struct {
	ID      string            `json:"id"`
	Value   int               `json:"value"`
	Tags    []string          `json:"tags,omitempty"`
	Meta    map[string]string `json:"meta,omitempty"`
	Comment string            `json:"comment,omitempty"`
}

// makePayloadJSON builds a JSON payload of approximately the given size.
// The size hint controls the comment field length.
func makePayloadJSON(approxSize int) []byte {
	p := benchPayload{
		ID:    "order-1234",
		Value: 42,
		Tags:  []string{"a", "b", "c"},
		Meta:  map[string]string{"trace": "abc", "tenant": "acme"},
	}
	if approxSize > 0 {
		p.Comment = strings.Repeat("x", approxSize)
	}
	out, _ := json.Marshal(p)
	return out
}

func BenchmarkDecodeInto(b *testing.B) {
	sizes := []struct {
		name string
		hint int
	}{
		{"small", 0},     // ~120B
		{"medium", 512},  // ~640B
		{"large", 4096},  // ~4.2KB
	}

	for _, s := range sizes {
		data := makePayloadJSON(s.hint)
		b.Run(fmt.Sprintf("size=%s/bytes=%d/value", s.name, len(data)), func(b *testing.B) {
			typ := reflect.TypeFor[benchPayload]()
			b.ReportAllocs()
			b.ResetTimer()
			for range b.N {
				_, _ = decodeInto(typ, data)
			}
		})

		b.Run(fmt.Sprintf("size=%s/bytes=%d/pointer", s.name, len(data)), func(b *testing.B) {
			typ := reflect.TypeFor[*benchPayload]()
			b.ReportAllocs()
			b.ResetTimer()
			for range b.N {
				_, _ = decodeInto(typ, data)
			}
		})
	}
}
