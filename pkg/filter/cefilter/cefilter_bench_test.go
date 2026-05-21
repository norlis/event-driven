package cefilter

import (
	"fmt"
	"strconv"
	"testing"

	cloudevents "github.com/cloudevents/sdk-go/v2/event"

	"github.com/norlis/event-driven/pkg/event"
	"github.com/norlis/event-driven/pkg/eventmux"
)

func benchMessage(evType, source string) *event.Message {
	ce := cloudevents.New()
	ce.SetID("x")
	ce.SetType(evType)
	ce.SetSource(source)
	return event.NewMessageWithoutAck(ce)
}

func BenchmarkByType(b *testing.B) {
	// Vary registered set size; ByType uses a map so we expect O(1) regardless.
	for _, n := range []int{1, 8, 64} {
		b.Run(fmt.Sprintf("registered=%d/hit", n), func(b *testing.B) {
			types := make([]string, n)
			for i := range types {
				types[i] = "t" + strconv.Itoa(i)
			}
			f := ByType(types...)
			msg := benchMessage("t0", "src") // always matches

			b.ReportAllocs()
			b.ResetTimer()
			for range b.N {
				_ = f.Match(msg)
			}
		})

		b.Run(fmt.Sprintf("registered=%d/miss", n), func(b *testing.B) {
			types := make([]string, n)
			for i := range types {
				types[i] = "t" + strconv.Itoa(i)
			}
			f := ByType(types...)
			msg := benchMessage("not-registered", "src")

			b.ReportAllocs()
			b.ResetTimer()
			for range b.N {
				_ = f.Match(msg)
			}
		})
	}
}

func BenchmarkBySource(b *testing.B) {
	f := BySource("//orders")
	msg := benchMessage("x", "//orders/svc/v1/path/to/somewhere")

	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		_ = f.Match(msg)
	}
}

func BenchmarkAll(b *testing.B) {
	// Composite of 3 filters that all match.
	f := All(
		ByType("order.created"),
		BySource("//orders"),
		ByType("order.created", "order.deleted"),
	)
	msg := benchMessage("order.created", "//orders/svc")

	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		_ = f.Match(msg)
	}
}

// Compile-time check: assertion that f is exactly eventmux.Filter.
// Keeps the import used and documents the abstraction we're benchmarking.
var _ eventmux.Filter = ByType("x")
