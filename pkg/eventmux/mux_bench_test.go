package eventmux

import (
	"context"
	"encoding/json"
	"fmt"
	"testing"

	cloudevents "github.com/cloudevents/sdk-go/v2/event"

	"github.com/norlis/event-driven/pkg/event"
)

// benchSubscription delivers a fresh *event.Message wrapping the same
// CloudEvent on every iteration. It bypasses the cost of building the
// underlying CloudEvent so the benchmark measures only the dispatch path.
type benchSubscription struct {
	ce cloudevents.Event
	n  int
}

func (s *benchSubscription) Start(_ context.Context, handler func(*event.Message)) error {
	noop := func() {}
	for range s.n {
		msg := event.NewMessage(s.ce, noop, noop)
		handler(msg)
	}
	return nil
}

// benchPublisher swallows everything. It allocates nothing on the hot path
// so we measure only the mux dispatch overhead.
type benchPublisher struct{}

func (benchPublisher) Publish(cloudevents.Event) error { return nil }

func newBenchCE(b *testing.B) cloudevents.Event {
	b.Helper()
	ce := cloudevents.New()
	ce.SetID("bench-1")
	ce.SetType("bench.event")
	ce.SetSource("//bench/src")
	if err := ce.SetData(cloudevents.ApplicationJSON, []byte(`{"id":"x","value":1}`)); err != nil {
		b.Fatalf("set data: %v", err)
	}
	return ce
}

// BenchmarkMux_Dispatch measures the full happy-path dispatch:
// decode → filter match → middleware chain → handler → ack.
// No publisher is registered.
func BenchmarkMux_Dispatch(b *testing.B) {
	for _, n := range []int{0, 1, 3, 5} {
		b.Run(fmt.Sprintf("middlewares=%d", n), func(b *testing.B) {
			ce := newBenchCE(b)
			sub := &benchSubscription{ce: ce, n: b.N}
			mux := newMux(sub)
			mws := make([]Middleware, n)
			for i := range mws {
				mws[i] = passthroughMiddleware
			}
			mux.Use(mws...)
			mux.Register(nil, staticFilter{match: true}, &benchPayload{},
				func(context.Context, any) (json.RawMessage, error) { return nil, nil })

			b.ReportAllocs()
			b.ResetTimer()
			if err := mux.Run(context.Background()); err != nil {
				b.Fatalf("Run: %v", err)
			}
		})
	}
}

// BenchmarkMux_DispatchWithPublish measures dispatch + publish-result, which
// exercises the metadata propagation and CloudEvent build paths.
func BenchmarkMux_DispatchWithPublish(b *testing.B) {
	ce := newBenchCE(b)
	sub := &benchSubscription{ce: ce, n: b.N}
	mux := newMux(sub)
	pub := benchPublisher{}
	mux.Register(pub, staticFilter{match: true}, &benchPayload{},
		func(context.Context, any) (json.RawMessage, error) {
			return json.RawMessage(`{"ok":true}`), nil
		})

	b.ReportAllocs()
	b.ResetTimer()
	if err := mux.Run(context.Background()); err != nil {
		b.Fatalf("Run: %v", err)
	}
}

// BenchmarkMux_NoRouteFound measures the cheap path: message in, no route
// matches, ack. Useful to verify the no-match path is allocation-free.
func BenchmarkMux_NoRouteFound(b *testing.B) {
	ce := newBenchCE(b)
	sub := &benchSubscription{ce: ce, n: b.N}
	mux := newMux(sub)
	mux.Register(nil, staticFilter{match: false}, &benchPayload{},
		func(context.Context, any) (json.RawMessage, error) { return nil, nil })

	b.ReportAllocs()
	b.ResetTimer()
	if err := mux.Run(context.Background()); err != nil {
		b.Fatalf("Run: %v", err)
	}
}
