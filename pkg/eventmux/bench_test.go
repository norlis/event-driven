package eventmux_test

import (
	"context"
	"encoding/json/jsontext"
	"log/slog"
	"testing"

	cloudevents "github.com/cloudevents/sdk-go/v2/event"

	"github.com/norlis/event-driven/v2/pkg/event"
	"github.com/norlis/event-driven/v2/pkg/eventmux"
	"github.com/norlis/event-driven/v2/pkg/filter/cefilter"
)

// benchSubscription hands the mux handler back to the benchmark so messages
// can be pushed without a broker.
type benchSubscription struct{ handler func(*event.Message) }

func (s *benchSubscription) Start(_ context.Context, h func(*event.Message), _ func(error)) error {
	s.handler = h
	return nil
}

func (s *benchSubscription) Stop(context.Context) error { return nil }

type noopPublisher struct{}

func (noopPublisher) Publish(context.Context, cloudevents.Event) error { return nil }

type benchPayload struct {
	Name string `json:"name"`
	Age  int    `json:"age"`
}

// newBenchHandler builds a mux with one typed route (INFO logger that
// discards, no Observer) and returns the per-message entry point.
func newBenchHandler(b *testing.B, pub eventmux.Publisher) func(*event.Message) {
	b.Helper()
	sub := &benchSubscription{}
	mux, err := eventmux.New(eventmux.Config{Subscription: sub, Logger: slog.New(slog.DiscardHandler)})
	if err != nil {
		b.Fatal(err)
	}
	mux.Handle(pub, cefilter.ByType("bench.event"), func(context.Context, benchPayload) (jsontext.Value, error) {
		return jsontext.Value(`{"ok":true}`), nil
	})
	if err := mux.Start(context.Background(), nil); err != nil {
		b.Fatal(err)
	}
	b.Cleanup(func() { _ = mux.Stop(context.Background()) })
	return sub.handler
}

func benchEvent(b *testing.B) cloudevents.Event {
	b.Helper()
	ce := cloudevents.New()
	ce.SetID("1")
	ce.SetType("bench.event")
	ce.SetSource("bench")
	if err := ce.SetData(cloudevents.ApplicationJSON, []byte(`{"name":"ada","age":36}`)); err != nil {
		b.Fatal(err)
	}
	return ce
}

// BenchmarkHandleNoPublish is the critical path for a terminal route: decode,
// preflight, handler, ack, one INFO record.
func BenchmarkHandleNoPublish(b *testing.B) {
	handle := newBenchHandler(b, nil)
	ce := benchEvent(b)
	b.ReportAllocs()
	for b.Loop() {
		handle(event.NewMessage(context.Background(), ce, event.Delivery{}))
	}
}

// BenchmarkHandlePublish adds the result event: metadata store, result
// CloudEvent, extension propagation and Publisher.Publish.
func BenchmarkHandlePublish(b *testing.B) {
	handle := newBenchHandler(b, noopPublisher{})
	ce := benchEvent(b)
	b.ReportAllocs()
	for b.Loop() {
		handle(event.NewMessage(context.Background(), ce, event.Delivery{}))
	}
}
