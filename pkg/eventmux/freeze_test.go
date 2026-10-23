package eventmux

import (
	"context"
	"encoding/json/jsontext"
	"slices"
	"sync"
	"testing"

	"github.com/norlis/event-driven/v2/pkg/event"
)

func noop(context.Context, payload) (jsontext.Value, error) { return nil, nil }

func mustPanic(t *testing.T, want string, fn func()) {
	t.Helper()
	defer func() {
		r := recover()
		if r == nil {
			t.Fatalf("no panic, want %q", want)
		}
		if got, _ := r.(string); got != want {
			t.Fatalf("panic = %v, want %q", r, want)
		}
	}()
	fn()
}

func TestRegistrationAfterStartPanics(t *testing.T) {
	t.Parallel()

	mux, err := New(Config{Subscription: newFakeSubscription()})
	if err != nil {
		t.Fatal(err)
	}
	mux.Handle(nil, nil, noop)
	if err := mux.Start(t.Context(), nil); err != nil {
		t.Fatal(err)
	}
	mustPanic(t, "eventmux: Handle after Start", func() { mux.Handle(nil, nil, noop) })
	mustPanic(t, "eventmux: Use after Start", func() { mux.Use(tag("x", new([]string))) })
	mustPanic(t, "eventmux: UsePreflight after Start", func() { mux.UsePreflight(tag("x", new([]string))) })

	// Stop does not reopen registration: one rule, nothing after the first Start.
	if err := mux.Stop(context.Background()); err != nil {
		t.Fatal(err)
	}
	mustPanic(t, "eventmux: Handle after Start", func() { mux.Handle(nil, nil, noop) })
}

func TestDuplicateRoutePanics(t *testing.T) {
	t.Parallel()

	mux, err := New(Config{Subscription: newFakeSubscription()})
	if err != nil {
		t.Fatal(err)
	}
	pub := &capturePublisher{}
	var filter Filter = &matchAll{}
	mux.Handle(pub, filter, noop)
	mustPanic(t, "eventmux: duplicate route", func() { mux.Handle(pub, filter, noop) })
	// Same filter, different publisher: still unreachable, still a duplicate.
	mustPanic(t, "eventmux: duplicate route", func() { mux.Handle(&capturePublisher{}, filter, noop) })
	// nil filter (match all) twice is a duplicate whatever the publishers.
	mux.Handle(nil, nil, noop)
	mustPanic(t, "eventmux: duplicate route", func() { mux.Handle(pub, nil, noop) })
	// A different filter value is a distinct route. (&matchAll{} is zero-sized,
	// so two of them may share an address: use a filter with state.)
	mux.Handle(pub, &namedFilter{name: "other"}, noop)
}

// matchAll is a pointer filter: identity is the pointer, as for cefilter and
// jmespath filters.
type matchAll struct{}

func (*matchAll) Match(*event.Message) bool { return true }

// Middlewares registered before Start keep applying after Stop+Start, and
// the chain is the one built at Start: the trace shows middleware then handler.
func TestChainsSurviveRestart(t *testing.T) {
	t.Parallel()

	var trace []string
	sub := newFakeSubscription()
	mux, err := New(Config{Subscription: sub})
	if err != nil {
		t.Fatal(err)
	}
	mux.UsePreflight(tag("pre", &trace))
	mux.Use(tag("mw", &trace))
	mux.Handle(nil, nil, func(context.Context, payload) (jsontext.Value, error) {
		trace = append(trace, "handler")
		return nil, nil
	})
	for range 2 {
		if err := mux.Start(t.Context(), nil); err != nil {
			t.Fatal(err)
		}
		sub.handler(newIncoming(t, `{"name":"a"}`))
		if err := mux.Stop(context.Background()); err != nil {
			t.Fatal(err)
		}
	}
	if want := []string{"pre", "mw", "handler", "pre", "mw", "handler"}; !slices.Equal(trace, want) {
		t.Errorf("trace = %v, want %v", trace, want)
	}
}

// Run with -race: concurrent deliveries after Start must not race with the
// route table, which is read-only once frozen.
func TestConcurrentDeliveriesDoNotRace(t *testing.T) {
	t.Parallel()

	sub := newFakeSubscription()
	mux, err := New(Config{Subscription: sub})
	if err != nil {
		t.Fatal(err)
	}
	mux.Use(func(next HandlerFunc) HandlerFunc { return next })
	mux.Handle(nil, nil, noop)
	if err := mux.Start(t.Context(), nil); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = mux.Stop(context.Background()) })
	var wg sync.WaitGroup
	for range 50 {
		wg.Go(func() { sub.handler(newIncoming(t, `{"name":"a"}`)) })
	}
	wg.Wait()
}

// Chains are built once, at the first Start. Rebuilding them on a restart
// would race with a handler still running from before Stop returned.
func TestChainsAreBuiltOnce(t *testing.T) {
	t.Parallel()

	builds := 0
	counting := func(next HandlerFunc) HandlerFunc { builds++; return next }
	mux, err := New(Config{Subscription: newFakeSubscription()})
	if err != nil {
		t.Fatal(err)
	}
	mux.UsePreflight(counting)
	mux.Use(counting)
	mux.Handle(nil, nil, noop)
	for range 2 {
		if err := mux.Start(t.Context(), nil); err != nil {
			t.Fatal(err)
		}
		if err := mux.Stop(context.Background()); err != nil {
			t.Fatal(err)
		}
	}
	if builds != 2 {
		t.Errorf("middleware wrapped %d times, want 2 (preflight + one route, once)", builds)
	}
}

// namedFilter is a pointer filter with state, so two instances never share
// an address the way two zero-sized &matchAll{} may.
type namedFilter struct{ name string }

func (*namedFilter) Match(*event.Message) bool { return true }
