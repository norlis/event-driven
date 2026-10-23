package lifecycle

import (
	"context"
	"errors"
	"testing"
	"time"
)

// A failure a component reports while the group is stopping it (a consumer
// the drain closes) is part of the shutdown, not a fatal.
func TestFatalDuringStopIsDropped(t *testing.T) {
	t.Parallel()
	rec := &recorder{}
	fs := fakes(rec, "a", "b")
	fs[1].fatalOnStop = errors.New("consume stopped unexpectedly")
	g := NewGroup(nil, time.Second)
	for _, f := range fs {
		g.Add(f.name, f)
	}
	fatal := make(chan error, 1)
	if err := g.Start(t.Context(), func(err error) { fatal <- err }); err != nil {
		t.Fatal(err)
	}
	if err := g.Stop(t.Context()); err != nil {
		t.Fatalf("Stop() = %v", err)
	}
	select {
	case err := <-fatal:
		t.Fatalf("onFatal(%v) fired for a failure reported during Stop", err)
	default:
	}
}

// An onFatal that stops the group synchronously from inside a component's
// Start must not deadlock: Start stops the component that had just started
// and reports ErrStoppedDuringStart.
func TestStopFromOnFatalDuringStartDoesNotDeadlock(t *testing.T) {
	t.Parallel()
	rec := &recorder{}
	fs := fakes(rec, "a", "b")
	fs[0].fatalOnStart = errors.New("boom")
	g := NewGroup(nil, time.Second)
	for _, f := range fs {
		g.Add(f.name, f)
	}
	done := make(chan error, 1)
	go func() {
		done <- g.Start(context.Background(), func(error) { _ = g.Stop(context.Background()) })
	}()
	select {
	case err := <-done:
		if !errors.Is(err, ErrStoppedDuringStart) {
			t.Fatalf("Start() = %v, want ErrStoppedDuringStart", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Start deadlocked on an onFatal that calls Stop")
	}
	if got := rec.entries(); len(got) != 2 || got[0] != "start a" || got[1] != "stop a" {
		t.Fatalf("log = %q, want [start a, stop a]: b must never start", got)
	}
	if err := g.Stop(t.Context()); err != nil {
		t.Fatalf("Stop after an aborted Start = %v", err)
	}
}
