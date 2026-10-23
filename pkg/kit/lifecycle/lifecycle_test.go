package lifecycle

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"
)

func TestCloserCallsOnce(t *testing.T) {
	t.Parallel()
	var calls atomic.Int32
	c := Closer(func() error { calls.Add(1); return errBoom })
	if err := c.Stop(t.Context()); !errors.Is(err, errBoom) {
		t.Fatalf("first Stop() error = %v, want boom", err)
	}
	if err := c.Stop(t.Context()); err != nil {
		t.Fatalf("second Stop() error = %v, want nil", err)
	}
	if n := calls.Load(); n != 1 {
		t.Errorf("fn called %d times, want 1", n)
	}
}

func TestCloserDeadline(t *testing.T) {
	t.Parallel()
	synctest.Test(t, func(t *testing.T) {
		release := make(chan struct{})
		c := Closer(func() error { <-release; return nil })
		ctx, cancel := context.WithTimeout(t.Context(), 20*time.Millisecond)
		defer cancel()
		if err := c.Stop(ctx); !errors.Is(err, context.DeadlineExceeded) {
			t.Errorf("Stop() error = %v, want DeadlineExceeded", err)
		}
		close(release)
		synctest.Wait()
	})
}

func TestCloserStartNoop(t *testing.T) {
	t.Parallel()
	var calls atomic.Int32
	c := Closer(func() error { calls.Add(1); return nil })
	if err := c.Start(t.Context(), nil); err != nil {
		t.Fatalf("Start() error = %v", err)
	}
	if n := calls.Load(); n != 0 {
		t.Errorf("Start called fn %d times", n)
	}
}
