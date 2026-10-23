package natsconn

import (
	"context"
	"errors"
	"strings"
	"testing"
	"testing/synctest"
	"time"

	natsgo "github.com/nats-io/nats.go"

	"github.com/norlis/event-driven/v2/pkg/kit/lifecycle"
)

var (
	_       lifecycle.Component = (*Conn)(nil)
	errBoom                     = errors.New("boom")
)

func TestConfigDefaults(t *testing.T) {
	t.Parallel()
	got := Config{}.withDefaults()
	if got.URL != natsgo.DefaultURL || got.MaxReconnects != -1 {
		t.Errorf("Config{}.withDefaults() = %+v, want DefaultURL and -1", got)
	}
	kept := Config{URL: "nats://x:1", MaxReconnects: 3}.withDefaults()
	if kept.URL != "nats://x:1" || kept.MaxReconnects != 3 {
		t.Errorf("explicit values changed: %+v", kept)
	}
}

func TestWaitClosedReturnsOnClose(t *testing.T) {
	t.Parallel()
	synctest.Test(t, func(t *testing.T) {
		closed := make(chan struct{})
		drain := func() error {
			go func() {
				time.Sleep(20 * time.Millisecond)
				close(closed)
			}()
			return nil
		}
		start := time.Now()
		if err := waitClosed(t.Context(), drain, closed); err != nil {
			t.Fatalf("waitClosed() error = %v", err)
		}
		if elapsed := time.Since(start); elapsed != 20*time.Millisecond {
			t.Errorf("waitClosed returned after %v, want 20ms", elapsed)
		}
	})
}

func TestWaitClosedDeadline(t *testing.T) {
	t.Parallel()
	synctest.Test(t, func(t *testing.T) {
		ctx, cancel := context.WithTimeout(t.Context(), 20*time.Millisecond)
		defer cancel()
		err := waitClosed(ctx, func() error { return nil }, make(chan struct{}))
		if !errors.Is(err, context.DeadlineExceeded) {
			t.Errorf("waitClosed() error = %v, want DeadlineExceeded", err)
		}
	})
}

func TestWaitClosedDrainError(t *testing.T) {
	t.Parallel()
	err := waitClosed(t.Context(), func() error { return errBoom }, make(chan struct{}))
	if !errors.Is(err, errBoom) {
		t.Errorf("waitClosed() error = %v, want boom", err)
	}
}

func TestDialUnreachable(t *testing.T) {
	t.Parallel()
	_, err := Dial(Config{URL: "nats://user:secret@127.0.0.1:1"}, nil)
	if err == nil {
		t.Fatal("Dial() to a closed port error = nil")
	}
	if !strings.Contains(err.Error(), "127.0.0.1:1") || strings.Contains(err.Error(), "secret") {
		t.Errorf("Dial() error = %q, want the redacted URL", err)
	}
}

// A connection the server already closed (auth error, reconnects exhausted)
// has nothing left to drain: stopping it is not a failure.
func TestWaitClosedAlreadyClosed(t *testing.T) {
	t.Parallel()
	closed := make(chan struct{})
	close(closed)
	err := waitClosed(t.Context(), func() error { return natsgo.ErrConnectionClosed }, closed)
	if err != nil {
		t.Fatalf("waitClosed() on a closed connection error = %v, want nil", err)
	}
}

// nats.go closes a reconnecting connection itself and reports
// ErrConnectionReconnecting; the close still completes.
func TestWaitClosedWhileReconnecting(t *testing.T) {
	t.Parallel()
	closed := make(chan struct{})
	drain := func() error {
		close(closed)
		return natsgo.ErrConnectionReconnecting
	}
	if err := waitClosed(t.Context(), drain, closed); err != nil {
		t.Fatalf("waitClosed() while reconnecting error = %v, want nil", err)
	}
}
