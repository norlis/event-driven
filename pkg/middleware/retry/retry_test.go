package retry

import (
	"context"
	"encoding/json"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/norlis/event-driven/pkg/event"
)

// counterHandler returns a handler that records every call and returns the
// programmed sequence of (result, error). After the last entry, the final value
// is repeated.
func counterHandler(results []struct {
	out json.RawMessage
	err error
}) (func(context.Context, any) (json.RawMessage, error), *atomic.Int32) {
	var calls atomic.Int32
	return func(_ context.Context, _ any) (json.RawMessage, error) {
		n := int(calls.Add(1)) - 1
		if n >= len(results) {
			n = len(results) - 1
		}
		return results[n].out, results[n].err
	}, &calls
}

func TestRetry_SuccessOnFirstAttempt(t *testing.T) {
	t.Parallel()

	want := json.RawMessage(`{"ok":true}`)
	h, calls := counterHandler([]struct {
		out json.RawMessage
		err error
	}{{out: want, err: nil}})

	mw := HTTPBackoff(time.Millisecond, 10*time.Millisecond, 3)
	out, err := mw(h)(context.Background(), nil)
	if err != nil {
		t.Fatalf("err = %v, want nil", err)
	}
	if string(out) != string(want) {
		t.Errorf("out = %s, want %s", out, want)
	}
	if calls.Load() != 1 {
		t.Errorf("handler called %d times, want 1", calls.Load())
	}
}

func TestRetry_SuccessAfterTransientFailures(t *testing.T) {
	t.Parallel()

	want := json.RawMessage(`{"ok":true}`)
	h, calls := counterHandler([]struct {
		out json.RawMessage
		err error
	}{
		{nil, errors.New("transient 1")},
		{nil, errors.New("transient 2")},
		{want, nil},
	})

	mw := HTTPBackoff(time.Millisecond, 10*time.Millisecond, 3)
	out, err := mw(h)(context.Background(), nil)
	if err != nil {
		t.Fatalf("err = %v, want nil", err)
	}
	if string(out) != string(want) {
		t.Errorf("out = %s, want %s", out, want)
	}
	if calls.Load() != 3 {
		t.Errorf("handler called %d times, want 3", calls.Load())
	}
}

func TestRetry_ExhaustsRetriesAndReturnsLastErr(t *testing.T) {
	t.Parallel()

	last := errors.New("final boom")
	h, calls := counterHandler([]struct {
		out json.RawMessage
		err error
	}{
		{nil, errors.New("boom-1")},
		{nil, errors.New("boom-2")},
		{nil, last},
	})

	mw := HTTPBackoff(time.Millisecond, 10*time.Millisecond, 2) // maxRetries=2 → 3 attempts total
	_, err := mw(h)(context.Background(), nil)
	if !errors.Is(err, last) {
		t.Errorf("err = %v, want %v", err, last)
	}
	if calls.Load() != 3 {
		t.Errorf("handler called %d times, want 3", calls.Load())
	}
}

func TestRetry_NonRetryableShortCircuits(t *testing.T) {
	t.Parallel()

	inner := errors.New("bad payload")
	nonRetryable := event.NewNonRetryableError(inner)

	h, calls := counterHandler([]struct {
		out json.RawMessage
		err error
	}{{nil, nonRetryable}})

	mw := HTTPBackoff(time.Millisecond, 10*time.Millisecond, 5)
	_, err := mw(h)(context.Background(), nil)
	if !errors.Is(err, inner) {
		t.Errorf("err = %v, want unwrap to %v", err, inner)
	}
	if calls.Load() != 1 {
		t.Errorf("handler called %d times, want 1 (no retries)", calls.Load())
	}
}

func TestRetry_CtxCancelledDuringBackoff(t *testing.T) {
	t.Parallel()

	h, calls := counterHandler([]struct {
		out json.RawMessage
		err error
	}{{nil, errors.New("always fails")}})

	// Long base delay so the cancel fires inside the backoff select.
	mw := HTTPBackoff(time.Second, 10*time.Second, 5)
	ctx, cancel := context.WithCancel(context.Background())

	// Cancel after a few ms — enough for the first attempt to fail and the
	// goroutine to enter the backoff wait.
	go func() {
		time.Sleep(20 * time.Millisecond)
		cancel()
	}()

	_, err := mw(h)(ctx, nil)
	if !errors.Is(err, context.Canceled) {
		t.Errorf("err = %v, want context.Canceled", err)
	}
	if calls.Load() != 1 {
		t.Errorf("handler called %d times, want 1 (cancelled before retry)", calls.Load())
	}
}

func TestRetry_MaxRetriesZeroMeansSingleAttempt(t *testing.T) {
	t.Parallel()

	h, calls := counterHandler([]struct {
		out json.RawMessage
		err error
	}{{nil, errors.New("boom")}})

	mw := HTTPBackoff(time.Millisecond, 10*time.Millisecond, 0)
	_, err := mw(h)(context.Background(), nil)
	if err == nil {
		t.Fatal("expected error, got nil")
	}
	if calls.Load() != 1 {
		t.Errorf("handler called %d times, want 1", calls.Load())
	}
}