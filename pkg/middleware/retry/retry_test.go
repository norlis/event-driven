package retry

import (
	"context"
	"encoding/json/jsontext"
	"errors"
	"math"
	"testing"
	"time"

	"github.com/norlis/event-driven/v2/pkg/event"
)

func TestBackoffRetriesThenReturnsLastError(t *testing.T) {
	t.Parallel()

	calls := 0
	boom := errors.New("boom")
	h := Backoff(time.Nanosecond, time.Microsecond, 3)(func(context.Context, any) (jsontext.Value, error) { calls++; return nil, boom })
	if _, err := h(t.Context(), nil); !errors.Is(err, boom) {
		t.Fatalf("err = %v, want boom", err)
	}
	if calls != 4 {
		t.Errorf("calls = %d, want 4 (1 + 3 retries)", calls)
	}
}

func TestBackoffStopsOnNonRetryable(t *testing.T) {
	t.Parallel()

	calls := 0
	h := Backoff(time.Nanosecond, time.Microsecond, 5)(func(context.Context, any) (jsontext.Value, error) {
		calls++
		return nil, event.NewNonRetryableError(errors.New("bad input"))
	})
	_, _ = h(t.Context(), nil)
	if calls != 1 {
		t.Errorf("calls = %d, want 1", calls)
	}
}

// Large bases and many attempts must never wrap around to a negative delay.
func TestBackoffDelayNeverOverflows(t *testing.T) {
	t.Parallel()

	const maxDelay = 3 * time.Second
	for attempt := range 70 {
		d := delayFor(time.Hour, maxDelay, attempt)
		if d < 0 || d > maxDelay {
			t.Fatalf("attempt %d: delay = %v, want 0 <= d <= %v", attempt, d, maxDelay)
		}
	}
	if d := delayFor(100*time.Millisecond, maxDelay, 2); d != 400*time.Millisecond {
		t.Errorf("delayFor(100ms, 3s, 2) = %v, want 400ms", d)
	}
}

// A negative maxRetries would run zero attempts and ack without calling the
// handler; a non-positive delay would spin. Both are programming errors.
func TestBackoffRejectsInvalidConfig(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name           string
		base, maxDelay time.Duration
		maxRetries     int
	}{
		{name: "negative maxRetries", base: time.Millisecond, maxDelay: time.Second, maxRetries: -1},
		{name: "zero baseDelay", base: 0, maxDelay: time.Second, maxRetries: 1},
		{name: "maxDelay below baseDelay", base: time.Second, maxDelay: time.Millisecond, maxRetries: 1},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			defer func() {
				if r := recover(); r == nil {
					t.Fatal("Backoff did not panic")
				}
			}()
			Backoff(tt.base, tt.maxDelay, tt.maxRetries)
		})
	}
}

// With no effective cap (maxDelay near MaxInt64) delay+jitter must not wrap
// around to a negative duration.
func TestJitterNeverOverflows(t *testing.T) {
	t.Parallel()

	const maxDelay = time.Duration(math.MaxInt64)
	for range 100 {
		d := jittered(maxDelay, maxDelay)
		if d < 0 || d > maxDelay {
			t.Fatalf("jittered(max, max) = %v, want 0 <= d <= max", d)
		}
	}
	for range 100 {
		d := jittered(time.Second, time.Minute)
		if d < time.Second || d > 1200*time.Millisecond {
			t.Fatalf("jittered(1s, 1m) = %v, want 1s <= d <= 1.2s", d)
		}
	}
}
