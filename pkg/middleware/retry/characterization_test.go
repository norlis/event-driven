package retry

import (
	"bytes"
	"context"
	"encoding/json/jsontext"
	"errors"
	"log/slog"
	"strings"
	"testing"
	"time"

	cloudevents "github.com/cloudevents/sdk-go/v2/event"

	"github.com/norlis/event-driven/v2/pkg/event"
	"github.com/norlis/event-driven/v2/pkg/kit/logfields"
)

func TestBackoffReturnsResultOnceAHandlerAttemptSucceeds(t *testing.T) {
	t.Parallel()

	calls := 0
	h := Backoff(time.Nanosecond, time.Microsecond, 3)(func(context.Context, any) (jsontext.Value, error) {
		calls++
		if calls < 3 {
			return nil, errors.New("transient")
		}
		return jsontext.Value(`{"ok":true}`), nil
	})
	out, err := h(t.Context(), nil)
	if err != nil || string(out) != `{"ok":true}` {
		t.Fatalf("got %s, %v; want ok", out, err)
	}
	if calls != 3 {
		t.Errorf("calls = %d, want 3", calls)
	}
}

func TestBackoffReturnsContextErrorWhileWaiting(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithCancel(t.Context())
	calls := 0
	h := Backoff(time.Hour, time.Hour, 3)(func(context.Context, any) (jsontext.Value, error) {
		calls++
		cancel() // cancelled before the first sleep
		return nil, errors.New("transient")
	})
	if _, err := h(ctx, nil); !errors.Is(err, context.Canceled) {
		t.Fatalf("err = %v, want context.Canceled", err)
	}
	if calls != 1 {
		t.Errorf("calls = %d, want 1", calls)
	}
}

func TestBackoffLogsOneWarnPerRetryWithLogger(t *testing.T) {
	t.Parallel()

	var logs bytes.Buffer
	logger := slog.New(slog.NewJSONHandler(&logs, nil))
	boom := errors.New("boom")
	h := Backoff(time.Nanosecond, time.Microsecond, 2, WithLogger(logger))(func(context.Context, any) (jsontext.Value, error) { return nil, boom })
	_, _ = h(t.Context(), nil)

	lines := strings.Split(strings.TrimSpace(logs.String()), "\n")
	if len(lines) != 2 {
		t.Fatalf("got %d records, want 2 (one per retry, none for the final failure):\n%s", len(lines), logs.String())
	}
	for i, line := range lines {
		for _, want := range []string{`"level":"WARN"`, `"msg":"retrying handler"`, `"log.logger":"retry"`, `"` + logfields.KeyRetryAttempt + `":` + string(rune('1'+i)), `"` + logfields.KeyRetryDelay + `":`, `"error.message":"boom"`} {
			if !strings.Contains(line, want) {
				t.Errorf("record %d lacks %s:\n%s", i+1, want, line)
			}
		}
	}
}

func TestBackoffSilentWithoutLogger(t *testing.T) {
	t.Parallel()

	h := Backoff(time.Nanosecond, time.Microsecond, 1)(func(context.Context, any) (jsontext.Value, error) { return nil, errors.New("boom") })
	_, _ = h(t.Context(), nil) // must not panic on a nil logger
}

func TestDelayForDoublesUntilMaxDelay(t *testing.T) {
	t.Parallel()

	base, maxDelay := 100*time.Millisecond, time.Second
	want := []time.Duration{100 * time.Millisecond, 200 * time.Millisecond, 400 * time.Millisecond, 800 * time.Millisecond, time.Second, time.Second}
	for attempt, w := range want {
		if got := delayFor(base, maxDelay, attempt); got != w {
			t.Errorf("delayFor(attempt=%d) = %s, want %s", attempt, got, w)
		}
	}
}

func TestJitteredStaysWithin20PercentAndMaxDelay(t *testing.T) {
	t.Parallel()

	delay, maxDelay := 100*time.Millisecond, 110*time.Millisecond
	for range 1000 {
		got := jittered(delay, maxDelay)
		if got < delay || got > maxDelay {
			t.Fatalf("jittered() = %s, want within [%s, %s]", got, delay, maxDelay)
		}
	}
	for range 1000 {
		if got := jittered(delay, time.Hour); got < delay || got > delay+delay/5 {
			t.Fatalf("jittered() = %s, want within [%s, %s]", got, delay, delay+delay/5)
		}
	}
}

func TestSkipOnRedeliveryRunsOnceWhenBrokerRedelivers(t *testing.T) {
	t.Parallel()

	for _, tt := range []struct {
		name      string
		count     uint64
		wantCalls int
	}{
		{name: "count unknown", count: 0, wantCalls: 3},
		{name: "first delivery", count: 1, wantCalls: 3},
		{name: "redelivery", count: 2, wantCalls: 1},
	} {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			msg := event.NewMessage(t.Context(), cloudevents.New(), event.Delivery{Count: tt.count})
			calls := 0
			h := Backoff(time.Nanosecond, time.Microsecond, 2, SkipOnRedelivery())(func(context.Context, any) (jsontext.Value, error) {
				calls++
				return nil, errors.New("transient")
			})
			_, _ = h(msg.Context(), nil)
			if calls != tt.wantCalls {
				t.Errorf("calls = %d, want %d", calls, tt.wantCalls)
			}
		})
	}
}
