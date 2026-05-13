package retry

import (
	"context"
	"encoding/json"
	"errors"
	"math/rand/v2"
	"time"

	"github.com/norlis/event-driven/pkg/event"
	"github.com/norlis/event-driven/pkg/eventmux"
)

// HTTPBackoff retries the handler up to maxRetries times with exponential backoff
// and jitter. Designed for HTTP subscribers where the client is waiting for a response.
//
// Non-retryable errors (domain.NonRetryable) break the retry loop immediately.
// The jitter prevents thundering herd when many requests fail simultaneously.
func HTTPBackoff(baseDelay, maxDelay time.Duration, maxRetries int) eventmux.Middleware {
	return func(next eventmux.HandlerFunc) eventmux.HandlerFunc {
		return func(ctx context.Context, data any) (json.RawMessage, error) {
			var lastErr error

			for attempt := range maxRetries + 1 {
				result, err := next(ctx, data)
				if err == nil {
					return result, nil
				}
				lastErr = err

				// Non-retryable error → stop immediately (e.g. validation, bad payload).
				if _, ok := errors.AsType[*event.NonRetryable](err); ok {
					return nil, err
				}

				if attempt == maxRetries {
					break
				}

				delay := baseDelay * time.Duration(1<<attempt) // 2^attempt * base
				// Add up to 20% jitter to avoid thundering herd.
				jitter := time.Duration(rand.Float64() * float64(delay) * 0.2) //nolint:gosec // jitter doesn't need crypto rand
				delay = min(delay+jitter, maxDelay)

				select {
				case <-time.After(delay):
				case <-ctx.Done():
					return nil, ctx.Err()
				}
			}

			return nil, lastErr
		}
	}
}
