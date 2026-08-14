// Package retry provides eventmux middlewares that re-invoke the handler on
// transient failures with exponential backoff. Non-retryable errors
// (event.NonRetryableError) short-circuit the loop.
package retry

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"math/rand/v2"
	"time"

	"github.com/norlis/httpgate/logging"

	"github.com/norlis/event-driven/pkg/event"
	"github.com/norlis/event-driven/pkg/eventmux"
	"github.com/norlis/event-driven/pkg/kit/logfields"
)

// Option configures the retry middlewares.
type Option func(*options)

type options struct {
	logger *slog.Logger
}

// WithLogger enables the per-retry WARN record ("retrying handler"). Without
// it the middleware stays silent, and the final error is logged once by the
// router when retries are exhausted.
func WithLogger(l *slog.Logger) Option {
	return func(o *options) { o.logger = l }
}

// HTTPBackoff retries the handler up to maxRetries times with exponential backoff
// and jitter. Designed for HTTP subscribers where the client is waiting for a response.
//
// Non-retryable errors (event.NonRetryableError) break the retry loop immediately.
// The jitter prevents thundering herd when many requests fail simultaneously.
func HTTPBackoff(baseDelay, maxDelay time.Duration, maxRetries int, opts ...Option) eventmux.Middleware {
	var o options
	for _, opt := range opts {
		opt(&o)
	}
	if o.logger != nil {
		o.logger = o.logger.With(slog.String(logfields.KeyLogLogger, "retry"))
	}

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
				if _, ok := errors.AsType[*event.NonRetryableError](err); ok {
					return nil, err
				}

				if attempt == maxRetries {
					break
				}

				delay := baseDelay * time.Duration(1<<attempt) // 2^attempt * base
				// Add up to 20% jitter to avoid thundering herd.
				jitter := time.Duration(rand.Float64() * float64(delay) * 0.2) //nolint:gosec // jitter doesn't need crypto rand
				delay = min(delay+jitter, maxDelay)

				if o.logger != nil {
					o.logger.WarnContext(
						ctx,
						"retrying handler",
						logging.Err(err),
						slog.Int(logfields.KeyRetryAttempt, attempt+1),
						slog.Duration(logfields.KeyRetryDelay, delay),
					)
				}

				select {
				case <-time.After(delay):
				case <-ctx.Done():
					return nil, ctx.Err()
				}
			}

			// Final exhausted-retries error is not logged here: it propagates
			// and the router logs it once.
			return nil, lastErr
		}
	}
}
