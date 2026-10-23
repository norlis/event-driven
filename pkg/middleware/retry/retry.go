// Package retry provides an eventmux middleware that re-invokes the handler
// on transient failures with exponential backoff and jitter. Non-retryable
// errors (event.NonRetryableError) short-circuit the loop.
package retry

import (
	"context"
	"encoding/json/jsontext"
	"errors"
	"fmt"
	"log/slog"
	"math/rand/v2"
	"time"

	"github.com/norlis/httpgate/logging"

	"github.com/norlis/event-driven/v2/pkg/event"
	"github.com/norlis/event-driven/v2/pkg/eventmux"
	"github.com/norlis/event-driven/v2/pkg/kit/logfields"
)

// Option configures the retry middlewares.
type Option func(*options)

type options struct {
	logger           *slog.Logger
	skipOnRedelivery bool
}

// WithLogger enables the per-retry WARN record ("retrying handler"). Without
// it the middleware stays silent, and the final error is logged once by the
// router when retries are exhausted.
func WithLogger(l *slog.Logger) Option {
	return func(o *options) { o.logger = l }
}

// SkipOnRedelivery runs the handler once, without in-process retries, when the
// broker reports that the message is being redelivered (event.DeliveryCount
// from the handler context > 1). It keeps the in-process retries from
// multiplying with the broker's own: the first delivery gets the quick
// retries, later ones are already the broker retrying. Transports that report
// no delivery count are unaffected.
func SkipOnRedelivery() Option {
	return func(o *options) { o.skipOnRedelivery = true }
}

// Backoff retries the handler up to maxRetries times with exponential backoff
// and jitter, for any transport whose caller is waiting (HTTP) or where a
// quick in-process retry is cheaper than a broker redelivery.
//
// Non-retryable errors (event.NonRetryableError) break the retry loop immediately.
func Backoff(baseDelay, maxDelay time.Duration, maxRetries int, opts ...Option) eventmux.Middleware {
	// A negative maxRetries would run zero attempts and ack without calling
	// the handler; a non-positive or inverted delay would spin.
	if maxRetries < 0 || baseDelay <= 0 || maxDelay < baseDelay {
		panic(fmt.Sprintf("retry: invalid config: baseDelay=%s maxDelay=%s maxRetries=%d", baseDelay, maxDelay, maxRetries))
	}
	var o options
	for _, opt := range opts {
		opt(&o)
	}
	if o.logger != nil {
		o.logger = logfields.Component(o.logger, "retry")
	}
	policy := backoff{base: baseDelay, maxDelay: maxDelay, maxRetries: maxRetries, options: o}

	return func(next eventmux.HandlerFunc) eventmux.HandlerFunc {
		return func(ctx context.Context, data any) (jsontext.Value, error) {
			return policy.run(ctx, data, next)
		}
	}
}

// backoff is the fixed retry policy of one middleware instance.
type backoff struct {
	options
	base       time.Duration
	maxDelay   time.Duration
	maxRetries int
}

// run calls next until it succeeds, returns a non-retryable error, exhausts
// the retries or ctx ends while waiting. The exhausted-retries error is not
// logged here: it propagates and the router logs it once.
func (b backoff) run(ctx context.Context, data any, next eventmux.HandlerFunc) (jsontext.Value, error) {
	retries := b.maxRetries
	if b.skipOnRedelivery && redelivered(ctx) {
		retries = 0
	}
	for attempt := 0; ; attempt++ {
		result, err := next(ctx, data)
		if err == nil {
			return result, nil
		}
		if attempt == retries || isNonRetryable(err) {
			return nil, err
		}
		if werr := b.wait(ctx, attempt, err); werr != nil {
			return nil, werr
		}
	}
}

// wait sleeps the jittered delay for attempt (logging it when configured)
// and reports ctx's error when the context ends first.
func (b backoff) wait(ctx context.Context, attempt int, cause error) error {
	delay := jittered(delayFor(b.base, b.maxDelay, attempt), b.maxDelay)
	if b.logger != nil {
		b.logger.WarnContext(ctx, "retrying handler",
			logging.Err(cause),
			slog.Int(logfields.KeyRetryAttempt, attempt+1),
			slog.Duration(logfields.KeyRetryDelay, delay),
		)
	}
	select {
	case <-time.After(delay):
		return nil
	case <-ctx.Done():
		return ctx.Err() //nolint:wrapcheck // the handler reports the bare cancellation, as before; the router logs it
	}
}

func isNonRetryable(err error) bool {
	_, ok := errors.AsType[*event.NonRetryableError](err)
	return ok
}

// redelivered reports whether the broker says this is not the first delivery.
func redelivered(ctx context.Context) bool {
	count, known := event.DeliveryCountFromContext(ctx)
	return known && count > 1
}

// delayFor is baseDelay·2^attempt capped at maxDelay without overflowing:
// the shift is checked against maxDelay before it happens, and attempts past
// 62 bits saturate.
func delayFor(baseDelay, maxDelay time.Duration, attempt int) time.Duration {
	if attempt >= 62 || baseDelay > maxDelay>>attempt {
		return maxDelay
	}
	return baseDelay << attempt
}

// jittered adds up to 20% random jitter to delay without exceeding maxDelay.
// The headroom is computed first so delay+jitter cannot wrap around when
// maxDelay is near the Duration limit.
func jittered(delay, maxDelay time.Duration) time.Duration {
	jitter := time.Duration(rand.Float64() * float64(delay) * 0.2) //nolint:gosec // jitter doesn't need crypto rand
	if room := maxDelay - delay; jitter > room {
		jitter = room
	}
	return delay + jitter
}
