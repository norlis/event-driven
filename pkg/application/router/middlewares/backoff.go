package middlewares

import (
	"context"
	"encoding/json"
	"strconv"
	"time"

	"github.com/norlis/event-driven/pkg/application/router"
	"github.com/norlis/event-driven/pkg/application/router/metadata"
)

// ExponentialBackoff returns a middleware that sleeps before executing the handler
// based on the retry count stored in metadata. First attempt has no delay.
func ExponentialBackoff(baseDelay, maxDelay time.Duration) router.Middleware {
	return func(next router.HandlerFunc) router.HandlerFunc {
		return func(ctx context.Context, data any) (json.RawMessage, error) {
			store, ok := metadata.FromContext(ctx)
			if !ok {
				return next(ctx, data)
			}

			retryStr := store.Get("retrycount")
			if retryStr == "" || retryStr == "1" {
				return next(ctx, data)
			}

			retry, _ := strconv.Atoi(retryStr)
			delay := min(
				// 2^(n-2) * base
				baseDelay*time.Duration(1<<(retry-2)), maxDelay)

			select {
			case <-time.After(delay):
				return next(ctx, data)
			case <-ctx.Done():
				return nil, ctx.Err()
			}
		}
	}
}
