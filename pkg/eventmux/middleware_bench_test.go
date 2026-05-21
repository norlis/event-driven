package eventmux

import (
	"context"
	"encoding/json"
	"fmt"
	"testing"
)

// passthroughMiddleware is a zero-logic wrapper: it just calls next.
// It isolates the cost of one additional stack frame from any real work.
func passthroughMiddleware(next HandlerFunc) HandlerFunc {
	return func(ctx context.Context, data any) (json.RawMessage, error) {
		return next(ctx, data)
	}
}

// terminalHandler does nothing and allocates nothing.
var terminalHandler HandlerFunc = func(context.Context, any) (json.RawMessage, error) {
	return nil, nil
}

func BenchmarkChain_Invocation(b *testing.B) {
	// Each sub-benchmark builds the chain once (build cost excluded by
	// ResetTimer) and then measures only invocation cost.
	for _, n := range []int{0, 1, 3, 5, 10} {
		b.Run(fmt.Sprintf("middlewares=%d", n), func(b *testing.B) {
			mws := make([]Middleware, n)
			for i := range mws {
				mws[i] = passthroughMiddleware
			}
			handler := Chain(terminalHandler, mws...)

			ctx := context.Background()
			b.ReportAllocs()
			b.ResetTimer()
			for range b.N {
				_, _ = handler(ctx, nil)
			}
		})
	}
}

func BenchmarkChain_Build(b *testing.B) {
	// Measures the cost of building the chain (function allocations), which
	// happens once per dispatch in mux.processAndHandle.
	for _, n := range []int{0, 1, 3, 5, 10} {
		b.Run(fmt.Sprintf("middlewares=%d", n), func(b *testing.B) {
			mws := make([]Middleware, n)
			for i := range mws {
				mws[i] = passthroughMiddleware
			}

			b.ReportAllocs()
			b.ResetTimer()
			for range b.N {
				_ = Chain(terminalHandler, mws...)
			}
		})
	}
}
