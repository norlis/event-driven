package eventmux

import (
	"context"
	"encoding/json/jsontext"
	"slices"
	"testing"
)

func tag(name string, trace *[]string) Middleware {
	return func(next HandlerFunc) HandlerFunc {
		return func(ctx context.Context, data any) (jsontext.Value, error) {
			*trace = append(*trace, name)
			return next(ctx, data)
		}
	}
}

// The first middleware passed to Chain (and to Use) is the outermost: it runs
// first on the way in. The README and Use's doc depend on this order.
func TestChainOrder(t *testing.T) {
	t.Parallel()

	var trace []string
	h := Chain(func(context.Context, any) (jsontext.Value, error) { trace = append(trace, "handler"); return nil, nil }, tag("A", &trace), tag("B", &trace))
	if _, err := h(t.Context(), nil); err != nil {
		t.Fatal(err)
	}
	if want := []string{"A", "B", "handler"}; !slices.Equal(trace, want) {
		t.Errorf("order = %v, want %v", trace, want)
	}
}
