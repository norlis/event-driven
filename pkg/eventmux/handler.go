package eventmux

import (
	"context"
	"encoding/json"
	"fmt"
	"reflect"
)

// Wrap converts a typed handler into a HandlerFunc, performing the type
// assertion from any to T. The error message uses reflect.TypeFor so pointer
// and interface types are printed correctly (e.g. "*Foo" instead of "<nil>").
func Wrap[T any](fn func(context.Context, T) (json.RawMessage, error)) HandlerFunc {
	expected := reflect.TypeFor[T]()

	return func(ctx context.Context, data any) (json.RawMessage, error) {
		casted, ok := data.(T)
		if !ok {
			return nil, fmt.Errorf("handler: expected %s but got %T", expected, data)
		}

		return fn(ctx, casted)
	}
}
