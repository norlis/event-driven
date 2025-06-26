package router

import (
	"context"
	"encoding/json"
	"fmt"
)

type HandlerResult struct {
	Output json.RawMessage
	Err    error
}

func WrapHandler[T any](fn func(context.Context, T) (json.RawMessage, error)) HandlerFunc {
	return func(ctx context.Context, data any) (json.RawMessage, error) {
		casted, ok := data.(T)
		if !ok {
			return nil, fmt.Errorf("handler: expected %T but got %T", *new(T), data)
		}

		return fn(ctx, casted)
	}
}
