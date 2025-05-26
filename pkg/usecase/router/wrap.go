package router

import (
	"encoding/json"
	"fmt"
)

func WrapHandler[T any](fn func(T) (json.RawMessage, error)) HandlerFunc {
	return func(data any) (json.RawMessage, error) {
		casted, ok := data.(T)
		if !ok {
			return nil, fmt.Errorf("handler: expected %T but got %T", *new(T), data)
		}
		return fn(casted)
	}
}
