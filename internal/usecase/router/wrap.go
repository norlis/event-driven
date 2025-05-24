package router

import "fmt"

func WrapHandler[T any](fn func(T) (any, error)) HandlerFunc {
	return func(data any) (any, error) {
		casted, ok := data.(T)
		if !ok {
			return nil, fmt.Errorf("handler: expected %T but got %T", *new(T), data)
		}
		return fn(casted)
	}
}
