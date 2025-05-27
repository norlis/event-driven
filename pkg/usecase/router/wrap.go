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

		result := make(chan HandlerResult, 1)

		go func() {
			output, err := fn(ctx, casted)
			result <- HandlerResult{Output: output, Err: err}
		}()

		go func() {
			defer func() {
				if r := recover(); r != nil {
					result <- HandlerResult{Output: nil, Err: fmt.Errorf("pánico en handler: %v", r)}
				}
			}()
			output, err := fn(ctx, casted) // Llamar al handler de negocio original
			result <- HandlerResult{Output: output, Err: err}
		}()

		select {
		case <-ctx.Done():
			// El contexto fue cancelado. El handler original puede seguir ejecutándose en su goroutine.
			// Devolvemos el error de cancelación.
			return nil, fmt.Errorf("handler execution cancelled by context: %w", ctx.Err())
		case res := <-result:
			// El handler original completó.
			return res.Output, res.Err
		}
	}
}
