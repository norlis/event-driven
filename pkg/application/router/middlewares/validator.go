package middlewares

import (
	"context"
	"encoding/json"

	"github.com/go-playground/validator/v10"
	"github.com/norlis/event-driven/pkg/application/router"
	"go.uber.org/zap"
)

// ValidationError es un error personalizado para fallos de validación.
type ValidationError struct {
	OriginalError error
}

func (v ValidationError) Error() string {
	return "validation failed: " + v.OriginalError.Error()
}

// ValidationMiddleware crea un middleware que valida la estructura de datos de entrada.
func ValidationMiddleware(logger *zap.Logger) router.Middleware {
	validate := validator.New()
	log := logger.Named("validation-middleware")

	return func(next router.HandlerFunc) router.HandlerFunc {
		return func(ctx context.Context, data any) (json.RawMessage, error) {
			// El router ya ha parseado 'data' a un struct.
			err := validate.Struct(data)
			if err != nil {
				// Si la validación falla, no llamamos al siguiente handler.
				// Envolvemos el error para poder identificarlo más tarde si es necesario.
				log.Warn("Input validation failed", zap.Any("data", data), zap.Error(err))
				return nil, ValidationError{OriginalError: err}
			}

			// Si la validación es exitosa, continuamos con el handler principal.
			return next(ctx, data)
		}
	}
}
