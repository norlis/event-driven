package validate

import (
	"context"
	"encoding/json"

	"github.com/go-playground/validator/v10"
	"github.com/norlis/event-driven/pkg/eventmux"
	"go.uber.org/zap"
)

// Error es un error personalizado para fallos de validación.
type Error struct {
	OriginalError error
}

func (v Error) Error() string {
	return "validation failed: " + v.OriginalError.Error()
}

// New crea un middleware que valida la estructura de datos de entrada.
func New(logger *zap.Logger) eventmux.Middleware {
	validate := validator.New()
	log := logger.Named("validation-middleware")

	return func(next eventmux.HandlerFunc) eventmux.HandlerFunc {
		return func(ctx context.Context, data any) (json.RawMessage, error) {
			// El router ya ha parseado 'data' a un struct.
			err := validate.Struct(data)
			if err != nil {
				// Si la validación falla, no llamamos al siguiente handler.
				// Envolvemos el error para poder identificarlo más tarde si es necesario.
				log.Warn("Input validation failed", zap.Any("data", data), zap.Error(err))
				return nil, Error{OriginalError: err}
			}

			// Si la validación es exitosa, continuamos con el handler principal.
			return next(ctx, data)
		}
	}
}
