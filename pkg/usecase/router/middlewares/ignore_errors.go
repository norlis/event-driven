package middlewares

import (
	"context"
	"encoding/json"
	"github.com/norlis/event-driven/pkg/usecase/router"
	"github.com/pkg/errors"
	"go.uber.org/zap"
)

type IgnoreErrors struct {
	targetsToIgnore []error
	logger          *zap.Logger
}

// NewIgnoreErrors creates a new IgnoreErrors middleware.
func NewIgnoreErrors(logger *zap.Logger, errsToIgnore ...error) IgnoreErrors {
	return IgnoreErrors{
		targetsToIgnore: errsToIgnore, logger: logger.Named("IgnoreErrors")}
}

// Middleware returns the IgnoreErrors middleware.
//
//	middlewares.NewIgnoreErrors(
//				logger,
//				validations.ErrInvalidObject,
//				validations.ErrInvalidValidations,
//			).Middleware
func (i IgnoreErrors) Middleware(next router.HandlerFunc) router.HandlerFunc {
	return func(ctx context.Context, data any) (result json.RawMessage, err error) {
		// Llamar al siguiente handler en la cadena
		result, err = next(ctx, data)

		if err != nil {
			// Obtener la causa raíz del error si está envuelto
			rootCause := errors.Cause(err)
			for _, targetErr := range i.targetsToIgnore {
				if errors.Is(rootCause, targetErr) { // Usar errors.Is para una comparación robusta
					if i.logger != nil {
						i.logger.Info("Middleware IgnoreErrors: Error ignorado intencionalmente",
							zap.Error(err), // Loguear el error original (envuelto)
							zap.String("causa_raiz_ignorada", rootCause.Error()),
							zap.String("target_ignorado", targetErr.Error()),
						)
					}
					// Error está en la lista de ignorados, se retorna nil como error,
					// pero se mantiene el resultado original del handler (result).
					return result, nil
				}
			}
			// Si no está en la lista de ignorados, se retorna el error original
			return result, err
		}

		// Sin error, se retorna el resultado y nil como error
		return result, nil
	}
}
