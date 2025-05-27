package middlewares

import (
	"context"
	"encoding/json"
	"errors"
	"github.com/norlis/event-driven/pkg/usecase/router"
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
		result, err = next(ctx, data)

		if err != nil {
			//rootCause := errors.Cause(err)
			for _, targetErr := range i.targetsToIgnore {
				if errors.Is(err, targetErr) {
					if i.logger != nil {
						i.logger.Info("Middleware IgnoreErrors: Error ignorado intencionalmente",
							zap.Error(err), // Loguear el error original (envuelto)
							//zap.String("causa_raiz_ignorada", rootCause.Error()),
							zap.String("target_ignorado", targetErr.Error()),
						)
					}
					return result, nil
				}
			}
			return result, err
		}
		return result, nil
	}
}
