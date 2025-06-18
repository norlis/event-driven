package middlewares

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/norlis/event-driven/pkg/usecase/router"
	"go.uber.org/zap"
)

type ErrorPredicate struct {
	Description string
	Matches     func(err error) bool
}

type IgnoreErrors struct {
	predicates []ErrorPredicate
	logger     *zap.Logger
}

// NewIgnoreErrors creates a new IgnoreErrors middleware.
func NewIgnoreErrors(logger *zap.Logger, predicates ...ErrorPredicate) IgnoreErrors {
	return IgnoreErrors{
		predicates: predicates,
		logger:     logger.Named("ignore-errors"),
	}
}

// Middleware returns the IgnoreErrors middleware.
// uso
//
//	middlewares.NewIgnoreErrors(
//				logger,
//				middlewares.IgnoreSpecificError(validations.ErrInvalidObject),
//				middlewares.IgnoreSpecificError(validations.ErrInvalidValidations),
//				middlewares.IgnoreErrorType[validator.ValidationErrors](),
//			).Middleware
func (i IgnoreErrors) Middleware(next router.HandlerFunc) router.HandlerFunc {
	return func(ctx context.Context, data any) (result json.RawMessage, err error) {
		result, err = next(ctx, data)

		if err != nil {
			for _, predicate := range i.predicates {
				if predicate.Matches(err) { // Llama a la función Matches del predicado
					if i.logger != nil {
						i.logger.Info("Middleware IgnoreErrors: Error ignorado por predicado",
							zap.Error(err),
							zap.String("predicate", predicate.Description),
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

// IgnoreSpecificError crea un predicado para ignorar una instancia de error centinela específica.
func IgnoreSpecificError(name string, targetErr error) ErrorPredicate {
	return ErrorPredicate{
		Description: fmt.Sprintf("Err: (%s)", targetErr.Error()),
		Matches: func(errToCheck error) bool {
			return errors.Is(errToCheck, targetErr)
		},
	}
}

// IgnoreErrorType crea un predicado genérico para ignorar cualquier error de un tipo específico.
func IgnoreErrorType[T error](name string) ErrorPredicate {
	return ErrorPredicate{
		Description: fmt.Sprintf("ErrType: (%s)", name),
		Matches: func(errToCheck error) bool {
			var target T
			return errors.As(errToCheck, &target)
		},
	}
}
