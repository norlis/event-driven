package skiperr

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/norlis/event-driven/pkg/eventmux"
	"go.uber.org/zap"
)

type Predicate struct {
	Description string
	Matches     func(err error) bool
}

type Skipper struct {
	predicates []Predicate
	logger     *zap.Logger
}

// New creates a new Skipper middleware.
func New(logger *zap.Logger, predicates ...Predicate) Skipper {
	return Skipper{
		predicates: predicates,
		logger:     logger.Named("ignore-errors"),
	}
}

// Middleware returns the Skipper middleware.
// uso
//
//	middlewares.New(
//				logger,
//				middlewares.ByErr(validations.ErrInvalidObject),
//				middlewares.ByErr(validations.ErrInvalidValidations),
//				middlewares.ByType[validator.ValidationErrors](),
//			).Middleware
func (i Skipper) Middleware(next eventmux.HandlerFunc) eventmux.HandlerFunc {
	return func(ctx context.Context, data any) (result json.RawMessage, err error) {
		result, err = next(ctx, data)
		if err != nil {
			for _, predicate := range i.predicates {
				if predicate.Matches(err) { // Llama a la función Matches del predicado
					if i.logger != nil {
						i.logger.Info("Middleware Skipper: Error ignorado por predicado",
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

// ByErr crea un predicado para ignorar una instancia de error centinela específica.
func ByErr(name string, targetErr error) Predicate {
	return Predicate{
		Description: fmt.Sprintf("Err: (%s)", targetErr.Error()),
		Matches: func(errToCheck error) bool {
			return errors.Is(errToCheck, targetErr)
		},
	}
}

// ByType crea un predicado genérico para ignorar cualquier error de un tipo específico.
func ByType[T error](name string) Predicate {
	return Predicate{
		Description: fmt.Sprintf("ErrType: (%s)", name),
		Matches: func(errToCheck error) bool {
			_, ok := errors.AsType[T](errToCheck)
			return ok
		},
	}
}
