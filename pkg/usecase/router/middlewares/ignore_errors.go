package middlewares

import (
	"context"
	"encoding/json"
	"github.com/norlis/event-driven/pkg/usecase/router"
	"github.com/pkg/errors"
)

type IgnoreErrors struct {
	ignoredErrors map[string]struct{}
}

// NewIgnoreErrors creates a new IgnoreErrors middleware.
func NewIgnoreErrors(errs []error) IgnoreErrors {
	errsMap := make(map[string]struct{}, len(errs))

	for _, err := range errs {
		errsMap[err.Error()] = struct{}{}
	}

	return IgnoreErrors{errsMap}
}

// Middleware returns the IgnoreErrors middleware.
//
//	middlewares.NewIgnoreErrors([]error{
//				validations.ErrInvalidObject,
//				validations.ErrInvalidValidations,
//			}).Middleware
func (i IgnoreErrors) Middleware(next router.HandlerFunc) router.HandlerFunc {
	return func(ctx context.Context, msg any) (json.RawMessage, error) {
		events, err := next(ctx, msg)
		if err != nil {
			if _, ok := i.ignoredErrors[errors.Cause(err).Error()]; ok {
				return events, nil
			}
			return events, err
		}
		return events, nil
	}
}
