// Package skiperr provides an eventmux middleware that swallows handler
// errors matching user-supplied predicates. Useful to treat specific
// domain errors as Ack-and-discard (e.g. validation, not-found).
package skiperr

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"

	"github.com/norlis/httpgate/logging"

	"github.com/norlis/event-driven/pkg/eventmux"
	"github.com/norlis/event-driven/pkg/kit/logfields"
)

// Predicate decides whether a handler error should be swallowed. Description
// is logged when the predicate fires.
type Predicate struct {
	Description string
	Matches     func(err error) bool
}

// Skipper is the middleware factory: New(...).Middleware satisfies
// eventmux.Middleware.
type Skipper struct {
	predicates []Predicate
	logger     *slog.Logger
}

// New creates a new Skipper middleware.
func New(logger *slog.Logger, predicates ...Predicate) Skipper {
	if logger == nil {
		logger = slog.New(slog.DiscardHandler)
	}
	return Skipper{
		predicates: predicates,
		logger:     logger.With(slog.String(logfields.KeyLogLogger, "skiperr")),
	}
}

// Middleware returns the Skipper middleware.
//
//	skiperr.New(
//	    logger,
//	    skiperr.ByErr("invalid-object", validations.ErrInvalidObject),
//	    skiperr.ByErr("invalid-validations", validations.ErrInvalidValidations),
//	    skiperr.ByType[validator.ValidationErrors]("validator-errors"),
//	).Middleware
func (i Skipper) Middleware(next eventmux.HandlerFunc) eventmux.HandlerFunc {
	return func(ctx context.Context, data any) (result json.RawMessage, err error) {
		result, err = next(ctx, data)
		if err != nil {
			for _, predicate := range i.predicates {
				if predicate.Matches(err) {
					// Skipper is exported with unexported fields, so a caller can
					// still build a zero-value Skipper{} without going through New,
					// leaving logger nil; New's guard alone can't cover that path.
					if i.logger != nil {
						i.logger.InfoContext(
							ctx,
							"error skipped",
							logging.Err(err),
							slog.String(logfields.KeyPredicate, predicate.Description),
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

// ByErr creates a predicate that ignores a specific sentinel error instance.
func ByErr(name string, targetErr error) Predicate {
	return Predicate{
		Description: fmt.Sprintf("Err: (%s)", targetErr.Error()),
		Matches: func(errToCheck error) bool {
			return errors.Is(errToCheck, targetErr)
		},
	}
}

// ByType creates a generic predicate that ignores any error of a specific type.
func ByType[T error](name string) Predicate {
	return Predicate{
		Description: fmt.Sprintf("ErrType: (%s)", name),
		Matches: func(errToCheck error) bool {
			_, ok := errors.AsType[T](errToCheck)
			return ok
		},
	}
}
