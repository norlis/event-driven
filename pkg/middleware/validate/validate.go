// Package validate provides an eventmux middleware that runs
// go-playground/validator on the decoded payload before reaching the handler.
package validate

import (
	"context"
	"encoding/json"

	"github.com/go-playground/validator/v10"

	"github.com/norlis/event-driven/pkg/eventmux"
)

// Error wraps a validator failure so callers can identify validation errors
// downstream (e.g. for HTTP 400 mapping).
type Error struct {
	OriginalError error
}

func (v Error) Error() string {
	return "validation failed: " + v.OriginalError.Error()
}

// New returns a middleware that validates the decoded payload using
// go-playground/validator struct tags. Validation failures are not logged
// here: the error propagates enriched (validate.Error) and the router emits
// the single "preflight failed" record.
func New() eventmux.Middleware {
	v := validator.New()
	return func(next eventmux.HandlerFunc) eventmux.HandlerFunc {
		return func(ctx context.Context, data any) (json.RawMessage, error) {
			if err := v.Struct(data); err != nil {
				return nil, Error{OriginalError: err}
			}
			return next(ctx, data)
		}
	}
}
