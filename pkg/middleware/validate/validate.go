package validate

import (
	"context"
	"encoding/json"
	"log/slog"

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
// go-playground/validator struct tags.
func New(logger *slog.Logger) eventmux.Middleware {
	v := validator.New()
	log := logger.With(slog.String("logger", "validation-middleware"))

	return func(next eventmux.HandlerFunc) eventmux.HandlerFunc {
		return func(ctx context.Context, data any) (json.RawMessage, error) {
			if err := v.Struct(data); err != nil {
				log.Warn("Input validation failed",
					slog.Any("data", data),
					slog.Any("error", err),
				)
				return nil, Error{OriginalError: err}
			}
			return next(ctx, data)
		}
	}
}
