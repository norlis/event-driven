// Package eventhttp provides an HTTP transport for eventmux: a synchronous
// Subscriber that accepts CloudEvents (binary, structured, or plain JSON)
// over HTTP, and a Publisher that forwards CloudEvents to an HTTP endpoint
// using the CloudEvents binary content mode.
package eventhttp

import (
	"errors"
	"log/slog"
	"net/http"

	"github.com/google/uuid"

	"github.com/norlis/event-driven/pkg/event"
	"github.com/norlis/event-driven/pkg/middleware/validate"
)

// ErrorRule maps an error predicate to an HTTP response shape (status, code,
// detail). Rules are evaluated in order; the first match wins.
type ErrorRule struct {
	Match      func(err error) bool
	StatusCode int
	ErrorCode  string
	LogMessage string
	DetailFunc func(err error) string
}

// ErrorResponder applies a chain of ErrorRule against handler errors and
// writes the matching HTTP response. The default ruleset covers validation
// errors (400) and "no matching route" (400).
type ErrorResponder struct {
	rules []ErrorRule
}

// NewErrorResponder returns an ErrorResponder with the default ruleset.
func NewErrorResponder() *ErrorResponder {
	return &ErrorResponder{
		rules: []ErrorRule{
			NewRuleForType[validate.Error](
				http.StatusBadRequest,
				"VALIDATION-FAILED",
				"Request rejected due to validation error",
			),
			NewRuleForValue(
				event.ErrNoRoute,
				http.StatusBadRequest,
				"ROUTE-NOT-FOUND",
				"Request rejected, no matching route found",
				"Command or event type not supported", // Mensaje de detalle fijo
			),
		},
	}
}

// Respond writes the response for the first matching rule and returns true.
// Returns false when no rule matches, leaving the caller to write its own
// response.
func (e *ErrorResponder) Respond(w http.ResponseWriter, r *http.Request, err error, log *slog.Logger, msgID string) bool {
	for _, rule := range e.rules {
		if !rule.Match(err) {
			continue
		}

		log.Warn(rule.LogMessage, slog.Any("error", err), slog.String("messageUUID", msgID))

		NewResponseBuilder().
			WithID(msgID, uuid.New().String(), uuid.New().String()).
			WithError(rule.DetailFunc(err), rule.ErrorCode).
			WithStatus(rule.StatusCode).
			Build().JSON(w, r)

		return true
	}
	return false
}

// NewRuleForType builds an ErrorRule that matches errors whose concrete type
// is T (using errors.As semantics).
func NewRuleForType[T error](statusCode int, code, logMsg string) ErrorRule {
	return ErrorRule{
		Match: func(err error) bool {
			_, ok := errors.AsType[T](err)
			return ok
		},
		StatusCode: statusCode,
		ErrorCode:  code,
		LogMessage: logMsg,
		DetailFunc: func(err error) string { return err.Error() },
	}
}

// NewRuleForValue builds an ErrorRule that matches against a specific sentinel
// error value (using errors.Is semantics) with a fixed detail string.
func NewRuleForValue(targetErr error, statusCode int, code, logMsg, detail string) ErrorRule {
	return ErrorRule{
		Match: func(err error) bool {
			return errors.Is(err, targetErr)
		},
		StatusCode: statusCode,
		ErrorCode:  code,
		LogMessage: logMsg,
		DetailFunc: func(err error) string { return detail },
	}
}
