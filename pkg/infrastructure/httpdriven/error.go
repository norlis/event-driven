package httpdriven

import (
	"errors"
	"net/http"

	"github.com/google/uuid"
	"github.com/norlis/event-driven/pkg/domain"
	"github.com/norlis/event-driven/pkg/usecase/router/middlewares"
	"go.uber.org/zap"
)

type ErrorRule struct {
	Match      func(err error) bool
	StatusCode int
	ErrorCode  string
	LogMessage string
	DetailFunc func(err error) string
}

type HTTPErrorResponder struct {
	rules []ErrorRule
}

func NewErrorResponder() *HTTPErrorResponder {
	return &HTTPErrorResponder{
		rules: []ErrorRule{
			NewRuleForType[middlewares.ValidationError](
				http.StatusBadRequest,
				"VALIDATION-FAILED",
				"Request rejected due to validation error",
			),
			NewRuleForValue(
				domain.ErrNoRouteMatched,
				http.StatusBadRequest,
				"ROUTE-NOT-FOUND",
				"Request rejected, no matching route found",
				"Command or event type not supported", // Mensaje de detalle fijo
			),
		},
	}
}

func (e *HTTPErrorResponder) Respond(w http.ResponseWriter, r *http.Request, err error, log *zap.Logger, msgID string) bool {
	for _, rule := range e.rules {
		if !rule.Match(err) {
			continue
		}

		log.Warn(rule.LogMessage, zap.Error(err), zap.String("messageUUID", msgID))

		NewResponseBuilder().
			WithId(msgID, uuid.New().String(), uuid.New().String()).
			WithError(rule.DetailFunc(err), rule.ErrorCode).
			WithStatus(rule.StatusCode).
			Build().Json(w, r)

		return true
	}
	return false
}

func NewRuleForType[T error](statusCode int, code, logMsg string) ErrorRule {
	return ErrorRule{
		Match: func(err error) bool {
			var target T
			return errors.As(err, &target)
		},
		StatusCode: statusCode,
		ErrorCode:  code,
		LogMessage: logMsg,
		DetailFunc: func(err error) string { return err.Error() },
	}
}

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
