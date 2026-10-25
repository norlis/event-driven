// Package eventhttp provides an HTTP transport for eventmux: a synchronous
// Subscriber that accepts CloudEvents (binary, structured, or plain JSON)
// over HTTP, and a Publisher that forwards CloudEvents to an HTTP endpoint
// using the CloudEvents binary content mode.
package eventhttp

import (
	"errors"
	"net/http"

	cloudevents "github.com/cloudevents/sdk-go/v2/event"

	"github.com/norlis/event-driven/v2/pkg/event"
)

// ClientError is implemented by errors whose message is safe to show to the
// caller (validation failures, for instance). The default rules answer 400
// VALIDATION-FAILED with ClientDetail.
type ClientError interface {
	error
	ClientDetail() string
}

// ErrorRule maps an error predicate to an HTTP response shape (status, code,
// detail). Rules are evaluated in order; the first match wins.
type ErrorRule struct {
	Match      func(err error) bool
	StatusCode int
	ErrorCode  ErrorCode
	DetailFunc func(err error) string
}

// ErrorResponder applies a chain of ErrorRule against the error that made
// the router Nack or Discard a message and writes the matching HTTP response.
type ErrorResponder struct {
	rules []ErrorRule
}

// NewErrorResponder returns an ErrorResponder that evaluates extra (caller
// rules) before the defaults: ClientError, "no matching route" and
// undecodable payloads, all 400.
func NewErrorResponder(extra ...ErrorRule) *ErrorResponder {
	defaults := []ErrorRule{
		NewRuleForTypeWithDetail[ClientError](http.StatusBadRequest, CodeValidationFailed,
			func(e ClientError) string { return e.ClientDetail() }),
		NewRuleForValue(event.ErrNoRoute, http.StatusBadRequest, CodeRouteNotFound,
			"Command or event type not supported"),
		NewRuleForValue(event.ErrDecode, http.StatusBadRequest, CodeInvalidPayload,
			"Payload could not be decoded"),
	}
	rules := make([]ErrorRule, 0, len(extra)+len(defaults))
	rules = append(rules, extra...)
	rules = append(rules, defaults...)
	return &ErrorResponder{rules: rules}
}

// Respond writes the response for the first matching rule and returns true.
// It deliberately does not log: the router already emitted the single
// record for this error.
func (e *ErrorResponder) Respond(w http.ResponseWriter, r *http.Request, err error, ce cloudevents.Event, instance string) bool {
	for _, rule := range e.rules {
		if !rule.Match(err) {
			continue
		}
		newResponse(ce, instance, rule.StatusCode, rule.ErrorCode, rule.DetailFunc(err)).JSON(w)
		return true
	}
	return false
}

// NewRuleForType matches errors of concrete type T (errors.As semantics)
// and answers a fixed, client-safe detail.
func NewRuleForType[T error](statusCode int, code ErrorCode, detail string) ErrorRule {
	return NewRuleForTypeWithDetail[T](statusCode, code, func(T) string { return detail })
}

// NewRuleForTypeWithDetail is NewRuleForType with a detail derived from the
// matched error. Use it only for errors whose message is meant for clients.
func NewRuleForTypeWithDetail[T error](statusCode int, code ErrorCode, detail func(T) string) ErrorRule {
	return ErrorRule{
		Match: func(err error) bool {
			_, ok := errors.AsType[T](err)
			return ok
		},
		StatusCode: statusCode,
		ErrorCode:  code,
		DetailFunc: func(err error) string {
			e, _ := errors.AsType[T](err)
			return detail(e)
		},
	}
}

// NewRuleForValue matches a sentinel error (errors.Is semantics) with a
// fixed detail string.
func NewRuleForValue(targetErr error, statusCode int, code ErrorCode, detail string) ErrorRule {
	return ErrorRule{
		Match:      func(err error) bool { return errors.Is(err, targetErr) },
		StatusCode: statusCode,
		ErrorCode:  code,
		DetailFunc: func(error) string { return detail },
	}
}
