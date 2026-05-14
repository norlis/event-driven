// Package event defines the Message type that flows through the event mux
// together with sentinel errors and the NonRetryableError marker used by
// retry-aware middlewares and the mux to decide between Ack and Nack.
package event

import "errors"

// Sentinel errors emitted by the eventmux pipeline. Consumers can match on
// these with errors.Is to drive retry / acking decisions.
var (
	// ErrSubscriptionFailed indicates the subscription source could not be started.
	ErrSubscriptionFailed = errors.New("failed to start subscription")
	// ErrPublicationFailed indicates a publisher returned an error.
	ErrPublicationFailed = errors.New("failed to publish message")
	// ErrFilterFailed indicates a filter expression could not be evaluated.
	ErrFilterFailed = errors.New("failed to evaluate filter expression")
	// ErrHandlerFailed indicates the user handler returned an error.
	ErrHandlerFailed = errors.New("handler execution failed")
	// ErrNoRoute indicates no registered route matched the incoming message.
	ErrNoRoute = errors.New("message does not match any route")
)

// NonRetryableError wraps an error that should not be retried.
//
//   - In Pub/Sub: the mux will Ack (discard) instead of Nack (redeliver).
//   - In HTTP retry middleware: the loop stops immediately.
type NonRetryableError struct {
	Err error
}

// Error implements error.
func (e *NonRetryableError) Error() string { return e.Err.Error() }

// Unwrap exposes the underlying error for errors.Is / errors.As.
func (e *NonRetryableError) Unwrap() error { return e.Err }

// NewNonRetryableError wraps err as non-retryable.
func NewNonRetryableError(err error) *NonRetryableError {
	return &NonRetryableError{Err: err}
}
