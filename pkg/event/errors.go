// Package event defines the Message type that flows through the event mux
// together with sentinel errors and the NonRetryableError marker used by
// retry-aware middlewares and the mux to decide between Ack and Nack.
package event

import "errors"

// Sentinel errors emitted by the eventmux pipeline. Consumers can match on
// these with errors.Is to drive retry / acking decisions.
var (
	// ErrPublicationFailed indicates a publisher returned an error.
	ErrPublicationFailed = errors.New("failed to publish message")
	// ErrNoRoute indicates no registered route matched the incoming message.
	ErrNoRoute = errors.New("message does not match any route")
	// ErrDecode indicates the payload could not be decoded into the route's type.
	ErrDecode = errors.New("payload decode failed")
	// ErrPanic wraps the value recovered from a panicking preflight or handler.
	ErrPanic = errors.New("handler panicked")
	// ErrAlreadyStarted is returned by Start on a mux or a Subscription that is
	// already running. It lives here so transports never import eventmux.
	ErrAlreadyStarted = errors.New("event: already started")
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
