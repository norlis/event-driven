package domain

import "errors"

var (
	ErrSubscriptionFailed = errors.New("failed to start subscription")
	ErrPublicationFailed  = errors.New("failed to publish message")
	ErrFilterFailed       = errors.New("failed to evaluate filter expression")
	ErrHandlerFailed      = errors.New("handler execution failed")
	ErrNoRouteMatched     = errors.New("message does not match any route")
)

// NonRetryableError wraps an error that should not be retried.
// In Pub/Sub: the EventMux will Ack (discard) instead of Nack (redeliver).
// In HTTP: the HTTPRetryBackoff middleware will stop retrying immediately.
type NonRetryableError struct {
	Err error
}

func (e *NonRetryableError) Error() string { return e.Err.Error() }
func (e *NonRetryableError) Unwrap() error { return e.Err }

// NewNonRetryableError wraps err as non-retryable.
func NewNonRetryableError(err error) *NonRetryableError {
	return &NonRetryableError{Err: err}
}
