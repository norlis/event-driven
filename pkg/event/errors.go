package event

import "errors"

var (
	ErrSubscriptionFailed = errors.New("failed to start subscription")
	ErrPublicationFailed  = errors.New("failed to publish message")
	ErrFilterFailed       = errors.New("failed to evaluate filter expression")
	ErrHandlerFailed      = errors.New("handler execution failed")
	ErrNoRoute            = errors.New("message does not match any route")
)

// NonRetryable wraps an error that should not be retried.
// In Pub/Sub: the EventMux will Ack (discard) instead of Nack (redeliver).
// In HTTP: the HTTPRetryBackoff middleware will stop retrying immediately.
type NonRetryable struct {
	Err error
}

func (e *NonRetryable) Error() string { return e.Err.Error() }
func (e *NonRetryable) Unwrap() error { return e.Err }

// NewNonRetryable wraps err as non-retryable.
func NewNonRetryable(err error) *NonRetryable {
	return &NonRetryable{Err: err}
}
