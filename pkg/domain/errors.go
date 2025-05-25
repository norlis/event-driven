package domain

import "errors"

var (
    ErrSubscriptionFailed = errors.New("failed to start subscription")
    ErrPublicationFailed  = errors.New("failed to publish message")
    ErrFilterFailed       = errors.New("failed to evaluate filter expression")
    ErrHandlerFailed      = errors.New("handler execution failed")
)
