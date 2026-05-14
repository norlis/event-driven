package example

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"

	"github.com/norlis/event-driven/pkg/eventmux/metadata"
)

// Person is the demo domain object the handlers receive.
type Person struct {
	Name string `json:"name" validate:"required"`
	Age  int    `json:"age"  validate:"required"`
}

// Sentinel errors raised by the demo handlers; used as inputs to the skiperr
// middleware so we can showcase ignoring specific failures without nacking.
var (
	ErrInvalidObject = errors.New("invalid object")
	ErrDataNotFound  = errors.New("data not found but not critical")
)

// UseCase is the application logic invoked by every route in this example.
// Execute is wired to Pub/Sub routes; Command is wired to HTTP routes.
type UseCase interface {
	Execute(ctx context.Context, evt Person) (json.RawMessage, error)
	Command(ctx context.Context, evt Person) (json.RawMessage, error)
}

type handler struct {
	logger *slog.Logger
}

// NewHandler builds the demo UseCase implementation.
func NewHandler(logger *slog.Logger) UseCase {
	return &handler{logger: logger}
}

func (h *handler) Execute(ctx context.Context, evt Person) (json.RawMessage, error) {
	evt.Age += 10
	if store, ok := metadata.FromContext(ctx); ok {
		store.Set("eventName", "execute")
	}
	h.logger.Info("Processing Pub/Sub event", slog.Any("event", evt))
	return json.Marshal(evt) //nolint:wrapcheck
}

func (h *handler) Command(ctx context.Context, evt Person) (json.RawMessage, error) {
	if store, ok := metadata.FromContext(ctx); ok {
		store.Set("eventName", "command")
		store.Set("name", evt.Name)
	}
	h.logger.Info("Processing HTTP command", slog.Any("event", evt))
	data, err := json.Marshal(evt)
	if err != nil {
		return nil, fmt.Errorf("marshal person: %w", err)
	}
	return data, nil
}
