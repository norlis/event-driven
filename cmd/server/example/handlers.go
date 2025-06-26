package example

import (
	"context"
	"encoding/json"
	"errors"
	"github.com/norlis/event-driven/pkg/application/router/metadata"

	"go.uber.org/zap"
)

type Person struct {
	Name string `name:"name" validate:"required" json:"name"`
	Age  int    `name:"age" validate:"required" json:"age"`
}

var ErrInvalidObject = errors.New("objeto inválido")
var ErrDataNotFound = errors.New("datos no encontrados pero no es crítico")

type UseCaseExample interface {
	Execute(ctx context.Context, event Person) (json.RawMessage, error)
	Command(ctx context.Context, event Person) (json.RawMessage, error)
}

func NewHandler(logger *zap.Logger) UseCaseExample {
	return &handler{logger}
}

type handler struct {
	logger *zap.Logger
}

func (h *handler) Execute(ctx context.Context, event Person) (json.RawMessage, error) {

	if envelope, ok := metadata.FromContext(ctx); ok {
		envelope.Set("eventName", "test")
	}

	h.logger.Info("Processing event from sub", zap.Any("event", event))

	//panic("test")

	return []byte(`{"success": true}`), nil

	// no publish
	//return nil, nil

	//err := errors.New("ErrValidate")
	//h.logger.Error("ErrValidate", zap.Error(err))
	//
	//return nil, errors.New("no data")
}

func (h *handler) Command(ctx context.Context, evt Person) (json.RawMessage, error) {
	evt.Age = 10
	h.logger.Info("Processing event from command", zap.Any("event", evt))

	if envelope, ok := metadata.FromContext(ctx); ok {
		envelope.Set("eventName", "test")
		envelope.Set("name", evt.Name)
	}

	return json.Marshal(evt)
}
