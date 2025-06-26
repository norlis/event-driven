package example

import (
	"context"
	"encoding/json"
	"errors"
	"github.com/norlis/event-driven/pkg/usecase/router/metadata"
	"time"

	"go.uber.org/zap"
)

type Person struct {
	Name string `name:"name" validate:"required"`
	Age  int    `name:"age" validate:"required"`
}

var ErrInvalidObject = errors.New("objeto inválido")
var ErrDataNotFound = errors.New("datos no encontrados pero no es crítico")

type UseCaseExample interface {
	Execute(ctx context.Context, event Person) (json.RawMessage, error)
}

func NewHandler(logger *zap.Logger) UseCaseExample {
	return &handler{logger}
}

type handler struct {
	logger *zap.Logger
}

func (h *handler) Execute(ctx context.Context, event Person) (json.RawMessage, error) {

	/*
		tracer := otel.Tracer("event-router-clean/handler1")      // Usar un nombre de instrumentación
		handlerCtx, span := tracer.Start(ctx, "handler1.Process") // Crear un span hijo
		defer span.End()

		// Usar el contexto del mensaje si es necesario: data.Msg.Context()
		// O el contexto global pasado al handler si se prefiere.
		select {
		case <-handlerCtx.Done():
			logger.Warn("[CuentasBancarias Handler] Contexto cancelado antes de procesar", zap.String("eventId", data.Header.EventId))
			return nil, handlerCtx.Err()
		default:
			// Continuar procesamiento
		}

		logger.Info("[CuentasBancarias Handler] Procesando evento", zap.String("eventId", data.Header.EventId), zap.Any("body", data.Body))

	*/

	if envelope, ok := metadata.FromContext(ctx); ok {
		envelope.Set("eventName", "test")
	}

	select {
	case <-time.After(15 * time.Second):
		h.logger.Info("Processing event", zap.Any("event", event))
		return []byte(`{"success": true}`), ErrInvalidObject
	case <-ctx.Done():
		h.logger.Info("HANDLER: Sleep interrumpido por cancelación de contexto")
		return nil, ctx.Err()
	}

	//time.Sleep(15 * time.Second)
	//h.logger.Info("Processing event", zap.Any("event", event))
	//
	//return []byte(`{"success": true}`), nil

	//err := errors.New("ErrValidate")
	//h.logger.Error("ErrValidate", zap.Error(err))
	//
	//return nil, errors.New("no data")
}
