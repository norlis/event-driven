package example

import (
	"encoding/json"
	"go.uber.org/zap"
)

type UseCaseExample interface {
	Execute(event map[string]any) (json.RawMessage, error)
}

func NewHandler(logger *zap.Logger) UseCaseExample {
	return &handler{logger}
}

type handler struct {
	logger *zap.Logger
}

func (h *handler) Execute(event map[string]any) (json.RawMessage, error) {

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
	h.logger.Info("Processing event", zap.Any("event", event))

	return []byte(`ok`), nil
}
