package httpdriven

import (
	"context"
	"io"
	"net/http"
	"time"

	"github.com/norlis/event-driven/pkg/domain/event"
	"github.com/norlis/event-driven/pkg/port"
	"github.com/norlis/httpgate/pkg/middleware"

	"github.com/google/uuid"
	"go.uber.org/zap"
)

type SubscriberConfig struct {
	Pattern    string
	Logger     *zap.Logger
	Middleware middleware.Middleware
}

type HttpSubscriber struct {
	server         *http.ServeMux
	config         SubscriberConfig
	logger         *zap.Logger
	errorResponder *HTTPErrorResponder
}

func NewSubscriber(server *http.ServeMux, cfg SubscriberConfig) (port.Subscription, error) {

	return &HttpSubscriber{
		server:         server,
		config:         cfg,
		logger:         cfg.Logger,
		errorResponder: NewErrorResponder(),
	}, nil
}

func (h *HttpSubscriber) Handler(handler func(msg *event.Message)) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		//reqCtx := r.Context()
		body, err := io.ReadAll(r.Body)

		messageUUID := r.Header.Get("X-Message-UUID")
		if messageUUID == "" {
			messageUUID = uuid.NewString()
		}

		if err != nil {
			h.logger.Error("Failed to read request body", zap.Error(err), zap.String("path", h.config.Pattern))
			NewResponseBuilder().
				WithId(messageUUID, uuid.New().String(), uuid.New().String()).
				WithInstance(r.Pattern).
				WithError("Failed to read request body", "001").
				WithStatus(http.StatusInternalServerError).
				Build().
				Json(w, r)
			return
		}
		defer func(Body io.ReadCloser) {
			_ = Body.Close()
		}(r.Body)

		h.logger.Debug(
			"Received HTTP message via shared Mux",
			zap.String("uuid", messageUUID),
			zap.Int("payloadSize", len(body)),
			zap.String("path", h.config.Pattern),
		)

		msg := event.NewNewMessageWithoutAck(messageUUID, body, map[string]string{})
		//msg := domain.NewNewMessageWithoutAck(messageUUID, body, headersAsMetadata)
		preflightResultChan := make(chan error, 1)
		msg.SetPreflightCallback(func(err error) {
			preflightResultChan <- err
		})

		handler(msg)

		select {
		case err = <-preflightResultChan:
			if err != nil {
				if h.errorResponder.Respond(w, r, err, h.logger, messageUUID) {
					return // La respuesta de error ya fue enviada.
				}
			}
		case <-time.After(5 * time.Second):
			h.logger.Error("Timeout esperando el resultado del pre-vuelo del router", zap.String("uuid", messageUUID))
			NewResponseBuilder().
				WithId(messageUUID, uuid.New().String(), uuid.New().String()).
				WithError("Internal processing timeout", "TIMEOUT").
				WithStatus(http.StatusInternalServerError).
				Build().Json(w, r)
			return
		}

		NewResponseBuilder().
			WithId(messageUUID, uuid.New().String(), uuid.New().String()).
			Build().Json(w, r)

		//processingErr := msg.ReportedError()

		//if errors.Is(processingErr, domain.ErrNoRouteMatched) {
		//	h.logger.Warn("Request rejected, no matching route found for command",
		//		zap.String("uuid", messageUUID),
		//		zap.String("path", h.config.Pattern),
		//	)
		//	NewResponseBuilder().
		//		WithId(messageUUID, uuid.New().String(), uuid.New().String()).
		//		WithInstance(r.URL.Path).
		//		WithError("Command or event type not supported", "CMD-001").
		//		WithStatus(http.StatusBadRequest).
		//		Build().
		//		Json(w, r)
		//	return
		//}
		//
		//NewResponseBuilder().
		//	WithId(messageUUID, uuid.New().String(), uuid.New().String()).
		//	WithInstance(r.Pattern).
		//	Build().
		//	Json(w, r)
	}
}

func (h *HttpSubscriber) Start(ctx context.Context, handler func(msg *event.Message)) error {
	h.config.Logger.Info("HttpSubscriber Start")
	//fn := h.Handler(handler)
	//if h.config.Middleware != nil {
	//	fn = h.config.Middleware(fn)
	//}
	h.server.HandleFunc(h.config.Pattern, h.Handler(handler))
	return nil
}
