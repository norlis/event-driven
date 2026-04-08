package httpdriven

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"time"

	cloudevents "github.com/cloudevents/sdk-go/v2/event"
	cehttp "github.com/cloudevents/sdk-go/v2/protocol/http"
	"github.com/google/uuid"
	"github.com/norlis/event-driven/pkg/domain/event"
	"github.com/norlis/event-driven/pkg/port"
	"github.com/norlis/httpgate/pkg/adapter/apidriven/middleware"
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
		ce, err := h.extractCloudEvent(r)
		if err != nil {
			messageID := uuid.NewString()
			h.logger.Error("Failed to extract CloudEvent from request", zap.Error(err), zap.String("path", h.config.Pattern))
			NewResponseBuilder().
				WithId(messageID, uuid.New().String(), uuid.New().String()).
				WithInstance(r.Pattern).
				WithError("Failed to read request body", "001").
				WithStatus(http.StatusInternalServerError).
				Build().
				Json(w, r)
			return
		}

		messageID := ce.ID()
		h.logger.Debug("Received HTTP message",
			zap.String("id", messageID),
			zap.Int("payloadSize", len(ce.Data())),
			zap.String("path", h.config.Pattern),
		)

		msg := event.NewMessageWithoutAck(*ce)
		preflightResultChan := make(chan error, 1)
		msg.SetPreflightCallback(func(err error) {
			preflightResultChan <- err
		})

		handler(msg)

		select {
		case err = <-preflightResultChan:
			if err != nil {
				if h.errorResponder.Respond(w, r, err, h.logger, messageID) {
					return
				}
			}
		case <-time.After(5 * time.Second):
			h.logger.Error("Timeout waiting for router preflight result", zap.String("id", messageID))
			NewResponseBuilder().
				WithId(messageID, uuid.New().String(), uuid.New().String()).
				WithError("Internal processing timeout", "TIMEOUT").
				WithStatus(http.StatusInternalServerError).
				Build().Json(w, r)
			return
		}

		NewResponseBuilder().
			WithId(messageID, uuid.New().String(), uuid.New().String()).
			Build().Json(w, r)
	}
}

// extractCloudEvent tries the CloudEvents SDK first (supports binary + structured content modes),
// then falls back to building a CloudEvent manually for plain HTTP clients.
func (h *HttpSubscriber) extractCloudEvent(r *http.Request) (*cloudevents.Event, error) {
	// Try CloudEvents SDK: handles both binary (Ce-* headers) and structured
	// (Content-Type: application/cloudevents+json) modes automatically.
	if ce, err := cehttp.NewEventFromHTTPRequest(r); err == nil {
		return ce, nil
	}

	// Fallback: plain HTTP request without CloudEvents headers.
	body, err := io.ReadAll(r.Body)
	if err != nil {
		return nil, fmt.Errorf("read request body: %w", err)
	}
	defer func(Body io.ReadCloser) {
		_ = Body.Close()
	}(r.Body)

	messageID := r.Header.Get("X-Message-UUID")
	if messageID == "" {
		messageID = uuid.NewString()
	}

	ce := cloudevents.New()
	ce.SetID(messageID)
	ce.SetSpecVersion("1.0")
	ce.SetType("com.example.http.command")
	ce.SetSource(fmt.Sprintf("%s//%s%s", r.Proto, r.Host, r.URL.Path))
	ce.SetTime(time.Now())
	_ = ce.SetData(cloudevents.ApplicationJSON, body)

	return &ce, nil
}

func (h *HttpSubscriber) Start(ctx context.Context, handler func(msg *event.Message)) error {
	h.config.Logger.Info("HttpSubscriber Start")
	h.server.HandleFunc(h.config.Pattern, h.Handler(handler))
	return nil
}
