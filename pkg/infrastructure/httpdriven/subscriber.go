package httpdriven

import (
	"context"
	"io"
	"net/http"

	"github.com/google/uuid"
	"github.com/norlis/event-driven/pkg/domain"
	"go.uber.org/zap"
)

type SubscriberConfig struct {
	Pattern string
	Logger  *zap.Logger
}

type HttpSubscriber struct {
	server *http.ServeMux
	config SubscriberConfig
	logger *zap.Logger

	//outputChannels     []chan *domain.Message
	//outputChannelsLock sync.Locker
	//closed bool
}

func NewSubscriber(server *http.ServeMux, cfg SubscriberConfig) (domain.Subscription, error) {

	return &HttpSubscriber{
		server: server,
		config: cfg,
		logger: cfg.Logger,
	}, nil
}

func (h *HttpSubscriber) Start(ctx context.Context, handler func(msg *domain.Message)) error {
	//func (h *HttpSubscriber) Subscribe(ctx context.Context, url string) (<-chan *domain.Message, error) {
	//	messages := make(chan *domain.Message)
	//
	//	h.outputChannelsLock.Lock()
	//	h.outputChannels = append(h.outputChannels, messages)
	//	h.outputChannelsLock.Unlock()
	h.config.Logger.Info("HttpSubscriber Start")

	h.server.HandleFunc(h.config.Pattern, func(w http.ResponseWriter, r *http.Request) {
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

		msg := domain.NewNewMessageWithoutAck(messageUUID, body, map[string]string{})
		handler(msg)

		//domainMsg := domain.NewMessage(m.ID, m.Data, m.Attributes, m.Ack, m.Nack)
		//handler(domainMsg)

		NewResponseBuilder().
			WithId(messageUUID, uuid.New().String(), uuid.New().String()).
			WithInstance(r.Pattern).
			Build().
			Json(w, r)
	})

	return nil
}
