package pubsub

import (
	"context"
	"errors"
	"fmt"
	"time"

	"cloud.google.com/go/pubsub/v2"
	"github.com/norlis/event-driven/pkg/domain/event"
	"go.uber.org/zap"
)

type SubscriberConfig struct {
	ProjectID              string
	SubscriptionID         string
	MaxOutstandingMessages int
	MaxOutstandingBytes    int
	NumGoroutines          int
	MaxExtension           time.Duration
}

type GCPSubscription struct {
	// mu       sync.RWMutex
	// lastPull time.Time
	client *pubsub.Client
	cfg    SubscriberConfig
	logger *zap.Logger
}

func NewSubscription(client *pubsub.Client, cfg SubscriberConfig, logger *zap.Logger) *GCPSubscription {
	return &GCPSubscription{
		client: client,
		cfg:    cfg,
		logger: logger,
	}
}

func (s *GCPSubscription) Start(ctx context.Context, handler func(msg *event.Message)) error {
	subscriber := s.client.Subscriber(s.cfg.SubscriptionID)

	subscriber.ReceiveSettings.MaxOutstandingMessages = s.cfg.MaxOutstandingMessages
	subscriber.ReceiveSettings.MaxOutstandingBytes = s.cfg.MaxOutstandingBytes
	subscriber.ReceiveSettings.NumGoroutines = s.cfg.NumGoroutines
	subscriber.ReceiveSettings.MaxExtension = s.cfg.MaxExtension

	s.logger.Info("Iniciando recepción de mensajes Pub/Sub",
		zap.String("subscriptionID", s.cfg.SubscriptionID),
		zap.Int("maxOutstandingMessages", subscriber.ReceiveSettings.MaxOutstandingMessages),
		zap.Int("numGoroutines", subscriber.ReceiveSettings.NumGoroutines),
	)

	// subscriber.Receive es bloqueante. Se ejecutará hasta que ctx sea cancelado o ocurra un error fatal.
	err := subscriber.Receive(ctx, func(ctx context.Context, m *pubsub.Message) {
		s.logger.Debug("Mensaje Pub/Sub recibido", zap.String("messageID", m.ID), zap.Any("attributes", m.Attributes))

		// Envolver el mensaje de pubsub.Message en domain.Message,
		// pasando las funciones Ack/Nack del mensaje original.
		domainMsg := event.NewMessage(m.ID, m.Data, m.Attributes, m.Ack, m.Nack)
		handler(domainMsg)
	})

	if err != nil && !errors.Is(err, context.Canceled) {
		s.logger.Error("Error en Pub/Sub Receive", zap.Error(err), zap.String("subscriptionID", s.cfg.SubscriptionID))
		return fmt.Errorf("sub.Receive para %s falló: %w", s.cfg.SubscriptionID, err)
	}
	s.logger.Info("Recepción de mensajes Pub/Sub detenida", zap.String("subscriptionID", s.cfg.SubscriptionID))
	return nil
}
