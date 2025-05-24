package pubsub

import (
	"context"
	"errors"
	"event-router/internal/domain"
	"fmt"
	"time"

	"cloud.google.com/go/pubsub"
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

func (s *GCPSubscription) Start(ctx context.Context, handler func(msg *domain.Message)) error {
	sub := s.client.Subscription(s.cfg.SubscriptionID)

	sub.ReceiveSettings.MaxOutstandingMessages = s.cfg.MaxOutstandingMessages
	sub.ReceiveSettings.MaxOutstandingBytes = s.cfg.MaxOutstandingBytes
	sub.ReceiveSettings.NumGoroutines = s.cfg.NumGoroutines
	sub.ReceiveSettings.MaxExtension = s.cfg.MaxExtension

	s.logger.Info("Iniciando recepción de mensajes Pub/Sub",
		zap.String("subscriptionID", s.cfg.SubscriptionID),
		zap.Int("maxOutstandingMessages", sub.ReceiveSettings.MaxOutstandingMessages),
		zap.Int("numGoroutines", sub.ReceiveSettings.NumGoroutines),
	)

	// sub.Receive es bloqueante. Se ejecutará hasta que ctx sea cancelado o ocurra un error fatal.
	err := sub.Receive(ctx, func(ctx context.Context, m *pubsub.Message) {
		s.logger.Debug("Mensaje Pub/Sub recibido", zap.String("messageID", m.ID), zap.Any("attributes", m.Attributes))

		// Envolver el mensaje de pubsub.Message en domain.Message,
		// pasando las funciones Ack/Nack del mensaje original.
		domainMsg := domain.NewMessage(m.ID, m.Data, m.Attributes, m.Ack, m.Nack)
		handler(domainMsg)
	})

	if err != nil && !errors.Is(err, context.Canceled) {
		s.logger.Error("Error en Pub/Sub Receive", zap.Error(err), zap.String("subscriptionID", s.cfg.SubscriptionID))
		return fmt.Errorf("sub.Receive para %s falló: %w", s.cfg.SubscriptionID, err)
	}
	s.logger.Info("Recepción de mensajes Pub/Sub detenida", zap.String("subscriptionID", s.cfg.SubscriptionID))
	return nil
}
