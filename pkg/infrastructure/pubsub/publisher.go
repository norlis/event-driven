package pubsub

import (
	"context"
	"event-router/pkg/domain"
	"fmt"

	"cloud.google.com/go/pubsub"
	"go.uber.org/zap"
)

type PublisherConfig struct {
	ProjectID string
	TopicID   string
}

type GCPPublisher struct {
	client  *pubsub.Client
	topicID string
	logger  *zap.Logger
}

func NewPublisher(client *pubsub.Client, cfg PublisherConfig, logger *zap.Logger) *GCPPublisher {
	return &GCPPublisher{
		client:  client,
		topicID: cfg.TopicID, // Usar topicID de la config
		logger:  logger,
	}
}

func (p *GCPPublisher) Publish(msg *domain.Message) error {
	ctx := context.Background() // TODO: Considerar pasar contexto si es necesario para timeouts
	topic := p.client.Topic(p.topicID)
	defer topic.Stop() // Importante para limpiar recursos del publicador del topic

	result := topic.Publish(ctx, &pubsub.Message{
		Data:       msg.Payload,
		Attributes: msg.Metadata,
	})

	// Bloquear hasta que el mensaje sea publicado o falle.
	// Para alto rendimiento, esto podría hacerse de forma no bloqueante.
	id, err := result.Get(ctx)
	if err != nil {
		p.logger.Error("Fallo al publicar mensaje en Pub/Sub",
			zap.Error(err),
			zap.String("topicID", p.topicID),
			zap.String("originalMessageUUID", msg.UUID))
		return fmt.Errorf("Pub/Sub Publish.Get: %w", err)
	}
	p.logger.Debug("Mensaje publicado en Pub/Sub", zap.String("topicID", p.topicID), zap.String("publishedMessageID", id), zap.String("originalMessageUUID", msg.UUID))
	return nil
}
