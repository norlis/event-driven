package pubsub

import (
	"context"
	"fmt"
	"time"

	"cloud.google.com/go/pubsub/v2"
	cloudevents "github.com/cloudevents/sdk-go/v2/event"
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
		topicID: cfg.TopicID,
		logger:  logger,
	}
}

func (p *GCPPublisher) Publish(ce cloudevents.Event) error {
	ctx := context.Background()
	publisher := p.client.Publisher(p.topicID)
	defer publisher.Stop()

	attrs := make(map[string]string)
	attrs["ce-id"] = ce.ID()
	attrs["ce-source"] = ce.Source()
	attrs["ce-type"] = ce.Type()
	attrs["ce-specversion"] = ce.SpecVersion()
	if !ce.Time().IsZero() {
		attrs["ce-time"] = ce.Time().Format(time.RFC3339)
	}
	if ce.Subject() != "" {
		attrs["ce-subject"] = ce.Subject()
	}
	for k, v := range ce.Extensions() {
		if s, ok := v.(string); ok {
			attrs[k] = s
		}
	}

	result := publisher.Publish(ctx, &pubsub.Message{
		Data:       ce.Data(),
		Attributes: attrs,
	})

	id, err := result.Get(ctx)
	if err != nil {
		p.logger.Error("Failed to publish message to Pub/Sub",
			zap.Error(err),
			zap.String("topicID", p.topicID),
			zap.String("originalID", ce.ID()))
		return fmt.Errorf("pubsub publish: %w", err)
	}
	p.logger.Debug("Message published to Pub/Sub",
		zap.String("topicID", p.topicID),
		zap.String("publishedID", id),
		zap.String("originalID", ce.ID()))
	return nil
}
