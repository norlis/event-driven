package pubsub

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	gpubsub "cloud.google.com/go/pubsub/v2"
	cloudevents "github.com/cloudevents/sdk-go/v2/event"
)

type PublisherConfig struct {
	ProjectID string
	TopicID   string
}

type Publisher struct {
	client  *gpubsub.Client
	topicID string
	logger  *slog.Logger
}

func NewPublisher(client *gpubsub.Client, cfg PublisherConfig, logger *slog.Logger) *Publisher {
	return &Publisher{
		client:  client,
		topicID: cfg.TopicID,
		logger:  logger,
	}
}

func (p *Publisher) Publish(ce cloudevents.Event) error {
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

	result := publisher.Publish(ctx, &gpubsub.Message{
		Data:       ce.Data(),
		Attributes: attrs,
	})

	id, err := result.Get(ctx)
	if err != nil {
		p.logger.Error("Failed to publish message to Pub/Sub",
			slog.Any("error", err),
			slog.String("topicID", p.topicID),
			slog.String("originalID", ce.ID()),
		)
		return fmt.Errorf("pubsub publish: %w", err)
	}
	p.logger.Debug("Message published to Pub/Sub",
		slog.String("topicID", p.topicID),
		slog.String("publishedID", id),
		slog.String("originalID", ce.ID()),
	)
	return nil
}
