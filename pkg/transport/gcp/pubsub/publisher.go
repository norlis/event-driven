package pubsub

import (
	"context"
	"fmt"
	"log/slog"

	gpubsub "cloud.google.com/go/pubsub/v2"
	cloudevents "github.com/cloudevents/sdk-go/v2/event"
)

// PublisherConfig configures a Publisher.
type PublisherConfig struct {
	ProjectID string
	TopicID   string

	// Marshaler converts a CloudEvent into a Pub/Sub *Message. Default:
	// DefaultMarshaler{} — propagates CE attributes as message attributes
	// prefixed with "ce-".
	Marshaler Marshaler
}

// Publisher publishes CloudEvents to a Pub/Sub topic. It satisfies
// eventmux.Publisher.
type Publisher struct {
	client *gpubsub.Client
	cfg    PublisherConfig
	logger *slog.Logger
}

// NewPublisher returns a Publisher backed by the given client.
func NewPublisher(client *gpubsub.Client, cfg PublisherConfig, logger *slog.Logger) *Publisher {
	if cfg.Marshaler == nil {
		cfg.Marshaler = DefaultMarshaler{}
	}
	if logger == nil {
		logger = slog.New(slog.DiscardHandler)
	}
	return &Publisher{
		client: client,
		cfg:    cfg,
		logger: logger,
	}
}

// Publish sends ce to the configured topic.
func (p *Publisher) Publish(ce cloudevents.Event) error {
	msg, err := p.cfg.Marshaler.Marshal(ce)
	if err != nil {
		return fmt.Errorf("pubsub marshal: %w", err)
	}

	ctx := context.Background()
	publisher := p.client.Publisher(p.cfg.TopicID)
	defer publisher.Stop()

	result := publisher.Publish(ctx, msg)
	id, err := result.Get(ctx)
	if err != nil {
		p.logger.Error(
			"Failed to publish message to Pub/Sub",
			slog.Any("error", err),
			slog.String("topicID", p.cfg.TopicID),
			slog.String("originalID", ce.ID()),
		)
		return fmt.Errorf("pubsub publish: %w", err)
	}
	p.logger.Debug(
		"Message published to Pub/Sub",
		slog.String("topicID", p.cfg.TopicID),
		slog.String("publishedID", id),
		slog.String("originalID", ce.ID()),
	)
	return nil
}
