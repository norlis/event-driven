package pubsub

import (
	"context"
	"fmt"
	"log/slog"

	gpubsub "cloud.google.com/go/pubsub/v2"
	cloudevents "github.com/cloudevents/sdk-go/v2/event"

	"github.com/norlis/httpgate/trace"

	"github.com/norlis/event-driven/pkg/event"
	"github.com/norlis/event-driven/pkg/kit/logfields"
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
	logger = logger.With(
		slog.String(logfields.KeyLogLogger, "pubsub-publisher"),
		slog.String(logfields.KeyMessagingSystem, logfields.SystemGCPPubSub),
		slog.String(logfields.KeyMessagingDestination, cfg.TopicID),
	)
	return &Publisher{
		client: client,
		cfg:    cfg,
		logger: logger,
	}
}

// Publish sends ce to the configured topic.
func (p *Publisher) Publish(ctx context.Context, ce cloudevents.Event) error {
	tc, hasTrace := event.InjectTraceFromContext(ctx, &ce)

	msg, err := p.cfg.Marshaler.Marshal(ce)
	if err != nil {
		return fmt.Errorf("pubsub marshal: %w", err)
	}
	if hasTrace {
		if msg.Attributes == nil {
			msg.Attributes = map[string]string{}
		}
		msg.Attributes[trace.Header] = tc.Traceparent()
	}

	publisher := p.client.Publisher(p.cfg.TopicID)
	defer publisher.Stop()

	result := publisher.Publish(ctx, msg)
	id, err := result.Get(ctx)
	if err != nil {
		// No log here: the caller (mux publishResult or the consuming service)
		// owns the single error log for this operation.
		return fmt.Errorf("pubsub publish: %w", err)
	}
	p.logger.DebugContext(
		ctx,
		"event published",
		slog.String(logfields.KeyMessagingBrokerID, id),
		slog.String(logfields.KeyMessagingMessageID, ce.ID()),
	)
	return nil
}
