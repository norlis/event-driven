package pubsub

import (
	"context"
	"errors"
	"fmt"
	"log/slog"

	gpubsub "cloud.google.com/go/pubsub/v2"
	cloudevents "github.com/cloudevents/sdk-go/v2/event"

	"github.com/norlis/httpgate/trace"

	"github.com/norlis/event-driven/v2/pkg/event"
	"github.com/norlis/event-driven/v2/pkg/kit/logfields"
)

// PublisherConfig configures a Publisher.
type PublisherConfig struct {
	TopicID string
}

// Publisher publishes CloudEvents to a Pub/Sub topic. It satisfies
// eventmux.Publisher.
type Publisher struct {
	pub    *gpubsub.Publisher
	cfg    PublisherConfig
	logger *slog.Logger
}

// NewPublisher validates cfg and creates the topic publisher once, so the
// SDK's batching and flow control apply across messages. Call Close to
// flush and release its goroutines.
func NewPublisher(client *gpubsub.Client, cfg PublisherConfig, logger *slog.Logger) (*Publisher, error) {
	if client == nil {
		return nil, errors.New("pubsub: client is nil")
	}
	if cfg.TopicID == "" {
		return nil, errors.New("pubsub: PublisherConfig.TopicID is empty")
	}
	logger = logfields.Transport(logger, "pubsub-publisher", logfields.SystemGCPPubSub, cfg.TopicID)
	return &Publisher{pub: client.Publisher(cfg.TopicID), cfg: cfg, logger: logger}, nil
}

// Close flushes pending messages and stops the publisher; Publish fails afterwards.
func (p *Publisher) Close() error {
	p.pub.Stop()
	return nil
}

// Publish sends ce to the configured topic.
func (p *Publisher) Publish(ctx context.Context, ce cloudevents.Event) error {
	tc, hasTrace := event.InjectTraceFromContext(ctx, &ce)

	msg, err := marshal(ce)
	if err != nil {
		return fmt.Errorf("pubsub marshal: %w", err)
	}
	if hasTrace {
		if msg.Attributes == nil {
			msg.Attributes = map[string]string{}
		}
		msg.Attributes[trace.Header] = tc.Traceparent()
	}

	id, err := p.pub.Publish(ctx, msg).Get(ctx)
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
