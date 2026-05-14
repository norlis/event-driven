package pubsub

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	gpubsub "cloud.google.com/go/pubsub/v2"

	"github.com/norlis/event-driven/pkg/event"
)

// SubscriberConfig configures a Subscriber.
type SubscriberConfig struct {
	ProjectID              string
	SubscriptionID         string
	MaxOutstandingMessages int
	MaxOutstandingBytes    int
	NumGoroutines          int
	MaxExtension           time.Duration

	// Unmarshaler converts a Pub/Sub message into a CloudEvent. Default:
	// DefaultUnmarshaler{} — handles both binary and structured content modes.
	Unmarshaler Unmarshaler
}

// Subscriber consumes messages from a Pub/Sub subscription. It satisfies
// eventmux.Subscription.
type Subscriber struct {
	client *gpubsub.Client
	cfg    SubscriberConfig
	logger *slog.Logger
}

// NewSubscriber returns a Subscriber backed by the given client.
func NewSubscriber(client *gpubsub.Client, cfg SubscriberConfig, logger *slog.Logger) *Subscriber {
	if cfg.Unmarshaler == nil {
		cfg.Unmarshaler = DefaultUnmarshaler{}
	}
	if logger == nil {
		logger = slog.New(slog.DiscardHandler)
	}
	return &Subscriber{
		client: client,
		cfg:    cfg,
		logger: logger,
	}
}

// Start opens the receive loop and blocks until ctx is cancelled.
func (s *Subscriber) Start(ctx context.Context, handler func(msg *event.Message)) error {
	subscriber := s.client.Subscriber(s.cfg.SubscriptionID)

	subscriber.ReceiveSettings.MaxOutstandingMessages = s.cfg.MaxOutstandingMessages
	subscriber.ReceiveSettings.MaxOutstandingBytes = s.cfg.MaxOutstandingBytes
	subscriber.ReceiveSettings.NumGoroutines = s.cfg.NumGoroutines
	subscriber.ReceiveSettings.MaxExtension = s.cfg.MaxExtension

	s.logger.Info("Starting Pub/Sub message reception",
		slog.String("subscriptionID", s.cfg.SubscriptionID),
		slog.Int("maxOutstandingMessages", subscriber.ReceiveSettings.MaxOutstandingMessages),
		slog.Int("numGoroutines", subscriber.ReceiveSettings.NumGoroutines),
	)

	err := subscriber.Receive(ctx, func(ctx context.Context, m *gpubsub.Message) {
		s.logger.Debug("Pub/Sub message received",
			slog.String("messageID", m.ID),
			slog.Any("attributes", m.Attributes),
		)

		ce, err := s.cfg.Unmarshaler.Unmarshal(m)
		if err != nil {
			s.logger.Error("Pub/Sub unmarshal failed",
				slog.Any("error", err),
				slog.String("messageID", m.ID),
			)
			// Leave the message: the SDK will redeliver after ack deadline.
			return
		}

		handler(event.NewMessage(ce, m.Ack, m.Nack))
	})

	if err != nil && !errors.Is(err, context.Canceled) {
		s.logger.Error("Pub/Sub Receive error",
			slog.Any("error", err),
			slog.String("subscriptionID", s.cfg.SubscriptionID),
		)
		return fmt.Errorf("sub.Receive for %s: %w", s.cfg.SubscriptionID, err)
	}
	s.logger.Info("Pub/Sub message reception stopped", slog.String("subscriptionID", s.cfg.SubscriptionID))
	return nil
}
