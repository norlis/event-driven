package pubsub

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"time"

	gpubsub "cloud.google.com/go/pubsub/v2"
	cloudevents "github.com/cloudevents/sdk-go/v2/event"
	"github.com/norlis/event-driven/pkg/event"
)

type SubscriberConfig struct {
	ProjectID              string
	SubscriptionID         string
	MaxOutstandingMessages int
	MaxOutstandingBytes    int
	NumGoroutines          int
	MaxExtension           time.Duration
}

type Subscriber struct {
	client *gpubsub.Client
	cfg    SubscriberConfig
	logger *slog.Logger
}

func NewSubscriber(client *gpubsub.Client, cfg SubscriberConfig, logger *slog.Logger) *Subscriber {
	return &Subscriber{
		client: client,
		cfg:    cfg,
		logger: logger,
	}
}

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

		ce := s.toCloudEvent(m)
		domainMsg := event.NewMessage(ce, m.Ack, m.Nack)
		handler(domainMsg)
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

func (s *Subscriber) toCloudEvent(m *gpubsub.Message) cloudevents.Event {
	// Structured content mode: entire CloudEvent is JSON-encoded in Message.Data.
	if ct := m.Attributes["Content-Type"]; ct == "application/cloudevents+json" {
		var ce cloudevents.Event
		if err := json.Unmarshal(m.Data, &ce); err == nil {
			s.logger.Debug("Decoded structured content mode CloudEvent", slog.String("id", ce.ID()))
			return ce
		}
		s.logger.Warn("Failed to decode structured CloudEvent, falling back to binary mode",
			slog.String("messageID", m.ID))
	}

	// Binary content mode: CE attributes in message attributes, data in Message.Data.
	ce := cloudevents.New()
	ce.SetSpecVersion("1.0")

	if id := m.Attributes["ce-id"]; id != "" {
		ce.SetID(id)
	} else {
		ce.SetID(m.ID)
	}
	if t := m.Attributes["ce-type"]; t != "" {
		ce.SetType(t)
	} else {
		ce.SetType("com.google.cloud.pubsub.message")
	}
	if src := m.Attributes["ce-source"]; src != "" {
		ce.SetSource(src)
	} else {
		ce.SetSource(fmt.Sprintf("//pubsub.googleapis.com/%s/subscriptions/%s",
			s.cfg.ProjectID, s.cfg.SubscriptionID))
	}
	if subj := m.Attributes["ce-subject"]; subj != "" {
		ce.SetSubject(subj)
	}
	if t := m.Attributes["ce-time"]; t != "" {
		if parsed, parseErr := time.Parse(time.RFC3339, t); parseErr == nil {
			ce.SetTime(parsed)
		}
	} else {
		ce.SetTime(time.Now())
	}

	_ = ce.SetData(cloudevents.ApplicationJSON, m.Data)

	// Carry non-ce attributes as extensions.
	for k, v := range m.Attributes {
		if !strings.HasPrefix(k, "ce-") && k != "Content-Type" {
			ce.SetExtension(k, v)
		}
	}

	return ce
}
