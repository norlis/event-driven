package pubsub

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"cloud.google.com/go/pubsub"
	gpubsub "cloud.google.com/go/pubsub/v2"
	cloudevents "github.com/cloudevents/sdk-go/v2/event"
	"github.com/norlis/event-driven/pkg/event"
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

type Subscriber struct {
	client *gpubsub.Client
	cfg    SubscriberConfig
	logger *zap.Logger
}

func NewSubscriber(client *gpubsub.Client, cfg SubscriberConfig, logger *zap.Logger) *Subscriber {
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
		zap.String("subscriptionID", s.cfg.SubscriptionID),
		zap.Int("maxOutstandingMessages", subscriber.ReceiveSettings.MaxOutstandingMessages),
		zap.Int("numGoroutines", subscriber.ReceiveSettings.NumGoroutines),
	)

	err := subscriber.Receive(ctx, func(ctx context.Context, m *pubsub.Message) {
		s.logger.Debug("Pub/Sub message received", zap.String("messageID", m.ID), zap.Any("attributes", m.Attributes))

		ce := s.toCloudEvent(m)
		domainMsg := event.NewMessage(ce, m.Ack, m.Nack)
		handler(domainMsg)
	})

	if err != nil && !errors.Is(err, context.Canceled) {
		s.logger.Error("Pub/Sub Receive error", zap.Error(err), zap.String("subscriptionID", s.cfg.SubscriptionID))
		return fmt.Errorf("sub.Receive for %s: %w", s.cfg.SubscriptionID, err)
	}
	s.logger.Info("Pub/Sub message reception stopped", zap.String("subscriptionID", s.cfg.SubscriptionID))
	return nil
}

func (s *Subscriber) toCloudEvent(m *pubsub.Message) cloudevents.Event {
	// Structured content mode: entire CloudEvent is JSON-encoded in Message.Data.
	if ct := m.Attributes["Content-Type"]; ct == "application/cloudevents+json" {
		var ce cloudevents.Event
		if err := json.Unmarshal(m.Data, &ce); err == nil {
			s.logger.Debug("Decoded structured content mode CloudEvent", zap.String("id", ce.ID()))
			return ce
		}
		s.logger.Warn("Failed to decode structured CloudEvent, falling back to binary mode",
			zap.String("messageID", m.ID))
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
