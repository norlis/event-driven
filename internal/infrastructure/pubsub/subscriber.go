package pubsub

import (
	"context"
	"event-router/internal/domain"
	"log"
	"time"

	"cloud.google.com/go/pubsub"
)

type gcpSubscription struct {
	subID string
}

func NewSubscription(subID string) *gcpSubscription {
	return &gcpSubscription{subID: subID}
}

func (s *gcpSubscription) Start(ctx context.Context, handler func(domain.Message)) error {
	client, err := pubsub.NewClient(ctx, "proteccion-davinci-iaas")
	if err != nil {
		return err
	}

	sub := client.Subscription(s.subID)

	// TODO: vars
	sub.ReceiveSettings.MaxOutstandingMessages = 100
	sub.ReceiveSettings.MaxOutstandingBytes = 10 * 1024 * 1024 // 10MB
	sub.ReceiveSettings.NumGoroutines = 10
	sub.ReceiveSettings.MaxExtension = 60 * time.Second

	return sub.Receive(ctx, func(ctx context.Context, m *pubsub.Message) {
		log.Printf("[Receive] Attributes: %v", m.Attributes)
		handler(domain.Message{
			ID:         m.ID,
			Attributes: m.Attributes,
			Data:       m.Data,
		})
		m.Ack()
	})
}
