package pubsub

import (
	"context"
	"fmt"
	"time"

	"cloud.google.com/go/pubsub"
	"go.uber.org/multierr"
)

type GooglePubSubHealthCheckerOption func(*GooglePubSubHealthChecker)

func WithTopics(topics ...string) GooglePubSubHealthCheckerOption {
	return func(c *GooglePubSubHealthChecker) {
		c.topics = append(c.topics, topics...)
	}
}

// WithSubscriptions es una opción para añadir suscripciones que deben ser verificadas.
func WithSubscriptions(subscriptions ...string) GooglePubSubHealthCheckerOption {
	return func(c *GooglePubSubHealthChecker) {
		c.subscriptions = append(c.subscriptions, subscriptions...)
	}
}

type GooglePubSubHealthChecker struct {
	client        *pubsub.Client
	topics        []string
	subscriptions []string
}

func (p *GooglePubSubHealthChecker) Check() error {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second) // Aumentamos el timeout para múltiples llamadas
	defer cancel()

	var errs []error

	for _, topicID := range p.topics {
		if topicID == "" {
			continue
		}
		topic := p.client.Topic(topicID)
		exists, err := topic.Exists(ctx)
		if err != nil {
			errs = append(errs, fmt.Errorf("error checking topic %s: %w", topicID, err))
			continue
		}
		if !exists {
			errs = append(errs, fmt.Errorf("required topic '%s' does not exist", topicID))
		}
	}

	for _, subID := range p.subscriptions {
		if subID == "" {
			continue
		}
		sub := p.client.Subscription(subID)
		exists, err := sub.Exists(ctx)
		if err != nil {
			errs = append(errs, fmt.Errorf("error checking subscription %s: %w", subID, err))
			continue
		}
		if !exists {
			errs = append(errs, fmt.Errorf("required subscription '%s' does not exist", subID))
		}
	}

	return multierr.Combine(errs...)
}

func NewGooglePubSubHealthChecker(client *pubsub.Client, opts ...GooglePubSubHealthCheckerOption) *GooglePubSubHealthChecker {
	checker := &GooglePubSubHealthChecker{
		client:        client,
		topics:        []string{},
		subscriptions: []string{},
	}

	for _, opt := range opts {
		opt(checker)
	}

	return checker
}
