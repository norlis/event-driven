package pubsub

import (
	"context"
	"fmt"
	"time"

	"cloud.google.com/go/pubsub/v2"
	"cloud.google.com/go/pubsub/v2/apiv1/pubsubpb"
	"go.uber.org/multierr"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
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
	projectID     string
	topics        []string
	subscriptions []string
}

func (p *GooglePubSubHealthChecker) Check() error {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	var errs []error

	// Check topics using TopicAdminClient
	topicAdmin := p.client.TopicAdminClient
	for _, topicID := range p.topics {
		if topicID == "" {
			continue
		}
		topicPath := fmt.Sprintf("projects/%s/topics/%s", p.projectID, topicID)
		_, err := topicAdmin.GetTopic(ctx, &pubsubpb.GetTopicRequest{
			Topic: topicPath,
		})
		if err != nil {
			if st, ok := status.FromError(err); ok && st.Code() == codes.NotFound {
				errs = append(errs, fmt.Errorf("required topic '%s' does not exist", topicID))
			} else {
				errs = append(errs, fmt.Errorf("error checking topic %s: %w", topicID, err))
			}
			continue
		}
	}

	// Check subscriptions using SubscriptionAdminClient
	subAdmin := p.client.SubscriptionAdminClient
	for _, subID := range p.subscriptions {
		if subID == "" {
			continue
		}
		subPath := fmt.Sprintf("projects/%s/subscriptions/%s", p.projectID, subID)
		_, err := subAdmin.GetSubscription(ctx, &pubsubpb.GetSubscriptionRequest{
			Subscription: subPath,
		})
		if err != nil {
			if st, ok := status.FromError(err); ok && st.Code() == codes.NotFound {
				errs = append(errs, fmt.Errorf("required subscription '%s' does not exist", subID))
			} else {
				errs = append(errs, fmt.Errorf("error checking subscription %s: %w", subID, err))
			}
			continue
		}
	}

	if combined := multierr.Combine(errs...); combined != nil {
		return fmt.Errorf("pubsub health check: %w", combined)
	}
	return nil
}

func NewGooglePubSubHealthChecker(client *pubsub.Client, projectID string, opts ...GooglePubSubHealthCheckerOption) *GooglePubSubHealthChecker {
	checker := &GooglePubSubHealthChecker{
		client:        client,
		projectID:     projectID,
		topics:        []string{},
		subscriptions: []string{},
	}

	for _, opt := range opts {
		opt(checker)
	}

	return checker
}
