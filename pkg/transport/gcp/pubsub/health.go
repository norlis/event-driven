// Package pubsub provides a Google Cloud Pub/Sub transport for eventmux:
// a Subscriber that polls a subscription, a Publisher that sends to a topic,
// and a HealthChecker that verifies existence of topics + subscriptions.
package pubsub

import (
	"context"
	"fmt"
	"time"

	gpubsub "cloud.google.com/go/pubsub/v2"
	"cloud.google.com/go/pubsub/v2/apiv1/pubsubpb"
	"go.uber.org/multierr"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// HealthCheckerOption configures the HealthChecker. Use WithTopics /
// WithSubscriptions to declare which resources should be verified.
type HealthCheckerOption func(*HealthChecker)

// WithTopics adds topic IDs whose existence should be checked.
func WithTopics(topics ...string) HealthCheckerOption {
	return func(c *HealthChecker) {
		c.topics = append(c.topics, topics...)
	}
}

// WithSubscriptions adds subscription IDs whose existence should be checked.
func WithSubscriptions(subscriptions ...string) HealthCheckerOption {
	return func(c *HealthChecker) {
		c.subscriptions = append(c.subscriptions, subscriptions...)
	}
}

// HealthChecker verifies that the configured Pub/Sub topics and subscriptions
// exist. Suitable for plug-in to a /ready probe.
type HealthChecker struct {
	client        *gpubsub.Client
	projectID     string
	topics        []string
	subscriptions []string
}

// Check performs a GetTopic + GetSubscription call for each registered
// resource and aggregates the results. Returns a wrapped multi-error when
// any resource is missing or unreachable. The caller's context is honored
// (e.g. cancellation when the probe's HTTP client disconnects) but capped
// at 15s so a single check cannot hang indefinitely.
func (p *HealthChecker) Check(ctx context.Context) error {
	ctx, cancel := context.WithTimeout(ctx, 15*time.Second)
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

// NewHealthChecker returns a HealthChecker bound to the given client and
// project ID. Use WithTopics / WithSubscriptions to populate the resources
// to verify.
func NewHealthChecker(client *gpubsub.Client, projectID string, opts ...HealthCheckerOption) *HealthChecker {
	checker := &HealthChecker{
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
