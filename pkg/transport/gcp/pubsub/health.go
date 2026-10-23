// Package pubsub provides a Google Cloud Pub/Sub transport for eventmux:
// a Subscriber that polls a subscription, a Publisher that sends to a topic,
// and a HealthChecker that verifies existence of topics + subscriptions.
package pubsub

import (
	"context"
	"errors"
	"fmt"
	"time"

	gpubsub "cloud.google.com/go/pubsub/v2"
	"cloud.google.com/go/pubsub/v2/apiv1/pubsubpb"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// DefaultCheckTimeout caps one Check call when WithTimeout is not given, so a
// readiness probe cannot hang on a stalled control-plane call.
const DefaultCheckTimeout = 15 * time.Second

// HealthCheckerOption configures the HealthChecker. Use WithTopics /
// WithSubscriptions to declare which resources should be verified.
type HealthCheckerOption func(*HealthChecker)

// WithTimeout caps each Check call; d must be positive.
func WithTimeout(d time.Duration) HealthCheckerOption {
	if d <= 0 {
		panic(fmt.Sprintf("pubsub: WithTimeout(%s): must be positive", d))
	}
	return func(c *HealthChecker) { c.timeout = d }
}

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
	timeout       time.Duration
}

// Check performs a GetTopic + GetSubscription call for each registered
// resource and aggregates the results. The caller's context is honored
// (e.g. cancellation when the probe's HTTP client disconnects) but capped
// at the configured timeout so a single check cannot hang indefinitely.
func (p *HealthChecker) Check(ctx context.Context) error {
	ctx, cancel := context.WithTimeout(ctx, p.timeout)
	defer cancel()

	errs := p.check(ctx, "topic", p.topics, func(ctx context.Context, path string) error {
		_, err := p.client.TopicAdminClient.GetTopic(ctx, &pubsubpb.GetTopicRequest{Topic: path})
		return err //nolint:wrapcheck // check wraps it with the resource kind and id
	})
	errs = append(errs, p.check(ctx, "subscription", p.subscriptions, func(ctx context.Context, path string) error {
		_, err := p.client.SubscriptionAdminClient.GetSubscription(ctx, &pubsubpb.GetSubscriptionRequest{Subscription: path})
		return err //nolint:wrapcheck // check wraps it with the resource kind and id
	})...)
	if combined := errors.Join(errs...); combined != nil {
		return fmt.Errorf("pubsub health check: %w", combined)
	}
	return nil
}

// check verifies each id of one resource kind; a NotFound is reported as a
// missing resource, anything else as an unreachable one.
func (p *HealthChecker) check(ctx context.Context, kind string, ids []string, get func(context.Context, string) error) []error {
	var errs []error
	for _, id := range ids {
		if id == "" {
			continue
		}
		path := fmt.Sprintf("projects/%s/%ss/%s", p.projectID, kind, id)
		err := get(ctx, path)
		if err == nil {
			continue
		}
		if st, ok := status.FromError(err); ok && st.Code() == codes.NotFound {
			errs = append(errs, fmt.Errorf("required %s %q does not exist", kind, id))
			continue
		}
		errs = append(errs, fmt.Errorf("error checking %s %s: %w", kind, id, err))
	}
	return errs
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
		timeout:       DefaultCheckTimeout,
	}

	for _, opt := range opts {
		opt(checker)
	}

	return checker
}
