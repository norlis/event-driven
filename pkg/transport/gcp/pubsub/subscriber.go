package pubsub

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"time"

	gpubsub "cloud.google.com/go/pubsub/v2"
	"cloud.google.com/go/pubsub/v2/apiv1/pubsubpb"

	"github.com/norlis/httpgate/trace"

	"github.com/norlis/event-driven/v2/pkg/event"
	"github.com/norlis/event-driven/v2/pkg/kit/logfields"
	"github.com/norlis/event-driven/v2/pkg/transport/internal/envelope"
)

// SubscriberConfig configures a Subscriber.
type SubscriberConfig struct {
	SubscriptionID         string
	MaxOutstandingMessages int
	MaxOutstandingBytes    int
	NumGoroutines          int
	MaxExtension           time.Duration
	// Delivery counts (eventmux.Config.MaxRetries) are only reported by Pub/Sub
	// when the subscription has a dead-letter policy configured in GCP.
}

// Subscriber consumes messages from a Pub/Sub subscription. It satisfies
// eventmux.Subscription.
type Subscriber struct {
	client *gpubsub.Client
	cfg    SubscriberConfig
	logger *slog.Logger

	mu     sync.Mutex
	cancel context.CancelFunc
	done   chan struct{}
}

// NewSubscriber validates cfg and returns a Subscriber backed by the given client.
func NewSubscriber(client *gpubsub.Client, cfg SubscriberConfig, logger *slog.Logger) (*Subscriber, error) {
	switch {
	case client == nil:
		return nil, errors.New("pubsub: client is nil")
	case cfg.SubscriptionID == "":
		return nil, errors.New("pubsub: SubscriberConfig.SubscriptionID is empty")
	case cfg.MaxOutstandingMessages < 0 || cfg.MaxOutstandingBytes < 0 || cfg.NumGoroutines < 0 || cfg.MaxExtension < 0:
		return nil, errors.New("pubsub: SubscriberConfig flow-control settings must not be negative")
	}
	logger = logfields.Transport(logger, "pubsub-subscriber", logfields.SystemGCPPubSub, cfg.SubscriptionID)
	return &Subscriber{client: client, cfg: cfg, logger: logger}, nil
}

// Start checks that the subscription exists (fail-fast on a typo or a
// missing pubsub.subscriptions.get permission) and starts receiving in the
// background. onError (nil tolerated) gets a Receive failure that is not a
// Stop; it must not block. A second Start returns event.ErrAlreadyStarted.
func (s *Subscriber) Start(ctx context.Context, handler func(msg *event.Message), onError func(error)) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.cancel != nil {
		return event.ErrAlreadyStarted
	}
	if onError == nil {
		onError = func(error) {}
	}
	name := fmt.Sprintf("projects/%s/subscriptions/%s", s.client.Project(), s.cfg.SubscriptionID)
	if _, err := s.client.SubscriptionAdminClient.GetSubscription(ctx, &pubsubpb.GetSubscriptionRequest{Subscription: name}); err != nil {
		return fmt.Errorf("pubsub: get subscription %q: %w", s.cfg.SubscriptionID, err)
	}

	subscriber := s.client.Subscriber(s.cfg.SubscriptionID)
	subscriber.ReceiveSettings.MaxOutstandingMessages = s.cfg.MaxOutstandingMessages
	subscriber.ReceiveSettings.MaxOutstandingBytes = s.cfg.MaxOutstandingBytes
	subscriber.ReceiveSettings.NumGoroutines = s.cfg.NumGoroutines
	subscriber.ReceiveSettings.MaxExtension = s.cfg.MaxExtension

	runCtx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	s.cancel, s.done = cancel, done

	go func() {
		err := subscriber.Receive(runCtx, func(_ context.Context, m *gpubsub.Message) {
			delivery := event.Delivery{
				Ack:   func() error { m.Ack(); return nil },
				Nack:  func() error { m.Nack(); return nil },
				Count: deliveryCount(m),
			}
			ce, err := unmarshal(m)
			if err != nil {
				// The router applies the poison-message policy: dead-letter copy
				// with the raw data, one log record, Observer, then ack.
				def := defaults
				def.ID = m.ID
				rescue := envelope.Rescue(def, m.Attributes[contentTypeAttribute], m.Data)
				handler(event.NewUndecodable(event.ContextWithTrace(context.Background(), m.Attributes[trace.Header], nil), rescue, delivery, err))
				return
			}
			handler(event.NewMessage(event.ContextWithTrace(context.Background(), m.Attributes[trace.Header], &ce), ce, delivery))
		})
		// Stop waits on done: close it before onError so a callback that
		// stops the subscriber cannot deadlock.
		close(done)
		if ferr := receiveOutcome(runCtx, s.cfg.SubscriptionID, err); ferr != nil {
			onError(ferr)
		}
	}()

	s.logger.Info(
		"subscription started",
		slog.Int(logfields.KeyConsumerMaxOutstanding, subscriber.ReceiveSettings.MaxOutstandingMessages),
		slog.Int(logfields.KeyConsumerGoroutines, subscriber.ReceiveSettings.NumGoroutines),
	)
	return nil
}

// receiveOutcome maps the result of Receive to the failure reported through
// onError: nil when Receive ended because Stop cancelled runCtx.
func receiveOutcome(runCtx context.Context, subscriptionID string, err error) error {
	if err == nil {
		return nil
	}
	if runCtx.Err() != nil {
		return nil //nolint:nilerr // Receive ended because Stop cancelled it: not a failure
	}
	return fmt.Errorf("pubsub: receive %q: %w", subscriptionID, err)
}

// Stop cancels the receive loop and waits for in-flight callbacks (Receive
// returns only after they finish) or for ctx to expire. After an expired ctx
// the loop is still running, so the subscriber stays started: Start refuses
// and a later Stop waits for it again.
func (s *Subscriber) Stop(ctx context.Context) error {
	s.mu.Lock()
	cancel, done := s.cancel, s.done
	s.mu.Unlock()
	if cancel == nil {
		return nil
	}
	cancel()
	select {
	case <-done:
	case <-ctx.Done():
		return fmt.Errorf("pubsub: stop %q: %w", s.cfg.SubscriptionID, ctx.Err())
	}
	s.mu.Lock()
	if s.done == done {
		s.cancel, s.done = nil, nil
	}
	s.mu.Unlock()
	s.logger.Info("subscription stopped")
	return nil
}
