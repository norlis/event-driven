package core

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"time"

	natsgo "github.com/nats-io/nats.go"

	"github.com/norlis/httpgate/trace"

	"github.com/norlis/event-driven/v2/pkg/event"
	"github.com/norlis/event-driven/v2/pkg/kit/logfields"
	"github.com/norlis/event-driven/v2/pkg/transport/nats/codec"
)

// SubscriberConfig configures a Subscriber.
type SubscriberConfig struct {
	// Subject is the NATS subject to subscribe to. Required.
	Subject string
	// QueueGroup is OPTIONAL. Leave EMPTY for fan-out (every subscriber
	// receives every message). Set it only to deliberately load-balance
	// (competing consumers) across instances.
	QueueGroup string
}

// Subscriber consumes messages from a NATS core subject. Satisfies eventmux.Subscription.
type Subscriber struct {
	nc     *natsgo.Conn
	cfg    SubscriberConfig
	logger *slog.Logger

	mu       sync.Mutex
	sub      *natsgo.Subscription
	draining bool // Drain already issued by a Stop whose ctx expired
}

// NewSubscriber returns a Subscriber backed by the given connection.
func NewSubscriber(nc *natsgo.Conn, cfg SubscriberConfig, logger *slog.Logger) (*Subscriber, error) {
	if nc == nil {
		return nil, errors.New("core subscriber: nil nats connection")
	}
	if cfg.Subject == "" {
		return nil, errors.New("core subscriber: empty subject")
	}
	logger = logfields.Transport(logger, "nats-core-subscriber", logfields.SystemNATS, cfg.Subject)
	return &Subscriber{nc: nc, cfg: cfg, logger: logger}, nil
}

// Start subscribes and returns. Core NATS has no fatal runtime failure of
// its own (connection events belong to whoever owns nc): onError is never
// called. A second Start returns event.ErrAlreadyStarted.
func (s *Subscriber) Start(_ context.Context, handler func(msg *event.Message), _ func(error)) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.sub != nil {
		return event.ErrAlreadyStarted
	}
	cb := func(m *natsgo.Msg) {
		traceparent := m.Header.Get(trace.Header)
		ce, err := codec.Unmarshal(m.Header, m.Data)
		if err != nil {
			// Core NATS has no redelivery: the router dead-letters a copy with
			// the raw data (when configured) and logs the discard once.
			rescue := codec.Rescue(m.Header, m.Data)
			handler(event.NewUndecodable(event.ContextWithTrace(context.Background(), traceparent, nil), rescue, event.Delivery{}, err))
			return
		}
		handler(event.NewMessage(event.ContextWithTrace(context.Background(), traceparent, &ce), ce, event.Delivery{}))
	}

	var (
		sub *natsgo.Subscription
		err error
	)
	if s.cfg.QueueGroup != "" {
		sub, err = s.nc.QueueSubscribe(s.cfg.Subject, s.cfg.QueueGroup, cb)
	} else {
		sub, err = s.nc.Subscribe(s.cfg.Subject, cb)
	}
	if err != nil {
		return fmt.Errorf("core subscribe %q: %w", s.cfg.Subject, err)
	}
	s.sub = sub

	s.logger.Info("subscription started",
		slog.String(logfields.KeyConsumerGroup, s.cfg.QueueGroup))
	return nil
}

// drainPoll is how often Stop checks whether the asynchronous Drain finished.
const drainPoll = 10 * time.Millisecond

// Stop drains the subscription and waits until the SDK marks it invalid
// (all pending callbacks done) or ctx expires. After an expired ctx the
// drain is still running, so the subscriber stays started: Start refuses and
// a later Stop waits for it again without draining twice.
func (s *Subscriber) Stop(ctx context.Context) error {
	s.mu.Lock()
	sub, draining := s.sub, s.draining
	s.draining = sub != nil
	s.mu.Unlock()
	if sub == nil {
		return nil
	}
	if !draining {
		if err := sub.Drain(); err != nil {
			return fmt.Errorf("core drain %q: %w", s.cfg.Subject, err)
		}
	}
	ticker := time.NewTicker(drainPoll)
	defer ticker.Stop()
	for sub.IsValid() {
		select {
		case <-ticker.C:
		case <-ctx.Done():
			return fmt.Errorf("core subscriber stop: %w", ctx.Err())
		}
	}
	s.mu.Lock()
	if s.sub == sub {
		s.sub, s.draining = nil, false
	}
	s.mu.Unlock()
	s.logger.Info("subscription stopped")
	return nil
}
