package core

import (
	"context"
	"errors"
	"fmt"
	"log/slog"

	natsgo "github.com/nats-io/nats.go"

	"github.com/norlis/httpgate/logging"
	"github.com/norlis/httpgate/trace"

	"github.com/norlis/event-driven/pkg/event"
	"github.com/norlis/event-driven/pkg/kit/logfields"
	"github.com/norlis/event-driven/pkg/transport/nats/codec"
)

// SubscriberConfig configures a Subscriber.
type SubscriberConfig struct {
	// Subject is the NATS subject to subscribe to. Required.
	Subject string
	// QueueGroup is OPTIONAL. Leave EMPTY for fan-out (every subscriber
	// receives every message). Set it only to deliberately load-balance
	// (competing consumers) across instances.
	QueueGroup string
	// Unmarshaler converts headers+data into a CloudEvent. Default: codec.DefaultUnmarshaler{}.
	Unmarshaler codec.Unmarshaler
}

// Subscriber consumes messages from a NATS core subject. Satisfies eventmux.Subscription.
type Subscriber struct {
	nc     *natsgo.Conn
	cfg    SubscriberConfig
	logger *slog.Logger
}

// NewSubscriber returns a Subscriber backed by the given connection.
func NewSubscriber(nc *natsgo.Conn, cfg SubscriberConfig, logger *slog.Logger) (*Subscriber, error) {
	if nc == nil {
		return nil, errors.New("core subscriber: nil nats connection")
	}
	if cfg.Subject == "" {
		return nil, errors.New("core subscriber: empty subject")
	}
	if cfg.Unmarshaler == nil {
		cfg.Unmarshaler = codec.DefaultUnmarshaler{}
	}
	if logger == nil {
		logger = slog.New(slog.DiscardHandler)
	}
	logger = logger.With(
		slog.String(logfields.KeyLogLogger, "nats-core-subscriber"),
		slog.String(logfields.KeyMessagingSystem, logfields.SystemNATS),
		slog.String(logfields.KeyMessagingDestination, cfg.Subject),
	)
	return &Subscriber{nc: nc, cfg: cfg, logger: logger}, nil
}

// Start subscribes and blocks until ctx is cancelled, then drains.
func (s *Subscriber) Start(ctx context.Context, handler func(msg *event.Message)) error {
	cb := func(m *natsgo.Msg) {
		ce, err := s.cfg.Unmarshaler.Unmarshal(m.Header, m.Data)
		if err != nil {
			// Core NATS has no redelivery; the message is definitively lost.
			s.logger.Error("message unmarshal failed", logging.Err(err))
			return
		}
		msgCtx := event.ContextWithTrace(context.Background(), m.Header.Get(trace.Header), &ce)
		handler(event.NewMessageWithParent(msgCtx, ce, nil, nil))
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

	s.logger.Info("subscription started",
		slog.String(logfields.KeyConsumerGroup, s.cfg.QueueGroup))
	<-ctx.Done()
	if err := sub.Drain(); err != nil {
		s.logger.Warn("nats drain failed", logging.Err(err))
	}
	s.logger.Info("subscription stopped")
	return nil
}
