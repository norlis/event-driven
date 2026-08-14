// Package core provides a NATS core (non-JetStream) transport for eventmux:
// fire-and-forget publishing and native fan-out subscription. There is no
// persistence and no acknowledgement — Ack/Nack on received messages are
// no-ops.
package core

import (
	"context"
	"errors"
	"fmt"
	"log/slog"

	cloudevents "github.com/cloudevents/sdk-go/v2/event"
	natsgo "github.com/nats-io/nats.go"

	"github.com/norlis/httpgate/trace"

	"github.com/norlis/event-driven/pkg/event"
	"github.com/norlis/event-driven/pkg/kit/logfields"
	"github.com/norlis/event-driven/pkg/transport/nats/codec"
)

// PublisherConfig configures a Publisher.
type PublisherConfig struct {
	// Subject is the NATS subject to publish to. Required.
	Subject string
	// Marshaler converts a CloudEvent into a *nats.Msg. Default: codec.DefaultMarshaler{}.
	Marshaler codec.Marshaler
}

// Publisher publishes CloudEvents to a NATS core subject. Satisfies eventmux.Publisher.
type Publisher struct {
	nc     *natsgo.Conn
	cfg    PublisherConfig
	logger *slog.Logger
}

// NewPublisher returns a Publisher backed by the given connection.
func NewPublisher(nc *natsgo.Conn, cfg PublisherConfig, logger *slog.Logger) (*Publisher, error) {
	if nc == nil {
		return nil, errors.New("core publisher: nil nats connection")
	}
	if cfg.Subject == "" {
		return nil, errors.New("core publisher: empty subject")
	}
	if cfg.Marshaler == nil {
		cfg.Marshaler = codec.DefaultMarshaler{}
	}
	if logger == nil {
		logger = slog.New(slog.DiscardHandler)
	}
	logger = logger.With(
		slog.String(logfields.KeyLogLogger, "nats-core-publisher"),
		slog.String(logfields.KeyMessagingSystem, logfields.SystemNATS),
		slog.String(logfields.KeyMessagingDestination, cfg.Subject),
	)
	return &Publisher{nc: nc, cfg: cfg, logger: logger}, nil
}

// Publish sends ce to the configured subject (fire-and-forget).
func (p *Publisher) Publish(ctx context.Context, ce cloudevents.Event) error {
	tc, hasTrace := event.InjectTraceFromContext(ctx, &ce)
	msg, err := p.cfg.Marshaler.Marshal(p.cfg.Subject, ce)
	if err != nil {
		return fmt.Errorf("core publish marshal: %w", err)
	}
	if hasTrace {
		if msg.Header == nil {
			msg.Header = natsgo.Header{}
		}
		msg.Header.Set(trace.Header, tc.Traceparent())
	}
	if err := p.nc.PublishMsg(msg); err != nil {
		return fmt.Errorf("core publish: %w", err)
	}
	p.logger.DebugContext(ctx, "event published",
		slog.String(logfields.KeyMessagingMessageID, ce.ID()))
	return nil
}
