// Package jetstream provides a NATS JetStream transport for eventmux. The
// subscriber uses a per-instance EPHEMERAL consumer so every running instance
// receives every event (fan-out), replacing an SQS competing-consumer setup.
package jetstream

import (
	"context"
	"errors"
	"fmt"
	"log/slog"

	cloudevents "github.com/cloudevents/sdk-go/v2/event"
	natsgo "github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"

	"github.com/norlis/httpgate/trace"

	"github.com/norlis/event-driven/pkg/event"
	"github.com/norlis/event-driven/pkg/kit/logfields"
	"github.com/norlis/event-driven/pkg/transport/nats/codec"
)

// PublisherConfig configures a Publisher.
type PublisherConfig struct {
	// Subject must map to a JetStream stream; otherwise publish returns an error.
	Subject string
	// Marshaler converts a CloudEvent into a *nats.Msg. Default: codec.DefaultMarshaler{}.
	Marshaler codec.Marshaler
	// AckAsync uses PublishMsgAsync for higher throughput at the cost of
	// per-message ack confirmation latency. When true, server-side publish
	// acknowledgements are delivered asynchronously; the caller MUST construct
	// the injected jetstream.JetStream with jetstream.WithPublishAsyncErrHandler(...)
	// to observe publish failures — otherwise async publish errors are NOT
	// reported by this Publisher (only local submission errors are returned).
	AckAsync bool
	// TrackMsgID sets jetstream.WithMsgID(ce.ID()) so the server deduplicates
	// retransmissions within its dedup window.
	TrackMsgID bool
	// PublishOpts is an escape hatch forwarded to every publish call.
	PublishOpts []jetstream.PublishOpt
}

// Publisher publishes CloudEvents to a JetStream subject. Satisfies eventmux.Publisher.
type Publisher struct {
	js     jetstream.JetStream
	cfg    PublisherConfig
	logger *slog.Logger
}

// NewPublisher returns a Publisher backed by the given JetStream context.
func NewPublisher(js jetstream.JetStream, cfg PublisherConfig, logger *slog.Logger) (*Publisher, error) {
	if js == nil {
		return nil, errors.New("jetstream publisher: nil JetStream")
	}
	if cfg.Subject == "" {
		return nil, errors.New("jetstream publisher: empty subject")
	}
	if cfg.Marshaler == nil {
		cfg.Marshaler = codec.DefaultMarshaler{}
	}
	if logger == nil {
		logger = slog.New(slog.DiscardHandler)
	}
	logger = logger.With(
		slog.String(logfields.KeyLogLogger, "jetstream-publisher"),
		slog.String(logfields.KeyMessagingSystem, logfields.SystemNATS),
		slog.String(logfields.KeyMessagingDestination, cfg.Subject),
	)
	return &Publisher{js: js, cfg: cfg, logger: logger}, nil
}

// Publish sends ce to the configured subject.
func (p *Publisher) Publish(ctx context.Context, ce cloudevents.Event) error {
	tc, hasTrace := event.InjectTraceFromContext(ctx, &ce)

	msg, err := p.cfg.Marshaler.Marshal(p.cfg.Subject, ce)
	if err != nil {
		return fmt.Errorf("jetstream publish marshal: %w", err)
	}
	if hasTrace {
		if msg.Header == nil {
			msg.Header = natsgo.Header{}
		}
		msg.Header.Set(trace.Header, tc.Traceparent())
	}

	opts := append([]jetstream.PublishOpt(nil), p.cfg.PublishOpts...)
	if p.cfg.TrackMsgID {
		opts = append(opts, jetstream.WithMsgID(ce.ID()))
	}

	if p.cfg.AckAsync {
		// Only submission (setup) errors are returned here; server ack/failure
		// is observed via the caller's async error handler (see AckAsync docs).
		if _, err := p.js.PublishMsgAsync(msg, opts...); err != nil {
			return fmt.Errorf("jetstream publish async: %w", err)
		}
		return nil
	}

	ack, err := p.js.PublishMsg(ctx, msg, opts...)
	if err != nil {
		return fmt.Errorf("jetstream publish: %w", err)
	}
	p.logger.DebugContext(
		ctx,
		"event published",
		slog.String(logfields.KeyNATSStream, ack.Stream),
		slog.Uint64(logfields.KeyNATSSequence, ack.Sequence),
		slog.String(logfields.KeyMessagingMessageID, ce.ID()),
	)
	return nil
}
