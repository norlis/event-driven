package jetstream

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/nats-io/nats.go/jetstream"

	"github.com/norlis/event-driven/pkg/event"
	"github.com/norlis/event-driven/pkg/transport/nats/codec"
)

const (
	defaultMaxOutstandingMessages = 50
	defaultAckWait                = 30 * time.Second
	defaultInactiveThreshold      = 30 * time.Second
)

// SubscriberConfig configures a fan-out Subscriber.
type SubscriberConfig struct {
	// Stream is the shared JetStream stream name. Required.
	Stream string
	// FilterSubject optionally restricts which subjects this instance receives.
	FilterSubject string

	// Flow control (mirrors gcp/pubsub MaxOutstanding*).
	MaxOutstandingMessages int           // → PullMaxMessages (default 50)
	MaxOutstandingBytes    int           // → PullMaxBytes (0 = unset)
	AckWait                time.Duration // redelivery window (default 30s)
	MaxDeliver             int           // attempts before the broker stops redelivering (0 = unlimited)
	NakDelay               time.Duration // delay applied on Nak (0 = immediate)

	// Ephemeral consumer lifecycle.
	InactiveThreshold time.Duration // auto-delete the consumer after inactivity (default 30s)
	// ConsumerName is OPTIONAL and for observability only (nats consumer ls).
	// It stays ephemeral (no Durable). It MUST be unique per instance (e.g.
	// include hostname/task-id) or instances collide. Empty → server-generated.
	ConsumerName string

	// Stream provisioning. Off in prod (assume-exists); on in dev.
	AutoProvisionStream bool
	StreamConfig        *jetstream.StreamConfig // required only when AutoProvisionStream

	// Unmarshaler converts headers+data into a CloudEvent. Default: codec.DefaultUnmarshaler{}.
	Unmarshaler codec.Unmarshaler
}

// Subscriber consumes a JetStream stream via a per-instance ephemeral consumer,
// giving fan-out delivery. Satisfies eventmux.Subscription.
type Subscriber struct {
	js     jetstream.JetStream
	cfg    SubscriberConfig
	logger *slog.Logger
}

// NewSubscriber returns a Subscriber and applies defaults.
func NewSubscriber(js jetstream.JetStream, cfg SubscriberConfig, logger *slog.Logger) (*Subscriber, error) {
	if js == nil {
		return nil, errors.New("jetstream subscriber: nil JetStream")
	}
	if cfg.Stream == "" {
		return nil, errors.New("jetstream subscriber: empty stream")
	}
	if cfg.MaxOutstandingMessages <= 0 {
		cfg.MaxOutstandingMessages = defaultMaxOutstandingMessages
	}
	if cfg.AckWait <= 0 {
		cfg.AckWait = defaultAckWait
	}
	if cfg.InactiveThreshold <= 0 {
		cfg.InactiveThreshold = defaultInactiveThreshold
	}
	if cfg.Unmarshaler == nil {
		cfg.Unmarshaler = codec.DefaultUnmarshaler{}
	}
	if logger == nil {
		logger = slog.New(slog.DiscardHandler)
	}
	return &Subscriber{js: js, cfg: cfg, logger: logger}, nil
}

// Start resolves/provisions the stream, creates a per-instance ephemeral
// consumer, and consumes until ctx is cancelled.
func (s *Subscriber) Start(ctx context.Context, handler func(msg *event.Message)) error {
	// 1. Resolve or provision the shared stream.
	var (
		stream jetstream.Stream
		err    error
	)
	if s.cfg.AutoProvisionStream {
		stream, err = ensureStream(ctx, s.js, s.cfg.StreamConfig)
	} else {
		stream, err = s.js.Stream(ctx, s.cfg.Stream)
	}
	if err != nil {
		return fmt.Errorf("jetstream subscriber resolve stream %q: %w", s.cfg.Stream, err)
	}

	// 2. Ephemeral, per-instance consumer → every instance gets every message.
	cons, err := stream.CreateConsumer(ctx, jetstream.ConsumerConfig{
		Name:              s.cfg.ConsumerName, // ephemeral (no Durable set)
		FilterSubject:     s.cfg.FilterSubject,
		DeliverPolicy:     jetstream.DeliverNewPolicy,
		AckPolicy:         jetstream.AckExplicitPolicy,
		AckWait:           s.cfg.AckWait,
		MaxDeliver:        s.cfg.MaxDeliver,
		InactiveThreshold: s.cfg.InactiveThreshold,
	})
	if err != nil {
		return fmt.Errorf("jetstream subscriber create consumer: %w", err)
	}

	// 3. Consume until ctx is cancelled.
	opts := []jetstream.PullConsumeOpt{jetstream.PullMaxMessages(s.cfg.MaxOutstandingMessages)}
	if s.cfg.MaxOutstandingBytes > 0 {
		opts = append(opts, jetstream.PullMaxBytes(s.cfg.MaxOutstandingBytes))
	}

	cc, err := cons.Consume(func(msg jetstream.Msg) {
		ce, uerr := s.cfg.Unmarshaler.Unmarshal(msg.Headers(), msg.Data())
		if uerr != nil {
			s.logger.Error("JetStream unmarshal failed", slog.Any("error", uerr))
			_ = msg.Nak() // redeliver; MaxDeliver routes poison messages to DLQ out-of-band
			return
		}
		handler(event.NewMessage(
			ce,
			func() { _ = msg.Ack() },
			func() {
				if s.cfg.NakDelay > 0 {
					_ = msg.NakWithDelay(s.cfg.NakDelay)
					return
				}
				_ = msg.Nak()
			},
		))
	}, opts...)
	if err != nil {
		return fmt.Errorf("jetstream subscriber consume: %w", err)
	}
	defer cc.Stop()

	s.logger.Info(
		"JetStream subscription started (fan-out)",
		slog.String("stream", s.cfg.Stream),
		slog.String("filterSubject", s.cfg.FilterSubject),
		slog.Int("maxOutstandingMessages", s.cfg.MaxOutstandingMessages),
	)

	<-ctx.Done()
	s.logger.Info("JetStream subscription stopped", slog.String("stream", s.cfg.Stream))
	return nil
}
