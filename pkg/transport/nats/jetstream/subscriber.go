package jetstream

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/nats-io/nats.go/jetstream"

	"github.com/norlis/httpgate/logging"
	"github.com/norlis/httpgate/trace"

	"github.com/norlis/event-driven/v2/pkg/event"
	"github.com/norlis/event-driven/v2/pkg/kit/logfields"
	"github.com/norlis/event-driven/v2/pkg/transport/nats/codec"
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

	// StreamConfig, when non-nil, creates or updates the stream at Start (dev).
	// Nil assumes the stream exists (prod). Name must equal Stream.
	StreamConfig *jetstream.StreamConfig
}

// ErrConsumeStopped is reported through onError when the server stops the
// consumer without a Stop: deleted consumer, closed connection.
var ErrConsumeStopped = errors.New("jetstream: consume stopped unexpectedly")

// Subscriber consumes a JetStream stream via a per-instance ephemeral consumer,
// giving fan-out delivery. Satisfies eventmux.Subscription.
type Subscriber struct {
	js     jetstream.JetStream
	cfg    SubscriberConfig
	logger *slog.Logger

	mu       sync.Mutex
	cc       jetstream.ConsumeContext
	stopping chan struct{}
}

// NewSubscriber returns a Subscriber and applies defaults.
func NewSubscriber(js jetstream.JetStream, cfg SubscriberConfig, logger *slog.Logger) (*Subscriber, error) {
	if js == nil {
		return nil, errors.New("jetstream subscriber: nil JetStream")
	}
	if cfg.Stream == "" {
		return nil, errors.New("jetstream subscriber: empty stream")
	}
	if cfg.StreamConfig != nil && cfg.StreamConfig.Name != cfg.Stream {
		return nil, fmt.Errorf("jetstream subscriber: StreamConfig.Name %q must equal Stream %q", cfg.StreamConfig.Name, cfg.Stream)
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
	logger = logfields.Transport(logger, "jetstream-subscriber", logfields.SystemNATS, cfg.Stream)
	return &Subscriber{js: js, cfg: cfg, logger: logger}, nil
}

// Start resolves or provisions the stream, creates the per-instance
// ephemeral consumer and begins consuming. onError (nil tolerated, must not
// block) is called once if the server stops the consumer without a Stop. A
// second Start returns event.ErrAlreadyStarted.
func (s *Subscriber) Start(ctx context.Context, handler func(msg *event.Message), onError func(error)) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.cc != nil {
		return event.ErrAlreadyStarted
	}
	if onError == nil {
		onError = func(error) {}
	}
	var (
		stream jetstream.Stream
		err    error
	)
	if s.cfg.StreamConfig != nil {
		stream, err = ensureStream(ctx, s.js, *s.cfg.StreamConfig)
	} else {
		stream, err = s.js.Stream(ctx, s.cfg.Stream)
	}
	if err != nil {
		return fmt.Errorf("jetstream subscriber resolve stream %q: %w", s.cfg.Stream, err)
	}

	// Ephemeral, per-instance consumer → every instance gets every message.
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

	opts := []jetstream.PullConsumeOpt{
		jetstream.PullMaxMessages(s.cfg.MaxOutstandingMessages),
		jetstream.ConsumeErrHandler(func(_ jetstream.ConsumeContext, cerr error) {
			// Heartbeats missed and pull errors are transient; the fatal case
			// shows up as a closed ConsumeContext below.
			s.logger.Warn("consume error", logging.Err(cerr))
		}),
	}
	if s.cfg.MaxOutstandingBytes > 0 {
		opts = append(opts, jetstream.PullMaxBytes(s.cfg.MaxOutstandingBytes))
	}

	cc, err := cons.Consume(func(msg jetstream.Msg) {
		traceparent := msg.Headers().Get(trace.Header)
		delivery := newDelivery(msg, s.cfg.NakDelay)
		ce, uerr := codec.Unmarshal(msg.Headers(), msg.Data())
		if uerr != nil {
			// The router applies the poison-message policy (dead-letter copy
			// with the raw data, one log record, Observer) and terminates it.
			rescue := codec.Rescue(msg.Headers(), msg.Data())
			handler(event.NewUndecodable(event.ContextWithTrace(context.Background(), traceparent, nil), rescue, delivery, uerr))
			return
		}
		handler(event.NewMessage(event.ContextWithTrace(context.Background(), traceparent, &ce), ce, delivery))
	}, opts...)
	if err != nil {
		return fmt.Errorf("jetstream subscriber consume: %w", err)
	}

	stopping := make(chan struct{})
	s.cc, s.stopping = cc, stopping
	go func() {
		<-cc.Closed()
		select {
		case <-stopping:
		default:
			onError(ErrConsumeStopped)
		}
	}()

	s.logger.Info(
		"subscription started",
		slog.String(logfields.KeyNATSFilterSubject, s.cfg.FilterSubject),
		slog.Int(logfields.KeyConsumerMaxOutstanding, s.cfg.MaxOutstandingMessages),
	)
	return nil
}

// Stop drains the consumer (buffered messages still reach the handler) and
// waits for the drain to complete or ctx to expire. After an expired ctx the
// drain is still running, so the subscriber stays started: Start refuses and
// a later Stop waits for it again without draining twice.
func (s *Subscriber) Stop(ctx context.Context) error {
	s.mu.Lock()
	cc, stopping := s.cc, s.stopping
	if cc != nil {
		select {
		case <-stopping: // a previous Stop already started the drain
		default:
			close(stopping)
			cc.Drain()
		}
	}
	s.mu.Unlock()
	if cc == nil {
		return nil
	}
	select {
	case <-cc.Closed():
	case <-ctx.Done():
		return fmt.Errorf("jetstream subscriber stop: %w", ctx.Err())
	}
	s.mu.Lock()
	if s.cc == cc {
		s.cc, s.stopping = nil, nil
	}
	s.mu.Unlock()
	s.logger.Info("subscription stopped")
	return nil
}
