// Package sqs provides an SQS-backed implementation of eventmux.Subscription.
//
// Wiring:
//
//	awsCfg, _ := aws.NewConfig()
//	client := awssqs.NewFromConfig(*awsCfg)
//
//	// Full URL (Terraform/env var) — used as-is.
//	sub, err := sqs.NewSubscriber(client, sqs.SubscriberConfig{
//	    Queue:          "https://sqs.us-east-1.amazonaws.com/123/orders",
//	    ConsumeWorkers: 4,
//	}, logger)
//
//	// Or queue name — resolved with GetQueueUrl at Start (no STS, any partition).
//	sub, err := sqs.NewSubscriber(client, sqs.SubscriberConfig{Queue: "orders"}, logger)
//
// For SNS→SQS fan-out, ensure the SNS subscription has
// RawMessageDelivery=true so payloads arrive without the Notification envelope.
package sqs

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"sync"
	"time"

	awssdk "github.com/aws/aws-sdk-go-v2/aws"
	awssqs "github.com/aws/aws-sdk-go-v2/service/sqs"
	sqstypes "github.com/aws/aws-sdk-go-v2/service/sqs/types"

	"github.com/norlis/httpgate/logging"
	"github.com/norlis/httpgate/trace"

	"github.com/norlis/event-driven/v2/pkg/event"
	"github.com/norlis/event-driven/v2/pkg/kit/logfields"
	"github.com/norlis/event-driven/v2/pkg/transport/internal/envelope"
)

// QueueURL is an SQS queue URL (e.g.
// "https://sqs.us-east-1.amazonaws.com/123456789012/orders").
type QueueURL string

// DefaultAckTimeout bounds DeleteMessage when SubscriberConfig.AckTimeout is 0.
const DefaultAckTimeout = 5 * time.Second

// GenerateReceiveMessageInputFunc builds the ReceiveMessage request. Override
// to tune AttributeNames, MessageSystemAttributeNames, etc.
type GenerateReceiveMessageInputFunc func(queueURL QueueURL) *awssqs.ReceiveMessageInput

// GenerateReceiveMessageInputDefault returns long-polling defaults: 20s wait,
// up to 10 messages per call, all user message attributes and the
// ApproximateReceiveCount system attribute (eventmux.Config.MaxRetries needs
// it; a custom GenerateReceiveMessageInput must request it too).
func GenerateReceiveMessageInputDefault(queueURL QueueURL) *awssqs.ReceiveMessageInput {
	return &awssqs.ReceiveMessageInput{
		QueueUrl:                    awssdk.String(string(queueURL)),
		WaitTimeSeconds:             20,
		MaxNumberOfMessages:         10,
		MessageAttributeNames:       []string{"All"},
		MessageSystemAttributeNames: []sqstypes.MessageSystemAttributeName{sqstypes.MessageSystemAttributeNameApproximateReceiveCount},
	}
}

// SubscriberConfig configures a Subscriber.
type SubscriberConfig struct {
	// Queue accepts either a full URL ("https://sqs....") used as-is, or a
	// queue name resolved with GetQueueUrl at Start.
	Queue string

	// AckTimeout bounds each DeleteMessage (the ack). It runs on its own
	// context so a shutdown never cancels the ack of a message already
	// processed. Default: DefaultAckTimeout.
	AckTimeout time.Duration

	// ConsumeWorkers is the number of goroutines polling the queue in
	// parallel. Default: 1.
	ConsumeWorkers int

	// ReconnectRetrySleep is the backoff between ReceiveMessage failures.
	// Default: 1s.
	ReconnectRetrySleep time.Duration

	// GenerateReceiveMessageInput builds each ReceiveMessage request.
	// Default: GenerateReceiveMessageInputDefault.
	GenerateReceiveMessageInput GenerateReceiveMessageInputFunc
}

// Subscriber polls an SQS queue and emits decoded CloudEvents through the
// handler passed to Start. It satisfies eventmux.Subscription.
type Subscriber struct {
	client *awssqs.Client
	cfg    SubscriberConfig
	logger *slog.Logger

	mu     sync.Mutex
	cancel context.CancelFunc
	done   chan struct{}
}

// NewSubscriber validates config and returns a Subscriber. Call Start to begin
// polling and Stop to drain.
func NewSubscriber(client *awssqs.Client, cfg SubscriberConfig, logger *slog.Logger) (*Subscriber, error) {
	if client == nil {
		return nil, errors.New("aws/sqs: client is nil")
	}
	if cfg.Queue == "" {
		return nil, errors.New("aws/sqs: SubscriberConfig.Queue is empty")
	}
	if cfg.AckTimeout < 0 {
		return nil, fmt.Errorf("aws/sqs: SubscriberConfig.AckTimeout is negative: %s", cfg.AckTimeout)
	}
	if cfg.AckTimeout == 0 {
		cfg.AckTimeout = DefaultAckTimeout
	}
	if cfg.ConsumeWorkers <= 0 {
		cfg.ConsumeWorkers = 1
	}
	if cfg.ReconnectRetrySleep <= 0 {
		cfg.ReconnectRetrySleep = time.Second
	}
	if cfg.GenerateReceiveMessageInput == nil {
		cfg.GenerateReceiveMessageInput = GenerateReceiveMessageInputDefault
	}
	// The queue URL is resolved later in Start, so destination is attached there.
	logger = logfields.Component(logger, "sqs-subscriber").With(slog.String(logfields.KeyMessagingSystem, logfields.SystemAWSSQS))

	return &Subscriber{client: client, cfg: cfg, logger: logger}, nil
}

// Start resolves the queue URL (GetQueueUrl for a name: no STS, any
// partition, and it fails fast when the queue is missing) and spawns the
// polling workers. SQS has no fatal runtime failure: receive errors are
// retried with backoff, so onError is never called. A second Start returns
// event.ErrAlreadyStarted.
func (s *Subscriber) Start(ctx context.Context, handler func(msg *event.Message), _ func(error)) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.cancel != nil {
		return event.ErrAlreadyStarted
	}
	url, err := s.resolveQueueURL(ctx)
	if err != nil {
		return err
	}
	logger := s.logger.With(slog.String(logfields.KeyMessagingDestination, string(url)))

	runCtx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	var wg sync.WaitGroup
	for range s.cfg.ConsumeWorkers {
		wg.Go(func() { s.worker(runCtx, logger, url, handler) })
	}
	go func() {
		wg.Wait()
		close(done)
	}()
	s.cancel, s.done = cancel, done

	logger.Info("subscription started", slog.Int(logfields.KeyConsumerWorkers, s.cfg.ConsumeWorkers))
	return nil
}

// Stop cancels the long-poll and waits for the workers, which finish the
// batch in hand (handler + ack) before exiting, or for ctx to expire. After
// an expired ctx the workers are still running, so the subscriber stays
// started: Start refuses and a later Stop waits for them again.
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
		return fmt.Errorf("aws/sqs: stop: %w", ctx.Err())
	}
	s.mu.Lock()
	if s.done == done {
		s.cancel, s.done = nil, nil
	}
	s.mu.Unlock()
	s.logger.Info("subscription stopped")
	return nil
}

func (s *Subscriber) resolveQueueURL(ctx context.Context) (QueueURL, error) {
	if strings.HasPrefix(s.cfg.Queue, "https://") {
		return QueueURL(s.cfg.Queue), nil
	}
	out, err := s.client.GetQueueUrl(ctx, &awssqs.GetQueueUrlInput{QueueName: awssdk.String(s.cfg.Queue)})
	if err != nil {
		return "", fmt.Errorf("aws/sqs: get queue url %q: %w", s.cfg.Queue, err)
	}
	return QueueURL(awssdk.ToString(out.QueueUrl)), nil
}

func (s *Subscriber) worker(ctx context.Context, logger *slog.Logger, url QueueURL, handler func(msg *event.Message)) {
	for {
		select {
		case <-ctx.Done():
			return
		default:
		}

		sleep, wait := s.receiveBatch(ctx, logger, url, handler)
		if !wait {
			continue
		}

		select {
		case <-time.After(sleep):
		case <-ctx.Done():
			return
		}
	}
}

// receiveBatch performs one ReceiveMessage round-trip and dispatches each
// message. wait reports whether the loop must back off for sleep before the
// next iteration.
func (s *Subscriber) receiveBatch(ctx context.Context, logger *slog.Logger, url QueueURL, handler func(msg *event.Message)) (sleep time.Duration, wait bool) {
	input := s.cfg.GenerateReceiveMessageInput(url)
	out, err := s.client.ReceiveMessage(ctx, input)
	if err != nil {
		if errors.Is(err, context.Canceled) {
			return 0, false
		}
		// Retried after backoff; the consumer keeps running, so this is a
		// warning, not an error.
		logger.Warn("subscription receive failed", logging.Err(err))
		return s.cfg.ReconnectRetrySleep, true
	}
	for i := range out.Messages {
		s.processMessage(url, &out.Messages[i], handler)
	}
	return 0, false
}

func (s *Subscriber) processMessage(url QueueURL, m *sqstypes.Message, handler func(msg *event.Message)) {
	receiptHandle := awssdk.ToString(m.ReceiptHandle)
	messageID := awssdk.ToString(m.MessageId)
	ack := func() error { return s.deleteMessage(url, receiptHandle) }

	var traceparent string
	if attr, ok := m.MessageAttributes[trace.Header]; ok {
		traceparent = awssdk.ToString(attr.StringValue)
	}
	// Nack is a no-op: not deleting lets the visibility timeout expire and SQS
	// redeliver. It must not be nil: a nil Nack tells the router the broker
	// cannot redeliver, which would dead-letter every retryable failure.
	delivery := event.Delivery{Ack: ack, Nack: func() error { return nil }, Count: deliveryCount(m)}

	ce, err := unmarshal(m)
	if err != nil {
		// The router applies the poison-message policy (dead-letter copy with
		// the raw body, one log record, Observer) and confirms the message.
		def := defaults
		def.ID = messageID
		rescue := envelope.Rescue(def, awssdk.ToString(m.MessageAttributes[contentTypeAttribute].StringValue), []byte(awssdk.ToString(m.Body)))
		handler(event.NewUndecodable(event.ContextWithTrace(context.Background(), traceparent, nil), rescue, delivery, err))
		return
	}
	handler(event.NewMessage(event.ContextWithTrace(context.Background(), traceparent, &ce), ce, delivery))
}

// deleteMessage is the ack. It runs on its own bounded context: the receive
// context is cancelled during shutdown and must not take the ack down with it.
func (s *Subscriber) deleteMessage(url QueueURL, receiptHandle string) error {
	ctx, cancel := context.WithTimeout(context.Background(), s.cfg.AckTimeout)
	defer cancel()
	in := &awssqs.DeleteMessageInput{QueueUrl: awssdk.String(string(url)), ReceiptHandle: awssdk.String(receiptHandle)}
	if _, err := s.client.DeleteMessage(ctx, in); err != nil {
		return fmt.Errorf("aws/sqs: delete message: %w", err)
	}
	return nil
}
