// Package sqs provides an SQS-backed implementation of eventmux.Subscription.
//
// Wiring:
//
//	awsCfg, _ := aws.NewConfig()
//	client := awssqs.NewFromConfig(*awsCfg)
//
//	// Full URL (Terraform/env var) — no STS call needed.
//	sub, err := sqs.NewSubscriber(client, sqs.SubscriberConfig{
//	    Queue:          "https://sqs.us-east-1.amazonaws.com/123/orders",
//	    ConsumeWorkers: 4,
//	}, logger)
//
//	// Or queue name — region from awsCfg, account from STS GetCallerIdentity.
//	sub, err := sqs.NewSubscriber(client, sqs.SubscriberConfig{
//	    Queue:    "orders",
//	    Identity: &aws.Identity{Config: awsCfg},
//	}, logger)
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

	"github.com/norlis/event-driven/pkg/event"
	"github.com/norlis/event-driven/pkg/kit/logfields"
	"github.com/norlis/event-driven/pkg/transport/aws"
)

// QueueURL is an SQS queue URL (e.g.
// "https://sqs.us-east-1.amazonaws.com/123456789012/orders").
type QueueURL string

// NoSleep is a sentinel returned by the receive loop to skip the backoff sleep
// and immediately start the next iteration.
const NoSleep = -1 * time.Nanosecond

// GenerateReceiveMessageInputFunc builds the ReceiveMessage request. Override
// to tune AttributeNames, MessageSystemAttributeNames, etc.
type GenerateReceiveMessageInputFunc func(queueURL QueueURL) *awssqs.ReceiveMessageInput

// GenerateReceiveMessageInputDefault returns long-polling defaults: 20s wait,
// up to 10 messages per call, all user message attributes returned.
func GenerateReceiveMessageInputDefault(queueURL QueueURL) *awssqs.ReceiveMessageInput {
	return &awssqs.ReceiveMessageInput{
		QueueUrl:              awssdk.String(string(queueURL)),
		WaitTimeSeconds:       20,
		MaxNumberOfMessages:   10,
		MessageAttributeNames: []string{"All"},
	}
}

// GenerateDeleteMessageInputFunc builds the DeleteMessage request.
type GenerateDeleteMessageInputFunc func(queueURL QueueURL, receiptHandle string) *awssqs.DeleteMessageInput

// GenerateDeleteMessageInputDefault returns the minimal DeleteMessage input.
func GenerateDeleteMessageInputDefault(queueURL QueueURL, receiptHandle string) *awssqs.DeleteMessageInput {
	return &awssqs.DeleteMessageInput{
		QueueUrl:      awssdk.String(string(queueURL)),
		ReceiptHandle: awssdk.String(receiptHandle),
	}
}

// SubscriberConfig configures a Subscriber.
type SubscriberConfig struct {
	// Queue accepts either a full URL ("https://sqs....") used as-is, or a
	// queue name. With a name, the URL is built from Identity.Region() and
	// Identity.AccountID() (cached after the first STS lookup).
	Queue string

	// Identity is required only when Queue is not already a URL. When Queue
	// is a full URL, Identity is unused and may be nil.
	Identity *aws.Identity

	// Unmarshaler converts an SQS message into a CloudEvent. Default:
	// DefaultUnmarshaler{}.
	Unmarshaler Unmarshaler

	// ConsumeWorkers is the number of goroutines polling the queue in
	// parallel. Default: 1.
	ConsumeWorkers int

	// ReconnectRetrySleep is the backoff between ReceiveMessage failures.
	// Default: 1s.
	ReconnectRetrySleep time.Duration

	// GenerateReceiveMessageInput builds each ReceiveMessage request.
	// Default: GenerateReceiveMessageInputDefault.
	GenerateReceiveMessageInput GenerateReceiveMessageInputFunc

	// GenerateDeleteMessageInput builds each DeleteMessage request.
	// Default: GenerateDeleteMessageInputDefault.
	GenerateDeleteMessageInput GenerateDeleteMessageInputFunc
}

// Subscriber polls an SQS queue and emits decoded CloudEvents through the
// handler passed to Start. It satisfies eventmux.Subscription.
type Subscriber struct {
	client *awssqs.Client
	cfg    SubscriberConfig
	logger *slog.Logger

	closeOnce sync.Once
	closing   chan struct{}
	wg        sync.WaitGroup
}

// NewSubscriber validates config and returns a Subscriber. Call Start to begin
// polling; it blocks until ctx is cancelled or Close is invoked.
func NewSubscriber(client *awssqs.Client, cfg SubscriberConfig, logger *slog.Logger) (*Subscriber, error) {
	if client == nil {
		return nil, errors.New("aws/sqs: client is nil")
	}
	if cfg.Queue == "" {
		return nil, errors.New("aws/sqs: SubscriberConfig.Queue is empty")
	}
	if cfg.Unmarshaler == nil {
		cfg.Unmarshaler = DefaultUnmarshaler{}
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
	if cfg.GenerateDeleteMessageInput == nil {
		cfg.GenerateDeleteMessageInput = GenerateDeleteMessageInputDefault
	}
	if logger == nil {
		logger = slog.New(slog.DiscardHandler)
	}
	// The queue URL is resolved later in Start, so destination is attached there.
	logger = logger.With(
		slog.String(logfields.KeyLogLogger, "sqs-subscriber"),
		slog.String(logfields.KeyMessagingSystem, logfields.SystemAWSSQS),
	)

	return &Subscriber{
		client:  client,
		cfg:     cfg,
		logger:  logger,
		closing: make(chan struct{}),
	}, nil
}

// Start resolves the queue URL, spawns ConsumeWorkers goroutines and blocks
// until ctx is cancelled or Close is called. Each worker long-polls SQS,
// decodes messages and hands them to handler via event.NewMessage with
// Ack (DeleteMessage) / Nack (no-op, visibility timeout handles redelivery)
// callbacks wired in.
func (s *Subscriber) Start(ctx context.Context, handler func(msg *event.Message)) error {
	url, err := resolveQueueURL(ctx, s.cfg.Queue, s.cfg.Identity)
	if err != nil {
		return err
	}

	// Scoped locally rather than assigned back to s.logger: Start may run
	// concurrently with the workers it spawns, and a shared field would race
	// on repeated or concurrent Start calls.
	logger := s.logger.With(slog.String(logfields.KeyMessagingDestination, string(url)))
	logger.Info("subscription started",
		slog.Int(logfields.KeyConsumerWorkers, s.cfg.ConsumeWorkers))

	for range s.cfg.ConsumeWorkers {
		s.wg.Add(1)
		go s.worker(ctx, logger, url, handler)
	}

	s.wg.Wait()
	logger.Info("subscription stopped")
	return nil
}

// Close signals all workers to stop and waits for them to drain. Safe to call
// multiple times.
func (s *Subscriber) Close() error {
	s.closeOnce.Do(func() { close(s.closing) })
	s.wg.Wait()
	return nil
}

func (s *Subscriber) worker(ctx context.Context, logger *slog.Logger, url QueueURL, handler func(msg *event.Message)) {
	defer s.wg.Done()

	for {
		select {
		case <-ctx.Done():
			return
		case <-s.closing:
			return
		default:
		}

		sleep := s.receiveBatch(ctx, logger, url, handler)
		if sleep == NoSleep {
			continue
		}

		select {
		case <-time.After(sleep):
		case <-ctx.Done():
			return
		case <-s.closing:
			return
		}
	}
}

// receiveBatch performs one ReceiveMessage round-trip and dispatches each
// message. Returns NoSleep when the loop should immediately retry, otherwise
// the duration to back off before the next iteration.
func (s *Subscriber) receiveBatch(ctx context.Context, logger *slog.Logger, url QueueURL, handler func(msg *event.Message)) time.Duration {
	input := s.cfg.GenerateReceiveMessageInput(url)
	out, err := s.client.ReceiveMessage(ctx, input)
	if err != nil {
		if errors.Is(err, context.Canceled) {
			return NoSleep
		}
		// Retried after backoff below; the consumer keeps running, so this is
		// a warning, not an error.
		logger.Warn("subscription receive failed", logging.Err(err))
		return s.cfg.ReconnectRetrySleep
	}
	if len(out.Messages) == 0 {
		return NoSleep
	}

	for i := range out.Messages {
		s.processMessage(ctx, logger, url, &out.Messages[i], handler)
	}
	return NoSleep
}

func (s *Subscriber) processMessage(ctx context.Context, logger *slog.Logger, url QueueURL, m *sqstypes.Message, handler func(msg *event.Message)) {
	ce, err := s.cfg.Unmarshaler.Unmarshal(m)
	if err != nil {
		logger.Error(
			"message unmarshal failed",
			logging.Err(err),
			slog.String(logfields.KeyMessagingBrokerID, awssdk.ToString(m.MessageId)),
		)
		// Leave the message: visibility timeout will redeliver.
		return
	}

	receiptHandle := awssdk.ToString(m.ReceiptHandle)
	messageID := awssdk.ToString(m.MessageId)

	var traceparent string
	if attr, ok := m.MessageAttributes[trace.Header]; ok {
		traceparent = awssdk.ToString(attr.StringValue)
	}
	msgCtx := event.ContextWithTrace(context.Background(), traceparent, &ce)

	ack := func() { s.deleteMessage(ctx, logger, url, receiptHandle, messageID) }
	nack := func() {
		// No-op: not calling DeleteMessage lets the visibility timeout expire
		// and SQS redeliver.
	}

	handler(event.NewMessageWithParent(msgCtx, ce, ack, nack))
}

func (s *Subscriber) deleteMessage(ctx context.Context, logger *slog.Logger, url QueueURL, receiptHandle, messageID string) {
	input := s.cfg.GenerateDeleteMessageInput(url, receiptHandle)
	if _, err := s.client.DeleteMessage(ctx, input); err != nil {
		// The delete IS the ack: its failure means an already-processed
		// message will be redelivered.
		logger.Error(
			"message ack failed",
			logging.Err(err),
			slog.String(logfields.KeyMessagingBrokerID, messageID),
		)
	}
}

// resolveQueueURL returns queue as-is when it already looks like a URL;
// otherwise it builds the URL from identity.Region() + identity.AccountID().
func resolveQueueURL(ctx context.Context, queue string, identity *aws.Identity) (QueueURL, error) {
	if strings.HasPrefix(queue, "https://") {
		return QueueURL(queue), nil
	}
	if identity == nil {
		return "", fmt.Errorf("aws/sqs: %q is not a URL and SubscriberConfig.Identity is nil", queue)
	}
	region, err := identity.Region()
	if err != nil {
		return "", fmt.Errorf("aws/sqs: %w", err)
	}
	accountID, err := identity.AccountID(ctx)
	if err != nil {
		return "", fmt.Errorf("aws/sqs: %w", err)
	}
	return QueueURL(fmt.Sprintf("https://sqs.%s.amazonaws.com/%s/%s", region, accountID, queue)), nil
}
