// Package sns provides an SNS-backed implementation of eventmux.Publisher.
//
// Wiring:
//
//	awsCfg, _ := aws.NewConfig()
//	client := awssns.NewFromConfig(*awsCfg)
//
//	// Full ARN (Terraform/env var) — no STS call needed.
//	pub, err := sns.NewPublisher(client, sns.PublisherConfig{
//	    Topic: "arn:aws:sns:us-east-1:123456789012:orders",
//	}, logger)
//
//	// Or topic name — region from awsCfg, account from STS GetCallerIdentity.
//	pub, err := sns.NewPublisher(client, sns.PublisherConfig{
//	    Topic:    "orders",
//	    Identity: &aws.Identity{Config: awsCfg},
//	}, logger)
//
// For SNS→SQS fan-out, set RawMessageDelivery=true on the SQS subscription so
// the payload arrives without the SNS Notification envelope.
package sns

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"maps"
	"slices"
	"strings"
	"time"

	awssdk "github.com/aws/aws-sdk-go-v2/aws"
	awssns "github.com/aws/aws-sdk-go-v2/service/sns"
	snstypes "github.com/aws/aws-sdk-go-v2/service/sns/types"
	cloudevents "github.com/cloudevents/sdk-go/v2/event"

	"github.com/norlis/httpgate/trace"

	"github.com/norlis/event-driven/v2/pkg/event"
	"github.com/norlis/event-driven/v2/pkg/kit/logfields"
	"github.com/norlis/event-driven/v2/pkg/transport/aws"
)

// PublisherConfig configures a Publisher.
type PublisherConfig struct {
	// Topic accepts either a full ARN ("arn:aws:sns:...") used as-is, or a
	// topic name. With a name, the ARN is built from Identity.Region() and
	// Identity.AccountID() (cached after the first STS lookup).
	Topic string

	// Identity is required only when Topic is not already an ARN. When Topic
	// is a full ARN, Identity is unused and may be nil.
	Identity *aws.Identity

	// Marshaler converts a CloudEvent into an SNS PublishInput. Default:
	// DefaultMarshaler{} — propagates CE attributes as MessageAttributes
	// prefixed with "ce-".
	Marshaler Marshaler

	// ResolveTimeout bounds the STS lookup NewPublisher performs when Topic is
	// a name: an unreachable STS fails startup instead of hanging it. Unused
	// when Topic is an ARN. Default: DefaultResolveTimeout.
	ResolveTimeout time.Duration
}

// DefaultResolveTimeout bounds the STS lookup when
// PublisherConfig.ResolveTimeout is 0.
const DefaultResolveTimeout = 10 * time.Second

// MaxMessageAttributes is the SQS hard limit of attributes per message, which
// binds SNS topics delivered to SQS with RawMessageDelivery. DefaultMarshaler
// keeps its binary output within MaxMessageAttributes-1 (the last slot is the
// native traceparent added by Publish) and switches to the structured mode
// above it, so every event fits regardless of its extensions.
const MaxMessageAttributes = 10

// Content modes logged in cloudevents.content_mode.
const (
	contentModeBinary     = "binary"
	contentModeStructured = "structured"
)

// ErrTooManyAttributes is returned (wrapped in event.NonRetryableError) when
// the marshaled input carries more than MaxMessageAttributes attributes. With
// DefaultMarshaler it cannot happen; it guards custom Marshalers, which SNS
// would otherwise reject on every redelivery.
var ErrTooManyAttributes = errors.New("aws/sns: too many message attributes")

// PublishAPI is the subset of *sns.Client the Publisher needs.
type PublishAPI interface {
	Publish(ctx context.Context, in *awssns.PublishInput, opts ...func(*awssns.Options)) (*awssns.PublishOutput, error)
}

// Publisher publishes CloudEvents to an SNS topic. It satisfies
// eventmux.Publisher.
type Publisher struct {
	client   PublishAPI
	cfg      PublisherConfig
	topicARN TopicARN
	logger   *slog.Logger
}

// NewPublisher resolves the topic ARN eagerly and returns a Publisher ready
// to send. Topic must be non-empty.
func NewPublisher(client PublishAPI, cfg PublisherConfig, logger *slog.Logger) (*Publisher, error) {
	if client == nil {
		return nil, errors.New("aws/sns: client is nil")
	}
	if cfg.Topic == "" {
		return nil, errors.New("aws/sns: PublisherConfig.Topic is empty")
	}
	if cfg.Marshaler == nil {
		cfg.Marshaler = DefaultMarshaler{}
	}
	if cfg.ResolveTimeout < 0 {
		return nil, fmt.Errorf("aws/sns: PublisherConfig.ResolveTimeout is negative: %s", cfg.ResolveTimeout)
	}
	if cfg.ResolveTimeout == 0 {
		cfg.ResolveTimeout = DefaultResolveTimeout
	}
	ctx, cancel := context.WithTimeout(context.Background(), cfg.ResolveTimeout)
	defer cancel()
	arn, err := resolveTopicARN(ctx, cfg.Topic, cfg.Identity)
	if err != nil {
		return nil, err
	}
	logger = logfields.Transport(logger, "sns-publisher", logfields.SystemAWSSNS, string(arn))

	return &Publisher{client: client, cfg: cfg, topicARN: arn, logger: logger}, nil
}

// Publish sends ce to the configured topic.
func (p *Publisher) Publish(ctx context.Context, ce cloudevents.Event) error {
	tc, hasTrace := event.InjectTraceFromContext(ctx, &ce)

	input, err := p.cfg.Marshaler.Marshal(p.topicARN, ce)
	if err != nil {
		return fmt.Errorf("aws/sns: marshal: %w", err)
	}
	if hasTrace {
		if input.MessageAttributes == nil {
			input.MessageAttributes = map[string]snstypes.MessageAttributeValue{}
		}
		input.MessageAttributes[trace.Header] = snstypes.MessageAttributeValue{
			DataType:    awssdk.String("String"),
			StringValue: awssdk.String(tc.Traceparent()),
		}
	}
	if n := len(input.MessageAttributes); n > MaxMessageAttributes {
		names := slices.Sorted(maps.Keys(input.MessageAttributes))
		return &event.NonRetryableError{Err: fmt.Errorf("%w: %d attributes (%s)", ErrTooManyAttributes, n, strings.Join(names, ", "))}
	}

	out, err := p.client.Publish(ctx, input)
	if err != nil {
		// No log here: the caller owns the single error log for this operation.
		return fmt.Errorf("aws/sns: publish: %w", err)
	}

	p.logger.DebugContext(
		ctx,
		"event published",
		slog.String(logfields.KeyMessagingBrokerID, awssdk.ToString(out.MessageId)),
		slog.String(logfields.KeyMessagingMessageID, ce.ID()),
		slog.String(logfields.KeyCloudEventsContentMode, contentMode(input)),
	)
	return nil
}

// contentMode reports which CloudEvents content mode the marshaled input uses.
func contentMode(input *awssns.PublishInput) string {
	if awssdk.ToString(input.MessageAttributes[contentTypeAttribute].StringValue) == cloudevents.ApplicationCloudEventsJSON {
		return contentModeStructured
	}
	return contentModeBinary
}

// resolveTopicARN returns topic as-is when it already looks like an ARN;
// otherwise it builds the ARN from the caller's partition, identity.Region()
// and identity.AccountID().
func resolveTopicARN(ctx context.Context, topic string, identity *aws.Identity) (TopicARN, error) {
	if strings.HasPrefix(topic, "arn:") {
		return TopicARN(topic), nil
	}
	if identity == nil {
		return "", fmt.Errorf("aws/sns: %q is not an ARN and PublisherConfig.Identity is nil", topic)
	}
	region, err := identity.Region()
	if err != nil {
		return "", fmt.Errorf("aws/sns: %w", err)
	}
	accountID, err := identity.AccountID(ctx)
	if err != nil {
		return "", fmt.Errorf("aws/sns: %w", err)
	}
	partition, err := identity.Partition(ctx)
	if err != nil {
		return "", fmt.Errorf("aws/sns: %w", err)
	}
	return TopicARN(fmt.Sprintf("arn:%s:sns:%s:%s:%s", partition, region, accountID, topic)), nil
}
