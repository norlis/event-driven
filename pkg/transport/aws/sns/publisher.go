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
	"strings"

	awssdk "github.com/aws/aws-sdk-go-v2/aws"
	awssns "github.com/aws/aws-sdk-go-v2/service/sns"
	cloudevents "github.com/cloudevents/sdk-go/v2/event"

	"github.com/norlis/event-driven/pkg/transport/aws"
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
}

// Publisher publishes CloudEvents to an SNS topic. It satisfies
// eventmux.Publisher.
type Publisher struct {
	client   *awssns.Client
	cfg      PublisherConfig
	topicARN TopicARN
	logger   *slog.Logger
}

// NewPublisher resolves the topic ARN eagerly and returns a Publisher ready
// to send. Topic must be non-empty.
func NewPublisher(client *awssns.Client, cfg PublisherConfig, logger *slog.Logger) (*Publisher, error) {
	if client == nil {
		return nil, errors.New("aws/sns: client is nil")
	}
	if cfg.Topic == "" {
		return nil, errors.New("aws/sns: PublisherConfig.Topic is empty")
	}
	if cfg.Marshaler == nil {
		cfg.Marshaler = DefaultMarshaler{}
	}
	if logger == nil {
		logger = slog.New(slog.DiscardHandler)
	}

	arn, err := resolveTopicARN(context.Background(), cfg.Topic, cfg.Identity)
	if err != nil {
		return nil, err
	}

	return &Publisher{client: client, cfg: cfg, topicARN: arn, logger: logger}, nil
}

// Publish sends ce to the configured topic.
func (p *Publisher) Publish(ce cloudevents.Event) error {
	input, err := p.cfg.Marshaler.Marshal(p.topicARN, ce)
	if err != nil {
		return fmt.Errorf("aws/sns: marshal: %w", err)
	}

	out, err := p.client.Publish(context.Background(), input)
	if err != nil {
		p.logger.Error("Failed to publish to SNS",
			slog.Any("error", err),
			slog.String("topicARN", string(p.topicARN)),
			slog.String("originalID", ce.ID()),
		)
		return fmt.Errorf("aws/sns: publish: %w", err)
	}

	p.logger.Debug("CloudEvent published to SNS",
		slog.String("topicARN", string(p.topicARN)),
		slog.String("messageID", awssdk.ToString(out.MessageId)),
		slog.String("originalID", ce.ID()),
	)
	return nil
}

// Close is a no-op (the SNS client is owned by the caller).
func (p *Publisher) Close() error { return nil }

// resolveTopicARN returns topic as-is when it already looks like an ARN;
// otherwise it builds the ARN from identity.Region() + identity.AccountID().
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
	return TopicARN(fmt.Sprintf("arn:aws:sns:%s:%s:%s", region, accountID, topic)), nil
}
