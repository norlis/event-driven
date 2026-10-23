package sns

import (
	"fmt"

	awssdk "github.com/aws/aws-sdk-go-v2/aws"
	awssns "github.com/aws/aws-sdk-go-v2/service/sns"
	snstypes "github.com/aws/aws-sdk-go-v2/service/sns/types"
	cloudevents "github.com/cloudevents/sdk-go/v2/event"

	"github.com/norlis/event-driven/v2/pkg/transport/internal/envelope"
)

// TopicARN is the fully-qualified ARN of an SNS topic.
type TopicARN string

// contentTypeAttribute mirrors the Content-Type the SQS unmarshaler reads.
const contentTypeAttribute = "Content-Type"

// Marshaler maps a CloudEvent and a destination TopicARN into a ready-to-send
// SNS *PublishInput. Swap to customize attribute placement, body encoding,
// FIFO MessageGroupId/MessageDeduplicationId, etc.
type Marshaler interface {
	Marshal(topic TopicARN, ce cloudevents.Event) (*awssns.PublishInput, error)
}

// binaryAttributeBudget is how many attributes the binary mode may use: one
// slot of MaxMessageAttributes is kept for the native traceparent that
// Publisher.Publish adds after Marshal. The slot is reserved even when the
// context carries no trace: the mux always seeds one, so an untraced publish
// is rare and only costs it the structured mode one attribute early.
const binaryAttributeBudget = MaxMessageAttributes - 1

// DefaultMarshaler uses the binary content mode (body as the SNS Message,
// "ce-*" attributes plus Content-Type as String MessageAttributes) when it
// fits binaryAttributeBudget. Otherwise it switches to the structured mode
// (the whole CloudEvent as JSON in the Message) and keeps only Content-Type,
// ce-id, ce-type and ce-source as attributes, so SNS filter policies on type
// and source keep working and no event is lost to the attribute limit.
type DefaultMarshaler struct{}

// Marshal implements Marshaler.
func (DefaultMarshaler) Marshal(topic TopicARN, ce cloudevents.Event) (*awssns.PublishInput, error) {
	pairs, err := envelope.Encode(ce)
	if err != nil {
		return nil, fmt.Errorf("aws/sns: marshal: %w", err)
	}
	pairs[contentTypeAttribute] = envelope.ContentType(ce)
	message := string(ce.Data())
	if len(pairs) > binaryAttributeBudget {
		body, err := envelope.EncodeStructured(ce)
		if err != nil {
			return nil, fmt.Errorf("aws/sns: marshal: %w", err)
		}
		message = string(body)
		pairs = map[string]string{
			contentTypeAttribute:       cloudevents.ApplicationCloudEventsJSON,
			envelope.Prefix + "id":     ce.ID(),
			envelope.Prefix + "type":   ce.Type(),
			envelope.Prefix + "source": ce.Source(),
		}
	}
	attrs := make(map[string]snstypes.MessageAttributeValue, len(pairs))
	for k, v := range pairs {
		attrs[k] = snstypes.MessageAttributeValue{DataType: awssdk.String("String"), StringValue: awssdk.String(v)}
	}
	return &awssns.PublishInput{
		TopicArn:          awssdk.String(string(topic)),
		Message:           awssdk.String(message),
		MessageAttributes: attrs,
	}, nil
}
