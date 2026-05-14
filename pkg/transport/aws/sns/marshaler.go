package sns

import (
	"time"

	awssdk "github.com/aws/aws-sdk-go-v2/aws"
	awssns "github.com/aws/aws-sdk-go-v2/service/sns"
	snstypes "github.com/aws/aws-sdk-go-v2/service/sns/types"
	cloudevents "github.com/cloudevents/sdk-go/v2/event"
)

// TopicARN is the fully-qualified ARN of an SNS topic.
type TopicARN string

// Marshaler maps a CloudEvent and a destination TopicARN into a ready-to-send
// SNS *PublishInput. Swap to customize attribute placement, body encoding,
// FIFO MessageGroupId/MessageDeduplicationId, etc.
type Marshaler interface {
	Marshal(topic TopicARN, ce cloudevents.Event) (*awssns.PublishInput, error)
}

// DefaultMarshaler stores the event body as the SNS Message and propagates
// CloudEvent context attributes as String-typed MessageAttributes prefixed
// with "ce-" (matching CloudEvents HTTP/Pub/Sub binary-mode conventions).
type DefaultMarshaler struct{}

// Marshal implements Marshaler.
func (DefaultMarshaler) Marshal(topic TopicARN, ce cloudevents.Event) (*awssns.PublishInput, error) {
	attrs := map[string]snstypes.MessageAttributeValue{
		"ce-id":          stringAttr(ce.ID()),
		"ce-specversion": stringAttr(ce.SpecVersion()),
		"ce-type":        stringAttr(ce.Type()),
		"ce-source":      stringAttr(ce.Source()),
	}
	if !ce.Time().IsZero() {
		attrs["ce-time"] = stringAttr(ce.Time().Format(time.RFC3339))
	}
	if subj := ce.Subject(); subj != "" {
		attrs["ce-subject"] = stringAttr(subj)
	}
	for k, v := range ce.Extensions() {
		if s, ok := v.(string); ok {
			attrs["ce-"+k] = stringAttr(s)
		}
	}

	return &awssns.PublishInput{
		TopicArn:          awssdk.String(string(topic)),
		Message:           awssdk.String(string(ce.Data())),
		MessageAttributes: attrs,
	}, nil
}

func stringAttr(v string) snstypes.MessageAttributeValue {
	return snstypes.MessageAttributeValue{
		DataType:    awssdk.String("String"),
		StringValue: awssdk.String(v),
	}
}
