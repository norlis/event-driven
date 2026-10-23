package sqs

import (
	"fmt"
	"iter"

	awssdk "github.com/aws/aws-sdk-go-v2/aws"
	sqstypes "github.com/aws/aws-sdk-go-v2/service/sqs/types"
	cloudevents "github.com/cloudevents/sdk-go/v2/event"

	"github.com/norlis/event-driven/v2/pkg/transport/internal/envelope"
)

// contentTypeAttribute is written by sns.DefaultMarshaler; the value
// application/cloudevents+json selects structured mode.
const contentTypeAttribute = "Content-Type"

var defaults = envelope.Defaults{Type: "com.aws.sqs.message", Source: "//sqs.amazonaws.com"}

// unmarshal reads "ce-*" String attributes (binary mode) or a structured
// envelope in Body. Assumes RawMessageDelivery=true on the SNS→SQS
// subscription so the body is not wrapped in the SNS envelope.
func unmarshal(m *sqstypes.Message) (cloudevents.Event, error) {
	def := defaults
	def.ID = awssdk.ToString(m.MessageId)
	contentType := awssdk.ToString(m.MessageAttributes[contentTypeAttribute].StringValue)
	ce, err := envelope.Decode(contentType, stringAttributes(m.MessageAttributes), []byte(awssdk.ToString(m.Body)), def)
	if err != nil {
		return cloudevents.Event{}, fmt.Errorf("aws/sqs: unmarshal: %w", err)
	}
	return ce, nil
}

// stringAttributes yields the String-typed attributes; Binary and Number
// values have no CloudEvents binary-mode representation here.
func stringAttributes(attrs map[string]sqstypes.MessageAttributeValue) iter.Seq2[string, string] {
	return func(yield func(string, string) bool) {
		for k, v := range attrs {
			if v.StringValue == nil {
				continue
			}
			if !yield(k, *v.StringValue) {
				return
			}
		}
	}
}
