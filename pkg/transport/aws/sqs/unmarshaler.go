package sqs

import (
	"strings"
	"time"

	awssdk "github.com/aws/aws-sdk-go-v2/aws"
	sqstypes "github.com/aws/aws-sdk-go-v2/service/sqs/types"
	cloudevents "github.com/cloudevents/sdk-go/v2/event"
)

// Unmarshaler maps a raw SQS *Message into a CloudEvent. Swap to customize
// attribute parsing, body decoding, or to bridge legacy formats.
type Unmarshaler interface {
	Unmarshal(m *sqstypes.Message) (cloudevents.Event, error)
}

// DefaultUnmarshaler reads CloudEvents binary-mode attributes ("ce-*") from
// MessageAttributes and treats the message Body as the event data.
//
// Assumes RawMessageDelivery=true is set on the SNS→SQS subscription so the
// payload is not wrapped in the SNS "Notification" envelope.
type DefaultUnmarshaler struct{}

// Unmarshal implements Unmarshaler.
func (DefaultUnmarshaler) Unmarshal(m *sqstypes.Message) (cloudevents.Event, error) {
	ce := cloudevents.New()
	ce.SetSpecVersion("1.0")

	attrs := m.MessageAttributes

	if id := attrStr(attrs, "ce-id"); id != "" {
		ce.SetID(id)
	} else {
		ce.SetID(awssdk.ToString(m.MessageId))
	}
	if t := attrStr(attrs, "ce-type"); t != "" {
		ce.SetType(t)
	} else {
		ce.SetType("com.aws.sqs.message")
	}
	if src := attrStr(attrs, "ce-source"); src != "" {
		ce.SetSource(src)
	} else {
		ce.SetSource("//sqs.amazonaws.com")
	}
	if subj := attrStr(attrs, "ce-subject"); subj != "" {
		ce.SetSubject(subj)
	}
	if t := attrStr(attrs, "ce-time"); t != "" {
		if parsed, err := time.Parse(time.RFC3339, t); err == nil {
			ce.SetTime(parsed)
		}
	} else {
		ce.SetTime(time.Now())
	}

	// Forward unknown ce-* attributes as CloudEvent extensions.
	for k, v := range attrs {
		if !strings.HasPrefix(k, "ce-") || v.StringValue == nil {
			continue
		}
		name := strings.TrimPrefix(k, "ce-")
		if isReservedCEAttr(name) {
			continue
		}
		ce.SetExtension(name, *v.StringValue)
	}

	_ = ce.SetData(cloudevents.ApplicationJSON, []byte(awssdk.ToString(m.Body)))
	return ce, nil
}

func attrStr(attrs map[string]sqstypes.MessageAttributeValue, key string) string {
	if v, ok := attrs[key]; ok && v.StringValue != nil {
		return *v.StringValue
	}
	return ""
}

func isReservedCEAttr(name string) bool {
	switch name {
	case "id", "specversion", "type", "source", "subject", "time":
		return true
	}
	return false
}
