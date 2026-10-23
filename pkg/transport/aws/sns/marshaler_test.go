package sns

import (
	"testing"
	"time"

	awssdk "github.com/aws/aws-sdk-go-v2/aws"
	cloudevents "github.com/cloudevents/sdk-go/v2/event"

	"github.com/norlis/event-driven/v2/pkg/transport/internal/envelope"
)

func TestDefaultMarshaler(t *testing.T) {
	t.Parallel()

	ce := cloudevents.New()
	ce.SetID("id-1")
	ce.SetSource("/s")
	ce.SetType("t")
	ce.SetTime(time.Date(2026, 1, 2, 3, 4, 5, 600000000, time.UTC))
	ce.SetExtension("attempt", int32(2))
	_ = ce.SetData(cloudevents.ApplicationJSON, []byte(`{"k":1}`))

	in, err := DefaultMarshaler{}.Marshal(TopicARN("arn:aws:sns:eu-west-1:1:orders"), ce)
	if err != nil {
		t.Fatalf("Marshal() error = %v", err)
	}
	want := map[string]string{
		"ce-id": "id-1", "ce-specversion": "1.0", "ce-type": "t", "ce-source": "/s",
		"ce-time": "2026-01-02T03:04:05.6Z", "ce-attempt": "2", "Content-Type": "application/json",
	}
	for k, v := range want {
		attr, ok := in.MessageAttributes[k]
		if !ok || awssdk.ToString(attr.DataType) != "String" || awssdk.ToString(attr.StringValue) != v {
			t.Errorf("MessageAttributes[%s] = %+v, want String %q", k, attr, v)
		}
	}
	if len(in.MessageAttributes) != len(want) {
		t.Errorf("got %d attributes, want %d", len(in.MessageAttributes), len(want))
	}
	if awssdk.ToString(in.Message) != `{"k":1}` {
		t.Errorf("Message = %q", awssdk.ToString(in.Message))
	}
	if awssdk.ToString(in.TopicArn) != "arn:aws:sns:eu-west-1:1:orders" {
		t.Errorf("TopicArn = %q", awssdk.ToString(in.TopicArn))
	}
}

func TestDefaultMarshalerBudgetBoundary(t *testing.T) {
	t.Parallel()

	// eventWithExtensions without traceparent costs 5 ce-* attributes plus
	// Content-Type; each extension adds one.
	tests := []struct {
		name        string
		extensions  int
		contentType string
		attributes  int
	}{
		{name: "9 attributes stay binary", extensions: 3, contentType: cloudevents.ApplicationJSON, attributes: 9},
		{name: "10 attributes go structured", extensions: 4, contentType: cloudevents.ApplicationCloudEventsJSON, attributes: 4},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			ce := eventWithExtensions(tt.extensions, "")
			_ = ce.SetData(cloudevents.ApplicationJSON, []byte(`{"k":1}`))
			in, err := DefaultMarshaler{}.Marshal(TopicARN("arn:aws:sns:eu-west-1:1:orders"), ce)
			if err != nil {
				t.Fatalf("Marshal() error = %v", err)
			}
			if got := awssdk.ToString(in.MessageAttributes[contentTypeAttribute].StringValue); got != tt.contentType {
				t.Errorf("Content-Type = %q, want %q", got, tt.contentType)
			}
			if got := len(in.MessageAttributes); got != tt.attributes {
				t.Errorf("got %d attributes, want %d", got, tt.attributes)
			}
		})
	}
}

func TestDefaultMarshalerFallsBackToStructured(t *testing.T) {
	t.Parallel()

	ce := eventWithExtensions(5, "42")
	ce.SetExtension("attempt", int32(2))
	_ = ce.SetData(cloudevents.ApplicationJSON, []byte(`{"k":1}`))

	in, err := DefaultMarshaler{}.Marshal(TopicARN("arn:aws:sns:eu-west-1:1:orders"), ce)
	if err != nil {
		t.Fatalf("Marshal() error = %v", err)
	}
	want := map[string]string{
		"Content-Type": cloudevents.ApplicationCloudEventsJSON,
		"ce-id":        "m1", "ce-type": "t", "ce-source": "/s",
	}
	for k, v := range want {
		attr, ok := in.MessageAttributes[k]
		if !ok || awssdk.ToString(attr.DataType) != "String" || awssdk.ToString(attr.StringValue) != v {
			t.Errorf("MessageAttributes[%s] = %+v, want String %q", k, attr, v)
		}
	}
	if len(in.MessageAttributes) != len(want) {
		t.Errorf("got %d attributes, want %d", len(in.MessageAttributes), len(want))
	}
	if awssdk.ToString(in.TopicArn) != "arn:aws:sns:eu-west-1:1:orders" {
		t.Errorf("TopicArn = %q", awssdk.ToString(in.TopicArn))
	}

	got, err := envelope.Decode(cloudevents.ApplicationCloudEventsJSON, nil, []byte(awssdk.ToString(in.Message)), envelope.Defaults{})
	if err != nil {
		t.Fatalf("Decode(Message) error = %v", err)
	}
	if got.Subject() != "42" || !got.Time().Equal(ce.Time()) || string(got.Data()) != `{"k":1}` {
		t.Errorf("decoded subject/time/data = %q/%v/%s", got.Subject(), got.Time(), got.Data())
	}
	for _, name := range []string{"ext0", "ext1", "ext2", "ext3", "ext4", "attempt"} {
		if _, ok := got.Extensions()[name]; !ok {
			t.Errorf("extension %s missing from structured body", name)
		}
	}
}
