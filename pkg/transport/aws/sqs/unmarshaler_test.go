package sqs

import (
	"errors"
	"strconv"
	"testing"

	awssdk "github.com/aws/aws-sdk-go-v2/aws"
	sqstypes "github.com/aws/aws-sdk-go-v2/service/sqs/types"
	cloudevents "github.com/cloudevents/sdk-go/v2/event"

	"github.com/norlis/event-driven/v2/pkg/event"
	"github.com/norlis/event-driven/v2/pkg/transport/aws/sns"
)

func str(v string) sqstypes.MessageAttributeValue {
	return sqstypes.MessageAttributeValue{DataType: awssdk.String("String"), StringValue: awssdk.String(v)}
}

func TestUnmarshalBinary(t *testing.T) {
	t.Parallel()

	m := &sqstypes.Message{
		MessageId: awssdk.String("sqs-1"),
		Body:      awssdk.String(`{"k":1}`),
		MessageAttributes: map[string]sqstypes.MessageAttributeValue{
			"ce-type":          str("order.created"),
			"ce-correlationid": str("c-1"),
			"ce-binaryattr":    {DataType: awssdk.String("Binary"), BinaryValue: []byte{1}},
			"traceparent":      str("00-abc-def-01"),
		},
	}
	ce, err := unmarshal(m)
	if err != nil {
		t.Fatalf("Unmarshal() error = %v", err)
	}
	if ce.ID() != "sqs-1" || ce.Type() != "order.created" || ce.Source() != "//sqs.amazonaws.com" {
		t.Errorf("id=%q type=%q source=%q", ce.ID(), ce.Type(), ce.Source())
	}
	if got := ce.Extensions()["correlationid"]; got != "c-1" {
		t.Errorf("correlationid = %v", got)
	}
	if _, ok := ce.Extensions()["binaryattr"]; ok {
		t.Error("Binary-typed attribute must be ignored")
	}
	if string(ce.Data()) != `{"k":1}` {
		t.Errorf("Data = %s", ce.Data())
	}
}

func TestUnmarshalStructured(t *testing.T) {
	t.Parallel()

	m := &sqstypes.Message{
		MessageId:         awssdk.String("sqs-1"),
		Body:              awssdk.String(`{"specversion":"1.0","id":"abc","source":"/s","type":"t","data":{"k":1}}`),
		MessageAttributes: map[string]sqstypes.MessageAttributeValue{"Content-Type": str(cloudevents.ApplicationCloudEventsJSON)},
	}
	ce, err := unmarshal(m)
	if err != nil {
		t.Fatalf("Unmarshal() error = %v", err)
	}
	if ce.ID() != "abc" {
		t.Errorf("ID = %q, want abc", ce.ID())
	}
}

func TestUnmarshalStructuredInvalidIsDecodeError(t *testing.T) {
	t.Parallel()

	m := &sqstypes.Message{
		MessageId:         awssdk.String("sqs-1"),
		Body:              awssdk.String(`{`),
		MessageAttributes: map[string]sqstypes.MessageAttributeValue{"Content-Type": str(cloudevents.ApplicationCloudEventsJSON)},
	}
	if _, err := unmarshal(m); !errors.Is(err, event.ErrDecode) {
		t.Fatalf("Unmarshal() error = %v, want ErrDecode", err)
	}
}

// TestDecodesSNSStructuredOutput feeds what sns.DefaultMarshaler emits for a
// dead-letter copy straight into the SQS consumer (RawMessageDelivery=true).
func TestDecodesSNSStructuredOutput(t *testing.T) {
	t.Parallel()

	const dlError = "bad \"quote\"\nline 2 ñ"
	ce := cloudevents.New()
	ce.SetID("m1")
	ce.SetType("t")
	ce.SetSource("/s")
	ce.SetExtension(event.ExtCorrelationID, "m1")
	ce.SetExtension(event.ExtDeadLetterReason, string(event.ReasonDecode))
	ce.SetExtension(event.ExtDeadLetterError, dlError)
	for i := range 3 {
		ce.SetExtension("ext"+strconv.Itoa(i), "v")
	}
	_ = ce.SetData(cloudevents.ApplicationJSON, []byte(`{"k":1}`))

	in, err := sns.DefaultMarshaler{}.Marshal("arn:aws:sns:eu-west-1:1:dlq", ce)
	if err != nil {
		t.Fatalf("Marshal() error = %v", err)
	}
	if got := awssdk.ToString(in.MessageAttributes["Content-Type"].StringValue); got != cloudevents.ApplicationCloudEventsJSON {
		t.Fatalf("Content-Type = %q, want structured mode", got)
	}

	m := &sqstypes.Message{
		MessageId:         awssdk.String("sqs-1"),
		Body:              in.Message,
		MessageAttributes: map[string]sqstypes.MessageAttributeValue{},
	}
	for k, v := range in.MessageAttributes {
		m.MessageAttributes[k] = str(awssdk.ToString(v.StringValue))
	}
	got, err := unmarshal(m)
	if err != nil {
		t.Fatalf("Unmarshal() error = %v", err)
	}
	if got.ID() != "m1" || string(got.Data()) != `{"k":1}` {
		t.Errorf("ID/Data = %q/%s", got.ID(), got.Data())
	}
	if v := got.Extensions()[event.ExtDeadLetterError]; v != dlError {
		t.Errorf("deadlettererror = %q, want %q", v, dlError)
	}
	if v := got.Extensions()[event.ExtDeadLetterReason]; v != string(event.ReasonDecode) {
		t.Errorf("deadletterreason = %q", v)
	}
	for i := range 3 {
		if _, ok := got.Extensions()["ext"+strconv.Itoa(i)]; !ok {
			t.Errorf("ext%d missing", i)
		}
	}
}
