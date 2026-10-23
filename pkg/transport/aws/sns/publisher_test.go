package sns

import (
	"bytes"
	"context"
	"errors"
	"log/slog"
	"strconv"
	"strings"
	"testing"
	"time"

	awssdk "github.com/aws/aws-sdk-go-v2/aws"
	awssns "github.com/aws/aws-sdk-go-v2/service/sns"
	snstypes "github.com/aws/aws-sdk-go-v2/service/sns/types"
	awssts "github.com/aws/aws-sdk-go-v2/service/sts"
	cloudevents "github.com/cloudevents/sdk-go/v2/event"

	"github.com/norlis/httpgate/trace"

	"github.com/norlis/event-driven/v2/pkg/event"
	"github.com/norlis/event-driven/v2/pkg/kit/logfields"
	"github.com/norlis/event-driven/v2/pkg/transport/aws"
)

type fakeSNS struct {
	calls  int
	lastIn *awssns.PublishInput
}

func (f *fakeSNS) Publish(_ context.Context, in *awssns.PublishInput, _ ...func(*awssns.Options)) (*awssns.PublishOutput, error) {
	f.calls++
	f.lastIn = in
	id := "broker-1"
	return &awssns.PublishOutput{MessageId: &id}, nil
}

// eventWithExtensions costs ce-id, ce-specversion, ce-type, ce-source,
// ce-time and Content-Type (6) in binary mode, + ce-subject when subject is
// set, + n. Publish adds ce-traceparent and the native traceparent when the
// context carries a trace.
func eventWithExtensions(n int, subject string) cloudevents.Event {
	ce := cloudevents.New()
	ce.SetID("m1")
	ce.SetType("t")
	ce.SetSource("/s")
	ce.SetTime(time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC))
	if subject != "" {
		ce.SetSubject(subject)
	}
	for i := range n {
		ce.SetExtension("ext"+strconv.Itoa(i), "v")
	}
	return ce
}

func withExtensions(ce cloudevents.Event, names ...string) cloudevents.Event {
	for _, name := range names {
		ce.SetExtension(name, "v")
	}
	return ce
}

// marshalerFunc adapts a function to Marshaler for custom-marshaler cases.
type marshalerFunc func(TopicARN, cloudevents.Event) (*awssns.PublishInput, error)

func (f marshalerFunc) Marshal(topic TopicARN, ce cloudevents.Event) (*awssns.PublishInput, error) {
	return f(topic, ce)
}

// overLimit returns a marshaler that emits MaxMessageAttributes+1 attributes.
func overLimit() Marshaler {
	return marshalerFunc(func(topic TopicARN, ce cloudevents.Event) (*awssns.PublishInput, error) {
		attrs := map[string]snstypes.MessageAttributeValue{}
		for i := range MaxMessageAttributes + 1 {
			attrs["a"+strconv.Itoa(i)] = snstypes.MessageAttributeValue{DataType: awssdk.String("String"), StringValue: awssdk.String("v")}
		}
		return &awssns.PublishInput{TopicArn: awssdk.String(string(topic)), Message: awssdk.String("{}"), MessageAttributes: attrs}, nil
	})
}

func TestPublishAttributeBudget(t *testing.T) {
	t.Parallel()

	traced := trace.NewContext(t.Context(), trace.New())
	base := eventWithExtensions(0, "")
	tests := []struct {
		name        string
		event       cloudevents.Event
		ctx         context.Context
		marshaler   Marshaler
		attributes  int
		contentType string
		wantTrace   bool
	}{
		{name: "fits in binary", event: eventWithExtensions(2, ""), ctx: traced, attributes: MaxMessageAttributes, contentType: cloudevents.ApplicationJSON, wantTrace: true},
		{name: "result with metadata", event: withExtensions(base.Clone(), event.ExtCorrelationID, event.ExtCausationID, "tenant"), ctx: traced, attributes: 5, contentType: cloudevents.ApplicationCloudEventsJSON, wantTrace: true},
		{name: "dead-letter copy", event: withExtensions(base.Clone(), event.ExtCorrelationID, event.ExtDeadLetterReason, event.ExtDeadLetterError), ctx: traced, attributes: 5, contentType: cloudevents.ApplicationCloudEventsJSON, wantTrace: true},
		{name: "many extensions with subject", event: eventWithExtensions(12, "42"), ctx: traced, attributes: 5, contentType: cloudevents.ApplicationCloudEventsJSON, wantTrace: true},
		{name: "structured without trace", event: eventWithExtensions(12, ""), ctx: t.Context(), attributes: 4, contentType: cloudevents.ApplicationCloudEventsJSON},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			client := &fakeSNS{}
			pub, err := NewPublisher(client, PublisherConfig{Topic: "arn:aws:sns:us-east-1:123456789012:orders", Marshaler: tt.marshaler}, slog.New(slog.DiscardHandler))
			if err != nil {
				t.Fatal(err)
			}
			if err := pub.Publish(tt.ctx, tt.event); err != nil {
				t.Fatalf("Publish() error = %v", err)
			}
			if client.calls != 1 {
				t.Fatalf("client calls = %d, want 1", client.calls)
			}
			attrs := client.lastIn.MessageAttributes
			if len(attrs) != tt.attributes {
				t.Errorf("sent %d attributes, want %d", len(attrs), tt.attributes)
			}
			if got := awssdk.ToString(attrs[contentTypeAttribute].StringValue); got != tt.contentType {
				t.Errorf("Content-Type = %q, want %q", got, tt.contentType)
			}
			if _, ok := attrs[trace.Header]; ok != tt.wantTrace {
				t.Errorf("traceparent attribute present = %v, want %v", ok, tt.wantTrace)
			}
		})
	}

	t.Run("custom marshaler over the limit", func(t *testing.T) {
		t.Parallel()
		client := &fakeSNS{}
		pub, err := NewPublisher(client, PublisherConfig{Topic: "arn:aws:sns:us-east-1:123456789012:orders", Marshaler: overLimit()}, slog.New(slog.DiscardHandler))
		if err != nil {
			t.Fatal(err)
		}
		err = pub.Publish(traced, base)
		if client.calls != 0 {
			t.Errorf("client calls = %d, want 0", client.calls)
		}
		if !errors.Is(err, ErrTooManyAttributes) {
			t.Fatalf("Publish() error = %v, want ErrTooManyAttributes", err)
		}
		if _, ok := errors.AsType[*event.NonRetryableError](err); !ok {
			t.Errorf("error %T is not NonRetryableError", err)
		}
		if !strings.Contains(err.Error(), "traceparent") {
			t.Errorf("error %q does not name the attributes", err.Error())
		}
	})
}

type fakeSTS struct{ arn string }

func (f *fakeSTS) GetCallerIdentity(context.Context, *awssts.GetCallerIdentityInput, ...func(*awssts.Options)) (*awssts.GetCallerIdentityOutput, error) {
	return &awssts.GetCallerIdentityOutput{Account: awssdk.String("123456789012"), Arn: awssdk.String(f.arn)}, nil
}

func TestTopicARNUsesCallerPartition(t *testing.T) {
	t.Parallel()
	identity := &aws.Identity{Config: &awssdk.Config{Region: "cn-north-1"}, Client: &fakeSTS{arn: "arn:aws-cn:sts::123456789012:assumed-role/app/x"}}
	pub, err := NewPublisher(&fakeSNS{}, PublisherConfig{Topic: "orders", Identity: identity}, nil)
	if err != nil {
		t.Fatal(err)
	}
	if got, want := string(pub.topicARN), "arn:aws-cn:sns:cn-north-1:123456789012:orders"; got != want {
		t.Errorf("topicARN = %q, want %q", got, want)
	}
}

func TestPublishLogsContentMode(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		name  string
		event cloudevents.Event
		mode  string
	}{
		{name: "binary", event: eventWithExtensions(0, ""), mode: "binary"},
		{name: "structured", event: eventWithExtensions(12, ""), mode: "structured"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			var buf bytes.Buffer
			logger := slog.New(slog.NewJSONHandler(&buf, &slog.HandlerOptions{Level: slog.LevelDebug}))
			pub, err := NewPublisher(&fakeSNS{}, PublisherConfig{Topic: "arn:aws:sns:us-east-1:123456789012:orders"}, logger)
			if err != nil {
				t.Fatal(err)
			}
			if err := pub.Publish(t.Context(), tc.event); err != nil {
				t.Fatalf("Publish() error = %v", err)
			}
			want := `"` + logfields.KeyCloudEventsContentMode + `":"` + tc.mode + `"`
			if !strings.Contains(buf.String(), want) {
				t.Errorf("log %s does not contain %s", buf.String(), want)
			}
		})
	}
}

// blockingSTS never answers: NewPublisher must give up by itself.
type blockingSTS struct{}

func (blockingSTS) GetCallerIdentity(ctx context.Context, _ *awssts.GetCallerIdentityInput, _ ...func(*awssts.Options)) (*awssts.GetCallerIdentityOutput, error) {
	<-ctx.Done()
	return nil, ctx.Err()
}

func TestNewPublisherBoundsTheSTSLookup(t *testing.T) {
	t.Parallel()
	identity := &aws.Identity{Config: &awssdk.Config{Region: "eu-west-1"}, Client: blockingSTS{}}
	start := time.Now()
	_, err := NewPublisher(&fakeSNS{}, PublisherConfig{Topic: "orders", Identity: identity, ResolveTimeout: 50 * time.Millisecond}, nil)
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("NewPublisher() = %v, want DeadlineExceeded", err)
	}
	if elapsed := time.Since(start); elapsed > 2*time.Second {
		t.Fatalf("NewPublisher took %s, want about ResolveTimeout", elapsed)
	}
	if _, err := NewPublisher(&fakeSNS{}, PublisherConfig{Topic: "orders", Identity: identity, ResolveTimeout: -time.Second}, nil); err == nil {
		t.Fatal("negative ResolveTimeout accepted")
	}
}
