package pubsub

import (
	"context"
	"errors"
	"log/slog"
	"strings"
	"testing"
	"time"

	gpubsub "cloud.google.com/go/pubsub/v2"
	"cloud.google.com/go/pubsub/v2/apiv1/pubsubpb"
	"cloud.google.com/go/pubsub/v2/pstest"
	cloudevents "github.com/cloudevents/sdk-go/v2/event"
	"google.golang.org/api/option"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/norlis/event-driven/v2/pkg/event"
)

const (
	testProject = "proj"
	testTopic   = "orders"
	testSub     = "orders-sub"
)

// newFakeClient runs an in-process Pub/Sub server with one topic and one
// subscription and returns a client bound to it.
func newFakeClient(t *testing.T) *gpubsub.Client {
	t.Helper()
	srv := pstest.NewServer()
	t.Cleanup(func() { _ = srv.Close() })
	conn, err := grpc.NewClient(srv.Addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	client, err := gpubsub.NewClient(t.Context(), testProject, option.WithGRPCConn(conn))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = client.Close() })
	topic := "projects/" + testProject + "/topics/" + testTopic
	if _, err := client.TopicAdminClient.CreateTopic(t.Context(), &pubsubpb.Topic{Name: topic}); err != nil {
		t.Fatal(err)
	}
	if _, err := client.SubscriptionAdminClient.CreateSubscription(t.Context(), &pubsubpb.Subscription{Name: "projects/" + testProject + "/subscriptions/" + testSub, Topic: topic}); err != nil {
		t.Fatal(err)
	}
	return client
}

func TestConstructorsValidate(t *testing.T) {
	t.Parallel()
	client := newFakeClient(t)
	if _, err := NewPublisher(nil, PublisherConfig{TopicID: testTopic}, nil); err == nil {
		t.Error("nil client accepted by NewPublisher")
	}
	if _, err := NewPublisher(client, PublisherConfig{}, nil); err == nil {
		t.Error("empty TopicID accepted")
	}
	if _, err := NewSubscriber(nil, SubscriberConfig{SubscriptionID: testSub}, nil); err == nil {
		t.Error("nil client accepted by NewSubscriber")
	}
	if _, err := NewSubscriber(client, SubscriberConfig{}, nil); err == nil {
		t.Error("empty SubscriptionID accepted")
	}
	if _, err := NewSubscriber(client, SubscriberConfig{SubscriptionID: testSub, NumGoroutines: -1}, nil); err == nil {
		t.Error("negative NumGoroutines accepted")
	}
}

func TestStartFailsWhenSubscriptionMissing(t *testing.T) {
	t.Parallel()
	sub, err := NewSubscriber(newFakeClient(t), SubscriberConfig{SubscriptionID: "nope"}, nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := sub.Start(t.Context(), func(*event.Message) {}, nil); err == nil {
		t.Fatal("Start on a missing subscription returned nil")
	}
}

func TestPublishDeliverAndStop(t *testing.T) {
	t.Parallel()
	client := newFakeClient(t)
	pub, err := NewPublisher(client, PublisherConfig{TopicID: testTopic}, nil)
	if err != nil {
		t.Fatal(err)
	}
	sub, err := NewSubscriber(client, SubscriberConfig{SubscriptionID: testSub}, slog.New(slog.DiscardHandler))
	if err != nil {
		t.Fatal(err)
	}

	got := make(chan *event.Message, 1)
	if err := sub.Start(t.Context(), func(m *event.Message) { _ = m.Ack(); got <- m }, nil); err != nil {
		t.Fatal(err)
	}

	ce := cloudevents.New()
	ce.SetID("ps-1")
	ce.SetSource("/test")
	ce.SetType("test.event")
	_ = ce.SetData(cloudevents.ApplicationJSON, []byte(`{"k":1}`))
	if err := pub.Publish(t.Context(), ce); err != nil {
		t.Fatal(err)
	}
	select {
	case m := <-got:
		if m.ID() != "ps-1" {
			t.Errorf("delivered id = %q, want ps-1", m.ID())
		}
	case <-time.After(5 * time.Second):
		t.Fatal("message not delivered")
	}

	stopCtx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	if err := sub.Stop(stopCtx); err != nil {
		t.Errorf("Stop() = %v", err)
	}
	if err := pub.Close(); err != nil {
		t.Errorf("Close() = %v", err)
	}
	if err := pub.Publish(t.Context(), ce); err == nil {
		t.Error("Publish after Close returned nil")
	}
}

func TestSubscriberStopWithoutStart(t *testing.T) {
	t.Parallel()
	sub, err := NewSubscriber(newFakeClient(t), SubscriberConfig{SubscriptionID: testSub}, nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := sub.Stop(t.Context()); err != nil {
		t.Errorf("Stop without Start = %v", err)
	}
}

func TestReceiveOutcome(t *testing.T) {
	t.Parallel()
	stopped, cancel := context.WithCancel(t.Context())
	cancel()
	if err := receiveOutcome(stopped, testSub, context.Canceled); err != nil {
		t.Errorf("cancelled by Stop reported %v, want nil", err)
	}
	if err := receiveOutcome(t.Context(), testSub, nil); err != nil {
		t.Errorf("nil receive error reported %v, want nil", err)
	}
	errBoom := errors.New("permission denied")
	err := receiveOutcome(t.Context(), testSub, errBoom)
	if !errors.Is(err, errBoom) || err.Error() != "pubsub: receive \"orders-sub\": permission denied" {
		t.Errorf("runtime failure = %v, want wrapped permission denied", err)
	}
}

// Deleting the subscription makes Receive fail with NotFound: the one
// end-to-end runtime failure pstest can produce.
func TestReceiveFailureReportsOnceAndStopFromOnErrorDoesNotDeadlock(t *testing.T) {
	t.Parallel()
	client := newFakeClient(t)
	sub, err := NewSubscriber(client, SubscriberConfig{SubscriptionID: testSub}, nil)
	if err != nil {
		t.Fatal(err)
	}
	reported := make(chan error, 2)
	stopErr := make(chan error, 1)
	onError := func(err error) {
		reported <- err
		// A caller may react to the failure by stopping right away.
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		stopErr <- sub.Stop(ctx)
	}
	if err := sub.Start(t.Context(), func(*event.Message) {}, onError); err != nil {
		t.Fatal(err)
	}
	if err := client.SubscriptionAdminClient.DeleteSubscription(t.Context(), &pubsubpb.DeleteSubscriptionRequest{Subscription: "projects/" + testProject + "/subscriptions/" + testSub}); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-reported:
		if err == nil || !strings.Contains(err.Error(), `pubsub: receive "orders-sub"`) {
			t.Errorf("onError(%v), want wrapped receive error", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("onError not called after the subscription vanished")
	}
	if err := <-stopErr; err != nil {
		t.Errorf("Stop from onError = %v, want nil (no deadlock)", err)
	}
	select {
	case err := <-reported:
		t.Errorf("onError called twice: %v", err)
	case <-time.After(100 * time.Millisecond):
	}
}

func TestSecondStartIsAlreadyStartedAndNilOnErrorIsTolerated(t *testing.T) {
	t.Parallel()
	client := newFakeClient(t)
	sub, err := NewSubscriber(client, SubscriberConfig{SubscriptionID: testSub}, nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := sub.Start(t.Context(), func(*event.Message) {}, nil); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = sub.Stop(context.Background()) })
	if err := sub.Start(t.Context(), func(*event.Message) {}, nil); !errors.Is(err, event.ErrAlreadyStarted) {
		t.Errorf("second Start() = %v, want event.ErrAlreadyStarted", err)
	}
	// Kill the subscription with onError == nil: must not panic.
	if err := client.SubscriptionAdminClient.DeleteSubscription(t.Context(), &pubsubpb.DeleteSubscriptionRequest{Subscription: "projects/" + testProject + "/subscriptions/" + testSub}); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()
	if err := sub.Stop(ctx); err != nil {
		t.Errorf("Stop() after receive failure = %v", err)
	}
}

// TestUndecodableMessageReachesTheHandler: an envelope the unmarshaler rejects
// is handed to the router as an undecodable rescue message instead of being
// acked by the transport.
func TestUndecodableMessageReachesTheHandler(t *testing.T) {
	t.Parallel()
	client := newFakeClient(t)
	sub, err := NewSubscriber(client, SubscriberConfig{SubscriptionID: testSub}, slog.New(slog.DiscardHandler))
	if err != nil {
		t.Fatal(err)
	}
	got := make(chan *event.Message, 1)
	if err := sub.Start(t.Context(), func(m *event.Message) { _ = m.Discard(m.DecodeError()); got <- m }, nil); err != nil {
		t.Fatal(err)
	}
	raw := client.Publisher(testTopic)
	defer raw.Stop()
	if _, err := raw.Publish(t.Context(), &gpubsub.Message{Data: []byte(`{"k":1}`), Attributes: map[string]string{"ce-id": "ps-2", "ce-type": "test.event", "ce-source": "/test", "ce-time": "yesterday"}}).Get(t.Context()); err != nil {
		t.Fatal(err)
	}
	select {
	case m := <-got:
		if !errors.Is(m.DecodeError(), event.ErrDecode) {
			t.Errorf("DecodeError() = %v, want ErrDecode", m.DecodeError())
		}
		if m.ID() == "" || m.Type() != defaults.Type || string(m.Data()) != `{"k":1}` {
			t.Errorf("rescue = %s/%s %s; want broker id, default type and the raw body", m.ID(), m.Type(), m.Data())
		}
	case <-time.After(5 * time.Second):
		t.Fatal("message not delivered")
	}
	stopCtx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	if err := sub.Stop(stopCtx); err != nil {
		t.Errorf("Stop() = %v", err)
	}
}

// TestStopTimeoutKeepsSubscriberStarted: a Stop that outlives its ctx must
// keep the receive loop accounted for: Start refuses and a later Stop waits.
func TestStopTimeoutKeepsSubscriberStarted(t *testing.T) {
	t.Parallel()
	client := newFakeClient(t)
	pub, err := NewPublisher(client, PublisherConfig{TopicID: testTopic}, nil)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = pub.Close() })
	sub, err := NewSubscriber(client, SubscriberConfig{SubscriptionID: testSub}, slog.New(slog.DiscardHandler))
	if err != nil {
		t.Fatal(err)
	}
	inHandler := make(chan struct{}, 1)
	release := make(chan struct{})
	handler := func(m *event.Message) { inHandler <- struct{}{}; <-release; _ = m.Ack() }
	if err := sub.Start(t.Context(), handler, nil); err != nil {
		t.Fatal(err)
	}
	ce := cloudevents.New()
	ce.SetID("ps-3")
	ce.SetSource("/test")
	ce.SetType("test.event")
	if err := pub.Publish(t.Context(), ce); err != nil {
		t.Fatal(err)
	}
	select {
	case <-inHandler:
	case <-time.After(5 * time.Second):
		t.Fatal("message not delivered")
	}

	shortCtx, cancel := context.WithTimeout(t.Context(), 50*time.Millisecond)
	defer cancel()
	if err := sub.Stop(shortCtx); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("Stop(expired) = %v, want DeadlineExceeded", err)
	}
	if err := sub.Start(t.Context(), handler, nil); !errors.Is(err, event.ErrAlreadyStarted) {
		t.Fatalf("Start after a timed-out Stop = %v, want ErrAlreadyStarted", err)
	}
	close(release)
	stopCtx, cancel2 := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel2()
	if err := sub.Stop(stopCtx); err != nil {
		t.Fatalf("second Stop() = %v, want nil", err)
	}
}
