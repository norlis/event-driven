package sqs

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"strings"
	"sync"
	"testing"
	"time"

	awssdk "github.com/aws/aws-sdk-go-v2/aws"
	awssqs "github.com/aws/aws-sdk-go-v2/service/sqs"

	"github.com/norlis/event-driven/v2/pkg/event"
)

const (
	targetGetQueueURL    = "AmazonSQS.GetQueueUrl"
	targetReceive        = "AmazonSQS.ReceiveMessage"
	targetDelete         = "AmazonSQS.DeleteMessage"
	testQueueURL         = "https://sqs.test/123/orders"
	oneMessageResponse   = `{"Messages":[{"MessageId":"m-1","ReceiptHandle":"rh-1","Body":"{\"k\":1}","Attributes":{"ApproximateReceiveCount":"1"},"MessageAttributes":{"ce-id":{"DataType":"String","StringValue":"sqs-1"},"ce-type":{"DataType":"String","StringValue":"test.event"},"ce-source":{"DataType":"String","StringValue":"/test"}}}]}`
	queueMissingResponse = `{"__type":"com.amazonaws.sqs#QueueDoesNotExist","message":"The specified queue does not exist."}`
	// badTimeResponse carries a ce-time the envelope cannot parse.
	badTimeResponse = `{"Messages":[{"MessageId":"m-2","ReceiptHandle":"rh-2","Body":"{\"k\":1}","Attributes":{"ApproximateReceiveCount":"3"},"MessageAttributes":{"ce-id":{"DataType":"String","StringValue":"sqs-2"},"ce-type":{"DataType":"String","StringValue":"test.event"},"ce-source":{"DataType":"String","StringValue":"/test"},"ce-time":{"DataType":"String","StringValue":"yesterday"}}}]}`
)

// deleteCall records what the fake saw on DeleteMessage: whether the request
// context was still alive and carried a deadline.
type deleteCall struct {
	ctxErr      error
	hasDeadline bool
}

// fakeSQS answers the AWS JSON 1.0 protocol by X-Amz-Target. ReceiveMessage
// hands out one message, then blocks until the request context ends, like a
// long-poll with an empty queue.
type fakeSQS struct {
	mu          sync.Mutex
	queueExists bool
	receive     string // first ReceiveMessage body; empty selects oneMessageResponse
	delivered   bool
	deletes     []deleteCall
}

func (f *fakeSQS) Do(r *http.Request) (*http.Response, error) {
	switch r.Header.Get("X-Amz-Target") {
	case targetGetQueueURL:
		if !f.queueExists {
			return jsonResponse(http.StatusBadRequest, queueMissingResponse), nil
		}
		return jsonResponse(http.StatusOK, `{"QueueUrl":"`+testQueueURL+`"}`), nil
	case targetReceive:
		f.mu.Lock()
		first := !f.delivered
		f.delivered = true
		f.mu.Unlock()
		if first {
			body := f.receive
			if body == "" {
				body = oneMessageResponse
			}
			return jsonResponse(http.StatusOK, body), nil
		}
		<-r.Context().Done()
		return nil, r.Context().Err()
	case targetDelete:
		_, hasDeadline := r.Context().Deadline()
		f.mu.Lock()
		f.deletes = append(f.deletes, deleteCall{ctxErr: r.Context().Err(), hasDeadline: hasDeadline})
		f.mu.Unlock()
		return jsonResponse(http.StatusOK, `{}`), nil
	}
	return jsonResponse(http.StatusNotImplemented, `{}`), nil
}

func jsonResponse(status int, body string) *http.Response {
	return &http.Response{
		StatusCode: status,
		Header:     http.Header{"Content-Type": {"application/x-amz-json-1.0"}},
		Body:       io.NopCloser(strings.NewReader(body)),
	}
}

func newTestClient(f *fakeSQS) *awssqs.Client {
	return awssqs.New(awssqs.Options{
		Region:       "us-east-1",
		Credentials:  awssdk.AnonymousCredentials{},
		HTTPClient:   f,
		BaseEndpoint: awssdk.String("https://sqs.test"),
		Retryer:      awssdk.NopRetryer{},
	})
}

func TestStartFailsWhenQueueMissing(t *testing.T) {
	t.Parallel()
	sub, err := NewSubscriber(newTestClient(&fakeSQS{}), SubscriberConfig{Queue: "orders"}, nil)
	if err != nil {
		t.Fatal(err)
	}
	err = sub.Start(t.Context(), func(*event.Message) {}, nil)
	if err == nil || !strings.Contains(err.Error(), `aws/sqs: get queue url "orders"`) {
		t.Fatalf("Start() = %v, want get queue url error", err)
	}
}

func TestAckUsesOwnContextDuringStop(t *testing.T) {
	t.Parallel()
	fake := &fakeSQS{queueExists: true}
	sub, err := NewSubscriber(newTestClient(fake), SubscriberConfig{Queue: "orders", AckTimeout: time.Second}, slog.New(slog.DiscardHandler))
	if err != nil {
		t.Fatal(err)
	}

	inHandler := make(chan *event.Message, 1)
	release := make(chan struct{})
	handler := func(m *event.Message) {
		inHandler <- m
		<-release // keep the message in flight while Stop runs
		_ = m.Ack()
	}
	if err := sub.Start(t.Context(), handler, nil); err != nil {
		t.Fatal(err)
	}
	var msg *event.Message
	select {
	case msg = <-inHandler:
	case <-time.After(5 * time.Second):
		t.Fatal("message never reached the handler")
	}
	if msg.ID() != "sqs-1" {
		t.Errorf("message id = %q, want sqs-1", msg.ID())
	}
	if count, known := msg.DeliveryCount(); !known || count != 1 {
		t.Errorf("DeliveryCount() = %d, %v; want 1, true", count, known)
	}

	stopped := make(chan error, 1)
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		stopped <- sub.Stop(ctx)
	}()
	time.Sleep(50 * time.Millisecond) // let Stop cancel the receive loop first
	close(release)
	if err := <-stopped; err != nil {
		t.Fatalf("Stop() = %v", err)
	}

	fake.mu.Lock()
	defer fake.mu.Unlock()
	if len(fake.deletes) != 1 {
		t.Fatalf("DeleteMessage called %d times, want 1", len(fake.deletes))
	}
	if d := fake.deletes[0]; d.ctxErr != nil || !d.hasDeadline {
		t.Errorf("DeleteMessage ctx: err=%v deadline=%v; want live ctx with a deadline", d.ctxErr, d.hasDeadline)
	}
}

func TestStopWithoutStartAndStopTimeout(t *testing.T) {
	t.Parallel()
	fake := &fakeSQS{queueExists: true}
	sub, err := NewSubscriber(newTestClient(fake), SubscriberConfig{Queue: testQueueURL}, nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := sub.Stop(t.Context()); err != nil {
		t.Errorf("Stop without Start = %v", err)
	}

	block := make(chan struct{})
	t.Cleanup(func() { close(block) })
	if err := sub.Start(t.Context(), func(m *event.Message) { <-block; _ = m.Ack() }, nil); err != nil {
		t.Fatal(err)
	}
	time.Sleep(50 * time.Millisecond) // the single message is now inside the handler
	ctx, cancel := context.WithTimeout(t.Context(), 20*time.Millisecond)
	defer cancel()
	if err := sub.Stop(ctx); err == nil {
		t.Error("Stop with an in-flight handler and an expired ctx returned nil")
	}
}

func TestNewSubscriberRejectsNegativeAckTimeout(t *testing.T) {
	t.Parallel()
	if _, err := NewSubscriber(newTestClient(&fakeSQS{}), SubscriberConfig{Queue: "orders", AckTimeout: -time.Second}, nil); err == nil {
		t.Error("negative AckTimeout accepted")
	}
}

func TestSecondStartIsAlreadyStarted(t *testing.T) {
	t.Parallel()
	sub, err := NewSubscriber(newTestClient(&fakeSQS{queueExists: true}), SubscriberConfig{Queue: testQueueURL}, nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := sub.Start(t.Context(), func(m *event.Message) { _ = m.Ack() }, nil); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = sub.Stop(context.Background()) })
	if err := sub.Start(t.Context(), func(*event.Message) {}, nil); !errors.Is(err, event.ErrAlreadyStarted) {
		t.Errorf("second Start() = %v, want event.ErrAlreadyStarted", err)
	}
}

// TestUndecodableMessageReachesTheHandler: an envelope the unmarshaler
// rejects is handed to the router as an undecodable rescue message, with the
// broker id, the raw body and the delivery mechanics, instead of being deleted
// by the transport.
func TestUndecodableMessageReachesTheHandler(t *testing.T) {
	t.Parallel()
	fake := &fakeSQS{queueExists: true, receive: badTimeResponse}
	sub, err := NewSubscriber(newTestClient(fake), SubscriberConfig{Queue: "orders"}, slog.New(slog.DiscardHandler))
	if err != nil {
		t.Fatal(err)
	}
	got := make(chan *event.Message, 1)
	if err := sub.Start(t.Context(), func(m *event.Message) { _ = m.Discard(m.DecodeError()); got <- m }, nil); err != nil {
		t.Fatal(err)
	}
	var msg *event.Message
	select {
	case msg = <-got:
	case <-time.After(5 * time.Second):
		t.Fatal("message never reached the handler")
	}
	stopCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := sub.Stop(stopCtx); err != nil {
		t.Fatalf("Stop() = %v", err)
	}

	if !errors.Is(msg.DecodeError(), event.ErrDecode) {
		t.Fatalf("DecodeError() = %v, want ErrDecode", msg.DecodeError())
	}
	if msg.ID() != "m-2" || msg.Type() != defaults.Type || string(msg.Data()) != `{"k":1}` {
		t.Errorf("rescue = %s/%s %s; want broker id m-2, default type and the raw body", msg.ID(), msg.Type(), msg.Data())
	}
	if count, known := msg.DeliveryCount(); !known || count != 3 {
		t.Errorf("DeliveryCount() = %d, %v; want 3, true", count, known)
	}
	fake.mu.Lock()
	defer fake.mu.Unlock()
	if len(fake.deletes) != 1 {
		t.Errorf("DeleteMessage called %d times, want 1 (the router's Discard)", len(fake.deletes))
	}
}

// TestStopTimeoutKeepsSubscriberStarted: a Stop that outlives its ctx must
// leave the workers accounted for, so Start refuses to spawn duplicates and a
// later Stop waits for them.
func TestStopTimeoutKeepsSubscriberStarted(t *testing.T) {
	t.Parallel()
	fake := &fakeSQS{queueExists: true}
	sub, err := NewSubscriber(newTestClient(fake), SubscriberConfig{Queue: "orders"}, slog.New(slog.DiscardHandler))
	if err != nil {
		t.Fatal(err)
	}
	inHandler := make(chan struct{}, 1)
	release := make(chan struct{})
	handler := func(m *event.Message) { inHandler <- struct{}{}; <-release; _ = m.Ack() }
	if err := sub.Start(t.Context(), handler, nil); err != nil {
		t.Fatal(err)
	}
	<-inHandler

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
		t.Fatalf("second Stop() = %v, want nil once the worker finished", err)
	}
	if err := sub.Stop(stopCtx); err != nil {
		t.Fatalf("Stop after Stop = %v, want nil", err)
	}
	fake.mu.Lock()
	defer fake.mu.Unlock()
	if len(fake.deletes) != 1 {
		t.Errorf("DeleteMessage called %d times, want 1", len(fake.deletes))
	}
}
