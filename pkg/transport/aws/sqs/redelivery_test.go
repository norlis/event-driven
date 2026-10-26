package sqs

import (
	"context"
	"encoding/json/jsontext"
	"errors"
	"log/slog"
	"testing"
	"time"

	"github.com/norlis/event-driven/v2/pkg/event"
	"github.com/norlis/event-driven/v2/pkg/eventmux"
)

// receiveOne starts a Subscriber on the fake, hands the first message to
// handler, stops, and returns the DeleteMessage calls the fake saw.
func receiveOne(t *testing.T, handler func(*event.Message)) []deleteCall {
	t.Helper()
	fake := &fakeSQS{queueExists: true}
	sub, err := NewSubscriber(newTestClient(fake), SubscriberConfig{Queue: "orders"}, slog.New(slog.DiscardHandler))
	if err != nil {
		t.Fatal(err)
	}
	done := make(chan struct{})
	if err := sub.Start(t.Context(), func(m *event.Message) { handler(m); close(done) }, nil); err != nil {
		t.Fatal(err)
	}
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("message never reached the handler")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := sub.Stop(ctx); err != nil {
		t.Fatalf("Stop() = %v", err)
	}
	fake.mu.Lock()
	defer fake.mu.Unlock()
	return append([]deleteCall(nil), fake.deletes...)
}

// SQS redelivers whatever is not deleted once the visibility timeout expires,
// so every message must be nackable: a nil Delivery.Nack tells the router the
// broker cannot redeliver.
func TestNackLeavesTheMessageForRedelivery(t *testing.T) {
	t.Parallel()
	var canNack bool
	var nackErr error
	deletes := receiveOne(t, func(m *event.Message) {
		canNack = m.CanNack()
		nackErr = m.Nack(errors.New("transient"))
	})

	if !canNack {
		t.Error("CanNack() = false, want true: SQS redelivers after the visibility timeout")
	}
	if nackErr != nil {
		t.Errorf("Nack() = %v, want nil", nackErr)
	}
	if len(deletes) != 0 {
		t.Errorf("DeleteMessage called %d times, want 0: a nacked message must stay in the queue", len(deletes))
	}
}

// Through the router: a retryable handler error on SQS is nacked (redelivered
// by the visibility timeout), not dead-lettered as "noredelivery".
func TestMuxNacksRetryableErrorOnSQS(t *testing.T) {
	t.Parallel()
	fake := &fakeSQS{queueExists: true}
	sub, err := NewSubscriber(newTestClient(fake), SubscriberConfig{Queue: "orders"}, slog.New(slog.DiscardHandler))
	if err != nil {
		t.Fatal(err)
	}
	mux, err := eventmux.New(eventmux.Config{Name: "t", Subscription: sub, Logger: slog.New(slog.DiscardHandler)})
	if err != nil {
		t.Fatal(err)
	}
	handled := make(chan struct{})
	mux.Handle(nil, nil, func(context.Context, map[string]int) (jsontext.Value, error) {
		defer close(handled)
		return nil, errors.New("transient")
	})
	if err := mux.Start(t.Context(), nil); err != nil {
		t.Fatal(err)
	}
	select {
	case <-handled:
	case <-time.After(5 * time.Second):
		t.Fatal("handler never ran")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := mux.Stop(ctx); err != nil {
		t.Fatalf("Stop() = %v", err)
	}

	fake.mu.Lock()
	defer fake.mu.Unlock()
	if len(fake.deletes) != 0 {
		t.Errorf("DeleteMessage called %d times, want 0: a retryable failure must be redelivered, not discarded", len(fake.deletes))
	}
}
