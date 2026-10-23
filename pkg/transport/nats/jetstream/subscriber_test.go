package jetstream

import (
	"context"
	"errors"
	"log/slog"
	"sync"
	"testing"
	"time"

	natsgo "github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"

	"github.com/norlis/event-driven/v2/pkg/event"
)

// The fakes embed the SDK interfaces so only the methods Subscriber calls
// need an implementation; anything else panics loudly.
type fakeConsumeContext struct {
	jetstream.ConsumeContext
	closed    chan struct{}
	once      sync.Once
	drained   bool
	holdDrain bool // Drain does not close: the test closes later, like a slow drain
}

// Drain closes once: the real SDK tolerates a Drain after the consumer died.
func (c *fakeConsumeContext) Drain() {
	c.drained = true
	if !c.holdDrain {
		c.once.Do(func() { close(c.closed) })
	}
}
func (c *fakeConsumeContext) Closed() <-chan struct{} { return c.closed }

type fakeConsumer struct {
	jetstream.Consumer
	cc      *fakeConsumeContext
	handler jetstream.MessageHandler
}

func (c *fakeConsumer) Consume(h jetstream.MessageHandler, _ ...jetstream.PullConsumeOpt) (jetstream.ConsumeContext, error) {
	c.handler = h
	return c.cc, nil
}

type fakeStream struct {
	jetstream.Stream
	consumer *fakeConsumer
}

func (s *fakeStream) CreateConsumer(context.Context, jetstream.ConsumerConfig) (jetstream.Consumer, error) {
	return s.consumer, nil
}

type fakeJS struct {
	jetstream.JetStream
	streams     map[string]*fakeStream
	provisioned []jetstream.StreamConfig
}

func (j *fakeJS) Stream(_ context.Context, name string) (jetstream.Stream, error) {
	s, ok := j.streams[name]
	if !ok {
		return nil, jetstream.ErrStreamNotFound
	}
	return s, nil
}

func (j *fakeJS) CreateOrUpdateStream(_ context.Context, cfg jetstream.StreamConfig) (jetstream.Stream, error) {
	j.provisioned = append(j.provisioned, cfg)
	s := &fakeStream{consumer: &fakeConsumer{cc: &fakeConsumeContext{closed: make(chan struct{})}}}
	j.streams[cfg.Name] = s
	return s, nil
}

const testStream = "EVENTS"

func newFakeJS() (*fakeJS, *fakeConsumeContext) {
	cc := &fakeConsumeContext{closed: make(chan struct{})}
	js := &fakeJS{streams: map[string]*fakeStream{testStream: {consumer: &fakeConsumer{cc: cc}}}}
	return js, cc
}

func TestNewSubscriberRejectsMismatchedStreamConfig(t *testing.T) {
	t.Parallel()
	js, _ := newFakeJS()
	if _, err := NewSubscriber(js, SubscriberConfig{Stream: "EVENTS", StreamConfig: &jetstream.StreamConfig{Name: "OTHER"}}, nil); err == nil {
		t.Error("StreamConfig.Name != Stream accepted")
	}
	if _, err := NewSubscriber(js, SubscriberConfig{Stream: "EVENTS", StreamConfig: &jetstream.StreamConfig{}}, nil); err == nil {
		t.Error("empty StreamConfig.Name accepted")
	}
}

func TestStartFailsWhenStreamMissing(t *testing.T) {
	t.Parallel()
	js, _ := newFakeJS()
	sub, _ := NewSubscriber(js, SubscriberConfig{Stream: "NOPE"}, nil)
	if err := sub.Start(t.Context(), func(*event.Message) {}, nil); !errors.Is(err, jetstream.ErrStreamNotFound) {
		t.Fatalf("Start() = %v, want ErrStreamNotFound", err)
	}
}

func TestStartProvisionsWhenStreamConfigSet(t *testing.T) {
	t.Parallel()
	js := &fakeJS{streams: map[string]*fakeStream{}}
	sub, _ := NewSubscriber(js, SubscriberConfig{Stream: "EVENTS", StreamConfig: &jetstream.StreamConfig{Name: "EVENTS"}}, nil)
	if err := sub.Start(t.Context(), func(*event.Message) {}, nil); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = sub.Stop(context.Background()) })
	if len(js.provisioned) != 1 || js.provisioned[0].Name != "EVENTS" {
		t.Errorf("provisioned = %+v, want one EVENTS stream", js.provisioned)
	}
}

func TestStopDrainsAndDoesNotReportError(t *testing.T) {
	t.Parallel()
	js, cc := newFakeJS()
	sub, _ := NewSubscriber(js, SubscriberConfig{Stream: "EVENTS"}, nil)
	reported := make(chan error, 1)
	if err := sub.Start(t.Context(), func(*event.Message) {}, func(err error) { reported <- err }); err != nil {
		t.Fatal(err)
	}
	if err := sub.Stop(t.Context()); err != nil || !cc.drained {
		t.Fatalf("Stop() = %v, drained=%v; want nil and true", err, cc.drained)
	}
	select {
	case err := <-reported:
		t.Errorf("onError(%v) after Stop, want none", err)
	case <-time.After(100 * time.Millisecond):
	}
	if err := sub.Stop(t.Context()); err != nil {
		t.Errorf("second Stop() = %v, want nil", err)
	}
}

func TestUnexpectedCloseReportsError(t *testing.T) {
	t.Parallel()
	js, cc := newFakeJS()
	sub, _ := NewSubscriber(js, SubscriberConfig{Stream: "EVENTS"}, nil)
	reported := make(chan error, 1)
	if err := sub.Start(t.Context(), func(*event.Message) {}, func(err error) { reported <- err }); err != nil {
		t.Fatal(err)
	}
	cc.once.Do(func() { close(cc.closed) }) // the server deleted the consumer or the connection died
	select {
	case err := <-reported:
		if !errors.Is(err, ErrConsumeStopped) {
			t.Errorf("onError(%v), want ErrConsumeStopped", err)
		}
	case <-time.After(time.Second):
		t.Fatal("onError not called")
	}
}

func TestStopWithoutStart(t *testing.T) {
	t.Parallel()
	js, _ := newFakeJS()
	sub, _ := NewSubscriber(js, SubscriberConfig{Stream: "EVENTS"}, nil)
	if err := sub.Stop(t.Context()); err != nil {
		t.Errorf("Stop without Start = %v", err)
	}
}

func TestSecondStartIsAlreadyStarted(t *testing.T) {
	t.Parallel()
	js, _ := newFakeJS()
	sub, _ := NewSubscriber(js, SubscriberConfig{Stream: testStream}, nil)
	if err := sub.Start(t.Context(), func(*event.Message) {}, nil); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = sub.Stop(context.Background()) })
	if err := sub.Start(t.Context(), func(*event.Message) {}, nil); !errors.Is(err, event.ErrAlreadyStarted) {
		t.Errorf("second Start() = %v, want event.ErrAlreadyStarted", err)
	}
}

func TestUnexpectedCloseWithNilOnErrorDoesNotPanic(t *testing.T) {
	t.Parallel()
	js, cc := newFakeJS()
	sub, _ := NewSubscriber(js, SubscriberConfig{Stream: testStream}, nil)
	if err := sub.Start(t.Context(), func(*event.Message) {}, nil); err != nil {
		t.Fatal(err)
	}
	cc.once.Do(func() { close(cc.closed) })
	time.Sleep(50 * time.Millisecond) // the watcher goroutine would have panicked by now
	if err := sub.Stop(t.Context()); err != nil {
		t.Errorf("Stop() = %v", err)
	}
}

// TestUndecodableMessageReachesTheHandler: an envelope the unmarshaler rejects
// is handed to the router as an undecodable rescue message, which terminates
// it through the router's Discard instead of a transport-side Term.
func TestUndecodableMessageReachesTheHandler(t *testing.T) {
	t.Parallel()
	js, _ := newFakeJS()
	sub, err := NewSubscriber(js, SubscriberConfig{Stream: testStream}, slog.New(slog.DiscardHandler))
	if err != nil {
		t.Fatal(err)
	}
	var got *event.Message
	if err := sub.Start(t.Context(), func(m *event.Message) { got = m; _ = m.Discard(m.DecodeError()) }, nil); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = sub.Stop(context.Background()) })

	headers := natsgo.Header{}
	headers.Set("ce-id", "js-2")
	headers.Set("ce-type", "test.event")
	headers.Set("ce-source", "/test")
	headers.Set("ce-time", "yesterday")
	headers.Set(natsgo.MsgIdHdr, "dedup-2")
	msg := &fakeMsg{md: &jetstream.MsgMetadata{NumDelivered: 2}, headers: headers, data: []byte(`{"k":1}`)}
	js.streams[testStream].consumer.handler(msg)

	if got == nil {
		t.Fatal("message never reached the handler")
	}
	if !errors.Is(got.DecodeError(), event.ErrDecode) {
		t.Errorf("DecodeError() = %v, want ErrDecode", got.DecodeError())
	}
	if got.ID() != "dedup-2" || string(got.Data()) != `{"k":1}` {
		t.Errorf("rescue = %s %s; want Nats-Msg-Id and the raw data", got.ID(), got.Data())
	}
	if count, known := got.DeliveryCount(); !known || count != 2 {
		t.Errorf("DeliveryCount() = %d, %v; want 2, true", count, known)
	}
	if len(msg.termed) != 1 || msg.termed[0] != termReasonDiscarded {
		t.Errorf("Term calls = %v, want one %q", msg.termed, termReasonDiscarded)
	}
}

// TestStopTimeoutKeepsSubscriberStarted: a drain that outlives Stop's ctx
// must keep the consumer accounted for: Start refuses, a later Stop waits for
// the drain and no onError fires for the ordered close.
func TestStopTimeoutKeepsSubscriberStarted(t *testing.T) {
	t.Parallel()
	js, cc := newFakeJS()
	cc.holdDrain = true
	sub, err := NewSubscriber(js, SubscriberConfig{Stream: testStream}, slog.New(slog.DiscardHandler))
	if err != nil {
		t.Fatal(err)
	}
	fatal := make(chan error, 1)
	if err := sub.Start(t.Context(), func(*event.Message) {}, func(err error) { fatal <- err }); err != nil {
		t.Fatal(err)
	}
	shortCtx, cancel := context.WithTimeout(t.Context(), 50*time.Millisecond)
	defer cancel()
	if err := sub.Stop(shortCtx); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("Stop(expired) = %v, want DeadlineExceeded", err)
	}
	if err := sub.Start(t.Context(), func(*event.Message) {}, nil); !errors.Is(err, event.ErrAlreadyStarted) {
		t.Fatalf("Start after a timed-out Stop = %v, want ErrAlreadyStarted", err)
	}
	cc.once.Do(func() { close(cc.closed) }) // the drain finishes
	if err := sub.Stop(t.Context()); err != nil {
		t.Fatalf("second Stop() = %v, want nil", err)
	}
	select {
	case err := <-fatal:
		t.Fatalf("onError(%v) fired for an ordered Stop", err)
	case <-time.After(100 * time.Millisecond):
	}
}
