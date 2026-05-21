package eventmux

import (
	"context"
	"encoding/json"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	cloudevents "github.com/cloudevents/sdk-go/v2/event"

	"github.com/norlis/event-driven/pkg/event"
	"github.com/norlis/event-driven/pkg/eventmux/metadata"
)

// ---------- fakes ----------

// fakeSubscription delivers a fixed slice of messages and optionally blocks
// until the context is cancelled (for RunBackground tests).
type fakeSubscription struct {
	messages []*event.Message
	startErr error
	block    bool
	started  atomic.Bool
}

func (s *fakeSubscription) Start(ctx context.Context, handler func(*event.Message)) error {
	s.started.Store(true)
	if s.startErr != nil {
		return s.startErr
	}
	for _, m := range s.messages {
		handler(m)
	}
	if s.block {
		<-ctx.Done()
	}
	return nil
}

// fakePublisher records every event passed to Publish.
type fakePublisher struct {
	mu        sync.Mutex
	published []cloudevents.Event
	err       error
}

func (p *fakePublisher) Publish(ev cloudevents.Event) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.published = append(p.published, ev)
	return p.err
}

func (p *fakePublisher) Events() []cloudevents.Event {
	p.mu.Lock()
	defer p.mu.Unlock()
	out := make([]cloudevents.Event, len(p.published))
	copy(out, p.published)
	return out
}

// staticFilter returns a fixed match decision.
type staticFilter struct{ match bool }

func (f staticFilter) Match(*event.Message) bool { return f.match }

// stuckSubscription deliberately ignores ctx cancellation; it only returns
// when the test closes its release channel. Used to exercise stop-timeout paths.
type stuckSubscription struct {
	release <-chan struct{}
	started atomic.Bool
}

func (s *stuckSubscription) Start(_ context.Context, _ func(*event.Message)) error {
	s.started.Store(true)
	<-s.release
	return nil
}

// recordingMessage wraps a *event.Message and records ack/nack/preflight signals.
type recordingMessage struct {
	msg          *event.Message
	ackCount     atomic.Int32
	nackCount    atomic.Int32
	preflightErr atomic.Value // holds error (nil-error stored as sentinel)
	preflightHit atomic.Bool
}

func newRecordingMessage(t *testing.T, ce cloudevents.Event) *recordingMessage {
	t.Helper()
	r := &recordingMessage{}
	r.msg = event.NewMessage(
		ce,
		func() { r.ackCount.Add(1) },
		func() { r.nackCount.Add(1) },
	)
	r.msg.SetPreflightCallback(func(err error) {
		r.preflightHit.Store(true)
		if err != nil {
			r.preflightErr.Store(err)
		}
	})
	return r
}

// newCloudEvent builds a CloudEvent with the given metadata and a JSON-marshalled payload.
func newCloudEvent(t *testing.T, id, evType, source string, payload any) cloudevents.Event {
	t.Helper()
	ce := cloudevents.New()
	ce.SetID(id)
	ce.SetType(evType)
	ce.SetSource(source)
	if payload != nil {
		body, err := json.Marshal(payload)
		if err != nil {
			t.Fatalf("marshal payload: %v", err)
		}
		if err := ce.SetData(cloudevents.ApplicationJSON, body); err != nil {
			t.Fatalf("set data: %v", err)
		}
	}
	return ce
}

// newCloudEventRaw is like newCloudEvent but lets you inject arbitrary bytes (e.g. invalid JSON).
func newCloudEventRaw(t *testing.T, id, evType, source string, raw []byte) cloudevents.Event {
	t.Helper()
	ce := cloudevents.New()
	ce.SetID(id)
	ce.SetType(evType)
	ce.SetSource(source)
	if err := ce.SetData(cloudevents.ApplicationJSON, raw); err != nil {
		t.Fatalf("set data: %v", err)
	}
	return ce
}

// testPayload is the canonical typed payload used across tests.
type testPayload struct {
	ID    string `json:"id"`
	Value int    `json:"value"`
}

// newMux builds a Mux with a discarding logger and the given subscription.
func newMux(sub Subscription) *Mux {
	return New(Config{
		Name:         "test-mux",
		Subscription: sub,
	})
}

// runMuxSync drives mux.Run synchronously and fails the test on a non-nil error.
func runMuxSync(t *testing.T, mux *Mux) {
	t.Helper()
	if err := mux.Run(context.Background()); err != nil {
		t.Fatalf("mux.Run: %v", err)
	}
}

// ---------- dispatch core tests ----------

func TestMux_DispatchesToMatchingRoute(t *testing.T) {
	t.Parallel()

	rec := newRecordingMessage(t, newCloudEvent(t, "1", "order.created", "test://src", testPayload{ID: "abc", Value: 7}))

	var got *testPayload
	handler := func(_ context.Context, data any) (json.RawMessage, error) {
		p, ok := data.(*testPayload)
		if !ok {
			t.Fatalf("unexpected payload type: %T", data)
		}
		got = p
		return nil, nil
	}

	sub := &fakeSubscription{messages: []*event.Message{rec.msg}}
	mux := newMux(sub)
	mux.Register(nil, staticFilter{match: true}, &testPayload{}, handler)

	runMuxSync(t, mux)

	if got == nil {
		t.Fatal("handler was not called")
	}
	if got.ID != "abc" || got.Value != 7 {
		t.Errorf("decoded payload = %+v, want {ID:abc, Value:7}", got)
	}
	if rec.ackCount.Load() != 1 {
		t.Errorf("ack called %d times, want 1", rec.ackCount.Load())
	}
	if rec.nackCount.Load() != 0 {
		t.Errorf("nack called %d times, want 0", rec.nackCount.Load())
	}
	if err, _ := rec.preflightErr.Load().(error); err != nil {
		t.Errorf("preflight error = %v, want nil", err)
	}
}

func TestMux_DecodeError_NacksAndNotifiesPreflight(t *testing.T) {
	t.Parallel()

	rec := newRecordingMessage(t, newCloudEventRaw(t, "1", "order.created", "test://src", []byte(`{"id":`))) // invalid JSON

	handlerCalled := false
	handler := func(context.Context, any) (json.RawMessage, error) {
		handlerCalled = true
		return nil, nil
	}

	sub := &fakeSubscription{messages: []*event.Message{rec.msg}}
	mux := newMux(sub)
	mux.Register(nil, staticFilter{match: true}, &testPayload{}, handler)

	runMuxSync(t, mux)

	if handlerCalled {
		t.Error("handler was called despite decode failure")
	}
	if rec.ackCount.Load() != 0 {
		t.Errorf("ack called %d times, want 0", rec.ackCount.Load())
	}
	if rec.nackCount.Load() != 1 {
		t.Errorf("nack called %d times, want 1", rec.nackCount.Load())
	}
	if !rec.preflightHit.Load() {
		t.Error("preflight callback was not invoked")
	}
	if err, _ := rec.preflightErr.Load().(error); err == nil {
		t.Error("preflight error is nil; expected decode error")
	}
}

func TestMux_PreflightMiddlewareError_NacksAndNotifies(t *testing.T) {
	t.Parallel()

	rec := newRecordingMessage(t, newCloudEvent(t, "1", "x", "test://src", testPayload{ID: "abc"}))

	preflightErr := errors.New("authz denied")
	denyingPreflight := func(next HandlerFunc) HandlerFunc {
		return func(ctx context.Context, data any) (json.RawMessage, error) {
			return nil, preflightErr
		}
	}

	handlerCalled := false
	handler := func(context.Context, any) (json.RawMessage, error) {
		handlerCalled = true
		return nil, nil
	}

	sub := &fakeSubscription{messages: []*event.Message{rec.msg}}
	mux := newMux(sub)
	mux.UsePreflight(denyingPreflight)
	mux.Register(nil, staticFilter{match: true}, &testPayload{}, handler)

	runMuxSync(t, mux)

	if handlerCalled {
		t.Error("main handler ran despite preflight failure")
	}
	if rec.nackCount.Load() != 1 {
		t.Errorf("nack count = %d, want 1", rec.nackCount.Load())
	}
	if rec.ackCount.Load() != 0 {
		t.Errorf("ack count = %d, want 0", rec.ackCount.Load())
	}
	if got, _ := rec.preflightErr.Load().(error); !errors.Is(got, preflightErr) {
		t.Errorf("preflight error = %v, want %v", got, preflightErr)
	}
}

// ---------- publish-result tests ----------

func TestMux_PublishesResultWhenPublisherSet(t *testing.T) {
	t.Parallel()

	rec := newRecordingMessage(t, newCloudEvent(t, "msg-1", "order.created", "test://orders", testPayload{ID: "abc"}))

	resultBody := json.RawMessage(`{"ok":true}`)
	handler := func(context.Context, any) (json.RawMessage, error) {
		return resultBody, nil
	}

	pub := &fakePublisher{}
	sub := &fakeSubscription{messages: []*event.Message{rec.msg}}
	mux := newMux(sub)
	mux.Register(pub, staticFilter{match: true}, &testPayload{}, handler)

	runMuxSync(t, mux)

	events := pub.Events()
	if len(events) != 1 {
		t.Fatalf("publisher got %d events, want 1", len(events))
	}
	got := events[0]
	if got.Type() != "order.created.result" {
		t.Errorf("Type() = %q, want %q", got.Type(), "order.created.result")
	}
	if got.Source() != "test://orders" {
		t.Errorf("Source() = %q, want %q", got.Source(), "test://orders")
	}
	if string(got.Data()) != string(resultBody) {
		t.Errorf("Data() = %s, want %s", string(got.Data()), string(resultBody))
	}
	if got.ID() == "" {
		t.Error("result event ID is empty; expected a uuid")
	}
	if rec.ackCount.Load() != 1 {
		t.Errorf("ack count = %d, want 1", rec.ackCount.Load())
	}
}

func TestMux_DoesNotPublishWhenHandlerReturnsNilData(t *testing.T) {
	t.Parallel()

	rec := newRecordingMessage(t, newCloudEvent(t, "1", "x", "test://src", testPayload{ID: "abc"}))

	handler := func(context.Context, any) (json.RawMessage, error) {
		return nil, nil
	}

	pub := &fakePublisher{}
	sub := &fakeSubscription{messages: []*event.Message{rec.msg}}
	mux := newMux(sub)
	mux.Register(pub, staticFilter{match: true}, &testPayload{}, handler)

	runMuxSync(t, mux)

	if got := len(pub.Events()); got != 0 {
		t.Errorf("publisher got %d events, want 0", got)
	}
	if rec.ackCount.Load() != 1 {
		t.Errorf("ack count = %d, want 1", rec.ackCount.Load())
	}
}

func TestMux_DoesNotPublishWhenNoPublisher(t *testing.T) {
	t.Parallel()

	rec := newRecordingMessage(t, newCloudEvent(t, "1", "x", "test://src", testPayload{ID: "abc"}))

	handler := func(context.Context, any) (json.RawMessage, error) {
		return json.RawMessage(`{"ok":true}`), nil
	}

	sub := &fakeSubscription{messages: []*event.Message{rec.msg}}
	mux := newMux(sub)
	mux.Register(nil, staticFilter{match: true}, &testPayload{}, handler)

	runMuxSync(t, mux) // must not panic
}

func TestMux_PropagatesMetadataAsExtensions(t *testing.T) {
	t.Parallel()

	rec := newRecordingMessage(t, newCloudEvent(t, "1", "x", "test://src", testPayload{ID: "abc"}))

	handler := func(ctx context.Context, _ any) (json.RawMessage, error) {
		store, ok := metadata.FromContext(ctx)
		if !ok {
			t.Fatal("metadata store not in context")
		}
		store.Set("traceid", "trace-123")
		store.Set("tenant", "acme")
		return json.RawMessage(`{"ok":true}`), nil
	}

	pub := &fakePublisher{}
	sub := &fakeSubscription{messages: []*event.Message{rec.msg}}
	mux := newMux(sub)
	mux.Register(pub, staticFilter{match: true}, &testPayload{}, handler)

	runMuxSync(t, mux)

	events := pub.Events()
	if len(events) != 1 {
		t.Fatalf("publisher got %d events, want 1", len(events))
	}
	ext := events[0].Extensions()
	if ext["traceid"] != "trace-123" {
		t.Errorf("ext[traceid] = %v, want %q", ext["traceid"], "trace-123")
	}
	if ext["tenant"] != "acme" {
		t.Errorf("ext[tenant] = %v, want %q", ext["tenant"], "acme")
	}
}

func TestMux_PreservesOriginalExtensions(t *testing.T) {
	t.Parallel()

	ce := newCloudEvent(t, "1", "x", "test://src", testPayload{ID: "abc"})
	ce.SetExtension("origonly", "from-source")
	ce.SetExtension("shared", "from-source")
	rec := newRecordingMessage(t, ce)

	handler := func(ctx context.Context, _ any) (json.RawMessage, error) {
		store, _ := metadata.FromContext(ctx)
		store.Set("shared", "from-handler")
		store.Set("handleronly", "from-handler")
		return json.RawMessage(`{"ok":true}`), nil
	}

	pub := &fakePublisher{}
	sub := &fakeSubscription{messages: []*event.Message{rec.msg}}
	mux := newMux(sub)
	mux.Register(pub, staticFilter{match: true}, &testPayload{}, handler)

	runMuxSync(t, mux)

	events := pub.Events()
	if len(events) != 1 {
		t.Fatalf("publisher got %d events, want 1", len(events))
	}
	ext := events[0].Extensions()
	if ext["origonly"] != "from-source" {
		t.Errorf("origonly = %v, want %q", ext["origonly"], "from-source")
	}
	if ext["handleronly"] != "from-handler" {
		t.Errorf("handleronly = %v, want %q", ext["handleronly"], "from-handler")
	}
	// Handler metadata wins over original extension on key collision.
	if ext["shared"] != "from-handler" {
		t.Errorf("shared = %v, want %q (handler should override)", ext["shared"], "from-handler")
	}
}

func TestMux_HandlerError(t *testing.T) {
	t.Parallel()

	retryable := errors.New("transient db failure")
	nonRetryable := event.NewNonRetryableError(errors.New("bad payload"))

	tests := []struct {
		name      string
		handler   HandlerFunc
		wantAck   int32
		wantNack  int32
		wantHCall bool
	}{
		{
			name: "retryable error -> nack",
			handler: func(context.Context, any) (json.RawMessage, error) {
				return nil, retryable
			},
			wantAck:   0,
			wantNack:  1,
			wantHCall: true,
		},
		{
			name: "NonRetryableError -> ack (discard)",
			handler: func(context.Context, any) (json.RawMessage, error) {
				return nil, nonRetryable
			},
			wantAck:   1,
			wantNack:  0,
			wantHCall: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			rec := newRecordingMessage(t, newCloudEvent(t, "1", "x", "test://src", testPayload{ID: "abc"}))

			sub := &fakeSubscription{messages: []*event.Message{rec.msg}}
			mux := newMux(sub)
			mux.Register(nil, staticFilter{match: true}, &testPayload{}, tt.handler)

			runMuxSync(t, mux)

			if rec.ackCount.Load() != tt.wantAck {
				t.Errorf("ack = %d, want %d", rec.ackCount.Load(), tt.wantAck)
			}
			if rec.nackCount.Load() != tt.wantNack {
				t.Errorf("nack = %d, want %d", rec.nackCount.Load(), tt.wantNack)
			}
		})
	}
}

// ---------- routing tests ----------

func TestMux_NoMatchingRoute_AcksWithoutCallingHandler(t *testing.T) {
	t.Parallel()

	rec := newRecordingMessage(t, newCloudEvent(t, "1", "x", "test://src", testPayload{ID: "abc"}))

	handlerCalled := false
	handler := func(context.Context, any) (json.RawMessage, error) {
		handlerCalled = true
		return nil, nil
	}

	sub := &fakeSubscription{messages: []*event.Message{rec.msg}}
	mux := newMux(sub)
	mux.Register(nil, staticFilter{match: false}, &testPayload{}, handler)

	runMuxSync(t, mux)

	if handlerCalled {
		t.Error("handler ran but no route should have matched")
	}
	if rec.ackCount.Load() != 1 {
		t.Errorf("ack = %d, want 1", rec.ackCount.Load())
	}
	if rec.nackCount.Load() != 0 {
		t.Errorf("nack = %d, want 0", rec.nackCount.Load())
	}
	if rec.preflightHit.Load() {
		t.Error("preflight callback fired but ReportOnNoMatch=false")
	}
}

func TestMux_NoMatchingRoute_WithReportOnNoMatch_FiresPreflightWithErrNoRoute(t *testing.T) {
	t.Parallel()

	rec := newRecordingMessage(t, newCloudEvent(t, "1", "x", "test://src", testPayload{ID: "abc"}))

	sub := &fakeSubscription{messages: []*event.Message{rec.msg}}
	mux := New(Config{
		Name:            "test-mux",
		Subscription:    sub,
		ReportOnNoMatch: true,
	})
	mux.Register(nil, staticFilter{match: false}, &testPayload{}, func(context.Context, any) (json.RawMessage, error) {
		return nil, nil
	})

	runMuxSync(t, mux)

	if !rec.preflightHit.Load() {
		t.Fatal("preflight callback did not fire")
	}
	got, _ := rec.preflightErr.Load().(error)
	if !errors.Is(got, event.ErrNoRoute) {
		t.Errorf("preflight error = %v, want ErrNoRoute", got)
	}
	if rec.ackCount.Load() != 1 {
		t.Errorf("ack = %d, want 1", rec.ackCount.Load())
	}
}

func TestMux_MultipleRoutes_FirstMatchWins(t *testing.T) {
	t.Parallel()

	rec := newRecordingMessage(t, newCloudEvent(t, "1", "x", "test://src", testPayload{ID: "abc"}))

	var called string
	handler := func(name string) HandlerFunc {
		return func(context.Context, any) (json.RawMessage, error) {
			called = name
			return nil, nil
		}
	}

	sub := &fakeSubscription{messages: []*event.Message{rec.msg}}
	mux := newMux(sub)
	mux.Register(nil, staticFilter{match: false}, &testPayload{}, handler("first"))
	mux.Register(nil, staticFilter{match: true}, &testPayload{}, handler("second"))
	mux.Register(nil, staticFilter{match: true}, &testPayload{}, handler("third"))

	runMuxSync(t, mux)

	if called != "second" {
		t.Errorf("handler called = %q, want %q", called, "second")
	}
}

func TestMux_NilFilterMatchesEverything(t *testing.T) {
	t.Parallel()

	rec := newRecordingMessage(t, newCloudEvent(t, "1", "x", "test://src", testPayload{ID: "abc"}))

	called := false
	handler := func(context.Context, any) (json.RawMessage, error) {
		called = true
		return nil, nil
	}

	sub := &fakeSubscription{messages: []*event.Message{rec.msg}}
	mux := newMux(sub)
	mux.Register(nil, nil, &testPayload{}, handler)

	runMuxSync(t, mux)

	if !called {
		t.Error("handler not called; nil filter should match")
	}
}

func TestMux_Name(t *testing.T) {
	t.Parallel()

	mux := New(Config{Subscription: &fakeSubscription{}})
	if got := mux.Name(); got != "eventmux" {
		t.Errorf("Name() = %q, want %q (default)", got, "eventmux")
	}

	named := New(Config{Name: "orders", Subscription: &fakeSubscription{}})
	if got := named.Name(); got != "orders" {
		t.Errorf("Name() = %q, want %q", got, "orders")
	}
}

// ---------- middleware order ----------

func TestMux_MiddlewareOrder_FirstUseIsOutermost(t *testing.T) {
	t.Parallel()

	var (
		mu    sync.Mutex
		calls []string
	)
	record := func(s string) {
		mu.Lock()
		defer mu.Unlock()
		calls = append(calls, s)
	}

	mw := func(name string) Middleware {
		return func(next HandlerFunc) HandlerFunc {
			return func(ctx context.Context, data any) (json.RawMessage, error) {
				record(name + ":before")
				out, err := next(ctx, data)
				record(name + ":after")
				return out, err
			}
		}
	}

	rec := newRecordingMessage(t, newCloudEvent(t, "1", "x", "test://src", testPayload{ID: "abc"}))

	sub := &fakeSubscription{messages: []*event.Message{rec.msg}}
	mux := newMux(sub)
	mux.Use(mw("a"), mw("b"), mw("c"))
	mux.Register(nil, staticFilter{match: true}, &testPayload{}, func(context.Context, any) (json.RawMessage, error) {
		record("handler")
		return nil, nil
	})

	runMuxSync(t, mux)

	want := []string{
		"a:before", "b:before", "c:before",
		"handler",
		"c:after", "b:after", "a:after",
	}
	if len(calls) != len(want) {
		t.Fatalf("calls = %v, want %v", calls, want)
	}
	for i, c := range calls {
		if c != want[i] {
			t.Errorf("calls[%d] = %q, want %q", i, c, want[i])
		}
	}
}

// ---------- RunBackground tests ----------

func TestMux_RunBackground_StopReturnsAfterShutdown(t *testing.T) {
	t.Parallel()

	sub := &fakeSubscription{block: true}
	mux := newMux(sub)

	stop := mux.RunBackground(context.Background(), nil)

	// Give the goroutine a chance to enter Start before stopping.
	deadline := time.After(time.Second)
	for !sub.started.Load() {
		select {
		case <-deadline:
			t.Fatal("subscription did not start")
		default:
			time.Sleep(time.Millisecond)
		}
	}

	if err := stop(time.Second); err != nil {
		t.Errorf("stop returned error: %v", err)
	}
}

func TestMux_RunBackground_OnErrorFiresOnSubscriptionFailure(t *testing.T) {
	t.Parallel()

	startErr := errors.New("subscription boom")
	sub := &fakeSubscription{startErr: startErr}

	mux := newMux(sub)

	errCh := make(chan error, 1)
	stop := mux.RunBackground(context.Background(), func(err error) { errCh <- err })

	select {
	case got := <-errCh:
		if !errors.Is(got, startErr) {
			t.Errorf("onError got %v, want wrap of %v", got, startErr)
		}
	case <-time.After(time.Second):
		t.Fatal("onError was not called within 1s")
	}

	// stop should return cleanly (the goroutine already exited).
	if err := stop(time.Second); err != nil {
		t.Errorf("stop returned error: %v", err)
	}
}

func TestMux_RunBackground_StopTimeoutWhenSubscriptionIgnoresCancellation(t *testing.T) {
	t.Parallel()

	// Subscription that ignores ctx cancellation. We release it via the test cleanup
	// channel to avoid leaking the goroutine past the test.
	release := make(chan struct{})
	t.Cleanup(func() { close(release) })

	sub := &stuckSubscription{release: release}
	mux := newMux(sub)

	stop := mux.RunBackground(context.Background(), nil)

	deadline := time.After(time.Second)
	for !sub.started.Load() {
		select {
		case <-deadline:
			t.Fatal("subscription did not start")
		default:
			time.Sleep(time.Millisecond)
		}
	}

	err := stop(50 * time.Millisecond)
	if err == nil {
		t.Fatal("expected stop timeout error, got nil")
	}
}

func TestMux_PublishResult_PublisherErrorIsSwallowed(t *testing.T) {
	t.Parallel()

	rec := newRecordingMessage(t, newCloudEvent(t, "1", "x", "test://src", testPayload{ID: "abc"}))

	pub := &fakePublisher{err: errors.New("publish boom")}
	sub := &fakeSubscription{messages: []*event.Message{rec.msg}}
	mux := newMux(sub)
	mux.Register(pub, staticFilter{match: true}, &testPayload{}, func(context.Context, any) (json.RawMessage, error) {
		return json.RawMessage(`{"ok":true}`), nil
	})

	runMuxSync(t, mux)

	// Publisher was still invoked once even though it returned an error.
	if got := len(pub.Events()); got != 1 {
		t.Errorf("publisher invocations = %d, want 1", got)
	}
	// The message was acked before publishing, so a publish error must not change that.
	if rec.ackCount.Load() != 1 {
		t.Errorf("ack = %d, want 1", rec.ackCount.Load())
	}
	if rec.nackCount.Load() != 0 {
		t.Errorf("nack = %d, want 0", rec.nackCount.Load())
	}
}

func TestMux_Run_SubscriptionErrorIsWrapped(t *testing.T) {
	t.Parallel()

	startErr := errors.New("boom")
	sub := &fakeSubscription{startErr: startErr}

	mux := newMux(sub)

	err := mux.Run(context.Background())
	if err == nil {
		t.Fatal("expected error, got nil")
	}
	if !errors.Is(err, startErr) {
		t.Errorf("err = %v, want wrap of %v", err, startErr)
	}
}

// Compile-time interface assertions for fakes.
var (
	_ Subscription = (*fakeSubscription)(nil)
	_ Publisher    = (*fakePublisher)(nil)
	_ Filter       = staticFilter{}
)

// Touch cloudevents to keep the import used in helpers visible to tooling.
var _ = cloudevents.New