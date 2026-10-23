package eventmux

import (
	"bytes"
	"context"
	"encoding/json/jsontext"
	"errors"
	"log/slog"
	"strconv"
	"strings"
	"testing"
	"uuid"

	cloudevents "github.com/cloudevents/sdk-go/v2/event"

	"github.com/norlis/httpgate/logging"

	"github.com/norlis/event-driven/v2/pkg/event"
	"github.com/norlis/event-driven/v2/pkg/eventmux/metadata"
	"github.com/norlis/event-driven/v2/pkg/kit/logfields"
)

type capturePublisher struct {
	got []cloudevents.Event
	err error // returned by Publish when set
}

func (p *capturePublisher) Publish(_ context.Context, ce cloudevents.Event) error {
	if p.err != nil {
		return p.err
	}
	p.got = append(p.got, ce)
	return nil
}

func TestPublishResultIDIsUUIDv7(t *testing.T) {
	t.Parallel()

	src := cloudevents.New()
	src.SetID("in-1")
	src.SetSource("/test")
	src.SetType("test.event")

	pub := &capturePublisher{}
	mux, err := New(Config{Subscription: newFakeSubscription()})
	if err != nil {
		t.Fatal(err)
	}
	mux.publishResult(t.Context(), event.NewMessage(t.Context(), src, event.Delivery{}), []byte(`{"ok":true}`), metadata.NewStore(), pub)

	if len(pub.got) != 1 {
		t.Fatalf("published %d events, want 1", len(pub.got))
	}
	gotID := pub.got[0].ID()
	id, err := uuid.Parse(gotID)
	if err != nil {
		t.Fatalf("result event ID %q is not a UUID: %v", gotID, err)
	}
	if id.String() != gotID {
		t.Errorf("round-trip ID = %q, want %q", id.String(), gotID)
	}
	if version := id[6] >> 4; version != 7 {
		t.Errorf("result event ID version = %d, want 7", version)
	}
	if got := pub.got[0].Type(); got != "test.event.result" {
		t.Errorf("result event type = %q, want %q", got, "test.event.result")
	}
}

// fakeSubscription hands the mux handler back to the test so messages can be
// injected synchronously, and records lifecycle calls.
type fakeSubscription struct {
	handler  func(*event.Message)
	onError  func(error)
	startErr error // returned by Start when set
	stopErr  error // returned by Stop when set
	stops    int
}

func newFakeSubscription() *fakeSubscription { return &fakeSubscription{} }

func (s *fakeSubscription) Start(_ context.Context, h func(*event.Message), onError func(error)) error {
	if s.startErr != nil {
		return s.startErr
	}
	s.handler, s.onError = h, onError
	return nil
}

func (s *fakeSubscription) Stop(context.Context) error {
	s.stops++
	return s.stopErr
}

type payload struct {
	Name string `json:"name"`
}

// errPreflight stands in for validate.Error: middleware/validate imports eventmux,
// so the real one cannot be used from this package's tests.
var errPreflight = errors.New("preflight: name is required")

func requireName(next HandlerFunc) HandlerFunc {
	return func(ctx context.Context, data any) (jsontext.Value, error) {
		if p, ok := data.(payload); ok && p.Name == "" {
			return nil, errPreflight
		}
		return next(ctx, data)
	}
}

type muxHarness struct {
	mux  *Mux
	sub  *fakeSubscription
	pub  *capturePublisher
	dlq  *capturePublisher
	logs *bytes.Buffer
}

// newMuxHarness wires a mux with one route, the requireName preflight and a
// capturing dead-letter publisher. tune may adjust Config before New.
func newMuxHarness(t *testing.T, handler func(context.Context, payload) (jsontext.Value, error), tune ...func(*Config)) *muxHarness {
	t.Helper()
	h := &muxHarness{sub: newFakeSubscription(), pub: &capturePublisher{}, dlq: &capturePublisher{}, logs: &bytes.Buffer{}}
	logger := slog.New(slog.NewJSONHandler(h.logs, &slog.HandlerOptions{Level: slog.LevelDebug}))
	cfg := Config{Name: "test", Subscription: h.sub, Logger: logger, DeadLetter: h.dlq}
	for _, f := range tune {
		f(&cfg)
	}
	mux, err := New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	mux.UsePreflight(requireName)
	mux.Handle(h.pub, nil, handler)
	if err := mux.Start(t.Context(), nil); err != nil {
		t.Fatal(err)
	}
	h.mux = mux
	t.Cleanup(func() { _ = mux.Stop(context.Background()) })
	return h
}

// replacePreflight rebuilds the mux with pf instead of requireName.
func (h *muxHarness) replacePreflight(t *testing.T, pf Middleware) {
	t.Helper()
	_ = h.mux.Stop(context.Background())
	h.sub = newFakeSubscription()
	h.logs.Reset()
	h.pub.got, h.dlq.got = nil, nil
	logger := slog.New(slog.NewJSONHandler(h.logs, &slog.HandlerOptions{Level: slog.LevelDebug}))
	mux, err := New(Config{Name: "test", Subscription: h.sub, Logger: logger, DeadLetter: h.dlq})
	if err != nil {
		t.Fatal(err)
	}
	mux.UsePreflight(pf)
	mux.Handle(h.pub, nil, func(context.Context, payload) (jsontext.Value, error) { return nil, nil })
	if err := mux.Start(t.Context(), nil); err != nil {
		t.Fatal(err)
	}
	h.mux = mux
	t.Cleanup(func() { _ = mux.Stop(context.Background()) })
}

func newIncoming(t *testing.T, data string) *event.Message {
	t.Helper()
	return newIncomingDelivered(t, data, 0)
}

func newIncomingDelivered(t *testing.T, data string, count uint64) *event.Message {
	t.Helper()
	ce := cloudevents.New()
	ce.SetID("in-1")
	ce.SetSource("/test")
	ce.SetType("test.event")
	ce.SetExtension(event.ExtCorrelationID, "corr-1")
	if err := ce.SetData(cloudevents.ApplicationJSON, []byte(data)); err != nil {
		t.Fatal(err)
	}
	// Nack present: the harness models a broker that can redeliver.
	return event.NewMessage(t.Context(), ce, event.Delivery{Nack: func() error { return nil }, Count: count})
}

// assertPanicLog checks the one record of a panicked message: the panic-site
// stack in a single error.stack_trace field and the bare panic value in
// error.message.
func assertPanicLog(t *testing.T, line string, outcomeErr error) {
	t.Helper()
	if n := strings.Count(line, `"`+logging.KeyErrorStackTrace+`":"`); n != 1 {
		t.Errorf("error.stack_trace appears %d times, want 1:\n%s", n, line)
	}
	// Only a stack captured inside the recover carries the runtime panic frame;
	// one captured at the logging call site does not.
	if !strings.Contains(line, "runtime/panic.go") || !strings.Contains(line, "mux_test.go") {
		t.Errorf("stack trace does not point at the panic site:\n%s", line)
	}
	if !strings.Contains(line, `"`+logging.KeyErrorMessage+`":"handler panicked: `) || strings.Contains(line, `"`+logging.KeyErrorMessage+`":"handler panicked: `+"nil map\\ngoroutine 1 [running]\\n") {
		t.Errorf("error.message must be the bare panic value:\n%s", line)
	}
	if strings.Contains(outcomeErr.Error(), "runtime/debug") {
		t.Errorf("stack leaked into error message: %q", outcomeErr.Error())
	}
}

// logLine returns the single JSON record whose msg is want.
func logLine(t *testing.T, logs, want string) string {
	t.Helper()
	for line := range strings.SplitSeq(strings.TrimSpace(logs), "\n") {
		if strings.Contains(line, `"msg":"`+want+`"`) {
			return line
		}
	}
	t.Fatalf("log %q not found in:\n%s", want, logs)
	return ""
}

func panicking(context.Context, payload) (jsontext.Value, error) {
	panic(errors.New("nil map\ngoroutine 1 [running]"))
}

func TestMuxOutcomes(t *testing.T) {
	t.Parallel()

	handlerErr := errors.New("db down")
	tests := []struct {
		name        string
		data        string
		count       uint64
		tune        func(*Config)
		preflight   Middleware // replaces requireName when set
		handler     func(context.Context, payload) (jsontext.Value, error)
		wantOutcome event.Outcome
		wantErr     error
		wantReason  event.DeadLetterReason
		wantLogMsg  string
		wantPublish int
		wantDLQ     int
	}{
		{name: "success", data: `{"name":"a"}`, handler: func(_ context.Context, p payload) (jsontext.Value, error) { return []byte(`{"ok":true}`), nil }, wantOutcome: event.OutcomeAcked, wantLogMsg: "message processed", wantPublish: 1},
		{name: "decode error is dead-lettered", data: `{"name":"a","name":"b"}`, wantOutcome: event.OutcomeDiscarded, wantErr: event.ErrDecode, wantReason: event.ReasonDecode, wantLogMsg: "message dead-lettered", wantDLQ: 1},
		{name: "preflight error is dead-lettered", data: `{}`, wantOutcome: event.OutcomeDiscarded, wantErr: errPreflight, wantReason: event.ReasonValidation, wantLogMsg: "message dead-lettered", wantDLQ: 1},
		{name: "preflight non-retryable is validation", data: `{"name":"a"}`, preflight: func(HandlerFunc) HandlerFunc {
			return func(context.Context, any) (jsontext.Value, error) {
				return nil, event.NewNonRetryableError(errPreflight)
			}
		}, wantOutcome: event.OutcomeDiscarded, wantErr: errPreflight, wantReason: event.ReasonValidation, wantLogMsg: "message dead-lettered", wantDLQ: 1},
		{name: "panic in handler", data: `{"name":"a"}`, handler: panicking, wantOutcome: event.OutcomeDiscarded, wantErr: event.ErrPanic, wantReason: event.ReasonPanic, wantLogMsg: "message dead-lettered", wantDLQ: 1},
		{name: "panic in preflight", data: `{"name":"a"}`, preflight: func(HandlerFunc) HandlerFunc {
			return func(context.Context, any) (jsontext.Value, error) { panic("preflight exploded") }
		}, wantOutcome: event.OutcomeDiscarded, wantErr: event.ErrPanic, wantReason: event.ReasonPanic, wantLogMsg: "message dead-lettered", wantDLQ: 1},
		{name: "non-retryable handler error is dead-lettered", data: `{"name":"a"}`, handler: func(context.Context, payload) (jsontext.Value, error) {
			return nil, &event.NonRetryableError{Err: handlerErr}
		}, wantOutcome: event.OutcomeDiscarded, wantErr: handlerErr, wantReason: event.ReasonNonRetryable, wantLogMsg: "message dead-lettered", wantDLQ: 1},
		{name: "retryable handler error is nacked", data: `{"name":"a"}`, handler: func(context.Context, payload) (jsontext.Value, error) { return nil, handlerErr }, wantOutcome: event.OutcomeNacked, wantErr: handlerErr, wantLogMsg: "message handling failed"},
		{name: "without dead letter publisher the message is discarded", data: `{}`, tune: func(c *Config) { c.DeadLetter = nil }, wantOutcome: event.OutcomeDiscarded, wantErr: errPreflight, wantReason: event.ReasonValidation, wantLogMsg: "message discarded"},
		{name: "retryable within MaxRetries is nacked", data: `{"name":"a"}`, count: 2, tune: func(c *Config) { c.MaxRetries = 2 }, handler: func(context.Context, payload) (jsontext.Value, error) { return nil, handlerErr }, wantOutcome: event.OutcomeNacked, wantErr: handlerErr, wantLogMsg: "message handling failed"},
		{name: "retryable past MaxRetries is dead-lettered", data: `{"name":"a"}`, count: 3, tune: func(c *Config) { c.MaxRetries = 2 }, handler: func(context.Context, payload) (jsontext.Value, error) { return nil, handlerErr }, wantOutcome: event.OutcomeDiscarded, wantErr: handlerErr, wantReason: event.ReasonMaxRetries, wantLogMsg: "message dead-lettered", wantDLQ: 1},
		{name: "unknown count never hits MaxRetries", data: `{"name":"a"}`, count: 0, tune: func(c *Config) { c.MaxRetries = 1 }, handler: func(context.Context, payload) (jsontext.Value, error) { return nil, handlerErr }, wantOutcome: event.OutcomeNacked, wantErr: handlerErr, wantLogMsg: "message handling failed"},
		{name: "MaxRetries zero is unlimited", data: `{"name":"a"}`, count: 100, handler: func(context.Context, payload) (jsontext.Value, error) { return nil, handlerErr }, wantOutcome: event.OutcomeNacked, wantErr: handlerErr, wantLogMsg: "message handling failed"},
		{name: "dlq non-retryable failure still discards", data: `{}`, tune: func(c *Config) {
			c.DeadLetter = &capturePublisher{err: event.NewNonRetryableError(errors.New("too many attributes"))}
		}, wantOutcome: event.OutcomeDiscarded, wantErr: errPreflight, wantReason: event.ReasonValidation, wantLogMsg: "message discarded"},
		{name: "dlq retryable failure nacks while MaxRetries bounds it", data: `{}`, count: 1, tune: func(c *Config) {
			c.MaxRetries = 2
			c.DeadLetter = &capturePublisher{err: errors.New("broker down")}
		}, wantOutcome: event.OutcomeNacked, wantErr: event.ErrPublicationFailed, wantLogMsg: "dead-letter publish failed"},
		{name: "dlq retryable failure without MaxRetries discards and loses the copy", data: `{}`, count: 1, tune: func(c *Config) {
			c.DeadLetter = &capturePublisher{err: errors.New("broker down")}
		}, wantOutcome: event.OutcomeDiscarded, wantErr: errPreflight, wantReason: event.ReasonValidation, wantLogMsg: "message discarded"},
		{name: "dlq retryable failure without delivery count discards and loses the copy", data: `{}`, count: 0, tune: func(c *Config) {
			c.MaxRetries = 2
			c.DeadLetter = &capturePublisher{err: errors.New("broker down")}
		}, wantOutcome: event.OutcomeDiscarded, wantErr: errPreflight, wantReason: event.ReasonValidation, wantLogMsg: "message discarded"},
		{name: "dlq retryable failure past MaxRetries discards", data: `{"name":"a"}`, count: 3, tune: func(c *Config) {
			c.MaxRetries = 2
			c.DeadLetter = &capturePublisher{err: errors.New("broker down")}
		}, handler: func(context.Context, payload) (jsontext.Value, error) { return nil, handlerErr }, wantOutcome: event.OutcomeDiscarded, wantErr: handlerErr, wantReason: event.ReasonMaxRetries, wantLogMsg: "message discarded"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			handler := tt.handler
			if handler == nil {
				handler = func(context.Context, payload) (jsontext.Value, error) {
					t.Fatal("handler must not run")
					return nil, nil
				}
			}
			var tune []func(*Config)
			if tt.tune != nil {
				tune = append(tune, tt.tune)
			}
			h := newMuxHarness(t, handler, tune...)
			if tt.preflight != nil {
				h.replacePreflight(t, tt.preflight)
			}
			msg := newIncomingDelivered(t, tt.data, tt.count)
			h.sub.handler(msg)

			outcome, err := msg.Outcome()
			if outcome != tt.wantOutcome {
				t.Errorf("Outcome() = %v, want %v", outcome, tt.wantOutcome)
			}
			if tt.wantErr != nil && !errors.Is(err, tt.wantErr) {
				t.Errorf("Outcome() err = %v, want %v", err, tt.wantErr)
			}
			if len(h.pub.got) != tt.wantPublish {
				t.Errorf("published %d, want %d", len(h.pub.got), tt.wantPublish)
			}
			if len(h.dlq.got) != tt.wantDLQ {
				t.Fatalf("dead-lettered %d, want %d", len(h.dlq.got), tt.wantDLQ)
			}
			if tt.wantDLQ == 1 {
				exts := h.dlq.got[0].Extensions()
				if got := exts[event.ExtDeadLetterReason]; got != string(tt.wantReason) {
					t.Errorf("deadletterreason = %v, want %v", got, tt.wantReason)
				}
				if got := exts[event.ExtDeadLetterError]; got != err.Error() {
					t.Errorf("deadlettererror = %v, want %v", got, err.Error())
				}
			}
			logs := h.logs.String()
			if !strings.Contains(logs, `"msg":"`+tt.wantLogMsg+`"`) {
				t.Errorf("log %q not found in:\n%s", tt.wantLogMsg, logs)
			}
			if !strings.Contains(logs, `"`+logfields.KeyCorrelationID+`":"corr-1"`) {
				t.Errorf("correlation id not logged in:\n%s", logs)
			}
			wantErrors := 1
			if tt.wantOutcome == event.OutcomeAcked {
				wantErrors = 0
			}
			if got := strings.Count(logs, `"level":"ERROR"`); got != wantErrors {
				t.Errorf("ERROR records = %d, want %d:\n%s", got, wantErrors, logs)
			}
			if strings.HasPrefix(tt.name, "dlq ") {
				if !strings.Contains(logs, `"`+logfields.KeyDeadLetterPublishError+`":"`) {
					t.Errorf("dead-letter publish error not logged in:\n%s", logs)
				}
				if !strings.Contains(logs, `"`+logging.KeyErrorMessage+`":"`+errPreflight.Error()+`"`) && !strings.Contains(logs, `"`+logging.KeyErrorMessage+`":"`+handlerErr.Error()+`"`) {
					t.Errorf("original cause not logged in:\n%s", logs)
				}
			}
			if tt.name == "dlq retryable failure nacks" && !errors.Is(err, errPreflight) {
				t.Errorf("Outcome() err = %v, want it to wrap the preflight cause", err)
			}
			if tt.count > 0 && !strings.Contains(logs, `"`+logfields.KeyDeliveryCount+`":`+strconv.FormatUint(tt.count, 10)) {
				t.Errorf("delivery count %d not logged in:\n%s", tt.count, logs)
			}
			if tt.wantReason != "" && !strings.Contains(logs, `"`+logfields.KeyDeadLetterReason+`":"`+string(tt.wantReason)+`"`) {
				t.Errorf("reason %q not logged in:\n%s", tt.wantReason, logs)
			}
			if tt.wantReason == event.ReasonPanic {
				assertPanicLog(t, logLine(t, logs, tt.wantLogMsg), err)
			}
		})
	}
}

func TestMuxNoRouteIsDiscardedAndLoggedInfo(t *testing.T) {
	t.Parallel()

	h := &muxHarness{sub: newFakeSubscription(), logs: &bytes.Buffer{}}
	logger := slog.New(slog.NewJSONHandler(h.logs, &slog.HandlerOptions{Level: slog.LevelInfo}))
	mux, err := New(Config{Name: "test", Subscription: h.sub, Logger: logger})
	if err != nil {
		t.Fatal(err)
	}
	if err := mux.Start(t.Context(), nil); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = mux.Stop(context.Background()) })

	msg := newIncoming(t, `{}`)
	h.sub.handler(msg)

	outcome, err := msg.Outcome()
	if outcome != event.OutcomeDiscarded || !errors.Is(err, event.ErrNoRoute) {
		t.Fatalf("Outcome() = %v, %v; want Discarded, ErrNoRoute", outcome, err)
	}
	if logs := h.logs.String(); !strings.Contains(logs, `"level":"INFO","msg":"no route matched"`) || !strings.Contains(logs, `"`+logfields.KeyCorrelationID+`":"corr-1"`) {
		t.Errorf("expected INFO 'no route matched' with correlation id, got:\n%s", logs)
	}
}

func TestPublishResultKeepsCorrelationID(t *testing.T) {
	t.Parallel()

	h := newMuxHarness(t, func(ctx context.Context, p payload) (jsontext.Value, error) {
		if store, ok := metadata.FromContext(ctx); ok {
			store.Set(event.ExtCorrelationID, "hijacked")
			store.Set("eventName", "execute")
		}
		return []byte(`{}`), nil
	})
	h.sub.handler(newIncoming(t, `{"name":"a"}`))

	if len(h.pub.got) != 1 {
		t.Fatalf("published %d, want 1", len(h.pub.got))
	}
	exts := h.pub.got[0].Extensions()
	if got := exts[event.ExtCorrelationID]; got != "corr-1" {
		t.Errorf("result correlationid = %v, want corr-1", got)
	}
	if got := exts["eventname"]; got != "execute" {
		t.Errorf("result eventname = %v, want execute", got)
	}
	if !strings.Contains(h.logs.String(), `"level":"WARN","msg":"reserved extension ignored"`) {
		t.Errorf("expected WARN 'reserved extension ignored', got:\n%s", h.logs.String())
	}
}

func TestPublishResultStripsDeadLetterExtensions(t *testing.T) {
	t.Parallel()

	h := newMuxHarness(t, func(ctx context.Context, p payload) (jsontext.Value, error) {
		if store, ok := metadata.FromContext(ctx); ok {
			store.Set(event.ExtDeadLetterReason, "forged")
		}
		return []byte(`{}`), nil
	})
	// A copy re-injected from the dead-letter topic that now succeeds.
	msg := newIncoming(t, `{"name":"a"}`)
	msg.SetExtension(event.ExtDeadLetterReason, "decode")
	msg.SetExtension(event.ExtDeadLetterError, "old failure")
	h.sub.handler(msg)

	if len(h.pub.got) != 1 {
		t.Fatalf("published %d, want 1", len(h.pub.got))
	}
	exts := h.pub.got[0].Extensions()
	if _, ok := exts[event.ExtDeadLetterReason]; ok {
		t.Errorf("result keeps deadletterreason: %v", exts)
	}
	if _, ok := exts[event.ExtDeadLetterError]; ok {
		t.Errorf("result keeps deadlettererror: %v", exts)
	}
	if got := exts[event.ExtCorrelationID]; got != "corr-1" {
		t.Errorf("correlationid = %v, want corr-1", got)
	}
	if got := strings.Count(h.logs.String(), `"msg":"reserved extension ignored"`); got != 1 {
		t.Errorf("WARN logged %d times, want 1 (metadata only):\n%s", got, h.logs.String())
	}
}

func TestMaxRetriesWithoutDeliveryCountWarnsOnce(t *testing.T) {
	t.Parallel()

	h := newMuxHarness(t, func(context.Context, payload) (jsontext.Value, error) { return nil, errors.New("flaky") },
		func(c *Config) { c.MaxRetries = 3 })
	h.sub.handler(newIncoming(t, `{"name":"a"}`))
	h.sub.handler(newIncoming(t, `{"name":"a"}`))

	if got := strings.Count(h.logs.String(), `"msg":"MaxRetries set but transport reports no delivery count"`); got != 1 {
		t.Errorf("warning logged %d times, want exactly 1:\n%s", got, h.logs.String())
	}
}

func TestNewRequiresSubscription(t *testing.T) {
	t.Parallel()
	if _, err := New(Config{}); err == nil {
		t.Fatal("New without Subscription returned nil error")
	}
}

func TestLifecycle(t *testing.T) {
	t.Parallel()

	t.Run("start error propagates and leaves the mux stopped", func(t *testing.T) {
		t.Parallel()
		errBoom := errors.New("queue missing")
		sub := &fakeSubscription{startErr: errBoom}
		mux, _ := New(Config{Subscription: sub})
		if err := mux.Start(t.Context(), nil); !errors.Is(err, errBoom) {
			t.Fatalf("Start() = %v, want %v", err, errBoom)
		}
		if err := mux.Stop(t.Context()); err != nil || sub.stops != 0 {
			t.Errorf("Stop after failed Start: err=%v stops=%d, want nil and 0", err, sub.stops)
		}
	})

	t.Run("second start is ErrAlreadyStarted", func(t *testing.T) {
		t.Parallel()
		mux, _ := New(Config{Subscription: newFakeSubscription()})
		if err := mux.Start(t.Context(), nil); err != nil {
			t.Fatal(err)
		}
		if err := mux.Start(t.Context(), nil); !errors.Is(err, event.ErrAlreadyStarted) {
			t.Errorf("second Start() = %v, want ErrAlreadyStarted", err)
		}
	})

	t.Run("stop without start and stop twice return nil", func(t *testing.T) {
		t.Parallel()
		sub := newFakeSubscription()
		mux, _ := New(Config{Subscription: sub})
		if err := mux.Stop(t.Context()); err != nil {
			t.Errorf("Stop without Start = %v", err)
		}
		_ = mux.Start(t.Context(), nil)
		_ = mux.Stop(t.Context())
		if err := mux.Stop(t.Context()); err != nil || sub.stops != 1 {
			t.Errorf("second Stop: err=%v stops=%d, want nil and 1", err, sub.stops)
		}
	})

	t.Run("stop error is wrapped and logged", func(t *testing.T) {
		t.Parallel()
		logs := &bytes.Buffer{}
		errBoom := errors.New("drain timeout")
		mux, _ := New(Config{Subscription: &fakeSubscription{stopErr: errBoom}, Logger: slog.New(slog.NewJSONHandler(logs, nil))})
		_ = mux.Start(t.Context(), nil)
		if err := mux.Stop(t.Context()); !errors.Is(err, errBoom) {
			t.Errorf("Stop() = %v, want %v", err, errBoom)
		}
		// Spec §3.6: INFO plus the error field; the error itself is returned to
		// the caller, who logs it once more at its own level.
		line := logLine(t, logs.String(), "mux stopped")
		if !strings.Contains(line, `"level":"INFO"`) || !strings.Contains(line, `"`+logging.KeyErrorMessage+`":"drain timeout"`) {
			t.Errorf("mux stopped log = %s, want INFO with the drain error", line)
		}
	})

	t.Run("subscription failure is logged and forwarded", func(t *testing.T) {
		t.Parallel()
		logs := &bytes.Buffer{}
		sub := newFakeSubscription()
		mux, _ := New(Config{Subscription: sub, Logger: slog.New(slog.NewJSONHandler(logs, nil))})
		var got error
		_ = mux.Start(t.Context(), func(err error) { got = err })
		errBoom := errors.New("receive died")
		sub.onError(errBoom)
		if !errors.Is(got, errBoom) {
			t.Errorf("onError got %v, want %v", got, errBoom)
		}
		if line := logLine(t, logs.String(), "subscription failed"); !strings.Contains(line, `"level":"ERROR"`) || !strings.Contains(line, `"`+logging.KeyErrorMessage+`":"receive died"`) {
			t.Errorf("subscription failed log = %s, want ERROR with the cause", line)
		}
	})
}

func TestAckFailureIsLoggedOnce(t *testing.T) {
	t.Parallel()

	errBroker := errors.New("broker down")
	tests := []struct {
		name        string
		data        string
		handler     func(context.Context, payload) (jsontext.Value, error)
		wantOutcome logfields.Outcome
	}{
		{name: "acked", data: `{"name":"a"}`, handler: func(context.Context, payload) (jsontext.Value, error) { return nil, nil }, wantOutcome: logfields.OutcomeAcked},
		{name: "nacked", data: `{"name":"a"}`, handler: func(context.Context, payload) (jsontext.Value, error) { return nil, errors.New("flaky") }, wantOutcome: logfields.OutcomeNacked},
		{name: "discarded", data: `not json`, handler: func(context.Context, payload) (jsontext.Value, error) { return nil, nil }, wantOutcome: logfields.OutcomeDiscarded},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			h := newMuxHarness(t, tt.handler)
			fail := func() error { return errBroker }
			ce := cloudevents.New()
			ce.SetID("ack-" + tt.name)
			ce.SetSource("/test")
			ce.SetType("test.event")
			_ = ce.SetData(cloudevents.ApplicationJSON, []byte(tt.data))
			h.sub.handler(event.NewMessage(t.Context(), ce, event.Delivery{Ack: fail, Nack: fail, Discard: fail}))

			if n := strings.Count(h.logs.String(), `"message ack failed"`); n != 1 {
				t.Fatalf("message ack failed logged %d times, want 1\n%s", n, h.logs.String())
			}
			line := logLine(t, h.logs.String(), "message ack failed")
			for _, want := range []string{
				`"level":"ERROR"`,
				`"` + logfields.KeyMessagingOutcome + `":"` + string(tt.wantOutcome) + `"`,
				`"` + logfields.KeyMessagingMessageID + `":"ack-` + tt.name + `"`,
				`"` + logging.KeyErrorMessage + `":"broker down"`,
			} {
				if !strings.Contains(line, want) {
					t.Errorf("message ack failed lacks %s:\n%s", want, line)
				}
			}
		})
	}
}

// publisherFunc adapts a closure to Publisher so a test can look at the
// message while the publish happens.
type publisherFunc func(context.Context, cloudevents.Event) error

func (f publisherFunc) Publish(ctx context.Context, ce cloudevents.Event) error { return f(ctx, ce) }

// newOrderHarness wires a mux whose route publisher records msg.Outcome() at
// publish time, so the test can tell whether the Ack happened first.
func newOrderHarness(t *testing.T, publishAfterAck bool) *event.Outcome {
	t.Helper()
	sub := newFakeSubscription()
	var msg *event.Message
	seen := new(event.Outcome)
	pub := publisherFunc(func(context.Context, cloudevents.Event) error {
		*seen, _ = msg.Outcome()
		return nil
	})
	mux, err := New(Config{Name: "order", Subscription: sub, Logger: slog.New(slog.DiscardHandler), PublishAfterAck: publishAfterAck})
	if err != nil {
		t.Fatal(err)
	}
	mux.Handle(pub, nil, func(context.Context, payload) (jsontext.Value, error) { return []byte(`{}`), nil })
	if err := mux.Start(t.Context(), nil); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = mux.Stop(context.Background()) })
	msg = newIncoming(t, `{"name":"a"}`)
	sub.handler(msg)
	return seen
}

func TestResultIsPublishedBeforeAckByDefault(t *testing.T) {
	t.Parallel()
	if seen := newOrderHarness(t, false); *seen != event.OutcomePending {
		t.Errorf("outcome at publish time = %v, want Pending (publish before Ack)", *seen)
	}
}

func TestPublishAfterAckAcksFirst(t *testing.T) {
	t.Parallel()
	if seen := newOrderHarness(t, true); *seen != event.OutcomeAcked {
		t.Errorf("outcome at publish time = %v, want Acked", *seen)
	}
}

func TestResultPublishFailure(t *testing.T) {
	t.Parallel()

	errBroker := errors.New("broker down")
	ok := func(context.Context, payload) (jsontext.Value, error) { return []byte(`{"ok":true}`), nil }
	tests := []struct {
		name        string
		count       uint64
		tune        func(*Config)
		handler     func(context.Context, payload) (jsontext.Value, error)
		pubErr      error
		wantOutcome event.Outcome
		wantErr     error
		wantReason  event.DeadLetterReason
		wantLogMsg  string
		wantDLQ     int
		wantErrors  int
	}{
		{name: "retryable publish error nacks", handler: ok, pubErr: errBroker, wantOutcome: event.OutcomeNacked, wantErr: event.ErrPublicationFailed, wantLogMsg: "message handling failed", wantErrors: 1},
		{name: "non-retryable publish error dead-letters with reason publish", handler: ok, pubErr: event.NewNonRetryableError(errBroker), wantOutcome: event.OutcomeDiscarded, wantErr: errBroker, wantReason: event.ReasonPublish, wantLogMsg: "message dead-lettered", wantDLQ: 1, wantErrors: 1},
		{name: "retryable publish error past MaxRetries dead-letters with reason publish", count: 3, tune: func(c *Config) { c.MaxRetries = 2 }, handler: ok, pubErr: errBroker, wantOutcome: event.OutcomeDiscarded, wantErr: errBroker, wantReason: event.ReasonPublish, wantLogMsg: "message dead-lettered", wantDLQ: 1, wantErrors: 1},
		{name: "undecodable result data dead-letters with reason publish", handler: func(context.Context, payload) (jsontext.Value, error) { return []byte(`not json`), nil }, wantOutcome: event.OutcomeDiscarded, wantReason: event.ReasonPublish, wantLogMsg: "message dead-lettered", wantDLQ: 1, wantErrors: 1},
		{name: "PublishAfterAck acks and only logs the publish error", tune: func(c *Config) { c.PublishAfterAck = true }, handler: ok, pubErr: errBroker, wantOutcome: event.OutcomeAcked, wantLogMsg: "result publish failed", wantErrors: 1},
		{name: "nil data publishes nothing and acks", handler: func(context.Context, payload) (jsontext.Value, error) { return nil, nil }, wantOutcome: event.OutcomeAcked, wantLogMsg: "message processed"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			var tune []func(*Config)
			if tt.tune != nil {
				tune = append(tune, tt.tune)
			}
			h := newMuxHarness(t, tt.handler, tune...)
			h.pub.err = tt.pubErr
			msg := newIncomingDelivered(t, `{"name":"a"}`, tt.count)
			h.sub.handler(msg)

			outcome, err := msg.Outcome()
			if outcome != tt.wantOutcome {
				t.Errorf("Outcome() = %v, want %v", outcome, tt.wantOutcome)
			}
			if tt.wantErr != nil && !errors.Is(err, tt.wantErr) {
				t.Errorf("Outcome() err = %v, want %v", err, tt.wantErr)
			}
			if len(h.pub.got) != 0 {
				t.Errorf("published %d results, want 0", len(h.pub.got))
			}
			if len(h.dlq.got) != tt.wantDLQ {
				t.Fatalf("dead-lettered %d, want %d", len(h.dlq.got), tt.wantDLQ)
			}
			if tt.wantDLQ == 1 {
				if got := h.dlq.got[0].Extensions()[event.ExtDeadLetterReason]; got != string(tt.wantReason) {
					t.Errorf("deadletterreason = %v, want %v", got, tt.wantReason)
				}
			}
			logs := h.logs.String()
			if !strings.Contains(logs, `"msg":"`+tt.wantLogMsg+`"`) {
				t.Errorf("log %q not found in:\n%s", tt.wantLogMsg, logs)
			}
			if tt.wantOutcome != event.OutcomeAcked && strings.Contains(logs, `"msg":"message processed"`) {
				t.Errorf("a failed publish must not log 'message processed':\n%s", logs)
			}
			if got := strings.Count(logs, `"level":"ERROR"`); got != tt.wantErrors {
				t.Errorf("ERROR records = %d, want %d:\n%s", got, tt.wantErrors, logs)
			}
		})
	}
}

// TestPublishContextCancellation: in the default order the message is still
// undecided when the result goes out, so the parent's cancellation (a drain
// deadline) must reach the publisher; after an Ack the per-message context is
// already cancelled and only its values travel.
func TestPublishContextCancellation(t *testing.T) {
	t.Parallel()

	for _, tt := range []struct {
		name            string
		publishAfterAck bool
		wantCancelled   bool
	}{
		{name: "default order propagates cancellation", wantCancelled: true},
		{name: "PublishAfterAck strips cancellation", publishAfterAck: true, wantCancelled: false},
	} {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			sub := newFakeSubscription()
			var seenErr error
			pub := publisherFunc(func(ctx context.Context, _ cloudevents.Event) error {
				seenErr = ctx.Err()
				return nil
			})
			mux, err := New(Config{Name: "ctx", Subscription: sub, Logger: slog.New(slog.DiscardHandler), PublishAfterAck: tt.publishAfterAck})
			if err != nil {
				t.Fatal(err)
			}
			mux.Handle(pub, nil, func(context.Context, payload) (jsontext.Value, error) { return []byte(`{}`), nil })
			if err := mux.Start(t.Context(), nil); err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = mux.Stop(context.Background()) })

			parent, cancel := context.WithCancel(t.Context())
			cancel() // the transport is draining
			ce := cloudevents.New()
			ce.SetID("in-ctx")
			ce.SetSource("/test")
			ce.SetType("test.event")
			_ = ce.SetData(cloudevents.ApplicationJSON, []byte(`{"name":"a"}`))
			sub.handler(event.NewMessage(parent, ce, event.Delivery{}))

			if got := seenErr != nil; got != tt.wantCancelled {
				t.Errorf("publisher saw cancelled ctx = %v (%v), want %v", got, seenErr, tt.wantCancelled)
			}
		})
	}
}
