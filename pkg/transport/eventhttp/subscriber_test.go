package eventhttp

import (
	"crypto/tls"
	"encoding/json/v2"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/norlis/event-driven/v2/pkg/event"
)

const testType = "com.acme.command"

// decide simulates the mux: it fixes the outcome synchronously.
func decide(fn func(*event.Message)) func(*event.Message) { return fn }

func newTestSubscriber(t *testing.T, cfg SubscriberConfig) *Subscriber {
	t.Helper()
	if cfg.Pattern == "" {
		cfg.Pattern = "POST /command"
	}
	if cfg.DefaultType == "" {
		cfg.DefaultType = testType
	}
	sub, err := NewSubscriber(http.NewServeMux(), cfg, slog.New(slog.DiscardHandler))
	if err != nil {
		t.Fatalf("NewSubscriber() error = %v", err)
	}
	return sub
}

func plainRequest(t *testing.T, body string, headers map[string]string) *http.Request {
	t.Helper()
	r := httptest.NewRequestWithContext(t.Context(), http.MethodPost, "/command", strings.NewReader(body))
	r.Pattern = "POST /command"
	r.Header.Set("Content-Type", "application/json")
	for k, v := range headers {
		r.Header.Set(k, v)
	}
	return r
}

func serve(t *testing.T, sub *Subscriber, r *http.Request, mux func(*event.Message)) (*httptest.ResponseRecorder, Response) {
	t.Helper()
	rec := httptest.NewRecorder()
	sub.Handler(mux)(rec, r)
	var body Response
	if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
		t.Fatalf("body is not a Response: %v (%q)", err, rec.Body.String())
	}
	return rec, body
}

func TestHandlerStatusByOutcome(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		mux        func(*event.Message)
		wantStatus int
		wantCode   ErrorCode
		wantDetail string
	}{
		{name: "acked → 202", mux: decide(func(m *event.Message) { _ = m.Ack() }), wantStatus: 202},
		{name: "nacked without rule → 500 generic", mux: decide(func(m *event.Message) { _ = m.Nack(errors.New("db password=hunter2 down")) }), wantStatus: 500, wantCode: CodeHandlerFailed, wantDetail: GenericFailureDetail},
		{name: "discarded without rule → 500 generic", mux: decide(func(m *event.Message) { _ = m.Discard(&event.NonRetryableError{Err: errors.New("x")}) }), wantStatus: 500, wantCode: CodeHandlerFailed, wantDetail: GenericFailureDetail},
		{name: "discarded no route → 400", mux: decide(func(m *event.Message) { _ = m.Discard(event.ErrNoRoute) }), wantStatus: 400, wantCode: CodeRouteNotFound, wantDetail: "Command or event type not supported"},
		{name: "nacked decode → 400", mux: decide(func(m *event.Message) { _ = m.Nack(errors.Join(event.ErrDecode)) }), wantStatus: 400, wantCode: CodeInvalidPayload, wantDetail: "Payload could not be decoded"},
		{name: "pending (mux bug) → 500 generic", mux: decide(func(*event.Message) {}), wantStatus: 500, wantCode: CodeHandlerFailed, wantDetail: GenericFailureDetail},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			sub := newTestSubscriber(t, SubscriberConfig{})
			rec, body := serve(t, sub, plainRequest(t, `{"name":"a"}`, map[string]string{HeaderMessageID: "m1", HeaderCorrelationID: "corr-1"}), tt.mux)
			if rec.Code != tt.wantStatus || body.Status != tt.wantStatus || body.Code != tt.wantCode || body.Detail != tt.wantDetail {
				t.Errorf("status=%d body=%+v; want status=%d code=%q detail=%q", rec.Code, body, tt.wantStatus, tt.wantCode, tt.wantDetail)
			}
			if body.ID != "m1" || body.Type != testType || body.CorrelationID != "corr-1" || body.Instance != "POST /command" {
				t.Errorf("identifiers = %+v, want id=m1 type=%s correlationId=corr-1 instance='POST /command'", body, testType)
			}
			if strings.Contains(rec.Body.String(), "hunter2") {
				t.Errorf("internal error leaked to client: %s", rec.Body.String())
			}
		})
	}
}

func TestHandlerUserErrorRulesTakePrecedence(t *testing.T) {
	t.Parallel()

	errExists := errors.New("exists")
	sub := newTestSubscriber(t, SubscriberConfig{ErrorRules: []ErrorRule{
		NewRuleForValue(errExists, http.StatusConflict, ErrorCode("ORDER-EXISTS"), "Order already exists"),
	}})
	rec, body := serve(t, sub, plainRequest(t, `{}`, nil), decide(func(m *event.Message) { _ = m.Nack(errExists) }))
	if rec.Code != http.StatusConflict || body.Code != "ORDER-EXISTS" || body.Detail != "Order already exists" {
		t.Errorf("status=%d body=%+v, want 409 ORDER-EXISTS", rec.Code, body)
	}
}

func TestHandlerCorrelationID(t *testing.T) {
	t.Parallel()

	t.Run("plain/header is kept and set on the event", func(t *testing.T) {
		t.Parallel()
		var seen string
		_, body := serve(t, newTestSubscriber(t, SubscriberConfig{}), plainRequest(t, `{}`, map[string]string{HeaderCorrelationID: "client-corr"}),
			decide(func(m *event.Message) { seen = event.CorrelationID(m.Event); _ = m.Ack() }))
		if seen != "client-corr" || body.CorrelationID != "client-corr" {
			t.Errorf("event=%q response=%q, want client-corr in both", seen, body.CorrelationID)
		}
	})
	t.Run("plain/missing is the event id", func(t *testing.T) {
		t.Parallel()
		var seen string
		_, body := serve(t, newTestSubscriber(t, SubscriberConfig{}), plainRequest(t, `{}`, map[string]string{HeaderMessageID: "m-7"}),
			decide(func(m *event.Message) { seen = event.CorrelationID(m.Event); _ = m.Ack() }))
		if body.CorrelationID != "m-7" || seen != "m-7" {
			t.Errorf("correlationId response=%q event=%q, want m-7 in both", body.CorrelationID, seen)
		}
	})
	t.Run("plain/invalid → 400 and mux not called", func(t *testing.T) {
		t.Parallel()
		rec, body := serve(t, newTestSubscriber(t, SubscriberConfig{}), plainRequest(t, `{}`, map[string]string{HeaderCorrelationID: "has space"}),
			decide(func(*event.Message) { t.Fatal("mux must not run") }))
		if rec.Code != 400 || body.Code != CodeInvalidCorrelationID {
			t.Errorf("status=%d code=%s, want 400 INVALID-CORRELATION-ID", rec.Code, body.Code)
		}
	})
	t.Run("binary/Ce-Correlationid is kept", func(t *testing.T) {
		t.Parallel()
		r := plainRequest(t, `{"k":1}`, map[string]string{"Ce-Id": "b1", "Ce-Specversion": "1.0", "Ce-Type": "com.acme.binary", "Ce-Source": "/c", "Ce-Correlationid": "bin-corr"})
		_, body := serve(t, newTestSubscriber(t, SubscriberConfig{}), r, decide(func(m *event.Message) { _ = m.Ack() }))
		if body.ID != "b1" || body.Type != "com.acme.binary" || body.CorrelationID != "bin-corr" {
			t.Errorf("body = %+v, want id=b1 type=com.acme.binary correlationId=bin-corr", body)
		}
	})
	t.Run("binary/invalid correlation → 400", func(t *testing.T) {
		t.Parallel()
		r := plainRequest(t, `{"k":1}`, map[string]string{"Ce-Id": "b1", "Ce-Specversion": "1.0", "Ce-Type": "t", "Ce-Source": "/c", "Ce-Correlationid": strings.Repeat("x", event.MaxCorrelationIDLen+1)})
		rec, body := serve(t, newTestSubscriber(t, SubscriberConfig{}), r, decide(func(*event.Message) { t.Fatal("mux must not run") }))
		if rec.Code != 400 || body.Code != CodeInvalidCorrelationID {
			t.Errorf("status=%d code=%s, want 400 INVALID-CORRELATION-ID", rec.Code, body.Code)
		}
	})
	t.Run("structured/field is kept", func(t *testing.T) {
		t.Parallel()
		r := plainRequest(t, `{"specversion":"1.0","id":"s1","type":"com.acme.structured","source":"/s","correlationid":"struct-corr","data":{"k":1}}`, nil)
		r.Header.Set("Content-Type", "application/cloudevents+json")
		_, body := serve(t, newTestSubscriber(t, SubscriberConfig{}), r, decide(func(m *event.Message) { _ = m.Ack() }))
		if body.ID != "s1" || body.CorrelationID != "struct-corr" {
			t.Errorf("body = %+v, want id=s1 correlationId=struct-corr", body)
		}
	})
}

func TestHandlerRequestRejections(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		cfg        SubscriberConfig
		req        func(t *testing.T) *http.Request
		wantStatus int
		wantCode   ErrorCode
	}{
		{name: "body too large → 413", cfg: SubscriberConfig{MaxBodyBytes: 16}, req: func(t *testing.T) *http.Request {
			return plainRequest(t, `{"name":"`+strings.Repeat("a", 32)+`"}`, nil)
		}, wantStatus: 413, wantCode: CodePayloadTooLarge},
		{name: "plain without json content type → 415", req: func(t *testing.T) *http.Request {
			r := plainRequest(t, `{}`, nil)
			r.Header.Set("Content-Type", "text/plain")
			return r
		}, wantStatus: 415, wantCode: CodeUnsupportedMediaType},
		{name: "plain/charset is accepted", req: func(t *testing.T) *http.Request {
			r := plainRequest(t, `{}`, nil)
			r.Header.Set("Content-Type", "application/json; charset=utf-8")
			return r
		}, wantStatus: 202},
		{name: "invalid client message id → 400", req: func(t *testing.T) *http.Request {
			return plainRequest(t, `{}`, map[string]string{HeaderMessageID: "ab\xffcd"})
		}, wantStatus: 400, wantCode: CodeInvalidRequest},
		{name: "structured with invalid json → 400", req: func(t *testing.T) *http.Request {
			r := plainRequest(t, `{"specversion":`, nil)
			r.Header.Set("Content-Type", "application/cloudevents+json")
			return r
		}, wantStatus: 400, wantCode: CodeInvalidRequest},
		{name: "binary too large → 413", cfg: SubscriberConfig{MaxBodyBytes: 16}, req: func(t *testing.T) *http.Request {
			return plainRequest(t, `{"k":"`+strings.Repeat("a", 32)+`"}`, map[string]string{"Ce-Id": "b1", "Ce-Specversion": "1.0", "Ce-Type": "t", "Ce-Source": "/c"})
		}, wantStatus: 413, wantCode: CodePayloadTooLarge},
		{name: "structured too large → 413", cfg: SubscriberConfig{MaxBodyBytes: 16}, req: func(t *testing.T) *http.Request {
			r := plainRequest(t, `{"specversion":"1.0","id":"s1","type":"t","source":"/s","data":{"k":"`+strings.Repeat("a", 32)+`"}}`, nil)
			r.Header.Set("Content-Type", "application/cloudevents+json")
			return r
		}, wantStatus: 413, wantCode: CodePayloadTooLarge},
		{name: "binary with non-UTF-8 type → 400 JSON body", req: func(t *testing.T) *http.Request {
			return plainRequest(t, `{"k":1}`, map[string]string{"Ce-Id": "b1", "Ce-Specversion": "1.0", "Ce-Type": "x\xff", "Ce-Source": "/c"})
		}, wantStatus: 400, wantCode: CodeInvalidRequest},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			sub := newTestSubscriber(t, tt.cfg)
			rec, body := serve(t, sub, tt.req(t), decide(func(m *event.Message) { _ = m.Ack() }))
			if rec.Code != tt.wantStatus || body.Code != tt.wantCode || body.Status != tt.wantStatus || body.Instance != "POST /command" {
				t.Errorf("status=%d body=%+v, want %d %q", rec.Code, body, tt.wantStatus, tt.wantCode)
			}
			if got := rec.Header().Get("Content-Type"); got != "application/json; charset=utf-8" {
				t.Errorf("Content-Type = %q, want JSON", got)
			}
		})
	}
}

func TestHandlerPlainRequestBuildsEvent(t *testing.T) {
	t.Parallel()

	var got *event.Message
	_, _ = serve(t, newTestSubscriber(t, SubscriberConfig{}), plainRequest(t, `{"name":"a"}`, map[string]string{HeaderMessageID: "m1"}),
		decide(func(m *event.Message) { got = m; _ = m.Ack() }))
	if got.ID() != "m1" || got.Type() != testType || string(got.Data()) != `{"name":"a"}` || got.Source() != "http://example.com/command" {
		t.Errorf("event = id=%q type=%q source=%q data=%q", got.ID(), got.Type(), got.Source(), got.Data())
	}
}

func TestHandlerPanicIsGeneric500(t *testing.T) {
	t.Parallel()

	// The mux discards a panic with event.ErrPanic as cause; the client gets
	// the generic detail, never the panic value or the stack trace.
	rec, body := serve(t, newTestSubscriber(t, SubscriberConfig{}), plainRequest(t, `{}`, nil),
		decide(func(m *event.Message) {
			_ = m.Discard(fmt.Errorf("%w: nil map dereference", event.ErrPanic))
		}))
	if rec.Code != 500 || body.Code != CodeHandlerFailed || body.Detail != GenericFailureDetail || strings.Contains(rec.Body.String(), "nil map") {
		t.Errorf("status=%d body=%s", rec.Code, rec.Body.String())
	}
}

func TestNewSubscriberValidation(t *testing.T) {
	t.Parallel()

	logger := slog.New(slog.DiscardHandler)
	if _, err := NewSubscriber(http.NewServeMux(), SubscriberConfig{DefaultType: "t"}, logger); err == nil {
		t.Error("missing Pattern accepted")
	}
	if _, err := NewSubscriber(http.NewServeMux(), SubscriberConfig{Pattern: "POST /x"}, logger); err == nil {
		t.Error("missing DefaultType accepted")
	}
	if _, err := NewSubscriber(http.NewServeMux(), SubscriberConfig{Pattern: "POST /x", DefaultType: "t", MaxBodyBytes: -1}, logger); err == nil {
		t.Error("negative MaxBodyBytes accepted")
	}
	if _, err := NewSubscriber(nil, SubscriberConfig{Pattern: "POST /x", DefaultType: "t"}, logger); err == nil {
		t.Error("nil server accepted")
	}
}

func TestStartIsSynchronousAndStopReturnsServiceUnavailable(t *testing.T) {
	t.Parallel()

	server := http.NewServeMux()
	sub, err := NewSubscriber(server, SubscriberConfig{Pattern: "POST /command", DefaultType: testType}, slog.New(slog.DiscardHandler))
	if err != nil {
		t.Fatal(err)
	}
	serve := func() int {
		rec := httptest.NewRecorder()
		server.ServeHTTP(rec, plainRequest(t, `{}`, nil))
		return rec.Code
	}

	if code := serve(); code != http.StatusServiceUnavailable {
		t.Fatalf("status before Start = %d, want 503", code)
	}
	if err := sub.Start(t.Context(), decide(func(m *event.Message) { _ = m.Ack() }), nil); err != nil {
		t.Fatal(err)
	}
	if code := serve(); code != http.StatusAccepted {
		t.Fatalf("status after Start = %d, want 202", code)
	}
	if err := sub.Start(t.Context(), nil, nil); !errors.Is(err, event.ErrAlreadyStarted) {
		t.Errorf("second Start() = %v, want event.ErrAlreadyStarted", err)
	}
	if err := sub.Stop(t.Context()); err != nil {
		t.Fatal(err)
	}
	if code := serve(); code != http.StatusServiceUnavailable {
		t.Errorf("status after Stop = %d, want 503", code)
	}
	if err := sub.Stop(t.Context()); err != nil {
		t.Errorf("second Stop() = %v, want nil", err)
	}
}

func TestHandlerPlainRequestSourceUsesTLSScheme(t *testing.T) {
	t.Parallel()

	var got *event.Message
	r := plainRequest(t, `{"name":"a"}`, nil)
	r.TLS = &tls.ConnectionState{}
	_, _ = serve(t, newTestSubscriber(t, SubscriberConfig{}), r, decide(func(m *event.Message) { got = m; _ = m.Ack() }))
	if got.Source() != "https://example.com/command" {
		t.Errorf("source = %q, want https://example.com/command", got.Source())
	}
}
