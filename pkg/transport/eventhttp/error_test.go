package eventhttp

import (
	"encoding/json/v2"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	cloudevents "github.com/cloudevents/sdk-go/v2/event"

	"github.com/norlis/event-driven/v2/pkg/event"
)

var errDomain = errors.New("order already exists")

func respond(t *testing.T, responder *ErrorResponder, err error) (bool, *httptest.ResponseRecorder, Response) {
	t.Helper()
	ce := cloudevents.New()
	ce.SetID("m1")
	ce.SetType("t")
	rec := httptest.NewRecorder()
	handled := responder.Respond(rec, httptest.NewRequestWithContext(t.Context(), http.MethodPost, "/command", http.NoBody), err, ce, "POST /command")
	var body Response
	if handled {
		if uerr := json.Unmarshal(rec.Body.Bytes(), &body); uerr != nil {
			t.Fatalf("body is not a Response: %v (%q)", uerr, rec.Body.String())
		}
	}
	return handled, rec, body
}

func TestErrorResponderDefaultRules(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		err        error
		wantStatus int
		wantCode   ErrorCode
		wantDetail string
	}{
		{name: "decode", err: fmt.Errorf("%w: dup", event.ErrDecode), wantStatus: 400, wantCode: CodeInvalidPayload, wantDetail: "Payload could not be decoded"},
		{name: "no route", err: event.ErrNoRoute, wantStatus: 400, wantCode: CodeRouteNotFound, wantDetail: "Command or event type not supported"},
		{name: "validation keeps its message", err: fakeClientError{detail: "Key: 'Age' required"}, wantStatus: 400, wantCode: CodeValidationFailed, wantDetail: "Key: 'Age' required"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			handled, rec, body := respond(t, NewErrorResponder(), tt.err)
			if !handled {
				t.Fatal("Respond() = false, want true")
			}
			if rec.Code != tt.wantStatus || body.Status != tt.wantStatus || body.Code != tt.wantCode || body.Detail != tt.wantDetail || body.ID != "m1" {
				t.Errorf("got status=%d body=%+v, want status=%d code=%s detail=%q", rec.Code, body, tt.wantStatus, tt.wantCode, tt.wantDetail)
			}
		})
	}
}

func TestErrorResponderUnmatchedReturnsFalse(t *testing.T) {
	t.Parallel()
	handled, _, _ := respond(t, NewErrorResponder(), errors.New("anything"))
	if handled {
		t.Fatal("Respond() = true for an unmatched error, want false")
	}
}

func TestErrorResponderUserRulesComeFirst(t *testing.T) {
	t.Parallel()

	responder := NewErrorResponder(
		NewRuleForValue(errDomain, http.StatusConflict, ErrorCode("ORDER-EXISTS"), "Order already exists"),
		NewRuleForValue(event.ErrNoRoute, http.StatusNotFound, ErrorCode("UNKNOWN-TYPE"), "Unknown type"),
	)
	handled, rec, body := respond(t, responder, fmt.Errorf("handler: %w", errDomain))
	if !handled || rec.Code != http.StatusConflict || body.Code != "ORDER-EXISTS" {
		t.Errorf("domain rule: handled=%v status=%d code=%s", handled, rec.Code, body.Code)
	}
	handled, rec, body = respond(t, responder, event.ErrNoRoute)
	if !handled || rec.Code != http.StatusNotFound || body.Code != "UNKNOWN-TYPE" {
		t.Errorf("user override of default: handled=%v status=%d code=%s", handled, rec.Code, body.Code)
	}
}

func TestNewRuleForTypeUsesFixedDetail(t *testing.T) {
	t.Parallel()

	type secretErr struct{ error }
	rule := NewRuleForType[secretErr](http.StatusBadGateway, ErrorCode("UPSTREAM"), "Upstream unavailable")
	err := secretErr{errors.New("password=hunter2")}
	if !rule.Match(err) {
		t.Fatal("rule did not match its type")
	}
	if got := rule.DetailFunc(err); got != "Upstream unavailable" {
		t.Errorf("DetailFunc() = %q, want fixed detail (must not leak err.Error())", got)
	}
}

type fakeClientError struct{ detail string }

func (e fakeClientError) Error() string        { return "internal: " + e.detail }
func (e fakeClientError) ClientDetail() string { return e.detail }

func TestDefaultRulesMapClientError(t *testing.T) {
	t.Parallel()

	rec := httptest.NewRecorder()
	ok := NewErrorResponder().Respond(rec, httptest.NewRequestWithContext(t.Context(), http.MethodPost, "/command", http.NoBody), fmt.Errorf("preflight: %w", fakeClientError{detail: "name is required"}), cloudevents.New(), "POST /command")
	if !ok || rec.Code != http.StatusBadRequest {
		t.Fatalf("Respond() = %v, status %d; want true and 400", ok, rec.Code)
	}
	if body := rec.Body.String(); !strings.Contains(body, `"VALIDATION-FAILED"`) || !strings.Contains(body, "name is required") || strings.Contains(body, "internal:") {
		t.Errorf("body = %s, want VALIDATION-FAILED with the client detail only", body)
	}
}
