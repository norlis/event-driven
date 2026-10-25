package eventhttp

import (
	"net/http"
	"net/http/httptest"
	"testing"

	cloudevents "github.com/cloudevents/sdk-go/v2/event"

	"github.com/norlis/event-driven/v2/pkg/event"
)

func BenchmarkResponseJSON(b *testing.B) {
	b.ReportAllocs()
	for b.Loop() {
		res := Response{ID: "m1", Type: "t", CorrelationID: "c1", Timestamp: 1700000000, Instance: "/command"}
		res.JSON(httptest.NewRecorder())
	}
}

func TestResponseJSONBody(t *testing.T) {
	t.Parallel()

	ce := cloudevents.New()
	ce.SetID("m1")
	ce.SetType("com.acme.order.created")
	ce.SetExtension(event.ExtCorrelationID, "corr-1")

	rec := httptest.NewRecorder()
	res := newResponse(ce, "POST /command", 0, "", "")
	res.Timestamp = 1700000000
	res.JSON(rec)

	if rec.Code != http.StatusAccepted {
		t.Errorf("status = %d, want %d", rec.Code, http.StatusAccepted)
	}
	want := `{"id":"m1","type":"com.acme.order.created","correlationId":"corr-1","timestamp":1700000000,"instance":"POST /command","status":202}`
	if got := rec.Body.String(); got != want {
		t.Errorf("body = %s, want %s", got, want)
	}
}

func TestResponseJSONErrorBody(t *testing.T) {
	t.Parallel()

	ce := cloudevents.New()
	ce.SetID("m1")
	ce.SetType("t")
	rec := httptest.NewRecorder()
	res := newResponse(ce, "POST /command", http.StatusInternalServerError, CodeHandlerFailed, GenericFailureDetail)
	res.Timestamp = 1700000000
	res.JSON(rec)

	want := `{"id":"m1","type":"t","correlationId":"","timestamp":1700000000,"instance":"POST /command","error":"HANDLER-FAILED","detail":"The event could not be processed","status":500}`
	if got := rec.Body.String(); got != want {
		t.Errorf("body = %s, want %s", got, want)
	}
}

// A client-supplied ID with invalid UTF-8 cannot be encoded by json/v2; the
// failure must surface as a 500 instead of a 202 with an error-text body.
func TestResponseJSONEncodeFailureIsServerError(t *testing.T) {
	t.Parallel()

	rec := httptest.NewRecorder()
	res := Response{ID: "ab\xffcd"}
	res.JSON(rec)

	if rec.Code != http.StatusInternalServerError {
		t.Errorf("status = %d, want %d", rec.Code, http.StatusInternalServerError)
	}
	if got := rec.Header().Get("Content-Type"); got == "application/json; charset=utf-8" {
		t.Errorf("Content-Type = %q, want a non-JSON error response", got)
	}
}
