package eventhttp

import (
	"encoding/json/v2"
	"net/http"
	"time"

	cloudevents "github.com/cloudevents/sdk-go/v2/event"

	"github.com/norlis/event-driven/v2/pkg/event"
)

// Response is the JSON envelope returned by the HTTP subscriber.
type Response struct {
	ID            string `json:"id"`            // CloudEvent id
	Type          string `json:"type"`          // CloudEvent type
	CorrelationID string `json:"correlationId"` // correlationid extension: client-supplied or minted by the subscriber
	Timestamp     int64  `json:"timestamp"`     // Unix seconds
	Instance      string `json:"instance"`      // ServeMux pattern that handled the request

	Code   ErrorCode `json:"error,omitempty"`
	Detail string    `json:"detail,omitempty"`
	Status int       `json:"status"`
}

// JSON writes the Response as JSON. If Status is unset it defaults to 202.
func (res *Response) JSON(w http.ResponseWriter, r *http.Request) {
	if res.Status == 0 {
		res.Status = http.StatusAccepted
	}

	// Encode before writing headers so a failure (e.g. invalid UTF-8 in a
	// client-supplied ID) can still change the status.
	body, err := json.Marshal(res)
	if err != nil {
		http.Error(w, http.StatusText(http.StatusInternalServerError), http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(res.Status)
	_, _ = w.Write(body)
}

// newResponse fills the envelope from the event identifiers, the ServeMux
// pattern that handled the request and the outcome. An empty code means success.
func newResponse(ce cloudevents.Event, instance string, status int, code ErrorCode, detail string) *Response {
	return &Response{
		ID:            ce.ID(),
		Type:          ce.Type(),
		CorrelationID: event.CorrelationID(ce),
		Timestamp:     time.Now().Unix(),
		Instance:      instance,
		Code:          code,
		Detail:        detail,
		Status:        status,
	}
}
