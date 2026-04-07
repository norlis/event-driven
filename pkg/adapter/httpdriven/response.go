package httpdriven

import (
	"encoding/json"
	"net/http"
	"time"
)

type Response struct {
	Id        string `json:"id"`
	Name      string `json:"name,omitempty"`
	SessionId string `json:"idSession,omitempty"`

	RequestId string `json:"requestId"`
	Timestamp int64  `json:"timestamp"`
	Instance  string `json:"instance"` // a URI that identifies the specific occurrence of the error

	Code   string `json:"error,omitempty"`  // a unique identifier for the error
	Detail string `json:"detail,omitempty"` // a human-readable explanation of the error
	Status int    `json:"status"`           //  the HTTP response code (optional) https://go.dev/src/net/http/status.go
}

func (res *Response) Json(w http.ResponseWriter, r *http.Request) {
	if res.Status == 0 {
		res.Status = http.StatusAccepted
	}

	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(res.Status)
	if err := json.NewEncoder(w).Encode(res); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
	}
}

type ResponseBuilder struct {
	response *Response
}

func NewResponseBuilder() *ResponseBuilder {
	return &ResponseBuilder{
		response: &Response{
			Timestamp: time.Now().Unix(),
		},
	}
}

func (b *ResponseBuilder) WithId(messageId, requestId, sessionId string) *ResponseBuilder {
	b.response.Id = messageId
	b.response.RequestId = requestId
	b.response.SessionId = sessionId
	return b
}

func (b *ResponseBuilder) WithName(name, instance string) *ResponseBuilder {
	b.response.Name = name
	b.response.Instance = instance
	return b
}

func (b *ResponseBuilder) WithError(detail, code string) *ResponseBuilder {
	b.response.Detail = detail
	b.response.Code = code
	return b
}

func (b *ResponseBuilder) WithStatus(status int) *ResponseBuilder {
	b.response.Status = status
	return b
}

func (b *ResponseBuilder) WithInstance(instance string) *ResponseBuilder {
	b.response.Instance = instance
	return b
}

func (b *ResponseBuilder) Build() *Response {
	return b.response
}
