package eventhttp

import (
	"bytes"
	"errors"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"testing"
	"time"

	cloudevents "github.com/cloudevents/sdk-go/v2/event"

	"github.com/norlis/event-driven/v2/pkg/event"
)

func TestNewPublisherValidatesConfig(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		cfg  PublisherConfig
	}{
		{name: "empty url", cfg: PublisherConfig{}},
		{name: "relative url", cfg: PublisherConfig{TargetURL: "/events"}},
		{name: "no host", cfg: PublisherConfig{TargetURL: "http://"}},
		{name: "unsupported scheme", cfg: PublisherConfig{TargetURL: "ftp://example.com/events"}},
		{name: "negative timeout", cfg: PublisherConfig{TargetURL: "https://example.com/events", Timeout: -time.Second}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			if _, err := NewPublisher(tt.cfg, slog.New(slog.DiscardHandler)); err == nil {
				t.Errorf("NewPublisher(%+v) returned nil error", tt.cfg)
			}
		})
	}
	if _, err := NewPublisher(PublisherConfig{TargetURL: "https://example.com/events"}, nil); err != nil {
		t.Errorf("valid config rejected: %v", err)
	}
}

func TestPublishSendsContentTypeAndNanoTime(t *testing.T) {
	t.Parallel()

	var got http.Header
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		got = r.Header.Clone()
		w.WriteHeader(http.StatusAccepted)
	}))
	t.Cleanup(srv.Close)

	pub, err := NewPublisher(PublisherConfig{TargetURL: srv.URL}, nil)
	if err != nil {
		t.Fatal(err)
	}
	ce := cloudevents.New()
	ce.SetID("id-1")
	ce.SetSource("/s")
	ce.SetType("t")
	ce.SetTime(time.Date(2026, 1, 2, 3, 4, 5, 600000000, time.UTC))
	ce.SetExtension("attempt", int32(2))
	_ = ce.SetData("application/avro", []byte(`x`))

	if err := pub.Publish(t.Context(), ce); err != nil {
		t.Fatalf("Publish() error = %v", err)
	}
	want := map[string]string{
		"Content-Type": "application/avro",
		"Ce-Id":        "id-1",
		"Ce-Time":      "2026-01-02T03:04:05.6Z",
		"Ce-Attempt":   "2",
	}
	for k, v := range want {
		if got.Get(k) != v {
			t.Errorf("header %s = %q, want %q", k, got.Get(k), v)
		}
	}
}

func TestPublishErrorAndLogsRedactTargetURL(t *testing.T) {
	t.Parallel()

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusBadGateway) }))
	t.Cleanup(srv.Close)

	var logs bytes.Buffer
	target := srv.URL + "/events?api_key=secret"
	pub, err := NewPublisher(PublisherConfig{TargetURL: target, Retry: &RetryConfig{Count: -1}}, slog.New(slog.NewJSONHandler(&logs, &slog.HandlerOptions{Level: slog.LevelDebug})))
	if err != nil {
		t.Fatal(err)
	}
	ce := cloudevents.New()
	ce.SetID("id-1")
	ce.SetSource("/s")
	ce.SetType("t")
	err = pub.Publish(t.Context(), ce)
	if err == nil {
		t.Fatal("Publish() returned nil error on 502")
	}
	if strings.Contains(err.Error(), "api_key") {
		t.Errorf("error leaks the query: %v", err)
	}
	pub.logger.Info("probe")
	if strings.Contains(logs.String(), "api_key") {
		t.Errorf("logger leaks the query: %s", logs.String())
	}
	if !strings.Contains(logs.String(), srv.URL+"/events") {
		t.Errorf("logger lost the endpoint: %s", logs.String())
	}
}

func TestPublishClassifiesStatusErrors(t *testing.T) {
	t.Parallel()

	tests := []struct {
		status           int
		wantNonRetryable bool
	}{
		{status: http.StatusBadRequest, wantNonRetryable: true},
		{status: http.StatusRequestEntityTooLarge, wantNonRetryable: true},
		{status: http.StatusUnprocessableEntity, wantNonRetryable: true},
		{status: http.StatusRequestTimeout, wantNonRetryable: false},
		{status: http.StatusTooManyRequests, wantNonRetryable: false},
		{status: http.StatusBadGateway, wantNonRetryable: false},
	}
	for _, tt := range tests {
		t.Run(strconv.Itoa(tt.status), func(t *testing.T) {
			t.Parallel()
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(tt.status) }))
			t.Cleanup(srv.Close)
			pub, err := NewPublisher(PublisherConfig{TargetURL: srv.URL, Retry: &RetryConfig{Count: -1}}, slog.New(slog.DiscardHandler))
			if err != nil {
				t.Fatal(err)
			}
			ce := cloudevents.New()
			ce.SetID("id-1")
			ce.SetSource("/s")
			ce.SetType("t")
			err = pub.Publish(t.Context(), ce)
			if err == nil {
				t.Fatalf("Publish() returned nil error on %d", tt.status)
			}
			if _, got := errors.AsType[*event.NonRetryableError](err); got != tt.wantNonRetryable {
				t.Errorf("NonRetryableError = %v, want %v (%v)", got, tt.wantNonRetryable, err)
			}
			if !strings.Contains(err.Error(), strconv.Itoa(tt.status)) {
				t.Errorf("error lacks the status: %v", err)
			}
		})
	}
}
