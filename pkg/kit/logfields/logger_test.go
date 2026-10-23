package logfields

import (
	"bytes"
	"encoding/json/v2"
	"log/slog"
	"testing"
)

func record(t *testing.T, logger *slog.Logger, buf *bytes.Buffer) map[string]string {
	t.Helper()
	logger.Info("hello")
	var got map[string]string
	if err := json.Unmarshal(buf.Bytes(), &got); err != nil {
		t.Fatalf("decode log line %q: %v", buf.String(), err)
	}
	return got
}

func TestComponentAddsLoggerName(t *testing.T) {
	t.Parallel()

	var buf bytes.Buffer
	logger := Component(slog.New(slog.NewJSONHandler(&buf, nil)), "sqs-subscriber")
	got := record(t, logger, &buf)
	if got[KeyLogLogger] != "sqs-subscriber" {
		t.Errorf("%s = %q, want %q", KeyLogLogger, got[KeyLogLogger], "sqs-subscriber")
	}
}

func TestComponentNilLoggerDiscards(t *testing.T) {
	t.Parallel()

	logger := Component(nil, "x")
	if logger == nil {
		t.Fatal("Component(nil) returned nil")
	}
	if logger.Enabled(t.Context(), slog.LevelError) {
		t.Error("Component(nil) logger is enabled, want discard")
	}
}

func TestTransportAddsSystemAndDestination(t *testing.T) {
	t.Parallel()

	var buf bytes.Buffer
	logger := Transport(slog.New(slog.NewJSONHandler(&buf, nil)), "sns-publisher", SystemAWSSNS, "arn:aws:sns:eu-west-1:1:orders")
	got := record(t, logger, &buf)
	want := map[string]string{
		KeyLogLogger:            "sns-publisher",
		KeyMessagingSystem:      SystemAWSSNS,
		KeyMessagingDestination: "arn:aws:sns:eu-west-1:1:orders",
	}
	for k, v := range want {
		if got[k] != v {
			t.Errorf("%s = %q, want %q", k, got[k], v)
		}
	}
}
