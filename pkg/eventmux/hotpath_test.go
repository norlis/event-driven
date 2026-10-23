package eventmux

import (
	"bytes"
	"context"
	"encoding/json/jsontext"
	"log/slog"
	"strings"
	"testing"

	"github.com/norlis/event-driven/v2/pkg/event"
	"github.com/norlis/event-driven/v2/pkg/eventmux/metadata"
)

// TestReceivedLogOnlyAtDebug fixes the DEBUG "message received" record: emitted
// when the logger accepts DEBUG, absent (and not built) at INFO.
func TestReceivedLogOnlyAtDebug(t *testing.T) {
	t.Parallel()

	for _, tt := range []struct {
		name  string
		level slog.Level
		want  bool
	}{
		{name: "debug", level: slog.LevelDebug, want: true},
		{name: "info", level: slog.LevelInfo, want: false},
	} {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			var logs bytes.Buffer
			logger := slog.New(slog.NewJSONHandler(&logs, &slog.HandlerOptions{Level: tt.level}))
			h := newMuxHarness(t, func(context.Context, payload) (jsontext.Value, error) { return nil, nil },
				func(c *Config) { c.Logger = logger })
			h.sub.handler(newIncoming(t, `{"name":"a"}`))

			got := strings.Contains(logs.String(), `"msg":"message received"`)
			if got != tt.want {
				t.Fatalf("'message received' logged = %v, want %v:\n%s", got, tt.want, logs.String())
			}
			if !strings.Contains(logs.String(), `"msg":"message processed"`) {
				t.Fatalf("expected INFO 'message processed':\n%s", logs.String())
			}
		})
	}
}

// TestMetadataStoreOnlyForPublishingRoutes: the store exists to feed result
// extensions, so a route without a Publisher gets no store in its context.
func TestMetadataStoreOnlyForPublishingRoutes(t *testing.T) {
	t.Parallel()

	for _, tt := range []struct {
		name      string
		withPub   bool
		wantStore bool
	}{
		{name: "publishing route", withPub: true, wantStore: true},
		{name: "terminal route", withPub: false, wantStore: false},
	} {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			var gotStore bool
			handler := func(ctx context.Context, _ payload) (jsontext.Value, error) {
				_, gotStore = metadata.FromContext(ctx)
				return []byte(`{}`), nil
			}
			var h *muxHarness
			if tt.withPub {
				h = newMuxHarness(t, handler)
			} else {
				h = newTerminalHarness(t, handler)
			}
			msg := newIncoming(t, `{"name":"a"}`)
			h.sub.handler(msg)

			if outcome, err := msg.Outcome(); outcome != event.OutcomeAcked || err != nil {
				t.Fatalf("Outcome() = %v, %v; want Acked", outcome, err)
			}
			if gotStore != tt.wantStore {
				t.Fatalf("metadata.FromContext ok = %v, want %v", gotStore, tt.wantStore)
			}
		})
	}
}

// newTerminalHarness is newMuxHarness with a route that has no Publisher.
func newTerminalHarness(t *testing.T, handler func(context.Context, payload) (jsontext.Value, error)) *muxHarness {
	t.Helper()
	h := &muxHarness{sub: newFakeSubscription(), logs: &bytes.Buffer{}}
	logger := slog.New(slog.NewJSONHandler(h.logs, &slog.HandlerOptions{Level: slog.LevelDebug}))
	mux, err := New(Config{Name: "test", Subscription: h.sub, Logger: logger})
	if err != nil {
		t.Fatal(err)
	}
	mux.UsePreflight(requireName)
	mux.Handle(nil, nil, handler)
	if err := mux.Start(t.Context(), nil); err != nil {
		t.Fatal(err)
	}
	h.mux = mux
	t.Cleanup(func() { _ = mux.Stop(context.Background()) })
	return h
}
