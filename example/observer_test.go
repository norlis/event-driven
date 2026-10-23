package example

import (
	"bytes"
	"context"
	"log/slog"
	"strings"
	"testing"
	"time"

	cloudevents "github.com/cloudevents/sdk-go/v2/event"

	"github.com/norlis/event-driven/v2/pkg/event"
	"github.com/norlis/event-driven/v2/pkg/kit/logfields"
)

func TestObserverCountsOutcomesPerType(t *testing.T) {
	t.Parallel()

	var logs bytes.Buffer
	obs := NewObserver(slog.New(slog.NewJSONHandler(&logs, &slog.HandlerOptions{Level: slog.LevelDebug})))
	ce := cloudevents.New()
	ce.SetID("1")
	ce.SetType("orders.created")
	msg := event.NewMessage(t.Context(), ce, event.Delivery{})

	obs.OnMessage(context.Background(), msg, logfields.OutcomeAcked, "", time.Millisecond)
	obs.OnMessage(context.Background(), msg, logfields.OutcomeAcked, "", time.Millisecond)
	obs.OnMessage(context.Background(), msg, logfields.OutcomeDiscarded, event.ReasonDecode, time.Millisecond)

	for _, want := range []string{
		`"msg":"message observed","log.logger":"observer","cloudevents.event_type":"orders.created","messaging.operation.outcome":"acked","example.outcome.count":2`,
		`"msg":"message observed","log.logger":"observer","cloudevents.event_type":"orders.created","messaging.operation.outcome":"discarded","messaging.deadletter.reason":"decode","example.outcome.count":1`,
	} {
		if !strings.Contains(logs.String(), want) {
			t.Errorf("logs lack %s:\n%s", want, logs.String())
		}
	}
}
