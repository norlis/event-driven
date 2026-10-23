package logfields

import (
	"log/slog"
	"testing"
)

func TestOutcomeAttr(t *testing.T) {
	t.Parallel()

	got := OutcomeDiscarded.Attr()
	if got.Key != KeyMessagingOutcome || got.Value.Kind() != slog.KindString || got.Value.String() != "discarded" {
		t.Errorf("OutcomeDiscarded.Attr() = %v, want %s=discarded", got, KeyMessagingOutcome)
	}
}
