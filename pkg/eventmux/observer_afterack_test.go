package eventmux

import (
	"context"
	"encoding/json/jsontext"
	"errors"
	"testing"

	"github.com/norlis/event-driven/v2/pkg/event"
	"github.com/norlis/event-driven/v2/pkg/kit/logfields"
)

// With PublishAfterAck the message is acked before the result goes out; a
// lost result is visible only through OnPublish, so that call must carry the
// error and the causationid that ties it to the acked message.
func TestObserverSeesResultLostAfterAck(t *testing.T) {
	t.Parallel()

	obs := &recordingObserver{}
	errBroker := errors.New("broker down")
	h := newMuxHarness(t, func(context.Context, payload) (jsontext.Value, error) { return []byte(`{}`), nil },
		withObserver(obs), func(c *Config) { c.PublishAfterAck = true })
	h.pub.err = errBroker
	msg := newIncoming(t, `{"name":"a"}`)
	h.sub.handler(msg)

	if outcome, _ := msg.Outcome(); outcome != event.OutcomeAcked {
		t.Fatalf("Outcome() = %v, want Acked", outcome)
	}
	if len(obs.messages) != 1 || obs.messages[0].outcome != logfields.OutcomeAcked {
		t.Fatalf("OnMessage = %+v, want one acked", obs.messages)
	}
	if len(obs.publishes) != 1 || !errors.Is(obs.publishes[0].err, errBroker) || obs.publishes[0].causationID != "in-1" {
		t.Fatalf("OnPublish = %+v, want one failed result with causationid in-1", obs.publishes)
	}
}
