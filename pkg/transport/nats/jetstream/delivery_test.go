package jetstream

import (
	"errors"
	"testing"
	"time"

	natsgo "github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

// fakeMsg implements only what newDelivery touches; the embedded interface
// is nil so any other call panics loudly.
type fakeMsg struct {
	jetstream.Msg
	md       *jetstream.MsgMetadata
	mdErr    error
	err      error // returned by every ack-family call
	acked    int
	naked    int
	nakDelay time.Duration
	termed   []string
	headers  natsgo.Header
	data     []byte
}

func (f *fakeMsg) Headers() natsgo.Header { return f.headers }
func (f *fakeMsg) Data() []byte           { return f.data }

func (f *fakeMsg) Metadata() (*jetstream.MsgMetadata, error) { return f.md, f.mdErr }
func (f *fakeMsg) Ack() error                                { f.acked++; return f.err }
func (f *fakeMsg) Nak() error                                { f.naked++; return f.err }
func (f *fakeMsg) NakWithDelay(d time.Duration) error        { f.naked++; f.nakDelay = d; return f.err }

func (f *fakeMsg) TermWithReason(reason string) error {
	f.termed = append(f.termed, reason)
	return f.err
}

func TestNewDeliveryCount(t *testing.T) {
	t.Parallel()

	if got := newDelivery(&fakeMsg{md: &jetstream.MsgMetadata{NumDelivered: 4}}, 0).Count; got != 4 {
		t.Errorf("Count = %d, want 4", got)
	}
	if got := newDelivery(&fakeMsg{mdErr: errors.New("no metadata")}, 0).Count; got != 0 {
		t.Errorf("Count without metadata = %d, want 0", got)
	}
}

func TestNewDeliveryCallbacks(t *testing.T) {
	t.Parallel()

	m := &fakeMsg{md: &jetstream.MsgMetadata{NumDelivered: 1}}
	d := newDelivery(m, 0)
	_ = d.Ack()
	_ = d.Nack()
	_ = d.Discard()
	if m.acked != 1 || m.naked != 1 || len(m.termed) != 1 || m.termed[0] != termReasonDiscarded {
		t.Errorf("acked=%d naked=%d termed=%v", m.acked, m.naked, m.termed)
	}

	delayed := &fakeMsg{md: &jetstream.MsgMetadata{}}
	_ = newDelivery(delayed, 2*time.Second).Nack()
	if delayed.nakDelay != 2*time.Second {
		t.Errorf("NakWithDelay = %s, want 2s", delayed.nakDelay)
	}
}

func TestDeliveryWrapsBrokerErrors(t *testing.T) {
	t.Parallel()
	errBroker := errors.New("timeout")
	d := newDelivery(&fakeMsg{md: &jetstream.MsgMetadata{}, err: errBroker}, 0)
	for name, fn := range map[string]func() error{"ack": d.Ack, "nack": d.Nack, "discard": d.Discard} {
		if err := fn(); !errors.Is(err, errBroker) {
			t.Errorf("%s = %v, want wrapped %v", name, err, errBroker)
		}
	}
	if err := d.Ack(); err.Error() != "jetstream: ack: timeout" {
		t.Errorf("ack error text = %q", err.Error())
	}
}
