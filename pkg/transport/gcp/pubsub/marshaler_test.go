package pubsub

import (
	"testing"
	"time"

	cloudevents "github.com/cloudevents/sdk-go/v2/event"
)

func TestMarshalBinary(t *testing.T) {
	t.Parallel()

	ce := cloudevents.New()
	ce.SetID("id-1")
	ce.SetSource("/s")
	ce.SetType("t")
	ce.SetTime(time.Date(2026, 1, 2, 3, 4, 5, 600000000, time.UTC))
	ce.SetExtension("correlationid", "c-1")
	ce.SetExtension("attempt", int32(2))
	_ = ce.SetData("application/avro", []byte(`x`))

	m, err := marshal(ce, nil)
	if err != nil {
		t.Fatalf("Marshal() error = %v", err)
	}
	want := map[string]string{
		"ce-id": "id-1", "ce-specversion": "1.0", "ce-type": "t", "ce-source": "/s",
		"ce-time": "2026-01-02T03:04:05.6Z", "ce-correlationid": "c-1", "ce-attempt": "2",
		"Content-Type": "application/avro",
	}
	for k, v := range want {
		if m.Attributes[k] != v {
			t.Errorf("Attributes[%s] = %q, want %q", k, m.Attributes[k], v)
		}
	}
	if len(m.Attributes) != len(want) {
		t.Errorf("Attributes = %v, want exactly %d keys", m.Attributes, len(want))
	}
	if string(m.Data) != "x" {
		t.Errorf("Data = %q, want x", m.Data)
	}
}

func TestMarshalPlainAttributes(t *testing.T) {
	t.Parallel()

	base := cloudevents.New()
	base.SetID("id-1")
	base.SetSource("/s")
	base.SetType("t")

	withScope := base.Clone()
	withScope.SetExtension("scope", "ALL")

	mixedCase := base.Clone()
	mixedCase.SetExtension("Scope", "ALL")

	typed := base.Clone()
	typed.SetExtension("attempt", int32(2))
	typed.SetExtension("urgent", true)

	tests := []struct {
		name   string
		ce     cloudevents.Event
		plain  []string
		want   map[string]string // unprefixed keys expected; must equal their ce- twin
		absent []string          // unprefixed keys that must not exist
	}{
		{"extension copied", withScope, []string{"scope"}, map[string]string{"scope": "ALL"}, nil},
		{"mixed-case extension name is stored lowercase", mixedCase, []string{"scope"}, map[string]string{"scope": "ALL"}, nil},
		{"absent extension writes nothing", base, []string{"scope"}, nil, []string{"scope"}},
		{"nil list keeps prefixed only", withScope, nil, nil, []string{"scope"}},
		{"empty list keeps prefixed only", withScope, []string{}, nil, []string{"scope"}},
		{"core attributes copied", base, []string{"type", "source", "id"}, map[string]string{"type": "t", "source": "/s", "id": "id-1"}, nil},
		{"unset optional core attribute writes nothing", base, []string{"subject", "time"}, nil, []string{"subject", "time"}},
		{"non-string extensions use the encoded text", typed, []string{"attempt", "urgent"}, map[string]string{"attempt": "2", "urgent": "true"}, nil},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			prefixed, err := marshal(tt.ce, nil)
			if err != nil {
				t.Fatalf("marshal(nil) error = %v", err)
			}
			m, err := marshal(tt.ce, tt.plain)
			if err != nil {
				t.Fatalf("marshal() error = %v", err)
			}
			for k, v := range tt.want {
				if m.Attributes[k] != v || m.Attributes["ce-"+k] != v {
					t.Errorf("Attributes[%s] = %q, ce-%s = %q, want both %q", k, m.Attributes[k], k, m.Attributes["ce-"+k], v)
				}
			}
			for _, k := range tt.absent {
				if _, ok := m.Attributes[k]; ok {
					t.Errorf("Attributes[%s] present, want absent", k)
				}
			}
			if len(m.Attributes) != len(prefixed.Attributes)+len(tt.want) {
				t.Errorf("Attributes = %v, want the %d default keys plus %d plain", m.Attributes, len(prefixed.Attributes), len(tt.want))
			}
		})
	}
}
