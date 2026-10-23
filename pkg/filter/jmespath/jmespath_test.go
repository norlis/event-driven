package jmespath

import (
	"testing"

	cloudevents "github.com/cloudevents/sdk-go/v2/event"

	"github.com/norlis/event-driven/v2/pkg/event"
)

func newMessage(t *testing.T, data string) *event.Message {
	t.Helper()
	ce := cloudevents.New()
	ce.SetID("m1")
	ce.SetSource("/test")
	ce.SetType("test.event")
	if err := ce.SetData(cloudevents.ApplicationJSON, []byte(data)); err != nil {
		t.Fatalf("SetData() error = %v", err)
	}
	return event.NewMessage(t.Context(), ce, event.Delivery{})
}

func TestFilterMatch(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		data string
		want bool
	}{
		{name: "matching value", data: `{"name":"a"}`, want: true},
		{name: "non-matching value", data: `{"name":"b"}`, want: false},
		// json/v2 rejects duplicate members, so the payload never reaches JMESPath.
		{name: "duplicate member is not a match", data: `{"name":"b","name":"a"}`, want: false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			f, err := New("name == 'a'", nil)
			if err != nil {
				t.Fatal(err)
			}
			if got := f.Match(newMessage(t, tt.data)); got != tt.want {
				t.Errorf("Match(%s) = %v, want %v", tt.data, got, tt.want)
			}
		})
	}
}

func TestNewRejectsInvalidExpression(t *testing.T) {
	t.Parallel()
	if _, err := New("total >", nil); err == nil {
		t.Fatal("invalid expression accepted")
	}
}
