package pubsub

import (
	"testing"

	gpubsub "cloud.google.com/go/pubsub/v2"
)

func TestDeliveryCount(t *testing.T) {
	t.Parallel()

	five, negative := 5, -1
	tests := []struct {
		name    string
		attempt *int
		want    uint64
	}{
		{name: "nil without dead-letter policy", attempt: nil, want: 0},
		{name: "reported", attempt: &five, want: 5},
		{name: "negative is unknown", attempt: &negative, want: 0},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			if got := deliveryCount(&gpubsub.Message{DeliveryAttempt: tt.attempt}); got != tt.want {
				t.Errorf("deliveryCount() = %d, want %d", got, tt.want)
			}
		})
	}
}
