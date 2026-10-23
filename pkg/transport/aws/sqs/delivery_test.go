package sqs

import (
	"slices"
	"testing"

	sqstypes "github.com/aws/aws-sdk-go-v2/service/sqs/types"
)

func TestDeliveryCount(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name  string
		attrs map[string]string
		want  uint64
	}{
		{name: "valid", attrs: map[string]string{"ApproximateReceiveCount": "3"}, want: 3},
		{name: "missing", attrs: nil, want: 0},
		{name: "not a number", attrs: map[string]string{"ApproximateReceiveCount": "many"}, want: 0},
		{name: "negative", attrs: map[string]string{"ApproximateReceiveCount": "-1"}, want: 0},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			if got := deliveryCount(&sqstypes.Message{Attributes: tt.attrs}); got != tt.want {
				t.Errorf("deliveryCount() = %d, want %d", got, tt.want)
			}
		})
	}
}

func TestReceiveInputRequestsReceiveCount(t *testing.T) {
	t.Parallel()

	in := GenerateReceiveMessageInputDefault("https://sqs.us-east-1.amazonaws.com/123/q")
	if !slices.Contains(in.MessageSystemAttributeNames, sqstypes.MessageSystemAttributeNameApproximateReceiveCount) {
		t.Errorf("MessageSystemAttributeNames = %v, want ApproximateReceiveCount", in.MessageSystemAttributeNames)
	}
}
