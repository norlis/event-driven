package sqs

import (
	"strconv"

	sqstypes "github.com/aws/aws-sdk-go-v2/service/sqs/types"
)

// deliveryCount reads the ApproximateReceiveCount system attribute; 0 when
// it was not requested or cannot be parsed (eventmux treats 0 as unknown).
func deliveryCount(m *sqstypes.Message) uint64 {
	n, err := strconv.ParseUint(m.Attributes[string(sqstypes.MessageSystemAttributeNameApproximateReceiveCount)], 10, 64)
	if err != nil {
		return 0
	}
	return n
}
