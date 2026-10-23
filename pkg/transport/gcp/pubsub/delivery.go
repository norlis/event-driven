package pubsub

import gpubsub "cloud.google.com/go/pubsub/v2"

// deliveryCount reads DeliveryAttempt, which the SDK only sets when the
// subscription has a dead-letter policy; 0 means unknown to eventmux.
func deliveryCount(m *gpubsub.Message) uint64 {
	if m.DeliveryAttempt == nil || *m.DeliveryAttempt < 0 {
		return 0
	}
	return uint64(*m.DeliveryAttempt)
}
