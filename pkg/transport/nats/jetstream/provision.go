package jetstream

import (
	"context"
	"fmt"

	"github.com/nats-io/nats.go/jetstream"
)

// ensureStream creates or updates the stream when SubscriberConfig.StreamConfig
// is set. Idempotent: CreateOrUpdateStream no-ops when the stream already matches cfg.
func ensureStream(ctx context.Context, js jetstream.JetStream, cfg jetstream.StreamConfig) (jetstream.Stream, error) {
	stream, err := js.CreateOrUpdateStream(ctx, cfg)
	if err != nil {
		return nil, fmt.Errorf("jetstream provision stream %q: %w", cfg.Name, err)
	}
	return stream, nil
}
