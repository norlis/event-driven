package jetstream

import (
	"context"
	"fmt"
	"time"

	"github.com/nats-io/nats.go/jetstream"
	"go.uber.org/multierr"
)

// HealthCheckerOption configures the HealthChecker.
type HealthCheckerOption func(*HealthChecker)

// WithStreams adds stream names whose existence should be verified.
func WithStreams(streams ...string) HealthCheckerOption {
	return func(c *HealthChecker) {
		c.streams = append(c.streams, streams...)
	}
}

// HealthChecker verifies that the configured JetStream streams exist. Suitable
// for a /ready probe. Consumers are NOT checked: they are ephemeral and
// per-instance in the fan-out model.
type HealthChecker struct {
	js      jetstream.JetStream
	streams []string
}

// NewHealthChecker returns a HealthChecker bound to the given JetStream context.
func NewHealthChecker(js jetstream.JetStream, opts ...HealthCheckerOption) *HealthChecker {
	c := &HealthChecker{js: js, streams: []string{}}
	for _, opt := range opts {
		opt(c)
	}
	return c
}

// Check verifies every registered stream exists, aggregating failures. The
// caller's context is honored but capped at 15s (matches gcp/pubsub).
func (h *HealthChecker) Check(ctx context.Context) error {
	ctx, cancel := context.WithTimeout(ctx, 15*time.Second)
	defer cancel()

	var errs []error
	for _, name := range h.streams {
		if name == "" {
			continue
		}
		if _, err := h.js.Stream(ctx, name); err != nil {
			errs = append(errs, fmt.Errorf("required stream %q: %w", name, err))
		}
	}
	if combined := multierr.Combine(errs...); combined != nil {
		return fmt.Errorf("nats health check: %w", combined)
	}
	return nil
}
