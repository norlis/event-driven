package pubsub

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"slices"
	"strings"

	gpubsub "cloud.google.com/go/pubsub/v2"
	cloudevents "github.com/cloudevents/sdk-go/v2/event"

	"github.com/norlis/httpgate/trace"

	"github.com/norlis/event-driven/v2/pkg/event"
	"github.com/norlis/event-driven/v2/pkg/kit/logfields"
)

// PublisherConfig configures a Publisher.
type PublisherConfig struct {
	TopicID string
	// PlainAttributes lists CloudEvents context attributes (core or
	// extension, e.g. "scope", "type") that are also written as plain Pub/Sub
	// attributes without the "ce-" prefix, for subscription filters such as
	// attributes.scope = "ALL" and consumers outside this library. The "ce-"
	// attribute is always kept, and an attribute the event does not carry
	// writes nothing. Names are lowercase, as the event stores them;
	// NewPublisher rejects "traceparent", "datacontenttype" and names
	// starting with "goog".
	PlainAttributes []string
}

// dataContentTypeAttribute has no "ce-" pair in binary mode: it travels as
// Content-Type, so there is nothing to copy.
const dataContentTypeAttribute = "datacontenttype"

// reservedAttributePrefix is refused by Pub/Sub for attribute keys: the
// "ce-" copy publishes fine, the plain one would fail every publish.
const reservedAttributePrefix = "goog"

// Publisher publishes CloudEvents to a Pub/Sub topic. It satisfies
// eventmux.Publisher.
type Publisher struct {
	pub    *gpubsub.Publisher
	cfg    PublisherConfig
	logger *slog.Logger
}

// NewPublisher validates cfg and creates the topic publisher once, so the
// SDK's batching and flow control apply across messages. Call Close to
// flush and release its goroutines.
func NewPublisher(client *gpubsub.Client, cfg PublisherConfig, logger *slog.Logger) (*Publisher, error) {
	if client == nil {
		return nil, errors.New("pubsub: client is nil")
	}
	if cfg.TopicID == "" {
		return nil, errors.New("pubsub: PublisherConfig.TopicID is empty")
	}
	if err := validatePlainAttributes(cfg.PlainAttributes); err != nil {
		return nil, err
	}
	// The caller keeps its slice; Publish must not see later mutations.
	cfg.PlainAttributes = slices.Clone(cfg.PlainAttributes)
	logger = logfields.Transport(logger, "pubsub-publisher", logfields.SystemGCPPubSub, cfg.TopicID)
	return &Publisher{pub: client.Publisher(cfg.TopicID), cfg: cfg, logger: logger}, nil
}

func validatePlainAttributes(names []string) error {
	seen := make(map[string]struct{}, len(names))
	for _, n := range names {
		switch {
		case !isAttributeName(n):
			return fmt.Errorf("pubsub: PlainAttributes: %q must be a lowercase CloudEvents attribute name (a-z, 0-9)", n)
		case n == trace.Header:
			return fmt.Errorf("pubsub: PlainAttributes: %q is already written by the transport", n)
		case n == dataContentTypeAttribute:
			return fmt.Errorf("pubsub: PlainAttributes: %q travels as %s", n, contentTypeAttribute)
		case strings.HasPrefix(n, reservedAttributePrefix):
			return fmt.Errorf("pubsub: PlainAttributes: %q starts with %q, which Pub/Sub reserves", n, reservedAttributePrefix)
		}
		if _, dup := seen[n]; dup {
			return fmt.Errorf("pubsub: PlainAttributes: %q repeated", n)
		}
		seen[n] = struct{}{}
	}
	return nil
}

// isAttributeName reports whether s is a CloudEvents attribute name as the
// SDK stores it: lowercase ASCII letters and digits. Length is not capped
// because the SDK accepts longer extension names.
func isAttributeName(s string) bool {
	if s == "" {
		return false
	}
	for _, c := range s {
		if (c < 'a' || c > 'z') && (c < '0' || c > '9') {
			return false
		}
	}
	return true
}

// Close flushes pending messages and stops the publisher; Publish fails afterwards.
func (p *Publisher) Close() error {
	p.pub.Stop()
	return nil
}

// Publish sends ce to the configured topic.
func (p *Publisher) Publish(ctx context.Context, ce cloudevents.Event) error {
	tc, hasTrace := event.InjectTraceFromContext(ctx, &ce)

	msg, err := marshal(ce, p.cfg.PlainAttributes)
	if err != nil {
		return fmt.Errorf("pubsub marshal: %w", err)
	}
	if hasTrace {
		if msg.Attributes == nil {
			msg.Attributes = map[string]string{}
		}
		msg.Attributes[trace.Header] = tc.Traceparent()
	}

	id, err := p.pub.Publish(ctx, msg).Get(ctx)
	if err != nil {
		// No log here: the caller (mux publishResult or the consuming service)
		// owns the single error log for this operation.
		return fmt.Errorf("pubsub publish: %w", err)
	}
	p.logger.DebugContext(
		ctx,
		"event published",
		slog.String(logfields.KeyMessagingBrokerID, id),
		slog.String(logfields.KeyMessagingMessageID, ce.ID()),
	)
	return nil
}
