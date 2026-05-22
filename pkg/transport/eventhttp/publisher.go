package eventhttp

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	cloudevents "github.com/cloudevents/sdk-go/v2/event"
	"resty.dev/v3"
)

// TokenFunc returns an authentication token. Implementations should handle
// caching and renewal internally. Called before each Publish.
type TokenFunc func(ctx context.Context) (string, error)

// PublisherConfig configures a Publisher. TargetURL is required; everything
// else has sensible defaults.
type PublisherConfig struct {
	TargetURL string
	Timeout   time.Duration

	// Headers are static headers added to every publish request.
	// Useful for API keys, correlation IDs, etc.
	// Example: {"X-API-Key": "abc123"}
	Headers map[string]string

	// TokenProvider optionally supplies a dynamic auth token before each request.
	// When set, the token is placed in TokenHeader.
	TokenProvider TokenFunc

	// TokenHeader is the header name for the token (default: "Authorization").
	TokenHeader string

	// Retry overrides the default retry policy of the publisher.
	// nil = defaults (3 retries on 5xx/429/network errors, 200ms→5s exp backoff).
	// Retry.Count < 0 disables retries explicitly.
	// Retries are at the HTTP level and are independent from any event-level
	// retry the caller may implement.
	Retry *RetryConfig
}

// Publisher publishes CloudEvents to an HTTP endpoint using binary content mode
// (Ce-* headers + body as data), following the CloudEvents HTTP protocol binding spec.
type Publisher struct {
	cfg         PublisherConfig
	client      *resty.Client
	logger      *slog.Logger
	tokenHeader string
}

// NewPublisher returns a Publisher with sensible defaults: 10s timeout,
// 3 retries on transient failures, and "Authorization" as the token header.
func NewPublisher(cfg PublisherConfig, logger *slog.Logger) *Publisher {
	tokenHeader := cfg.TokenHeader
	if tokenHeader == "" {
		tokenHeader = "Authorization"
	}
	return &Publisher{
		cfg: cfg,
		client: newClient(clientOpts{
			Timeout:      cfg.Timeout,
			Retry:        cfg.Retry,
			Logger:       logger,
			DefaultRetry: 3,
		}),
		logger:      logger,
		tokenHeader: tokenHeader,
	}
}

// Publish sends ce to TargetURL using CloudEvents binary content mode.
func (p *Publisher) Publish(ce cloudevents.Event) error {
	ctx := context.Background()

	req := p.client.R().
		SetContext(ctx).
		SetHeaders(p.cfg.Headers).
		SetHeader("Content-Type", "application/json").
		SetHeader("Ce-Id", ce.ID()).
		SetHeader("Ce-Specversion", ce.SpecVersion()).
		SetHeader("Ce-Type", ce.Type()).
		SetHeader("Ce-Source", ce.Source()).
		SetBody(ce.Data())

	if !ce.Time().IsZero() {
		req.SetHeader("Ce-Time", ce.Time().Format(time.RFC3339))
	}
	if ce.Subject() != "" {
		req.SetHeader("Ce-Subject", ce.Subject())
	}
	for k, v := range ce.Extensions() {
		if s, ok := v.(string); ok {
			req.SetHeader("Ce-"+k, s)
		}
	}

	if p.cfg.TokenProvider != nil {
		token, err := p.cfg.TokenProvider(ctx)
		if err != nil {
			return fmt.Errorf("http publisher: token provider: %w", err)
		}
		req.SetHeader(p.tokenHeader, token)
	}

	resp, err := req.Post(p.cfg.TargetURL)
	if err != nil {
		return fmt.Errorf("http publisher: send: %w", err)
	}
	if resp.IsError() {
		return fmt.Errorf("http publisher: unexpected status %d from %s", resp.StatusCode(), p.cfg.TargetURL)
	}

	p.logger.Debug(
		"CloudEvent published via HTTP",
		slog.String("targetURL", p.cfg.TargetURL),
		slog.String("ceId", ce.ID()),
		slog.Int("status", resp.StatusCode()),
	)
	return nil
}
