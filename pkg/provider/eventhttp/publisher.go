package eventhttp

import (
	"bytes"
	"context"
	"fmt"
	"net/http"
	"time"

	cloudevents "github.com/cloudevents/sdk-go/v2/event"
	"go.uber.org/zap"
)

// TokenFunc returns an authentication token. Implementations should handle
// caching and renewal internally. Called before each Publish.
type TokenFunc func(ctx context.Context) (string, error)

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
}

// Publisher publishes CloudEvents to an HTTP endpoint using binary content mode
// (Ce-* headers + body as data), following the CloudEvents HTTP protocol binding spec.
type Publisher struct {
	cfg    PublisherConfig
	client *http.Client
	logger      *zap.Logger
	tokenHeader string
}

func NewPublisher(cfg PublisherConfig, logger *zap.Logger) *Publisher {
	timeout := cfg.Timeout
	if timeout == 0 {
		timeout = 10 * time.Second
	}
	tokenHeader := cfg.TokenHeader
	if tokenHeader == "" {
		tokenHeader = "Authorization"
	}
	return &Publisher{
		cfg:         cfg,
		client:      &http.Client{Timeout: timeout},
		logger:      logger,
		tokenHeader: tokenHeader,
	}
}

func (p *Publisher) Publish(ce cloudevents.Event) error {
	ctx := context.Background()

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, p.cfg.TargetURL, bytes.NewReader(ce.Data()))
	if err != nil {
		return fmt.Errorf("http publisher: create request: %w", err)
	}

	// Static headers.
	for k, v := range p.cfg.Headers {
		req.Header.Set(k, v)
	}

	// Dynamic token.
	if p.cfg.TokenProvider != nil {
		token, tokenErr := p.cfg.TokenProvider(ctx)
		if tokenErr != nil {
			return fmt.Errorf("http publisher: token provider: %w", tokenErr)
		}
		req.Header.Set(p.tokenHeader, token)
	}

	// Binary content mode: CloudEvent attributes as headers.
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Ce-Id", ce.ID())
	req.Header.Set("Ce-Specversion", ce.SpecVersion())
	req.Header.Set("Ce-Type", ce.Type())
	req.Header.Set("Ce-Source", ce.Source())
	if !ce.Time().IsZero() {
		req.Header.Set("Ce-Time", ce.Time().Format(time.RFC3339))
	}
	if ce.Subject() != "" {
		req.Header.Set("Ce-Subject", ce.Subject())
	}
	for k, v := range ce.Extensions() {
		if s, ok := v.(string); ok {
			req.Header.Set("Ce-"+k, s)
		}
	}

	resp, err := p.client.Do(req)
	if err != nil {
		return fmt.Errorf("http publisher: send: %w", err)
	}
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode >= http.StatusBadRequest {
		return fmt.Errorf("http publisher: unexpected status %d from %s", resp.StatusCode, p.cfg.TargetURL)
	}

	p.logger.Debug("CloudEvent published via HTTP",
		zap.String("targetURL", p.cfg.TargetURL),
		zap.String("ceId", ce.ID()),
		zap.Int("status", resp.StatusCode),
	)
	return nil
}
