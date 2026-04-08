package httpdriven

import (
	"bytes"
	"context"
	"fmt"
	"net/http"
	"time"

	cloudevents "github.com/cloudevents/sdk-go/v2/event"
	"go.uber.org/zap"
)

type HTTPPublisherConfig struct {
	TargetURL string
	Timeout   time.Duration
}

// HTTPPublisher publishes CloudEvents to an HTTP endpoint using binary content mode
// (Ce-* headers + body as data), following the CloudEvents HTTP protocol binding spec.
type HTTPPublisher struct {
	cfg    HTTPPublisherConfig
	client *http.Client
	logger *zap.Logger
}

func NewHTTPPublisher(cfg HTTPPublisherConfig, logger *zap.Logger) *HTTPPublisher {
	timeout := cfg.Timeout
	if timeout == 0 {
		timeout = 10 * time.Second
	}
	return &HTTPPublisher{
		cfg:    cfg,
		client: &http.Client{Timeout: timeout},
		logger: logger,
	}
}

func (p *HTTPPublisher) Publish(ce cloudevents.Event) error {
	req, err := http.NewRequestWithContext(context.Background(), http.MethodPost, p.cfg.TargetURL, bytes.NewReader(ce.Data()))
	if err != nil {
		return fmt.Errorf("http publisher: create request: %w", err)
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
