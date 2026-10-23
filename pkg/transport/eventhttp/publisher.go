package eventhttp

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"net/url"
	"time"

	cloudevents "github.com/cloudevents/sdk-go/v2/event"
	"resty.dev/v3"

	"github.com/norlis/httpgate/logging"
	"github.com/norlis/httpgate/trace"

	"github.com/norlis/event-driven/v2/pkg/event"
	"github.com/norlis/event-driven/v2/pkg/kit/logfields"
	"github.com/norlis/event-driven/v2/pkg/transport/internal/envelope"
	"github.com/norlis/event-driven/v2/pkg/transport/internal/redact"
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

	// UserAgent overrides the User-Agent header sent by this publisher.
	// Empty falls back to the package default (see defaultUserAgent in client.go).
	// Useful when a deployment wants to identify itself to webhook destinations
	// or observability tooling.
	UserAgent string
}

// Publisher publishes CloudEvents to an HTTP endpoint using binary content mode
// (Ce-* headers + body as data), following the CloudEvents HTTP protocol binding spec.
type Publisher struct {
	cfg         PublisherConfig
	target      string // cfg.TargetURL without credentials, for logs and errors
	client      *resty.Client
	logger      *slog.Logger
	tokenHeader string
}

// NewPublisher validates cfg and returns a Publisher with sensible defaults:
// 10s timeout, 3 retries on transient failures, "Authorization" as the token
// header. TargetURL must be an absolute http(s) URL.
func NewPublisher(cfg PublisherConfig, logger *slog.Logger) (*Publisher, error) {
	if cfg.TargetURL == "" {
		return nil, errors.New("eventhttp: PublisherConfig.TargetURL is empty")
	}
	// The URL may carry credentials in its query: never echo it in the error.
	u, err := url.Parse(cfg.TargetURL)
	switch {
	case err != nil:
		return nil, errors.New("eventhttp: PublisherConfig.TargetURL is not a valid URL")
	case (u.Scheme != "http" && u.Scheme != "https") || u.Host == "":
		return nil, errors.New("eventhttp: PublisherConfig.TargetURL must be an absolute http(s) URL")
	case cfg.Timeout < 0:
		return nil, fmt.Errorf("eventhttp: PublisherConfig.Timeout is negative: %s", cfg.Timeout)
	}
	tokenHeader := cfg.TokenHeader
	if tokenHeader == "" {
		tokenHeader = "Authorization"
	}
	target := redact.URL(cfg.TargetURL)
	logger = logfields.Transport(logger, "eventhttp-publisher", logfields.SystemHTTP, target)
	return &Publisher{
		cfg:    cfg,
		target: target,
		client: newClient(clientOpts{
			Timeout:      cfg.Timeout,
			Retry:        cfg.Retry,
			UserAgent:    cfg.UserAgent,
			DefaultRetry: 3,
		}),
		logger:      logger,
		tokenHeader: tokenHeader,
	}, nil
}

// Publish sends ce to TargetURL using CloudEvents binary content mode.
func (p *Publisher) Publish(ctx context.Context, ce cloudevents.Event) error {
	tc, hasTrace := event.InjectTraceFromContext(ctx, &ce)

	attrs, err := envelope.Encode(ce)
	if err != nil {
		return fmt.Errorf("http publisher: %w", err)
	}
	req := p.client.R().
		SetContext(ctx).
		SetHeaders(p.cfg.Headers).
		SetHeader("Content-Type", envelope.ContentType(ce)).
		SetHeaders(attrs).
		SetBody(ce.Data())

	if hasTrace {
		req.SetHeader(trace.Header, tc.Traceparent())
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
	if resp.IsStatusFailure() {
		err := fmt.Errorf("http publisher: unexpected status %d from %s", resp.StatusCode(), p.target)
		if permanentStatus(resp.StatusCode()) {
			// The mux nacks a plain publish error and the handler runs again on
			// redelivery; a client error the destination will repeat must not
			// turn into an endless loop.
			return event.NewNonRetryableError(err)
		}
		return err
	}

	p.logger.DebugContext(
		ctx,
		"event published",
		slog.String(logfields.KeyURLFull, p.target),
		slog.String(logfields.KeyMessagingMessageID, ce.ID()),
		slog.Int(logging.KeyHTTPResponseStatusCode, resp.StatusCode()),
	)
	return nil
}

// permanentStatus reports whether a response status describes the request
// itself, so retrying the same event cannot succeed: every 4xx except 408
// (timeout) and 429 (throttling), which the destination asks to retry.
func permanentStatus(status int) bool {
	return status >= 400 && status < 500 && status != http.StatusRequestTimeout && status != http.StatusTooManyRequests
}
