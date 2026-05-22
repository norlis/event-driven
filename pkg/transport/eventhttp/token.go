package eventhttp

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"sync"
	"time"

	"resty.dev/v3"
)

// TokenConfig configures a token provider that fetches tokens via HTTP.
//
// Supports any auth endpoint that returns a JSON response with a token field.
// Examples: OAuth2 client_credentials, custom auth APIs, token exchange services.
type TokenConfig struct {
	// URL of the token endpoint.
	URL string

	// Method is the HTTP method (default: POST).
	Method string

	// Headers sent with the token request (e.g., Content-Type, client credentials).
	Headers map[string]string

	// Body is the request body (e.g., JSON credentials, form-encoded grant).
	Body []byte

	// ResponseTokenField is the JSON field name that contains the token in the response
	// (default: "access_token").
	ResponseTokenField string

	// TokenPrefix is prepended to the token value (e.g., "Bearer ").
	TokenPrefix string

	// ExpiresInField is the OAuth2-style field name in the response that
	// indicates the token's lifetime in seconds (commonly "expires_in").
	// When set and the field is present and numeric, it takes precedence over
	// CacheTTL. When set but the field is missing or non-numeric, falls back
	// to CacheTTL. Accepts float64 (default JSON number decoding) and
	// json.Number; string values are not parsed (known limitation, extendable
	// later without breaking the API).
	ExpiresInField string

	// CacheTTL controls how long the token is cached when ExpiresInField is
	// unset or absent in the response. Zero means no caching — a new token is
	// fetched on every publish.
	CacheTTL time.Duration

	// Timeout for the token HTTP request (default: 10s).
	Timeout time.Duration

	// Retry overrides the default retry policy of the token provider.
	// nil = defaults (2 retries on 5xx/429/network errors).
	// Retry.Count < 0 disables retries explicitly.
	Retry *RetryConfig

	// UserAgent overrides the User-Agent header sent to the IdP.
	// Empty falls back to the package default (see defaultUserAgent in client.go).
	UserAgent string
}

// NewTokenProvider creates a TokenFunc that fetches tokens from an HTTP endpoint.
//
// Usage examples:
//
//	// OAuth2 client_credentials with server-driven TTL.
//	provider := NewTokenProvider(TokenConfig{
//	    URL:    "https://auth.example.com/oauth/token",
//	    Method: "POST",
//	    Headers: map[string]string{
//	        "Content-Type": "application/x-www-form-urlencoded",
//	    },
//	    Body:           []byte("grant_type=client_credentials&client_id=ID&client_secret=SECRET"),
//	    TokenPrefix:    "Bearer ",
//	    ExpiresInField: "expires_in",
//	    CacheTTL:       50 * time.Minute, // fallback if the IdP omits expires_in
//	})
//
//	// Custom auth API with fixed-TTL cache.
//	provider := NewTokenProvider(TokenConfig{
//	    URL:    "https://api.example.com/auth",
//	    Headers: map[string]string{
//	        "Content-Type": "application/json",
//	    },
//	    Body:               []byte(`{"user":"svc","password":"secret"}`),
//	    ResponseTokenField: "token",
//	    TokenPrefix:        "Token ",
//	    CacheTTL:           30 * time.Minute,
//	})
func NewTokenProvider(cfg TokenConfig) TokenFunc {
	if cfg.Method == "" {
		cfg.Method = http.MethodPost
	}
	if cfg.ResponseTokenField == "" {
		cfg.ResponseTokenField = "access_token"
	}

	tp := &httpTokenProvider{
		cfg: cfg,
		client: newClient(clientOpts{
			Timeout:      cfg.Timeout,
			Retry:        cfg.Retry,
			Logger:       nil,
			UserAgent:    cfg.UserAgent,
			DefaultRetry: 2,
		}),
	}
	return tp.Token
}

type httpTokenProvider struct {
	cfg    TokenConfig
	client *resty.Client

	mu        sync.Mutex
	cached    string
	expiresAt time.Time
}

func (tp *httpTokenProvider) Token(ctx context.Context) (string, error) {
	tp.mu.Lock()
	defer tp.mu.Unlock()

	if tp.cached != "" && time.Now().Before(tp.expiresAt) {
		return tp.cached, nil
	}

	token, ttl, err := tp.fetch(ctx)
	if err != nil {
		return "", err
	}

	if ttl > 0 {
		tp.cached = token
		tp.expiresAt = time.Now().Add(ttl)
	}

	return token, nil
}

func (tp *httpTokenProvider) fetch(ctx context.Context) (string, time.Duration, error) {
	var result map[string]any

	resp, err := tp.client.R().
		SetContext(ctx).
		SetHeaders(tp.cfg.Headers).
		SetBody(tp.cfg.Body).
		SetResult(&result).
		Execute(tp.cfg.Method, tp.cfg.URL)
	if err != nil {
		return "", 0, fmt.Errorf("token provider: request failed: %w", err)
	}
	if resp.IsError() {
		return "", 0, fmt.Errorf("token provider: unexpected status %d from %s", resp.StatusCode(), tp.cfg.URL)
	}

	tokenVal, ok := result[tp.cfg.ResponseTokenField]
	if !ok {
		return "", 0, fmt.Errorf("token provider: field %q not found in response", tp.cfg.ResponseTokenField)
	}
	token, ok := tokenVal.(string)
	if !ok {
		return "", 0, fmt.Errorf("token provider: field %q is not a string", tp.cfg.ResponseTokenField)
	}

	return tp.cfg.TokenPrefix + token, tp.resolveTTL(result), nil
}

// resolveTTL returns the effective TTL for a freshly fetched token.
// Priority: ExpiresInField from the response > CacheTTL > 0 (no caching).
// If ExpiresInField is configured but missing or non-numeric, falls back to
// CacheTTL.
func (tp *httpTokenProvider) resolveTTL(result map[string]any) time.Duration {
	if tp.cfg.ExpiresInField == "" {
		return tp.cfg.CacheTTL
	}
	raw, ok := result[tp.cfg.ExpiresInField]
	if !ok {
		return tp.cfg.CacheTTL
	}
	switch v := raw.(type) {
	case float64:
		if v > 0 {
			return time.Duration(v) * time.Second
		}
	case json.Number:
		if n, err := v.Int64(); err == nil && n > 0 {
			return time.Duration(n) * time.Second
		}
	}
	return tp.cfg.CacheTTL
}
