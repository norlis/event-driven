package eventhttp

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"sync"
	"time"
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

	// CacheTTL controls how long the token is cached before re-fetching.
	// Zero means no caching — a new token is fetched on every publish.
	CacheTTL time.Duration

	// Timeout for the token HTTP request (default: 10s).
	Timeout time.Duration
}

// NewTokenProvider creates a TokenFunc that fetches tokens from an HTTP endpoint.
//
// Usage examples:
//
//	// OAuth2 client_credentials
//	provider := NewTokenProvider(TokenConfig{
//	    URL:    "https://auth.example.com/oauth/token",
//	    Method: "POST",
//	    Headers: map[string]string{
//	        "Content-Type": "application/x-www-form-urlencoded",
//	    },
//	    Body:        []byte("grant_type=client_credentials&client_id=ID&client_secret=SECRET"),
//	    TokenPrefix: "Bearer ",
//	    CacheTTL:    50 * time.Minute,
//	})
//
//	// Custom auth API
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
	timeout := cfg.Timeout
	if timeout == 0 {
		timeout = 10 * time.Second
	}

	tp := &httpTokenProvider{
		cfg:    cfg,
		client: &http.Client{Timeout: timeout},
	}
	return tp.Token
}

type httpTokenProvider struct {
	cfg    TokenConfig
	client *http.Client

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

	token, err := tp.fetch(ctx)
	if err != nil {
		return "", err
	}

	if tp.cfg.CacheTTL > 0 {
		tp.cached = token
		tp.expiresAt = time.Now().Add(tp.cfg.CacheTTL)
	}

	return token, nil
}

func (tp *httpTokenProvider) fetch(ctx context.Context) (string, error) {
	req, err := http.NewRequestWithContext(ctx, tp.cfg.Method, tp.cfg.URL, bytes.NewReader(tp.cfg.Body))
	if err != nil {
		return "", fmt.Errorf("token provider: create request: %w", err)
	}

	for k, v := range tp.cfg.Headers {
		req.Header.Set(k, v)
	}

	resp, err := tp.client.Do(req)
	if err != nil {
		return "", fmt.Errorf("token provider: request failed: %w", err)
	}
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode >= http.StatusBadRequest {
		return "", fmt.Errorf("token provider: unexpected status %d from %s", resp.StatusCode, tp.cfg.URL)
	}

	respBody, err := io.ReadAll(resp.Body)
	if err != nil {
		return "", fmt.Errorf("token provider: read response: %w", err)
	}

	var result map[string]any
	if err = json.Unmarshal(respBody, &result); err != nil {
		return "", fmt.Errorf("token provider: parse response: %w", err)
	}

	tokenVal, ok := result[tp.cfg.ResponseTokenField]
	if !ok {
		return "", fmt.Errorf("token provider: field %q not found in response", tp.cfg.ResponseTokenField)
	}

	token, ok := tokenVal.(string)
	if !ok {
		return "", fmt.Errorf("token provider: field %q is not a string", tp.cfg.ResponseTokenField)
	}

	return tp.cfg.TokenPrefix + token, nil
}
