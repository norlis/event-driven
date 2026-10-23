package eventhttp

import (
	"runtime/debug"
	"sync"
	"time"

	"resty.dev/v3"
)

// RetryConfig controls retries for the outbound HTTP clients built by this
// package. Retries operate at the HTTP level (not at the event level): if the
// caller already has application-level retry, keep the compound effect in mind.
//
// Count semantics:
//   - 0:  use the component default (publisher=3, token=2, etc.).
//   - <0: retries explicitly disabled.
//   - >0: that many retries.
//
// WaitTime and MaxWaitTime with zero-value fall back to the package defaults
// (200ms and 5s). RetryOn appends an extra condition to the defaults (5xx,
// 429, network errors): if RetryOn returns true the request is retried even
// when the default condition would not.
type RetryConfig struct {
	Count       int
	WaitTime    time.Duration
	MaxWaitTime time.Duration
	RetryOn     func(resp *resty.Response, err error) bool
}

// clientOpts are the internal parameters used to build a *resty.Client with
// the package defaults. It is not exported — each component fills it from its
// own public Config.
type clientOpts struct {
	Timeout      time.Duration
	Retry        *RetryConfig
	UserAgent    string
	DefaultRetry int
}

const (
	defaultTimeout      = 10 * time.Second
	defaultWaitTime     = 200 * time.Millisecond
	defaultMaxWaitTime  = 5 * time.Second
	transientStatusFrom = 500
)

// modulePath is this library's module, looked up in the binary's build info
// so the User-Agent carries the version the consumer actually linked.
const modulePath = "github.com/norlis/event-driven/v2"

// defaultUserAgent identifies outbound requests issued by this package.
// Overrides Resty's built-in "go-resty/..." so external systems see the
// framework name and version instead of the HTTP client library.
var defaultUserAgent = sync.OnceValue(func() string {
	bi, ok := debug.ReadBuildInfo()
	if !ok {
		bi = nil
	}
	return userAgent(bi)
})

// userAgent is "go event-driven/<version>", with the version taken from the
// module among the dependencies (consumer binary) or from the main module
// (this repo's own binaries); no suffix when neither is known.
func userAgent(bi *debug.BuildInfo) string {
	const name = "go event-driven"
	if bi == nil {
		return name
	}
	version := ""
	if bi.Main.Path == modulePath {
		version = bi.Main.Version
	}
	for _, dep := range bi.Deps {
		if dep.Path == modulePath {
			version = dep.Version
		}
	}
	if version == "" || version == "(devel)" {
		return name
	}
	return name + "/" + version
}

// newClient builds a *resty.Client with the package defaults. Close + drain
// of the response body is handled internally by Resty.
func newClient(opts clientOpts) *resty.Client {
	c := resty.New()

	timeout := opts.Timeout
	if timeout == 0 {
		timeout = defaultTimeout
	}
	c.SetTimeout(timeout)

	count := opts.DefaultRetry
	wait := defaultWaitTime
	maxWait := defaultMaxWaitTime

	if opts.Retry != nil {
		switch {
		case opts.Retry.Count < 0:
			count = 0
		case opts.Retry.Count > 0:
			count = opts.Retry.Count
		}
		if opts.Retry.WaitTime > 0 {
			wait = opts.Retry.WaitTime
		}
		if opts.Retry.MaxWaitTime > 0 {
			maxWait = opts.Retry.MaxWaitTime
		}
	}

	c.SetRetryCount(count).
		SetRetryWaitTime(wait).
		SetRetryMaxWaitTime(maxWait).
		AddRetryConditions(defaultRetryCondition)

	if opts.Retry != nil && opts.Retry.RetryOn != nil {
		c.AddRetryConditions(opts.Retry.RetryOn)
	}

	ua := opts.UserAgent
	if ua == "" {
		ua = defaultUserAgent()
	}
	c.SetHeader("User-Agent", ua)

	return c
}

// defaultRetryCondition retries on transport errors (DNS, EOF, connection
// refused, low-level deadlines) and on 5xx or 429 responses. 4xx (other than
// 429) are not retried: they indicate a request problem, not a server one.
func defaultRetryCondition(resp *resty.Response, err error) bool {
	if err != nil {
		return true
	}
	if resp == nil {
		return false
	}
	status := resp.StatusCode()
	return status == 429 || status >= transientStatusFrom
}
