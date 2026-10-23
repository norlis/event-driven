// Package redact strips credentials from values before they reach logs or
// errors.
package redact

import (
	"net/url"
	"strings"
)

// URL keeps scheme, host and path of raw and drops userinfo, query and
// fragment, which may carry credentials. A value that does not parse, or has
// no scheme (so its userinfo cannot be told apart from a host), becomes "".
func URL(raw string) string {
	if !strings.Contains(raw, "://") {
		return ""
	}
	u, err := url.Parse(raw)
	if err != nil {
		return ""
	}
	return (&url.URL{Scheme: u.Scheme, Host: u.Host, Path: u.Path}).String()
}

// URLs redacts a comma-separated server list, such as a NATS URL, element by
// element. Only list values use it: a single URL may hold commas in its
// password or query, which URL must not split on.
func URLs(raw string) string {
	parts := strings.Split(raw, ",")
	for i, p := range parts {
		parts[i] = URL(strings.TrimSpace(p))
	}
	return strings.Join(parts, ",")
}
