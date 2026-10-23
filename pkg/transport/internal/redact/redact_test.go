package redact

import "testing"

// URLs may carry credentials in userinfo or query; logs and errors keep only
// what identifies the endpoint.
func TestURL(t *testing.T) {
	t.Parallel()

	tests := map[string]string{
		"https://user:pw@hooks.example.com/v1/events?api_key=secret#frag": "https://hooks.example.com/v1/events",
		"http://localhost:8080/cb":       "http://localhost:8080/cb",
		"::not a url":                    "",
		"https://user:pa,ss@host/x":      "https://host/x", // a comma in the password is not a list separator
		"https://h/x?token=a,secretpart": "https://h/x",    // nor is one in the query
		"user:pass@host:4222":            "",               // no scheme: refuse rather than guess
	}
	for raw, want := range tests {
		if got := URL(raw); got != want {
			t.Errorf("URL(%q) = %q, want %q", raw, got, want)
		}
	}
}

// NATS takes a comma-separated server list; each element is redacted alone.
func TestURLs(t *testing.T) {
	t.Parallel()

	tests := map[string]string{
		"nats://u:p@a:4222,nats://b:4222?x=1": "nats://a:4222,nats://b:4222",
		"nats://a:4222, ::bad ,nats://c:4222": "nats://a:4222,,nats://c:4222",
		"user:pass@host:4222":                 "",
		"nats://a:4222":                       "nats://a:4222",
	}
	for raw, want := range tests {
		if got := URLs(raw); got != want {
			t.Errorf("URLs(%q) = %q, want %q", raw, got, want)
		}
	}
}
