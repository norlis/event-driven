package eventhttp

import (
	"testing"
	"time"
)

func TestResolveTTL(t *testing.T) {
	t.Parallel()

	const cacheTTL = 30 * time.Second
	tests := []struct {
		name   string
		field  string
		result map[string]any
		want   time.Duration
	}{
		{name: "no field configured", field: "", result: map[string]any{"expires_in": 60.0}, want: cacheTTL},
		{name: "positive number", field: "expires_in", result: map[string]any{"expires_in": 60.0}, want: 60 * time.Second},
		{name: "field missing", field: "expires_in", result: map[string]any{}, want: cacheTTL},
		{name: "zero", field: "expires_in", result: map[string]any{"expires_in": 0.0}, want: cacheTTL},
		{name: "negative", field: "expires_in", result: map[string]any{"expires_in": -5.0}, want: cacheTTL},
		{name: "string is not parsed", field: "expires_in", result: map[string]any{"expires_in": "60"}, want: cacheTTL},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			tp := &httpTokenProvider{cfg: TokenConfig{ExpiresInField: tt.field, CacheTTL: cacheTTL}}
			if got := tp.resolveTTL(tt.result); got != tt.want {
				t.Errorf("resolveTTL() = %v, want %v", got, tt.want)
			}
		})
	}
}
