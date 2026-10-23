package eventhttp

import (
	"runtime/debug"
	"testing"
)

func TestUserAgentFromBuildInfo(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		bi   *debug.BuildInfo
		want string
	}{
		{name: "consumer binary uses the dependency version", bi: &debug.BuildInfo{
			Main: debug.Module{Path: "example.com/app", Version: "v3.1.0"},
			Deps: []*debug.Module{{Path: modulePath, Version: "v2.0.0"}},
		}, want: "go event-driven/v2.0.0"},
		{name: "own binary uses the main version", bi: &debug.BuildInfo{
			Main: debug.Module{Path: modulePath, Version: "v2.1.0"},
		}, want: "go event-driven/v2.1.0"},
		{name: "unknown version has no suffix", bi: &debug.BuildInfo{Main: debug.Module{Path: "example.com/app"}}, want: "go event-driven"},
		{name: "nil build info", bi: nil, want: "go event-driven"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			if got := userAgent(tt.bi); got != tt.want {
				t.Errorf("userAgent() = %q, want %q", got, tt.want)
			}
		})
	}
}
