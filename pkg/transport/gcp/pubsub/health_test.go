package pubsub

import (
	"testing"
	"time"
)

func TestHealthCheckerTimeout(t *testing.T) {
	t.Parallel()

	if got := NewHealthChecker(nil, "p").timeout; got != DefaultCheckTimeout {
		t.Errorf("default timeout = %s, want %s", got, DefaultCheckTimeout)
	}
	if got := NewHealthChecker(nil, "p", WithTimeout(2*time.Second)).timeout; got != 2*time.Second {
		t.Errorf("WithTimeout = %s, want 2s", got)
	}
	defer func() {
		if recover() == nil {
			t.Error("WithTimeout(0) did not panic")
		}
	}()
	NewHealthChecker(nil, "p", WithTimeout(0))
}
