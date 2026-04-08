package signal

import (
	"syscall"
	"testing"
	"time"
)

func TestNotifyContext_CancelsOnSignal(t *testing.T) {
	t.Parallel()

	ctx, stop := NotifyContext()
	defer stop()

	// Send SIGINT to self
	if err := syscall.Kill(syscall.Getpid(), syscall.SIGINT); err != nil {
		t.Fatalf("failed to send SIGINT: %v", err)
	}

	select {
	case <-ctx.Done():
		// expected
	case <-time.After(2 * time.Second):
		t.Fatal("context was not cancelled after SIGINT")
	}
}

func TestNotifyContext_StopReleasesResources(t *testing.T) {
	t.Parallel()

	ctx, stop := NotifyContext()
	stop() // calling stop immediately should cancel context

	select {
	case <-ctx.Done():
		// expected
	case <-time.After(1 * time.Second):
		t.Fatal("context was not cancelled after stop()")
	}
}
