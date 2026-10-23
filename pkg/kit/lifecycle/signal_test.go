//go:build unix

package lifecycle

import (
	"syscall"
	"testing"
	"time"
)

// Not parallel on purpose: SIGTERM reaches the whole process, and Go runs
// sequential tests while the parallel ones in this package are paused.
//
//nolint:paralleltest // see above
func TestRunStopsOnSIGTERM(t *testing.T) {
	rec := &recorder{}
	g := groupOf(fakes(rec, "a", "b")...)
	errc := make(chan error, 1)
	go func() { errc <- g.Run(t.Context()) }()

	deadline := time.Now().Add(time.Second)
	for len(rec.entries()) < 2 {
		if time.Now().After(deadline) {
			t.Fatal("components did not start")
		}
		time.Sleep(time.Millisecond)
	}
	if err := syscall.Kill(syscall.Getpid(), syscall.SIGTERM); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-errc:
		if err != nil {
			t.Fatalf("Run() error = %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("Run did not return after SIGTERM")
	}
	wantLog(t, rec, "start a", "start b", "stop b", "stop a")
}
