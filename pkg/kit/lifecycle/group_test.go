package lifecycle

import (
	"bytes"
	"context"
	"errors"
	"log/slog"
	"slices"
	"strings"
	"sync"
	"testing"
	"testing/synctest"
	"time"
)

var errBoom = errors.New("boom")

// recorder is the shared, ordered log of Start/Stop calls.
type recorder struct {
	mu  sync.Mutex
	log []string
}

func (r *recorder) add(s string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.log = append(r.log, s)
}

func (r *recorder) entries() []string {
	r.mu.Lock()
	defer r.mu.Unlock()
	return slices.Clone(r.log)
}

// fake records Start/Stop into a shared log; it can fail, stall in Stop
// ignoring ctx, or report a fatal error before Start returns.
type fake struct {
	name         string
	rec          *recorder
	startErr     error
	stopErr      error
	stopDelay    time.Duration
	fatalOnStart error
	fatalOnStop  error // reported through onFatal from inside Stop

	mu         sync.Mutex
	onFatal    func(error)
	stopCtxErr error
}

func (f *fake) Start(_ context.Context, onFatal func(error)) error {
	f.rec.add("start " + f.name)
	f.mu.Lock()
	f.onFatal = onFatal
	f.mu.Unlock()
	if f.fatalOnStart != nil {
		onFatal(f.fatalOnStart)
	}
	return f.startErr
}

func (f *fake) Stop(ctx context.Context) error {
	if f.stopDelay > 0 {
		time.Sleep(f.stopDelay)
	}
	f.mu.Lock()
	f.stopCtxErr = ctx.Err()
	fn := f.onFatal
	f.mu.Unlock()
	if f.fatalOnStop != nil && fn != nil {
		fn(f.fatalOnStop)
	}
	f.rec.add("stop " + f.name)
	return f.stopErr
}

func (f *fake) fatal(err error) {
	f.mu.Lock()
	fn := f.onFatal
	f.mu.Unlock()
	fn(err)
}

func fakes(rec *recorder, names ...string) []*fake {
	out := make([]*fake, 0, len(names))
	for _, n := range names {
		out = append(out, &fake{name: n, rec: rec})
	}
	return out
}

func groupOf(fs ...*fake) *Group {
	g := NewGroup(nil, 0)
	for _, f := range fs {
		g.Add(f.name, f)
	}
	return g
}

func wantLog(t *testing.T, rec *recorder, want ...string) {
	t.Helper()
	if got := rec.entries(); !slices.Equal(got, want) {
		t.Errorf("log = %q, want %q", got, want)
	}
}

func TestStartStopOrder(t *testing.T) {
	t.Parallel()
	rec := &recorder{}
	g := groupOf(fakes(rec, "a", "b", "c")...)
	if err := g.Start(t.Context(), nil); err != nil {
		t.Fatalf("Start() error = %v", err)
	}
	if err := g.Stop(t.Context()); err != nil {
		t.Fatalf("Stop() error = %v", err)
	}
	wantLog(t, rec, "start a", "start b", "start c", "stop c", "stop b", "stop a")
}

func TestStartFailureStopsStarted(t *testing.T) {
	t.Parallel()
	rec := &recorder{}
	fs := fakes(rec, "a", "b", "c")
	fs[1].startErr = errBoom
	err := groupOf(fs...).Start(t.Context(), nil)
	if !errors.Is(err, errBoom) || !strings.Contains(err.Error(), "start b") {
		t.Fatalf("Start() error = %v, want boom naming b", err)
	}
	wantLog(t, rec, "start a", "start b", "stop a")
}

func TestStartFailureLeavesGroupStopped(t *testing.T) {
	t.Parallel()
	rec := &recorder{}
	fs := fakes(rec, "a", "b")
	fs[1].startErr = errBoom
	g := groupOf(fs...)
	_ = g.Start(t.Context(), nil)
	before := len(rec.entries())
	if err := g.Stop(t.Context()); err != nil {
		t.Fatalf("Stop() error = %v", err)
	}
	if got := len(rec.entries()); got != before {
		t.Errorf("Stop after failed Start touched components: %q", rec.entries())
	}
}

func TestStopJoinsErrors(t *testing.T) {
	t.Parallel()
	rec := &recorder{}
	errA, errC := errors.New("a failed"), errors.New("c failed")
	fs := fakes(rec, "a", "b", "c")
	fs[0].stopErr, fs[2].stopErr = errA, errC
	g := groupOf(fs...)
	if err := g.Start(t.Context(), nil); err != nil {
		t.Fatal(err)
	}
	err := g.Stop(t.Context())
	if !errors.Is(err, errA) || !errors.Is(err, errC) {
		t.Fatalf("Stop() error = %v, want both", err)
	}
	if !strings.Contains(err.Error(), "stop a") || !strings.Contains(err.Error(), "stop c") {
		t.Errorf("Stop() error %q does not name a and c", err)
	}
	wantLog(t, rec, "start a", "start b", "start c", "stop c", "stop b", "stop a")
}

func TestStopDeadline(t *testing.T) {
	t.Parallel()
	synctest.Test(t, func(t *testing.T) {
		rec := &recorder{}
		fs := fakes(rec, "a", "b", "c")
		fs[2].stopDelay = 200 * time.Millisecond
		g := groupOf(fs...)
		if err := g.Start(t.Context(), nil); err != nil {
			t.Fatal(err)
		}
		ctx, cancel := context.WithTimeout(t.Context(), 50*time.Millisecond)
		defer cancel()
		start := time.Now()
		_ = g.Stop(ctx)
		if elapsed := time.Since(start); elapsed != 200*time.Millisecond {
			t.Errorf("Stop took %v, want 200ms", elapsed)
		}
		for _, f := range fs[:2] {
			if f.stopCtxErr == nil {
				t.Errorf("%s stopped with a live context", f.name)
			}
		}
		wantLog(t, rec, "start a", "start b", "start c", "stop c", "stop b", "stop a")
	})
}

func TestRunCancel(t *testing.T) {
	t.Parallel()
	synctest.Test(t, func(t *testing.T) {
		rec := &recorder{}
		g := groupOf(fakes(rec, "a", "b")...)
		ctx, cancel := context.WithCancel(t.Context())
		errc := make(chan error, 1)
		go func() { errc <- g.run(ctx, func() {}) }()
		synctest.Wait()
		cancel()
		if err := <-errc; err != nil {
			t.Fatalf("Run() error = %v", err)
		}
		wantLog(t, rec, "start a", "start b", "stop b", "stop a")
	})
}

func TestRunWithCancelledContext(t *testing.T) {
	t.Parallel()
	synctest.Test(t, func(t *testing.T) {
		rec := &recorder{}
		g := groupOf(fakes(rec, "a", "b")...)
		ctx, cancel := context.WithCancel(t.Context())
		cancel()
		if err := g.run(ctx, func() {}); err != nil {
			t.Fatalf("Run() error = %v", err)
		}
		wantLog(t, rec, "start a", "start b", "stop b", "stop a")
	})
}

func assertFatal(t *testing.T, err error) {
	t.Helper()
	if !errors.Is(err, ErrComponentFailed) || !errors.Is(err, errBoom) || !strings.Contains(err.Error(), "b") {
		t.Fatalf("Run() error = %v, want ErrComponentFailed + boom naming b", err)
	}
}

func TestRunFatal(t *testing.T) {
	t.Parallel()
	synctest.Test(t, func(t *testing.T) {
		rec := &recorder{}
		fs := fakes(rec, "a", "b")
		g := groupOf(fs...)
		errc := make(chan error, 1)
		go func() { errc <- g.run(t.Context(), func() {}) }()
		synctest.Wait()
		fs[1].fatal(errBoom)
		fs[1].fatal(errors.New("second"))
		err := <-errc
		assertFatal(t, err)
		if strings.Contains(err.Error(), "second") {
			t.Errorf("second fatal leaked into %v", err)
		}
		wantLog(t, rec, "start a", "start b", "stop b", "stop a")
	})
}

func TestRunFatalDuringStart(t *testing.T) {
	t.Parallel()
	synctest.Test(t, func(t *testing.T) {
		rec := &recorder{}
		fs := fakes(rec, "a", "b")
		fs[1].fatalOnStart = errBoom
		assertFatal(t, groupOf(fs...).run(t.Context(), func() {}))
		wantLog(t, rec, "start a", "start b", "stop b", "stop a")
	})
}

func TestRunNoErrorLogs(t *testing.T) {
	t.Parallel()
	synctest.Test(t, func(t *testing.T) {
		var buf bytes.Buffer
		logger := slog.New(slog.NewJSONHandler(&buf, &slog.HandlerOptions{Level: slog.LevelDebug}))
		rec := &recorder{}
		fs := fakes(rec, "a", "b")
		fs[1].fatalOnStart = errBoom
		g := NewGroup(logger, 0)
		for _, f := range fs {
			g.Add(f.name, f)
		}
		_ = g.run(t.Context(), func() {})
		if strings.Contains(buf.String(), `"level":"ERROR"`) {
			t.Errorf("group logged an ERROR record: %s", buf.String())
		}
		if !strings.Contains(buf.String(), "component started") {
			t.Errorf("missing DEBUG lifecycle records: %s", buf.String())
		}
	})
}

func TestNewGroupDefaultTimeout(t *testing.T) {
	t.Parallel()
	for _, timeout := range []time.Duration{0, -time.Second} {
		synctest.Test(t, func(t *testing.T) {
			rec := &recorder{}
			stall := &stallingStop{rec: rec}
			g := NewGroup(nil, timeout)
			g.Add("stall", stall)
			ctx, cancel := context.WithCancel(t.Context())
			cancel()
			start := time.Now()
			_ = g.run(ctx, func() {})
			if elapsed := time.Since(start); elapsed != defaultShutdownTimeout {
				t.Errorf("timeout %v: Run took %v, want %v", timeout, elapsed, defaultShutdownTimeout)
			}
		})
	}
}

// stallingStop blocks in Stop until its context ends.
type stallingStop struct{ rec *recorder }

func (*stallingStop) Start(context.Context, func(error)) error { return nil }
func (s *stallingStop) Stop(ctx context.Context) error {
	<-ctx.Done()
	s.rec.add("stopped")
	return ctx.Err()
}

func mustPanic(t *testing.T, fn func()) {
	t.Helper()
	defer func() {
		r := recover()
		if s, ok := r.(string); !ok || !strings.Contains(s, "lifecycle") {
			t.Errorf("recover() = %v, want a lifecycle panic", r)
		}
	}()
	fn()
}

func TestAddAfterStartPanics(t *testing.T) {
	t.Parallel()
	g := groupOf(fakes(&recorder{}, "a")...)
	if err := g.Start(t.Context(), nil); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = g.Stop(context.WithoutCancel(t.Context())) })
	mustPanic(t, func() { g.Add("b", &fake{name: "b", rec: &recorder{}}) })
}

func TestAddNilPanics(t *testing.T) {
	t.Parallel()
	mustPanic(t, func() { NewGroup(nil, 0).Add("a", nil) })
}

func TestAddEmptyNamePanics(t *testing.T) {
	t.Parallel()
	mustPanic(t, func() { NewGroup(nil, 0).Add("", &fake{rec: &recorder{}}) })
}

func TestSecondStartFails(t *testing.T) {
	t.Parallel()
	rec := &recorder{}
	g := groupOf(fakes(rec, "a")...)
	if err := g.Start(t.Context(), nil); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = g.Stop(context.WithoutCancel(t.Context())) })
	if err := g.Start(t.Context(), nil); err == nil {
		t.Fatal("second Start() error = nil")
	}
	wantLog(t, rec, "start a")
}

// ctxFake fails Start when its context is already done, as real components
// do (Pub/Sub GetSubscription, JetStream CreateConsumer, a hostname Listen).
type ctxFake struct {
	name string
	rec  *recorder
}

func (f *ctxFake) Start(ctx context.Context, _ func(error)) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	f.rec.add("start " + f.name)
	return nil
}

func (f *ctxFake) Stop(context.Context) error {
	f.rec.add("stop " + f.name)
	return nil
}

func TestRunFatalDuringStartKeepsStartContext(t *testing.T) {
	t.Parallel()
	synctest.Test(t, func(t *testing.T) {
		rec := &recorder{}
		g := NewGroup(nil, 0)
		g.Add("a", &fake{name: "a", rec: rec, fatalOnStart: errBoom})
		g.Add("b", &ctxFake{name: "b", rec: rec})
		err := g.run(t.Context(), func() {})
		if !errors.Is(err, ErrComponentFailed) || !errors.Is(err, errBoom) {
			t.Fatalf("Run() error = %v, want the fatal, not a cancelled start", err)
		}
		wantLog(t, rec, "start a", "start b", "stop b", "stop a")
	})
}

func TestRunCancelledBeforeStartIsClean(t *testing.T) {
	t.Parallel()
	synctest.Test(t, func(t *testing.T) {
		rec := &recorder{}
		g := NewGroup(nil, 0)
		g.Add("a", &ctxFake{name: "a", rec: rec})
		ctx, cancel := context.WithCancel(t.Context())
		cancel()
		if err := g.run(ctx, func() {}); err != nil {
			t.Fatalf("Run() with a cancelled context error = %v, want nil (shutdown requested)", err)
		}
	})
}
