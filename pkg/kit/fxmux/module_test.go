package fxmux

import (
	"bytes"
	"context"
	"errors"
	"log/slog"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"go.uber.org/fx"
	"go.uber.org/fx/fxtest"

	"github.com/norlis/event-driven/v2/pkg/event"
	"github.com/norlis/event-driven/v2/pkg/eventmux"
	"github.com/norlis/event-driven/v2/pkg/kit/lifecycle"
)

var errBoom = errors.New("boom")

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

type fakeComp struct {
	name     string
	rec      *recorder
	startErr error

	mu      sync.Mutex
	onFatal func(error)
}

func (f *fakeComp) Start(_ context.Context, onFatal func(error)) error {
	f.rec.add("start " + f.name)
	f.mu.Lock()
	f.onFatal = onFatal
	f.mu.Unlock()
	return f.startErr
}

func (f *fakeComp) Stop(context.Context) error {
	f.rec.add("stop " + f.name)
	return nil
}

func (f *fakeComp) fatal(err error) {
	f.mu.Lock()
	fn := f.onFatal
	f.mu.Unlock()
	fn(err)
}

// Distinct types so fx builds them in dependency order c, b, a: the opposite
// of the order they are added to the group.
type (
	compA struct{ *fakeComp }
	compB struct{ *fakeComp }
	compC struct{ *fakeComp }
)

func TestBindGroupOrder(t *testing.T) {
	t.Parallel()
	rec := &recorder{}
	app := fxtest.New(t,
		fx.NopLogger,
		fx.Supply(rec),
		fx.Provide(
			func(r *recorder) compC { return compC{&fakeComp{name: "c", rec: r}} },
			func(c compC) compB { return compB{&fakeComp{name: "b", rec: c.rec}} },
			func(b compB) compA { return compA{&fakeComp{name: "a", rec: b.rec}} },
		),
		fx.Invoke(func(lc fx.Lifecycle, sd fx.Shutdowner, a compA, b compB, c compC) {
			g := lifecycle.NewGroup(nil, 0)
			g.Add("a", a)
			g.Add("b", b)
			g.Add("c", c)
			BindGroup(lc, sd, g, nil)
		}),
	)
	app.RequireStart().RequireStop()
	want := []string{"start a", "start b", "start c", "stop c", "stop b", "stop a"}
	if got := rec.entries(); !slices.Equal(got, want) {
		t.Errorf("log = %q, want %q", got, want)
	}
}

func TestBindGroupStartError(t *testing.T) {
	t.Parallel()
	app := fxtest.New(t,
		fx.NopLogger,
		fx.Invoke(func(lc fx.Lifecycle, sd fx.Shutdowner) {
			g := lifecycle.NewGroup(nil, 0)
			g.Add("a", &fakeComp{name: "a", rec: &recorder{}, startErr: errBoom})
			BindGroup(lc, sd, g, nil)
		}),
	)
	if err := app.Start(t.Context()); !errors.Is(err, errBoom) {
		t.Fatalf("app.Start() error = %v, want boom", err)
	}
}

func TestBindGroupFatalShutsDown(t *testing.T) {
	t.Parallel()
	var logs bytes.Buffer
	comp := &fakeComp{name: "a", rec: &recorder{}}
	app := fxtest.New(t,
		fx.NopLogger,
		fx.Invoke(func(lc fx.Lifecycle, sd fx.Shutdowner) {
			g := lifecycle.NewGroup(nil, 0)
			g.Add("a", comp)
			BindGroup(lc, sd, g, slog.New(slog.NewJSONHandler(&logs, nil)))
		}),
	)
	app.RequireStart()
	defer app.RequireStop()

	comp.fatal(errBoom)
	select {
	case sig := <-app.Wait():
		if sig.ExitCode != 1 {
			t.Errorf("ExitCode = %d, want 1", sig.ExitCode)
		}
		if n := strings.Count(logs.String(), `"msg":"component failed"`); n != 1 || !strings.Contains(logs.String(), "boom") {
			t.Errorf("logged %d component failed records, want 1 with the cause: %s", n, logs.String())
		}
	case <-time.After(time.Second):
		t.Fatal("no shutdown after a component failure")
	}
}

// fakeSubscription records whether the mux started and stopped it.
type fakeSubscription struct{ rec *recorder }

func (s *fakeSubscription) Start(context.Context, func(*event.Message), func(error)) error {
	s.rec.add("subscription started")
	return nil
}

func (s *fakeSubscription) Stop(context.Context) error {
	s.rec.add("subscription stopped")
	return nil
}

func TestBindGroupWithMux(t *testing.T) {
	t.Parallel()
	rec := &recorder{}
	app := fxtest.New(t,
		fx.NopLogger,
		fx.Invoke(func(lc fx.Lifecycle, sd fx.Shutdowner) error {
			mux, err := eventmux.New(eventmux.Config{Name: "m", Subscription: &fakeSubscription{rec: rec}})
			if err != nil {
				return err
			}
			g := lifecycle.NewGroup(nil, 0)
			g.Add(mux.Name(), mux)
			BindGroup(lc, sd, g, nil)
			return nil
		}),
	)
	app.RequireStart().RequireStop()
	want := []string{"subscription started", "subscription stopped"}
	if got := rec.entries(); !slices.Equal(got, want) {
		t.Errorf("log = %q, want %q", got, want)
	}
}
