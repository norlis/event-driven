package lifecycle

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"slices"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/norlis/event-driven/v2/pkg/kit/logfields"
)

// defaultShutdownTimeout stays below the 30 s ECS/Fargate stop timeout, like
// httpgate/server.
const defaultShutdownTimeout = 25 * time.Second

// ErrComponentFailed wraps a failure a component reported after Start.
var ErrComponentFailed = errors.New("lifecycle: component failed")

// ErrStoppedDuringStart is returned by Start when Stop ran while a component
// was still starting (an onFatal that stops the group, for instance); the
// component that had just started is stopped before Start returns.
var ErrStoppedDuringStart = errors.New("lifecycle: group stopped during start")

type member struct {
	name string
	c    Component
}

// Group starts components in Add order and stops them in reverse. It logs
// only its own DEBUG start/stop records: component failures go to onFatal or
// are returned, and the caller logs them.
type Group struct {
	logger          *slog.Logger
	shutdownTimeout time.Duration

	mu      sync.Mutex
	members []member
	started bool
	running int // how many members Start brought up, the ones Stop walks back

	// stopping is set once Stop begins: a failure a component reports while
	// being stopped (a consumer closed by the drain) is not fatal.
	stopping atomic.Bool
}

// NewGroup returns an empty Group. shutdownTimeout bounds Run's stop phase;
// zero or negative selects 25 s. A nil logger discards.
func NewGroup(logger *slog.Logger, shutdownTimeout time.Duration) *Group {
	if logger == nil {
		logger = slog.New(slog.DiscardHandler)
	}
	if shutdownTimeout <= 0 {
		shutdownTimeout = defaultShutdownTimeout
	}
	return &Group{logger: logger, shutdownTimeout: shutdownTimeout}
}

// Add appends c under name. Add in dependency order: what a component uses is
// added before it, so it is closed after it. Add panics after Start, on a nil
// component and on an empty name: all three are wiring bugs.
func (g *Group) Add(name string, c Component) {
	switch {
	case c == nil:
		panic("lifecycle: Add of a nil component")
	case name == "":
		panic("lifecycle: Add with an empty name")
	}
	g.mu.Lock()
	defer g.mu.Unlock()
	if g.started {
		panic("lifecycle: Add after Start")
	}
	g.members = append(g.members, member{name: name, c: c})
}

// Start starts every component in Add order. When one fails, the ones already
// started are stopped in reverse and the error names the failing component.
// onFatal (may be nil) receives the first failure a component reports later,
// wrapped in ErrComponentFailed; later ones, and any reported once Stop has
// begun, are dropped. The group's lock is not held while a component starts,
// so an onFatal may call Stop synchronously: Start then stops the component
// that had just started and returns ErrStoppedDuringStart.
func (g *Group) Start(ctx context.Context, onFatal func(error)) error {
	g.mu.Lock()
	if g.started {
		g.mu.Unlock()
		return errors.New("lifecycle: already started")
	}
	g.started, g.running = true, 0
	g.stopping.Store(false)
	members := slices.Clone(g.members)
	g.mu.Unlock()

	var once sync.Once
	report := func(name string) func(error) {
		// Never takes g.mu: a component may report from inside its own Start
		// or Stop, while the group holds the lock.
		return func(err error) {
			if g.stopping.Load() {
				return
			}
			once.Do(func() {
				if onFatal != nil {
					onFatal(fmt.Errorf("%w: %s: %w", ErrComponentFailed, name, err))
				}
			})
		}
	}

	for _, m := range members {
		if err := m.c.Start(ctx, report(m.name)); err != nil {
			return g.abortStart(ctx, m.name, err)
		}
		g.mu.Lock()
		if !g.started {
			// Stop ran while m was starting: it is not in running, so stop it here.
			g.mu.Unlock()
			return g.stopUnaccounted(ctx, m)
		}
		g.running++
		g.mu.Unlock()
		g.logger.DebugContext(ctx, "component started", slog.String(logfields.KeyComponentName, m.name))
	}
	return nil
}

// abortStart stops what Start already brought up. It gets its own deadline:
// ctx may be the very thing that just expired.
func (g *Group) abortStart(ctx context.Context, name string, err error) error {
	g.mu.Lock()
	defer g.mu.Unlock()
	stopCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), g.shutdownTimeout)
	defer cancel()
	return errors.Join(fmt.Errorf("lifecycle: start %s: %w", name, err), g.stopLocked(stopCtx))
}

// stopUnaccounted stops a component that finished starting after Stop had
// already walked the group back.
func (g *Group) stopUnaccounted(ctx context.Context, m member) error {
	stopCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), g.shutdownTimeout)
	defer cancel()
	err := fmt.Errorf("lifecycle: start %s: %w", m.name, ErrStoppedDuringStart)
	if stopErr := m.c.Stop(stopCtx); stopErr != nil {
		return errors.Join(err, fmt.Errorf("lifecycle: stop %s: %w", m.name, stopErr))
	}
	return err
}

// Stop stops the started components in reverse order with ctx and joins
// their errors. Every component is called even after ctx expires, so each
// can release what it holds. Stop without Start returns nil.
func (g *Group) Stop(ctx context.Context) error {
	g.mu.Lock()
	defer g.mu.Unlock()
	if !g.started {
		return nil
	}
	return g.stopLocked(ctx)
}

func (g *Group) stopLocked(ctx context.Context) error {
	g.stopping.Store(true)
	var errs []error
	for i := g.running - 1; i >= 0; i-- {
		m := g.members[i]
		if err := m.c.Stop(ctx); err != nil {
			errs = append(errs, fmt.Errorf("lifecycle: stop %s: %w", m.name, err))
			continue
		}
		g.logger.DebugContext(ctx, "component stopped", slog.String(logfields.KeyComponentName, m.name))
	}
	g.started, g.running = false, 0
	return errors.Join(errs...)
}

// Run starts the group, waits for SIGINT, SIGTERM, ctx's end or the first
// component failure, then stops everything within the shutdown timeout. It
// returns nil on a clean shutdown, including a shutdown requested while the
// group was still starting; a component failure is reported with
// ErrComponentFailed, joined with any start or stop error.
func (g *Group) Run(ctx context.Context) error {
	sigCtx, stop := signal.NotifyContext(ctx, os.Interrupt, syscall.SIGTERM)
	defer stop()
	// stop runs again once the wait ends: a second Ctrl-C during a stuck drain
	// then kills the process instead of being swallowed.
	return g.run(sigCtx, stop)
}

// run is Run without the signal wiring, so tests drive it with a plain ctx.
// Components start with ctx; a component failure ends only the wait, never the
// start context, or every later Start would see a cancelled context.
func (g *Group) run(ctx context.Context, afterWait func()) error {
	waitCtx, endWait := context.WithCancel(ctx)
	defer endWait()

	var fatal atomic.Pointer[error]
	startErr := g.Start(ctx, func(err error) {
		fatal.CompareAndSwap(nil, &err)
		endWait()
	})
	switch f := fatal.Load(); {
	case startErr != nil && f != nil:
		return errors.Join(*f, startErr)
	case startErr != nil && ctx.Err() != nil:
		return nil // shutdown requested during startup; Start stopped what it began
	case startErr != nil:
		return startErr
	}
	<-waitCtx.Done()
	afterWait()

	stopCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), g.shutdownTimeout)
	defer cancel()
	stopErr := g.Stop(stopCtx)
	if f := fatal.Load(); f != nil {
		return errors.Join(*f, stopErr)
	}
	return stopErr
}
