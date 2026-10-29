// Package lifecycle starts and stops a service's long-running parts as one
// unit: muxes, the HTTP server and the resources they share.
//
// Components are added in dependency order (resources such as clients,
// publishers and connections first, then muxes, the HTTP server last) and
// stopped in reverse, so the server stops accepting requests, the muxes
// drain, and only then are the resources they publish through closed.
package lifecycle

import (
	"context"
	"fmt"
	"sync"
)

// Component is anything with a start/stop lifecycle. onFatal reports a
// failure that ends the component for good after Start returned, and whoever
// receives it logs it; with a nil onFatal the component logs it itself, so a
// failure is reported exactly once. *eventmux.Mux satisfies Component as is (the package does not
// import it: the test file asserts it).
type Component interface {
	Start(ctx context.Context, onFatal func(error)) error
	Stop(ctx context.Context) error
}

var _ Component = (*Group)(nil)

// Closer adapts a resource that only needs closing, such as a Pub/Sub client
// or publisher, to Component. fn runs once.
func Closer(fn func() error) Component {
	return &closer{fn: fn}
}

type closer struct {
	fn   func() error
	once sync.Once
}

func (*closer) Start(context.Context, func(error)) error { return nil }

// Stop runs fn in a goroutine because fn takes no context: a Close that hangs
// must not hold the shutdown past its deadline. The goroutine keeps running
// until fn returns; nothing can interrupt it.
func (c *closer) Stop(ctx context.Context) error {
	var run bool
	c.once.Do(func() { run = true })
	if !run {
		return nil
	}
	done := make(chan error, 1)
	go func() { done <- c.fn() }()
	select {
	case err := <-done:
		return err
	case <-ctx.Done():
		return fmt.Errorf("lifecycle: close: %w", ctx.Err())
	}
}
