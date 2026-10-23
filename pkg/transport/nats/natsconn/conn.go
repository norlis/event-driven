// Package natsconn opens a NATS connection that is also a lifecycle
// component: its Stop drains and waits until the connection is closed, so
// pending JetStream publishes are flushed before the process exits.
package natsconn

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"

	natsgo "github.com/nats-io/nats.go"

	"github.com/norlis/event-driven/v2/pkg/kit/logfields"
	"github.com/norlis/event-driven/v2/pkg/transport/internal/redact"
)

// Config configures Dial.
type Config struct {
	URL           string // one or more comma-separated server URLs; empty selects nats.DefaultURL
	Name          string // client name shown by the server
	MaxReconnects int    // 0 selects -1: reconnect forever
}

func (c Config) withDefaults() Config {
	if c.URL == "" {
		c.URL = natsgo.DefaultURL
	}
	if c.MaxReconnects == 0 {
		c.MaxReconnects = -1
	}
	return c
}

// Conn is a NATS connection with a Start/Stop lifecycle. It embeds
// *nats.Conn, so jetstream.New(conn.Conn) and every SDK method work as usual.
type Conn struct {
	*natsgo.Conn

	closed   chan struct{}
	stopOnce sync.Once
}

// Dial connects to NATS. Neither the log line nor the error carries the
// URL's credentials.
func Dial(cfg Config, logger *slog.Logger) (*Conn, error) {
	cfg = cfg.withDefaults()
	if logger == nil {
		logger = slog.New(slog.DiscardHandler)
	}
	safeURL := redact.URLs(cfg.URL)
	logger.Info("nats connecting", slog.String(logfields.KeyServerAddress, safeURL))

	c := &Conn{closed: make(chan struct{})}
	var closeOnce sync.Once
	nc, err := natsgo.Connect(cfg.URL,
		natsgo.Name(cfg.Name),
		natsgo.MaxReconnects(cfg.MaxReconnects),
		natsgo.ClosedHandler(func(*natsgo.Conn) { closeOnce.Do(func() { close(c.closed) }) }),
	)
	if err != nil {
		return nil, fmt.Errorf("natsconn: connect %q: %w", safeURL, err)
	}
	c.Conn = nc
	return c, nil
}

// Start is a no-op: Dial already connected.
func (*Conn) Start(context.Context, func(error)) error { return nil }

// Stop drains the connection and waits until it is closed or ctx expires.
// A second Stop returns nil.
func (c *Conn) Stop(ctx context.Context) error {
	err := error(nil)
	c.stopOnce.Do(func() { err = waitClosed(ctx, c.Drain, c.closed) })
	return err
}

// waitClosed starts the drain and waits for closed: nats.Conn.Drain returns
// before the drain finishes. A connection already closed by the server, or
// one nats.go closes itself because it was reconnecting, still ends closed,
// so neither is a stop failure.
func waitClosed(ctx context.Context, drain func() error, closed <-chan struct{}) error {
	err := drain()
	if err != nil && !errors.Is(err, natsgo.ErrConnectionClosed) && !errors.Is(err, natsgo.ErrConnectionReconnecting) {
		return fmt.Errorf("natsconn: drain: %w", err)
	}
	select {
	case <-closed:
		return nil
	case <-ctx.Done():
		return fmt.Errorf("natsconn: drain: %w", ctx.Err())
	}
}
