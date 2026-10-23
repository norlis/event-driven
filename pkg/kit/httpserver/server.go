// Package httpserver runs an http.Server as a start/stop component: the bind
// happens in Start, so a busy port fails startup instead of surfacing after
// the service reported itself up.
package httpserver

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"sync"

	"github.com/norlis/httpgate/logging"
	"github.com/norlis/httpgate/server"

	"github.com/norlis/event-driven/v2/pkg/kit/logfields"
)

// Server is an http.Server with httpgate's hardened defaults and a
// Start/Stop lifecycle. It satisfies lifecycle.Component.
type Server struct {
	srv    *http.Server
	logger *slog.Logger

	mu      sync.Mutex
	addr    string // real address after Start, ":0" resolved
	started bool
	stopped bool // an http.Server cannot serve again after Shutdown
}

// New builds the server with httpgate/server.New (anti-Slowloris timeouts,
// bounded headers, ":8080" unless server.WithAddr says otherwise). A nil
// handler panics: http.Server would fall back to http.DefaultServeMux and
// expose whatever is registered there, such as net/http/pprof.
func New(handler http.Handler, logger *slog.Logger, opts ...server.Option) *Server {
	if handler == nil {
		panic("httpserver: nil handler")
	}
	if logger == nil {
		logger = slog.New(slog.DiscardHandler)
	}
	return &Server{srv: server.New(handler, opts...), logger: logger}
}

// Start binds the listen address and serves in the background. onFatal (may
// be nil) receives a serve failure after Start returned.
func (s *Server) Start(ctx context.Context, onFatal func(error)) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	switch {
	case s.started:
		return errors.New("httpserver: already started")
	case s.stopped:
		return errors.New("httpserver: cannot restart after Stop")
	}
	var lc net.ListenConfig
	ln, err := lc.Listen(ctx, "tcp", s.srv.Addr)
	if err != nil {
		return fmt.Errorf("httpserver: listen %s: %w", s.srv.Addr, err)
	}
	s.addr, s.started = ln.Addr().String(), true
	s.logger.InfoContext(ctx, "http server started", slog.String(logfields.KeyServerAddress, s.addr))
	go serve(s.srv, ln, onFatal, s.logger)
	return nil
}

// serve runs srv on ln until it is shut down. Any other end is a failure,
// logged once here and reported through onFatal.
func serve(srv *http.Server, ln net.Listener, onFatal func(error), logger *slog.Logger) {
	err := srv.Serve(ln)
	if err == nil || errors.Is(err, http.ErrServerClosed) {
		return
	}
	logger.Error("http server failed", logging.Err(err))
	if onFatal != nil {
		onFatal(err)
	}
}

// Stop shuts the server down, waiting for in-flight requests until ctx
// expires. Stop before Start, or a second Stop, returns nil. A stopped Server
// cannot be started again.
func (s *Server) Stop(ctx context.Context) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if !s.started {
		return nil
	}
	s.started, s.stopped = false, true
	if err := s.srv.Shutdown(ctx); err != nil {
		return fmt.Errorf("httpserver: shutdown: %w", err)
	}
	return nil
}
