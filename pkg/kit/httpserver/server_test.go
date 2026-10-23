package httpserver

import (
	"bytes"
	"context"
	"errors"
	"log/slog"
	"net"
	"net/http"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/norlis/httpgate/server"

	"github.com/norlis/event-driven/v2/pkg/kit/lifecycle"
)

var (
	_       lifecycle.Component = (*Server)(nil)
	errBoom                     = errors.New("boom")
)

func localServer(h http.Handler) *Server {
	return New(h, nil, server.WithAddr("127.0.0.1:0"))
}

func TestStartServes(t *testing.T) {
	t.Parallel()
	s := localServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusNoContent) }))
	if err := s.Start(t.Context(), nil); err != nil {
		t.Fatalf("Start() error = %v", err)
	}
	t.Cleanup(func() { _ = s.Stop(t.Context()) })

	if status := get(t, s.addr); status != http.StatusNoContent {
		t.Errorf("status = %d, want 204", status)
	}
}

func TestStartBindError(t *testing.T) {
	t.Parallel()
	ln, err := new(net.ListenConfig).Listen(t.Context(), "tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = ln.Close() })

	var fatal atomic.Bool
	s := New(http.NotFoundHandler(), nil, server.WithAddr(ln.Addr().String()))
	if err := s.Start(t.Context(), func(error) { fatal.Store(true) }); err == nil {
		t.Fatal("Start() on a busy port error = nil")
	}
	if fatal.Load() {
		t.Error("onFatal called for a bind error")
	}
}

func TestStopDrainsInFlight(t *testing.T) {
	t.Parallel()
	entered, release := make(chan struct{}), make(chan struct{})
	s := localServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		close(entered)
		<-release
		w.WriteHeader(http.StatusOK)
	}))
	if err := s.Start(t.Context(), nil); err != nil {
		t.Fatal(err)
	}

	var wg sync.WaitGroup
	var status int
	wg.Go(func() { status = get(t, s.addr) })
	<-entered

	stopErr := make(chan error, 1)
	wg.Go(func() {
		ctx, cancel := context2s(t)
		defer cancel()
		stopErr <- s.Stop(ctx)
	})
	close(release)
	wg.Wait()
	if status != http.StatusOK {
		t.Errorf("in-flight status = %d, want 200", status)
	}
	if err := <-stopErr; err != nil {
		t.Errorf("Stop() error = %v", err)
	}
}

// failingListener accepts nothing: Accept fails at once, as a dead socket does.
type failingListener struct{}

func (failingListener) Accept() (net.Conn, error) { return nil, errBoom }
func (failingListener) Close() error              { return nil }
func (failingListener) Addr() net.Addr            { return &net.TCPAddr{} }

func TestServeErrorIsFatal(t *testing.T) {
	t.Parallel()
	var buf bytes.Buffer
	logger := slog.New(slog.NewJSONHandler(&buf, nil))
	var calls atomic.Int32
	var got error
	serve(&http.Server{ReadHeaderTimeout: time.Second}, failingListener{}, func(err error) {
		calls.Add(1)
		got = err
	}, logger)
	if n := calls.Load(); n != 1 {
		t.Fatalf("onFatal called %d times, want 1", n)
	}
	if !errors.Is(got, errBoom) {
		t.Errorf("onFatal error = %v, want boom", got)
	}
	if n := strings.Count(buf.String(), `"msg":"http server failed"`); n != 1 {
		t.Errorf("logged %d failure records, want 1: %s", n, buf.String())
	}
}

func TestStopWithoutStart(t *testing.T) {
	t.Parallel()
	if err := localServer(http.NotFoundHandler()).Stop(t.Context()); err != nil {
		t.Fatalf("Stop() error = %v", err)
	}
}

func TestStopTwice(t *testing.T) {
	t.Parallel()
	s := localServer(http.NotFoundHandler())
	if err := s.Start(t.Context(), nil); err != nil {
		t.Fatal(err)
	}
	if err := s.Stop(t.Context()); err != nil {
		t.Fatal(err)
	}
	if err := s.Stop(t.Context()); err != nil {
		t.Fatalf("second Stop() error = %v", err)
	}
}

func TestNewNilHandlerPanics(t *testing.T) {
	t.Parallel()
	defer func() {
		if r, ok := recover().(string); !ok || !strings.Contains(r, "httpserver") {
			t.Errorf("recover() = %v, want an httpserver panic", r)
		}
	}()
	New(nil, nil)
}

func context2s(t *testing.T) (context.Context, context.CancelFunc) {
	t.Helper()
	return context.WithTimeout(t.Context(), 2*time.Second)
}

// get returns the status of GET http://addr, or 0 after reporting the error.
func get(t *testing.T, addr string) int {
	t.Helper()
	req, err := http.NewRequestWithContext(t.Context(), http.MethodGet, "http://"+addr, http.NoBody)
	if err != nil {
		t.Error(err)
		return 0
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Errorf("request error = %v", err)
		return 0
	}
	_ = resp.Body.Close()
	return resp.StatusCode
}

func TestStartAfterStopFails(t *testing.T) {
	t.Parallel()
	s := localServer(http.NotFoundHandler())
	if err := s.Start(t.Context(), nil); err != nil {
		t.Fatal(err)
	}
	if err := s.Stop(t.Context()); err != nil {
		t.Fatal(err)
	}
	if err := s.Start(t.Context(), nil); err == nil {
		t.Fatal("Start() after Stop error = nil; an http.Server cannot serve again after Shutdown")
	}
}
