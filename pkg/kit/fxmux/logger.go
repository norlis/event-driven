// Package fxmux integrates eventmux.Mux with uber/fx: it hooks Run/Stop into
// the fx.Lifecycle and provides an fxevent.Logger that pipes FX events to slog.
package fxmux

import (
	"log/slog"
	"strings"

	"go.uber.org/fx/fxevent"

	"github.com/norlis/httpgate/logging"

	"github.com/norlis/event-driven/pkg/kit/logfields"
)

// NewLogger returns an fxevent.Logger that pipes FX lifecycle events to slog.
//
// Verbose events (Provided, Invoked, hook execution, BeforeRun, Run, etc.) are
// silenced. Errors land on Error level; high-level lifecycle transitions
// (Started, Stopping) land on Info.
//
// Wire it into fx.New with:
//
//	fx.WithLogger(fxmux.NewLogger),
func NewLogger(log *slog.Logger) fxevent.Logger {
	return &slogLogger{log: log.With(slog.String(logfields.KeyLogLogger, "fx"))}
}

type slogLogger struct {
	log *slog.Logger
}

// LogEvent is a pure dispatcher; its cyclomatic complexity tracks the number
// of fxevent types we care about, not branching logic.
//
//nolint:gocyclo,cyclop // dispatcher
func (l *slogLogger) LogEvent(event fxevent.Event) {
	switch e := event.(type) {
	case *fxevent.Started:
		if e.Err != nil {
			l.log.Error("app start failed", logging.Err(e.Err))
		} else {
			l.log.Info("app started")
		}
	case *fxevent.Stopping:
		if e.Signal != nil {
			l.log.Info("app stopping", slog.String(logfields.KeyProcessSignal, strings.ToUpper(e.Signal.String())))
		} else {
			l.log.Info("app stopping")
		}
	case *fxevent.Stopped:
		if e.Err != nil {
			l.log.Error("app stop failed", logging.Err(e.Err))
		}
	case *fxevent.RollingBack:
		l.log.Error("rolling back after start failure", logging.Err(e.StartErr))
	case *fxevent.RolledBack:
		if e.Err != nil {
			l.log.Error("rollback failed", logging.Err(e.Err))
		}
	case *fxevent.OnStartExecuted:
		if e.Err != nil {
			l.log.Error("onstart hook failed",
				slog.String(logfields.KeyCodeFunction, e.FunctionName),
				logging.Err(e.Err))
		}
	case *fxevent.OnStopExecuted:
		if e.Err != nil {
			l.log.Error("onstop hook failed",
				slog.String(logfields.KeyCodeFunction, e.FunctionName),
				logging.Err(e.Err))
		}
	case *fxevent.Provided:
		if e.Err != nil {
			l.log.Error("provide failed",
				slog.String(logfields.KeyCodeFunction, e.ConstructorName),
				logging.Err(e.Err))
		}
	case *fxevent.Invoked:
		if e.Err != nil {
			l.log.Error("invoke failed", slog.String(logfields.KeyCodeFunction, e.FunctionName), logging.Err(e.Err))
		}
	case *fxevent.LoggerInitialized:
		if e.Err != nil {
			l.log.Error("logger initialization failed", logging.Err(e.Err))
		}
	}
}
