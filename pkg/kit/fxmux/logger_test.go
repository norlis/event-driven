package fxmux

import (
	"bytes"
	"errors"
	"log/slog"
	"strings"
	"testing"

	"go.uber.org/fx/fxevent"
)

// fx reports one failed OnStart hook through four events; the slog adapter
// must turn them into a single ERROR record, not three for the same error.
func TestLogEventStartFailureIsOneRecord(t *testing.T) {
	t.Parallel()

	var logs bytes.Buffer
	l := NewLogger(slog.New(slog.NewJSONHandler(&logs, nil)))
	boom := errors.New("fxmux: lifecycle: start m: boom")
	for _, e := range []fxevent.Event{
		&fxevent.OnStartExecuted{FunctionName: "fxmux.BindGroup.func1", Err: boom},
		&fxevent.RollingBack{StartErr: boom},
		&fxevent.RolledBack{},
		&fxevent.Started{Err: boom},
	} {
		l.LogEvent(e)
	}
	if n := strings.Count(logs.String(), `"level":"ERROR"`); n != 1 {
		t.Fatalf("ERROR records = %d, want 1:\n%s", n, logs.String())
	}
	if !strings.Contains(logs.String(), `"msg":"app start failed"`) || !strings.Contains(logs.String(), `"error.message":"fxmux: lifecycle: start m: boom"`) {
		t.Fatalf("unexpected record:\n%s", logs.String())
	}
}

func TestLogEventStopFailureIsOneRecord(t *testing.T) {
	t.Parallel()

	var logs bytes.Buffer
	l := NewLogger(slog.New(slog.NewJSONHandler(&logs, nil)))
	boom := errors.New("drain timed out")
	l.LogEvent(&fxevent.OnStopExecuted{FunctionName: "fxmux.BindGroup.func2", Err: boom})
	l.LogEvent(&fxevent.Stopped{Err: boom})
	if n := strings.Count(logs.String(), `"level":"ERROR"`); n != 1 {
		t.Fatalf("ERROR records = %d, want 1:\n%s", n, logs.String())
	}
	if !strings.Contains(logs.String(), `"msg":"app stop failed"`) {
		t.Fatalf("unexpected record:\n%s", logs.String())
	}
}

// A rollback that itself fails is a distinct error and keeps its record.
func TestLogEventRollbackFailureIsLogged(t *testing.T) {
	t.Parallel()

	var logs bytes.Buffer
	l := NewLogger(slog.New(slog.NewJSONHandler(&logs, nil)))
	l.LogEvent(&fxevent.RolledBack{Err: errors.New("close hung")})
	if !strings.Contains(logs.String(), `"msg":"rollback failed"`) {
		t.Fatalf("unexpected record:\n%s", logs.String())
	}
}
