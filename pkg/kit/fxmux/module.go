package fxmux

import (
	"context"
	"log/slog"
	"time"

	"go.uber.org/fx"

	"github.com/norlis/event-driven/pkg/eventmux"
)

const defaultStopTimeout = 30 * time.Second

// Bind hooks a Mux into the FX lifecycle.
// On a fatal mux error, it triggers fx.Shutdowner to restart the pod. It does
// not log the mux lifecycle — the Mux logs its own start/stop/failure — so the
// logger parameter is kept only for API stability of consumers' wiring.
func Bind(lc fx.Lifecycle, mux *eventmux.Mux, logger *slog.Logger, shutdown fx.Shutdowner) {
	_ = logger
	var stop func(time.Duration) error

	lc.Append(fx.Hook{
		OnStart: func(ctx context.Context) error {
			stop = mux.RunBackground(context.Background(), func(err error) {
				// The mux already logged "mux run failed"; logging here again
				// would duplicate the same error (§2.5 rule 1).
				_ = shutdown.Shutdown(fx.ExitCode(1))
			})
			return nil
		},
		OnStop: func(ctx context.Context) error {
			if stop != nil {
				return stop(defaultStopTimeout)
			}
			return nil
		},
	})
}
