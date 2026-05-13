package fxmux

import (
	"context"
	"log/slog"
	"time"

	"github.com/norlis/event-driven/pkg/eventmux"
	"go.uber.org/fx"
)

const defaultStopTimeout = 30 * time.Second

// Bind hooks a Mux into the FX lifecycle.
// On a fatal mux error, it triggers fx.Shutdowner to restart the pod.
func Bind(lc fx.Lifecycle, mux *eventmux.Mux, logger *slog.Logger, shutdown fx.Shutdowner) {
	var stop func(time.Duration) error

	lc.Append(fx.Hook{
		OnStart: func(ctx context.Context) error {
			logger.Info("Starting Mux...", slog.String("name", mux.Name()))
			stop = mux.RunBackground(context.Background(), func(err error) {
				logger.Error("Mux crashed, requesting app shutdown",
					slog.String("name", mux.Name()),
					slog.Any("error", err),
				)
				_ = shutdown.Shutdown(fx.ExitCode(1))
			})
			return nil
		},
		OnStop: func(ctx context.Context) error {
			logger.Info("Stopping Mux...", slog.String("name", mux.Name()))
			if stop != nil {
				return stop(defaultStopTimeout)
			}
			return nil
		},
	})
}
