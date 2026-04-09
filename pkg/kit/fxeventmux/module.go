package fxeventmux

import (
	"context"
	"time"

	"github.com/norlis/event-driven/pkg/application/router"
	"go.uber.org/fx"
	"go.uber.org/zap"
)

const defaultStopTimeout = 30 * time.Second

// BindLifecycle hooks an EventMux into the FX lifecycle.
// On fatal mux error, it triggers fx.Shutdowner to restart the pod.
func BindLifecycle(lc fx.Lifecycle, mux *router.EventMux, logger *zap.Logger, shutdowner fx.Shutdowner) {
	var stop func(time.Duration) error

	lc.Append(fx.Hook{
		OnStart: func(ctx context.Context) error {
			logger.Info("Starting EventMux...", zap.String("name", mux.Name()))
			stop = mux.RunBackground(context.Background(), func(err error) {
				logger.Error("EventMux crashed, requesting app shutdown",
					zap.String("name", mux.Name()),
					zap.Error(err),
				)
				_ = shutdowner.Shutdown(fx.ExitCode(1))
			})
			return nil
		},
		OnStop: func(ctx context.Context) error {
			logger.Info("Stopping EventMux...", zap.String("name", mux.Name()))
			if stop != nil {
				return stop(defaultStopTimeout)
			}
			return nil
		},
	})
}
