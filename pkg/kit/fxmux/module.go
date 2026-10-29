package fxmux

import (
	"context"
	"fmt"
	"log/slog"

	"go.uber.org/fx"

	"github.com/norlis/httpgate/logging"

	"github.com/norlis/event-driven/v2/pkg/kit/lifecycle"
)

// BindGroup runs g inside the fx lifecycle with a single hook, so the group's
// Add order, not the order fx built each part, decides how the service starts
// and stops. OnStart is g.Start (a failure aborts fx startup); OnStop is
// g.Stop with fx's stop context, so fx.StopTimeout governs the drain. A
// component failing after startup is logged as "component failed" and asks fx
// to shut down with exit code 1. logger (nil discards) also records a failed
// shutdown request.
func BindGroup(lc fx.Lifecycle, sd fx.Shutdowner, g *lifecycle.Group, logger *slog.Logger) {
	if logger == nil {
		logger = slog.New(slog.DiscardHandler)
	}
	lc.Append(fx.Hook{
		OnStart: func(ctx context.Context) error {
			err := g.Start(ctx, func(err error) {
				logger.Error("component failed", logging.Err(err))
				if serr := sd.Shutdown(fx.ExitCode(1)); serr != nil {
					logger.Error("shutdown request failed", logging.Err(serr))
				}
			})
			if err != nil {
				return fmt.Errorf("fxmux: %w", err)
			}
			return nil
		},
		OnStop: func(ctx context.Context) error {
			if err := g.Stop(ctx); err != nil {
				return fmt.Errorf("fxmux: %w", err)
			}
			return nil
		},
	})
}
