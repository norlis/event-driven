// Package signal provides signal-aware context helpers for graceful shutdown.
//
// Usage:
//
//	ctx, stop := signal.NotifyContext()
//	defer stop()
//	router.Run(ctx) // will cancel on SIGINT or SIGTERM
package signal

import (
	"context"
	"os"
	"os/signal"
	"syscall"
)

// NotifyContext returns a context that is cancelled when the process receives
// SIGINT or SIGTERM. Call the returned stop function to release resources.
func NotifyContext() (context.Context, context.CancelFunc) {
	return signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
}
