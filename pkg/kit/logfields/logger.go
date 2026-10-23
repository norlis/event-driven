package logfields

import "log/slog"

// Component returns logger tagged with log.logger=name. A nil logger becomes
// a discarding one so constructors stop checking for nil themselves.
func Component(logger *slog.Logger, name string) *slog.Logger {
	if logger == nil {
		logger = slog.New(slog.DiscardHandler)
	}
	return logger.With(slog.String(KeyLogLogger, name))
}

// Transport is Component plus the messaging.system and
// messaging.destination.name fields every transport record carries.
func Transport(logger *slog.Logger, name, system, destination string) *slog.Logger {
	return Component(logger, name).With(
		slog.String(KeyMessagingSystem, system),
		slog.String(KeyMessagingDestination, destination),
	)
}
