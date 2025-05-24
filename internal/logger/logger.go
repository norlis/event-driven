package logger

import (
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

// New crea una nueva instancia de zap.Logger para producción.
// La aplicación que usa la librería puede optar por crear su propia configuración de Zap.
func New(debug bool) (*zap.Logger, error) {
	config := zap.NewProductionConfig()
	if debug {
		config.Level = zap.NewAtomicLevelAt(zap.DebugLevel)
	}
	config.EncoderConfig.EncodeTime = zapcore.ISO8601TimeEncoder
	config.EncoderConfig.TimeKey = "timestamp"
	config.DisableStacktrace = true // Deshabilitar para no ser tan verboso, habilitar si se necesita para debug.
	return config.Build()
}
