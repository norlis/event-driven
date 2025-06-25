package logger

import (
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

func New(debug bool) (*zap.Logger, error) {
	config := zap.NewProductionConfig()

	encoderConfig := zap.NewProductionEncoderConfig()

	encoderConfig.EncodeDuration = zapcore.StringDurationEncoder // Cambiamos el formato de la duración.
	encoderConfig.EncodeTime = zapcore.ISO8601TimeEncoder        // Formato de tiempo legible.
	//encoderConfig.EncodeLevel = zapcore.CapitalColorLevelEncoder // Niveles con colores.
	encoderConfig.TimeKey = "timestamp"

	if debug {
		config.Level = zap.NewAtomicLevelAt(zap.DebugLevel)
	}

	config.EncoderConfig = encoderConfig
	config.DisableStacktrace = true // Deshabilitar para no ser tan verboso, habilitar si se necesita para debug.
	return config.Build()
}
