package logger

import (
	"testing"

	"go.uber.org/zap"
)

func TestNew(t *testing.T) {
	t.Parallel()

	t.Run("Debug_true", func(t *testing.T) {
		t.Parallel()
		l, err := New(true)
		if err != nil {
			t.Fatalf("New(true) retornó error: %v", err)
		}
		if l == nil {
			t.Fatal("New(true) retornó un logger nil")
		}
		// Verificar si el nivel es Debug (esto es un poco intrusivo en Zap)
		if !l.Core().Enabled(zap.DebugLevel) {
			t.Error("Se esperaba que el logger estuviera en DebugLevel")
		}
	})

	t.Run("Debug_false", func(t *testing.T) {
		t.Parallel()
		l, err := New(false)
		if err != nil {
			t.Fatalf("New(false) retornó error: %v", err)
		}
		if l == nil {
			t.Fatal("New(false) retornó un logger nil")
		}
		// Verificar si el nivel es Info (ProductionConfig por defecto es Info)
		if !l.Core().Enabled(zap.InfoLevel) {
			t.Error("Se esperaba que el logger estuviera en InfoLevel")
		}
		if l.Core().Enabled(zap.DebugLevel) {
			t.Error("No se esperaba que el logger estuviera en DebugLevel cuando debug es false")
		}
	})
}
