// Package health proporciona una implementación para sondas de salud HTTP (Liveness y Readiness).
package health

import (
	"encoding/json"
	"net/http"
	"sync"
)

// Checker define la interfaz para una comprobación de salud individual.
// Cualquier componente que necesite ser verificado (BD, API externa, etc.)
// debe implementar esta interfaz.
type Checker interface {
	Check() error
}

// Probe es un manejador HTTP que ejecuta un conjunto de comprobaciones de salud.
type Probe struct {
	// Usamos un mutex para proteger el acceso concurrente al mapa de checkers,
	// aunque en la práctica se suele configurar una vez al inicio.
	mu       sync.RWMutex
	checkers map[string]Checker
}

// NewProbe crea y devuelve un nuevo Probe.
// Puede ser inicializado sin checkers, lo cual es útil para una sonda de Liveness.
func NewProbe(checkers map[string]Checker) *Probe {
	if checkers == nil {
		checkers = make(map[string]Checker)
	}
	return &Probe{
		checkers: checkers,
	}
}

// ServeHTTP implementa la interfaz http.Handler.
// Ejecuta todas las comprobaciones registradas y devuelve un resultado consolidado.
func (p *Probe) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	p.mu.RLock()
	defer p.mu.RUnlock()

	// Mapa para almacenar el resultado de cada comprobación.
	results := make(map[string]string, len(p.checkers))
	httpStatus := http.StatusOK

	// Si no hay checkers, es un simple OK. Ideal para Liveness.
	if len(p.checkers) == 0 {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("OK"))
		return
	}

	// Ejecutar cada comprobación.
	for name, checker := range p.checkers {
		if err := checker.Check(); err != nil {
			// Si una sola comprobación falla, el estado general es de fallo.
			httpStatus = http.StatusServiceUnavailable
			results[name] = err.Error()
		} else {
			results[name] = "OK"
		}
	}

	// Escribir la respuesta JSON.
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(httpStatus)
	_ = json.NewEncoder(w).Encode(results)
}
