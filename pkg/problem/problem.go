// Package problem - RFC 7807 (Problem Details for HTTP APIs).
// https://tools.ietf.org/html/rfc7807
package problem

import (
	"encoding/json"
	"net/http"
	"time"
)

// ProblemDetail define la estructura estándar para un error HTTP, según el RFC 7807.
// El struct satisface la interfaz `error` de Go, por lo que puede ser usado
// en flujos de manejo de errores estándar.
type ProblemDetail struct {
	// Type es un URI que identifica el tipo de problema.
	// Se recomienda que provea documentación legible para los desarrolladores.
	Type string `json:"type,omitempty"`

	// Title es un resumen corto y legible del tipo de problema.
	// No debería cambiar entre distintas ocurrencias del mismo problema.
	Title string `json:"title"`

	// Status es el código de estado HTTP reflejado para esta ocurrencia del problema.
	Status int `json:"status"`

	// Detail es una explicación legible y específica de esta ocurrencia del problema.
	Detail string `json:"detail,omitempty"`

	// Instance es un URI que identifica la ocurrencia específica del problema.
	Instance string `json:"instance,omitempty"`

	Extension
}

type Extension struct {
	RequestId  string    `json:"requestId,omitempty"`
	Timestamp  time.Time `json:"timestamp"`
	StackTrace string    `json:"stackTrace,omitempty"`
}

// Error interface `error`.
func (p *ProblemDetail) Error() string {
	return p.Title
}

// Option es una función que configura un ProblemDetail.
type Option func(*ProblemDetail)

// New crea un nuevo ProblemDetail. Los campos `title` y `status` son requeridos.
// Opciones adicionales pueden ser pasadas para configurar los campos opcionales.
func New(title string, status int, opts ...Option) *ProblemDetail {
	p := &ProblemDetail{
		Title:  title,
		Status: status,
		Extension: Extension{
			Timestamp: time.Now().UTC(),
		},
	}

	for _, opt := range opts {
		opt(p)
	}

	return p
}

// FromError es una función de ayuda para crear un ProblemDetail a partir de un error nativo de Go.
// Es útil para envolver errores de capas inferiores (ej: base de datos, servicios externos)
// en un problema HTTP estándar.
func FromError(err error, status int, opts ...Option) *ProblemDetail {
	// Usa el texto del status HTTP como título por defecto.
	// El detalle se toma del mensaje del error original.
	p := New(http.StatusText(status), status, opts...)
	p.Detail = err.Error()
	return p
}

// --- Opciones Funcionales ---

// WithType asigna el URI que identifica el tipo de problema.
func WithType(uri string) Option {
	return func(p *ProblemDetail) {
		p.Type = uri
	}
}

// WithDetail asigna una explicación específica de la ocurrencia del problema.
func WithDetail(detail string) Option {
	return func(p *ProblemDetail) {
		p.Detail = detail
	}
}

// WithInstance asigna el URI de la petición actual como la instancia del problema.
func WithInstance(r *http.Request) Option {
	return func(p *ProblemDetail) {
		if r != nil {
			p.RequestId = r.Header.Get("X-Request-Id")
			p.Instance = r.URL.Path
		}
	}
}

// --- Ayudante de Respuesta HTTP ---

// RespondError serializa un ProblemDetail a JSON y lo escribe en el http.ResponseWriter.
// Centraliza la lógica de respuesta de errores, asegurando consistencia.
func RespondError(w http.ResponseWriter, p *ProblemDetail) {
	w.Header().Set("Content-Type", "application/problem+json; charset=utf-8")
	w.WriteHeader(p.Status)
	_ = json.NewEncoder(w).Encode(p)
}
