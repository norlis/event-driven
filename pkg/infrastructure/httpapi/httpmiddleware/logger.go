package httpmiddleware

import (
	"go.uber.org/zap"
	"net/http"
	"time"
)

type responseWriter struct {
	http.ResponseWriter
	statusCode int
}

func (rw *responseWriter) Status() int {
	return rw.statusCode
}

func (rw *responseWriter) WriteHeader(code int) {
	rw.statusCode = code
	rw.ResponseWriter.WriteHeader(code)
}

type WrapResponseWriter interface {
	http.ResponseWriter
	Status() int
}

func NewWrapResponseWriter(w http.ResponseWriter, protoMajor int) WrapResponseWriter {
	return &responseWriter{w, protoMajor}
}

func RequestLogger(log *zap.Logger) func(next http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		fn := func(w http.ResponseWriter, r *http.Request) {
			ww := NewWrapResponseWriter(w, http.StatusOK)
			t1 := time.Now()
			defer func() {
				log.Named("middleware").
					Info("request",
						zap.Int("status", ww.Status()),
						zap.Duration("duration", time.Since(t1)),
						zap.String("uri", r.RequestURI),
						zap.String("remoteAddr", r.RemoteAddr),
					)
			}()

			next.ServeHTTP(ww, r)
		}
		return http.HandlerFunc(fn)
	}
}
