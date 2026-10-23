package eventmux

import "slices"

// Middleware wraps a HandlerFunc to add behaviour around it.
type Middleware func(next HandlerFunc) HandlerFunc

// Chain wraps handler with mws so that the first middleware is the outermost:
// Chain(h, A, B) returns A(B(h)), and a call runs A, then B, then h.
func Chain(handler HandlerFunc, mws ...Middleware) HandlerFunc {
	chained := handler
	for _, mw := range slices.Backward(mws) {
		chained = mw(chained)
	}
	return chained
}
