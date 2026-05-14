package eventmux

// Middleware es una función que envuelve un HandlerFunc para añadir comportamiento.
type Middleware func(next HandlerFunc) HandlerFunc

// Chain aplica una cadena de middlewares a un handler.
// Los middlewares se aplican en orden inverso (el último añadido es el más externo).
func Chain(handler HandlerFunc, mws ...Middleware) HandlerFunc {
	chainedHandler := handler
	for i := len(mws) - 1; i >= 0; i-- {
		chainedHandler = mws[i](chainedHandler)
	}
	return chainedHandler
}
