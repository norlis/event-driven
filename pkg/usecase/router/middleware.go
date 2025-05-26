package router

// Middleware es una función que envuelve un HandlerFunc para añadir comportamiento.
// TODO: add context -> type Middleware func(ctx context.Context, next HandlerFunc) HandlerFunc
type Middleware func(next HandlerFunc) HandlerFunc

func ChainMiddlewares(handler HandlerFunc, mws ...Middleware) HandlerFunc {
	chainedHandler := handler
	for i := len(mws) - 1; i >= 0; i-- {
		chainedHandler = mws[i](chainedHandler)
	}
	return chainedHandler
}
