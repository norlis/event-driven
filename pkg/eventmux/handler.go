package eventmux

import (
	"context"
	"encoding/json/jsontext"
)

// HandlerFunc is the type-erased handler the middleware chain sees. data is
// the payload Handle decoded into the route's T; the optional jsontext.Value
// becomes the body of the result event when the route has a Publisher.
type HandlerFunc func(ctx context.Context, data any) (jsontext.Value, error)
