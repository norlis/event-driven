package recover

import (
	"context"
	"encoding/json"
	"fmt"
	"runtime/debug"

	"github.com/norlis/event-driven/pkg/eventmux"
)

// PanicError holds the recovered panic's error along with the stacktrace.
type PanicError struct {
	V          any
	Stacktrace string
}

func (p PanicError) Error() string {
	return fmt.Sprintf("panic occurred: %#v, stacktrace: \n%s", p.V, p.Stacktrace)
}

// Middleware recovers from any panic in the handler and appends PanicError with the stacktrace
// to any error returned from the handler.
func Middleware(h eventmux.HandlerFunc) eventmux.HandlerFunc {
	return func(ctx context.Context, event any) (result json.RawMessage, err error) {
		panicked := true

		defer func() {
			if r := recover(); r != nil || panicked {
				err = PanicError{V: r, Stacktrace: string(debug.Stack())}
			}
		}()

		result, err = h(ctx, event)
		panicked = false
		return result, err
	}
}
