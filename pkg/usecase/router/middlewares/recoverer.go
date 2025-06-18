package middlewares

import (
	"context"
	"encoding/json"
	"fmt"
	"runtime/debug"

	"github.com/norlis/event-driven/pkg/usecase/router"

	"github.com/pkg/errors"
)

// RecoveredPanicError holds the recovered panic's error along with the stacktrace.
type RecoveredPanicError struct {
	V          interface{}
	Stacktrace string
}

func (p RecoveredPanicError) Error() string {
	return fmt.Sprintf("panic occurred: %#v, stacktrace: \n%s", p.V, p.Stacktrace)
}

// Recoverer recovers from any panic in the handler and appends RecoveredPanicError with the stacktrace
// to any error returned from the handler.
func Recoverer(h router.HandlerFunc) router.HandlerFunc {
	return func(ctx context.Context, event any) (result json.RawMessage, err error) {
		panicked := true

		defer func() {
			if r := recover(); r != nil || panicked {
				err = errors.WithStack(RecoveredPanicError{V: r, Stacktrace: string(debug.Stack())})
			}
		}()

		result, err = h(ctx, event)
		panicked = false
		return result, err
	}
}
