package port

import (
	"context"

	"github.com/norlis/event-driven/pkg/domain/event"
)

type Subscription interface {
	Start(ctx context.Context, handler func(*event.Message)) error
}
