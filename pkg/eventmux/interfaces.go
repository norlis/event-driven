package eventmux

import (
	"context"

	cloudevents "github.com/cloudevents/sdk-go/v2/event"
	"github.com/norlis/event-driven/pkg/event"
)

type Filter interface {
	Match(msg *event.Message) bool
}

type Publisher interface {
	Publish(cloudevents.Event) error
}

type Subscription interface {
	Start(ctx context.Context, handler func(*event.Message)) error
}
