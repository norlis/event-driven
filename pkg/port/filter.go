package port

import "github.com/norlis/event-driven/pkg/domain/event"

type Filter interface {
	Match(msg *event.Message) bool
}
