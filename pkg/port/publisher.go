package port

import "github.com/norlis/event-driven/pkg/domain/event"

type Publisher interface {
	Publish(*event.Message) error
}
