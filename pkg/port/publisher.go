package port

import cloudevents "github.com/cloudevents/sdk-go/v2/event"

type Publisher interface {
	Publish(cloudevents.Event) error
}
