package event

import (
	"iter"

	cloudevents "github.com/cloudevents/sdk-go/v2/event"
)

// extensions returns the event's extension map without copying it. The SDK's
// Event.Extensions() allocates a copy on every call; the router reads
// extensions several times per message, so the hot path goes through here.
// Callers must not mutate the returned map. A context that is not V1 (the only
// version the library produces) falls back to the SDK copy.
func extensions(ce cloudevents.Event) map[string]any {
	if v1, ok := ce.Context.(*cloudevents.EventContextV1); ok {
		return v1.Extensions
	}
	return ce.Extensions()
}

// extension returns one extension value, or nil when absent.
func extension(ce cloudevents.Event, name string) any {
	return extensions(ce)[name]
}

// Extensions iterates the event's extensions without copying them. Values are
// the SDK's canonical types (string, int32, bool, …). Do not modify ce while
// iterating.
func Extensions(ce cloudevents.Event) iter.Seq2[string, any] {
	return func(yield func(string, any) bool) {
		for k, v := range extensions(ce) {
			if !yield(k, v) {
				return
			}
		}
	}
}
