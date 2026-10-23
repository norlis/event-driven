package example

import "testing"

// A nil *pubsub.Publisher must reach the routes as an untyped nil: wrapped in
// the interface it would be non-nil and the mux would call Publish on it.
func TestResultPublisherNil(t *testing.T) {
	t.Parallel()
	if got := NewResultPublisher(nil); got != nil {
		t.Fatalf("NewResultPublisher(nil) = %#v, want untyped nil", got)
	}
}
