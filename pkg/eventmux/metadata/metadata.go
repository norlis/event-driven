// Package metadata exposes a per-message string-keyed Store that handlers can
// use to attach values which the mux then propagates as CloudEvent
// extensions on the result event.
package metadata

import (
	"context"
	"maps"
	"sync"
)

// Store is a thread-safe string→string container for per-message metadata.
// Every key becomes a CloudEvents extension of the result event and, on
// SNS/SQS, one of the 10 MessageAttributes allowed per message; after the
// fixed attributes only 1–2 keys fit (see sns.MaxMessageAttributes). Reserved
// keys the mux owns and ignores here with a WARN: "correlationid",
// "deadletterreason" and "deadlettererror".
type Store struct {
	mu   sync.RWMutex
	data map[string]string
}

// NewStore returns an empty Store ready for concurrent use.
func NewStore() *Store {
	return &Store{data: make(map[string]string)}
}

// Set inserts or overwrites a metadata entry.
func (b *Store) Set(key, value string) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.data[key] = value
}

// All returns a snapshot copy of every entry.
func (b *Store) All() map[string]string {
	b.mu.RLock()
	defer b.mu.RUnlock()
	out := make(map[string]string, len(b.data))
	maps.Copy(out, b.data)
	return out
}

type contextKey struct{}

var key = contextKey{}

// NewContext returns a child context carrying the given Store, retrievable
// via FromContext.
func NewContext(ctx context.Context, b *Store) context.Context {
	return context.WithValue(ctx, key, b)
}

// FromContext returns the Store carried by ctx (if any).
func FromContext(ctx context.Context) (*Store, bool) {
	b, ok := ctx.Value(key).(*Store)
	return b, ok
}
