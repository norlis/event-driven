package metadata

import (
	"context"
	"sync"
)

// Store es un contenedor seguro para metadatos.
type Store struct {
	mu   sync.RWMutex
	data map[string]string
}

func NewStore() *Store {
	return &Store{data: make(map[string]string)}
}

func (b *Store) Set(key, value string) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.data[key] = value
}

func (b *Store) All() map[string]string {
	b.mu.RLock()
	defer b.mu.RUnlock()
	out := make(map[string]string, len(b.data))
	for k, v := range b.data {
		out[k] = v
	}
	return out
}

type contextKey struct{}

var key = contextKey{}

func NewContext(ctx context.Context, b *Store) context.Context {
	return context.WithValue(ctx, key, b)
}

func FromContext(ctx context.Context) (*Store, bool) {
	b, ok := ctx.Value(key).(*Store)
	return b, ok
}
