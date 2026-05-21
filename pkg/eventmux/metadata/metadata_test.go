package metadata

import (
	"context"
	"strconv"
	"sync"
	"testing"
)

func TestStore_GetReturnsEmptyForMissingKey(t *testing.T) {
	t.Parallel()

	s := NewStore()
	if got := s.Get("missing"); got != "" {
		t.Errorf("Get(missing) = %q, want \"\"", got)
	}
}

func TestStore_SetThenGet(t *testing.T) {
	t.Parallel()

	s := NewStore()
	s.Set("traceid", "abc-123")

	if got := s.Get("traceid"); got != "abc-123" {
		t.Errorf("Get(traceid) = %q, want %q", got, "abc-123")
	}
}

func TestStore_SetOverwritesExistingKey(t *testing.T) {
	t.Parallel()

	s := NewStore()
	s.Set("k", "v1")
	s.Set("k", "v2")

	if got := s.Get("k"); got != "v2" {
		t.Errorf("Get(k) = %q, want %q", got, "v2")
	}
}

func TestStore_All_ReturnsAllEntries(t *testing.T) {
	t.Parallel()

	s := NewStore()
	s.Set("a", "1")
	s.Set("b", "2")
	s.Set("c", "3")

	all := s.All()
	if len(all) != 3 {
		t.Fatalf("All() len = %d, want 3", len(all))
	}
	for k, v := range map[string]string{"a": "1", "b": "2", "c": "3"} {
		if all[k] != v {
			t.Errorf("All()[%q] = %q, want %q", k, all[k], v)
		}
	}
}

func TestStore_All_ReturnsSnapshotCopy(t *testing.T) {
	t.Parallel()

	s := NewStore()
	s.Set("k", "original")

	snap := s.All()
	snap["k"] = "mutated"
	snap["new"] = "added"

	// Mutations on the returned map must not bleed into the store.
	if got := s.Get("k"); got != "original" {
		t.Errorf("Get(k) = %q, want %q (snapshot must be a copy)", got, "original")
	}
	if got := s.Get("new"); got != "" {
		t.Errorf("Get(new) = %q, want \"\" (snapshot must be a copy)", got)
	}
}

func TestContext_RoundTrip(t *testing.T) {
	t.Parallel()

	s := NewStore()
	s.Set("k", "v")

	ctx := NewContext(context.Background(), s)
	got, ok := FromContext(ctx)
	if !ok {
		t.Fatal("FromContext returned ok=false")
	}
	if got != s {
		t.Error("FromContext returned a different *Store")
	}
	if got.Get("k") != "v" {
		t.Errorf("got.Get(k) = %q, want %q", got.Get("k"), "v")
	}
}

func TestContext_FromBareContextReturnsFalse(t *testing.T) {
	t.Parallel()

	_, ok := FromContext(context.Background())
	if ok {
		t.Error("FromContext on bare context returned ok=true")
	}
}

// TestStore_ConcurrentAccess exercises the RWMutex under -race. Without proper
// locking the race detector would flag this; the test passing under -race is
// the assertion.
func TestStore_ConcurrentAccess(t *testing.T) {
	t.Parallel()

	s := NewStore()

	const goroutines = 50
	const iterations = 200

	var wg sync.WaitGroup
	wg.Add(goroutines * 3)

	// Writers
	for g := range goroutines {
		go func(id int) {
			defer wg.Done()
			for i := range iterations {
				s.Set("w-"+strconv.Itoa(id), strconv.Itoa(i))
			}
		}(g)
	}

	// Readers (Get)
	for g := range goroutines {
		go func(id int) {
			defer wg.Done()
			for range iterations {
				_ = s.Get("w-" + strconv.Itoa(id))
			}
		}(g)
	}

	// Readers (All snapshot)
	for range goroutines {
		go func() {
			defer wg.Done()
			for range iterations {
				_ = s.All()
			}
		}()
	}

	wg.Wait()

	// Sanity: each writer's last value should be the final iteration count.
	for g := range goroutines {
		want := strconv.Itoa(iterations - 1)
		if got := s.Get("w-" + strconv.Itoa(g)); got != want {
			t.Errorf("Get(w-%d) = %q, want %q", g, got, want)
		}
	}
}
