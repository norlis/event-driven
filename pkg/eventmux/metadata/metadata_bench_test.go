package metadata

import (
	"fmt"
	"strconv"
	"testing"
)

func BenchmarkStore_Set(b *testing.B) {
	s := NewStore()
	b.ReportAllocs()
	b.ResetTimer()
	for i := range b.N {
		s.Set("k", strconv.Itoa(i))
	}
}

func BenchmarkStore_Get(b *testing.B) {
	s := NewStore()
	s.Set("k", "v")
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		_ = s.Get("k")
	}
}

func BenchmarkStore_All(b *testing.B) {
	// The snapshot allocates a fresh map every call. Size matters: more keys
	// → more bytes/op.
	for _, n := range []int{1, 4, 16, 64} {
		b.Run(fmt.Sprintf("entries=%d", n), func(b *testing.B) {
			s := NewStore()
			for i := range n {
				s.Set("k"+strconv.Itoa(i), "v")
			}
			b.ReportAllocs()
			b.ResetTimer()
			for range b.N {
				_ = s.All()
			}
		})
	}
}

// BenchmarkStore_ParallelGet measures RWMutex behavior under high read
// concurrency. We expect near-linear scaling because RLock is non-exclusive.
func BenchmarkStore_ParallelGet(b *testing.B) {
	s := NewStore()
	s.Set("k", "v")
	b.ReportAllocs()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			_ = s.Get("k")
		}
	})
}

// BenchmarkStore_ParallelMixed measures contention when reads and writes
// interleave. This is the realistic dispatch scenario (handler writes,
// publishResult reads).
func BenchmarkStore_ParallelMixed(b *testing.B) {
	s := NewStore()
	b.ReportAllocs()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			if i%10 == 0 {
				s.Set("k", "v")
			} else {
				_ = s.Get("k")
			}
			i++
		}
	})
}
