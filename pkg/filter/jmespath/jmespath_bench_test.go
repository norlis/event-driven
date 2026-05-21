package jmespath

import (
	"fmt"
	"log/slog"
	"strings"
	"testing"

	cloudevents "github.com/cloudevents/sdk-go/v2/event"

	"github.com/norlis/event-driven/pkg/event"
)

func benchMessage(body []byte) *event.Message {
	ce := cloudevents.New()
	ce.SetID("x")
	ce.SetType("t")
	ce.SetSource("src")
	_ = ce.SetData(cloudevents.ApplicationJSON, body)
	return event.NewMessageWithoutAck(ce)
}

func BenchmarkJMESPath_Match(b *testing.B) {
	logger := slog.New(slog.DiscardHandler)

	// Small body, simple expression — typical filter use case.
	b.Run("simple-expr/small-body", func(b *testing.B) {
		f := New("status == 'paid'", logger)
		msg := benchMessage([]byte(`{"status":"paid","amount":100}`))
		b.ReportAllocs()
		b.ResetTimer()
		for range b.N {
			_ = f.Match(msg)
		}
	})

	// Nested-path expression with deeper JSON.
	b.Run("nested-expr/medium-body", func(b *testing.B) {
		f := New("user.role == 'admin'", logger)
		body := []byte(`{"user":{"id":42,"role":"admin","email":"u@example.com","tags":["a","b","c"]},"version":3}`)
		msg := benchMessage(body)
		b.ReportAllocs()
		b.ResetTimer()
		for range b.N {
			_ = f.Match(msg)
		}
	})

	// Large body, same simple expression — exposes JSON-decode cost.
	for _, size := range []int{1024, 8192} {
		b.Run(fmt.Sprintf("simple-expr/body=%dB", size), func(b *testing.B) {
			f := New("status == 'paid'", logger)
			body := []byte(`{"status":"paid","filler":"` + strings.Repeat("x", size) + `"}`)
			msg := benchMessage(body)
			b.ReportAllocs()
			b.ResetTimer()
			for range b.N {
				_ = f.Match(msg)
			}
		})
	}
}
