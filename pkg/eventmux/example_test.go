package eventmux_test

import (
	"context"
	"encoding/json/jsontext"
	"fmt"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"

	"github.com/norlis/event-driven/v2/pkg/eventmux"
	"github.com/norlis/event-driven/v2/pkg/filter/cefilter"
	"github.com/norlis/event-driven/v2/pkg/transport/eventhttp"
)

type Order struct {
	ID string `json:"id"`
}

// The README Quick Start: an HTTP subscription routed by CloudEvent type to a
// typed handler. Any other transport plugs into Config.Subscription the same way.
func Example() {
	ctx := context.Background()
	logger := slog.New(slog.DiscardHandler)

	serveMux := http.NewServeMux()
	sub, err := eventhttp.NewSubscriber(serveMux, eventhttp.SubscriberConfig{
		Pattern:     "POST /orders",
		DefaultType: "com.example.order.created", // type for plain JSON requests
	}, logger)
	if err != nil {
		panic(err)
	}

	mux, err := eventmux.New(eventmux.Config{Name: "orders", Subscription: sub, Logger: logger})
	if err != nil {
		panic(err)
	}
	mux.Handle(
		nil, // no result publisher: fire-and-forget
		cefilter.ByType("com.example.order.created"),
		func(_ context.Context, o Order) (jsontext.Value, error) {
			fmt.Println("order", o.ID)
			return nil, nil
		},
	)

	if err := mux.Start(ctx, nil); err != nil { // returns once the subscription is up
		panic(err)
	}
	defer func() { _ = mux.Stop(ctx) }() // drains in-flight handlers

	req := httptest.NewRequestWithContext(ctx, http.MethodPost, "/orders", strings.NewReader(`{"id":"o-1"}`))
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()
	serveMux.ServeHTTP(rec, req)
	fmt.Println("status", rec.Code)

	// Output:
	// order o-1
	// status 202
}
