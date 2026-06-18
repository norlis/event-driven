// Command nats is a self-contained demo of an HTTP → NATS (JetStream) → log
// round trip built on the eventmux library. It needs nothing but a local NATS
// server with JetStream enabled:
//
//	docker compose -f example/nats/compose.yaml up -d
//	go run ./example/nats
//
// Then POST a CloudEvent of type "http.command.nats":
//
//	curl -XPOST localhost:8080/publish \
//	  -H 'Ce-Id: 1' -H 'Ce-Source: demo' -H 'Ce-Specversion: 1.0' \
//	  -H 'Ce-Type: http.command.nats' -H 'Content-Type: application/json' \
//	  -d '{"name":"ada","age":36}'
//
// Flow: the HTTP mux handles the command and publishes the result to NATS on
// subject events.http.command.nats.result; the NATS mux consumes it via an
// ephemeral per-instance consumer (fan-out) and logs it.
package main

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"
	"os"
	"time"

	cloudevents "github.com/cloudevents/sdk-go/v2/event"
	natsgo "github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"

	"github.com/norlis/event-driven/pkg/eventmux"
	"github.com/norlis/event-driven/pkg/filter/cefilter"
	"github.com/norlis/event-driven/pkg/kit/signal"
	"github.com/norlis/event-driven/pkg/middleware/recover"
	"github.com/norlis/event-driven/pkg/middleware/validate"
	"github.com/norlis/event-driven/pkg/transport/eventhttp"
	"github.com/norlis/event-driven/pkg/transport/nats/codec"
	natsjs "github.com/norlis/event-driven/pkg/transport/nats/jetstream"
)

const (
	httpAddr     = ":8080"
	natsStream   = "EVENTS"
	natsSubjects = "events.>"
	subjectNS    = "events"
)

// Person is the demo payload.
type Person struct {
	Name string `json:"name" validate:"required"`
	Age  int    `json:"age"  validate:"required"`
}

// subjectFromType derives the publish subject dynamically from the CloudEvent
// type: "events." + ce.Type(). It delegates encoding to codec.DefaultMarshaler,
// demonstrating the codec.Marshaler extension point.
type subjectFromType struct{}

func (subjectFromType) Marshal(_ string, ce cloudevents.Event) (*natsgo.Msg, error) {
	msg, err := codec.DefaultMarshaler{}.Marshal(subjectNS+"."+ce.Type(), ce)
	if err != nil {
		return nil, err //nolint:wrapcheck // DefaultMarshaler never returns a non-nil error
	}
	return msg, nil
}

func main() {
	logger := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelDebug}))

	if err := run(logger); err != nil {
		logger.Error("fatal", slog.Any("error", err))
		os.Exit(1)
	}
}

func run(logger *slog.Logger) error {
	ctx, stop := signal.NotifyContext()
	defer stop()

	// ── NATS connection + JetStream ─────────────────────────────────────
	url := os.Getenv("NATS_URL")
	if url == "" {
		url = natsgo.DefaultURL
	}
	nc, err := natsgo.Connect(url, natsgo.Name("nats-example"), natsgo.MaxReconnects(-1))
	if err != nil {
		return err //nolint:wrapcheck
	}
	defer func() { _ = nc.Drain() }()

	js, err := jetstream.New(nc)
	if err != nil {
		return err //nolint:wrapcheck
	}

	// ── Publisher (dynamic subject) ─────────────────────────────────────
	pub, err := natsjs.NewPublisher(js, natsjs.PublisherConfig{
		Subject:   subjectNS, // overridden per-event by subjectFromType
		Marshaler: subjectFromType{},
	}, logger.With(slog.String("logger", "nats-publisher")))
	if err != nil {
		return err //nolint:wrapcheck
	}

	// ── Subscriber: ephemeral fan-out consumer; auto-provision the stream ─
	natsSub, err := natsjs.NewSubscriber(js, natsjs.SubscriberConfig{
		Stream:              natsStream,
		FilterSubject:       natsSubjects,
		AutoProvisionStream: true,
		StreamConfig: &jetstream.StreamConfig{
			Name:      natsStream,
			Subjects:  []string{natsSubjects},
			Storage:   jetstream.FileStorage,
			Retention: jetstream.LimitsPolicy,
		},
		MaxOutstandingMessages: 50,
	}, logger.With(slog.String("logger", "nats-subscription")))
	if err != nil {
		return err //nolint:wrapcheck
	}

	// ── HTTP subscriber ─────────────────────────────────────────────────
	serveMux := http.NewServeMux()
	httpSub, err := eventhttp.NewSubscriber(serveMux, eventhttp.SubscriberConfig{
		Pattern: "POST /publish",
		Logger:  logger.With(slog.String("logger", "http-subscriber")),
	})
	if err != nil {
		return err //nolint:wrapcheck
	}

	// ── HTTP mux: HTTP command → publish to NATS ────────────────────────
	httpMux := eventmux.New(eventmux.Config{
		Name:            "http-mux",
		Subscription:    httpSub,
		Logger:          logger.With(slog.String("logger", "http-mux")),
		ReportOnNoMatch: true,
	})
	httpMux.Use(recover.Middleware)
	httpMux.UsePreflight(validate.New(logger))
	httpMux.Register(
		pub,
		cefilter.ByType("http.command.nats"),
		Person{},
		eventmux.Wrap(func(ctx context.Context, p Person) (json.RawMessage, error) {
			logger.Info("HTTP command received, publishing to NATS", slog.Any("event", p))
			return json.Marshal(p)
		}),
	)

	// ── NATS mux: receive from NATS → log ───────────────────────────────
	natsMux := eventmux.New(eventmux.Config{
		Name:         "nats-mux",
		Subscription: natsSub,
		Logger:       logger.With(slog.String("logger", "nats-mux")),
	})
	natsMux.Use(recover.Middleware)
	natsMux.Register(
		nil, // terminal: log only, no republish
		cefilter.ByType("http.command.nats.result"),
		Person{},
		eventmux.Wrap(func(ctx context.Context, p Person) (json.RawMessage, error) {
			logger.Info("Received event from NATS", slog.Any("event", p))
			return nil, nil
		}),
	)

	// ── Start both muxes and the HTTP server ────────────────────────────
	onError := func(name string) eventmux.OnErrorFunc {
		return func(err error) { logger.Error("mux crashed", slog.String("mux", name), slog.Any("error", err)) }
	}
	stopHTTP := httpMux.RunBackground(ctx, onError("http-mux"))
	stopNats := natsMux.RunBackground(ctx, onError("nats-mux"))

	server := &http.Server{Addr: httpAddr, Handler: serveMux, ReadHeaderTimeout: 30 * time.Second}
	go func() {
		logger.Info("HTTP server listening", slog.String("addr", httpAddr))
		if serr := server.ListenAndServe(); serr != nil && !errors.Is(serr, http.ErrServerClosed) {
			logger.Error("HTTP server failed", slog.Any("error", serr))
		}
	}()

	<-ctx.Done()
	logger.Info("shutting down...")

	shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_ = server.Shutdown(shutdownCtx)
	_ = stopHTTP(5 * time.Second)
	_ = stopNats(5 * time.Second)
	return nil
}
