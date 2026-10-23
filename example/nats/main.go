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
	"encoding/json/jsontext"
	"encoding/json/v2"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"strings"

	cloudevents "github.com/cloudevents/sdk-go/v2/event"
	natsgo "github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"

	"github.com/norlis/httpgate/logging"
	"github.com/norlis/httpgate/server"

	"github.com/norlis/event-driven/v2/pkg/eventmux"
	"github.com/norlis/event-driven/v2/pkg/filter/cefilter"
	"github.com/norlis/event-driven/v2/pkg/kit/httpserver"
	"github.com/norlis/event-driven/v2/pkg/kit/lifecycle"
	"github.com/norlis/event-driven/v2/pkg/middleware/validate"
	"github.com/norlis/event-driven/v2/pkg/transport/eventhttp"
	"github.com/norlis/event-driven/v2/pkg/transport/nats/codec"
	natsjs "github.com/norlis/event-driven/v2/pkg/transport/nats/jetstream"
	"github.com/norlis/event-driven/v2/pkg/transport/nats/natsconn"
)

const (
	natsStream   = "EVENTS"
	natsSubjects = "events.>"
	subjectNS    = "events"

	envLogLevel    = "LOG_LEVEL"       // "debug" or "info" (default)
	envServiceName = "SERVICE_NAME"    // service.name (default "event-driven-nats-example")
	envServiceVer  = "SERVICE_VERSION" // service.version (default "dev")
	envEnvironment = "ENVIRONMENT"     // deployment.environment.name (default "local")
	envHTTPAddr    = "HTTP_ADDR"       // HTTP listen address (default ":8080")
	envNATSURL     = "NATS_URL"        // NATS server(s) (default nats://localhost:4222)
)

// getenvDefault returns the environment variable value, or fallback when unset.
func getenvDefault(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

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
		return nil, fmt.Errorf("subject-from-type marshal: %w", err)
	}
	return msg, nil
}

// newLogger returns the platform-standard logger. DEBUG stays off unless
// LOG_LEVEL=debug.
func newLogger() *slog.Logger {
	level := slog.LevelInfo
	if strings.EqualFold(os.Getenv(envLogLevel), "debug") {
		level = slog.LevelDebug
	}
	return logging.New(
		os.Stdout,
		logging.WithService(getenvDefault(envServiceName, "event-driven-nats-example"), getenvDefault(envServiceVer, "dev")),
		logging.WithEnvironment(getenvDefault(envEnvironment, "local")),
		logging.WithLevel(level),
	)
}

func main() {
	logger := newLogger()

	if err := run(logger); err != nil {
		// A component failure was already logged by the component itself
		// ("subscription failed", "http server failed"): log only the rest.
		if !errors.Is(err, lifecycle.ErrComponentFailed) {
			logger.Error("app stopped", logging.Err(err))
		}
		os.Exit(1)
	}
}

func run(logger *slog.Logger) error {
	conn, err := natsconn.Dial(natsconn.Config{URL: os.Getenv(envNATSURL), Name: "nats-example"}, logger)
	if err != nil {
		return fmt.Errorf("nats dial: %w", err)
	}
	js, err := jetstream.New(conn.Conn)
	if err != nil {
		_ = conn.Stop(context.Background())
		return fmt.Errorf("jetstream new: %w", err)
	}

	pub, err := newPublisher(js, logger)
	if err != nil {
		return err
	}
	natsSub, err := newSubscriber(js, logger)
	if err != nil {
		return err
	}

	serveMux := http.NewServeMux()
	httpSub, err := eventhttp.NewSubscriber(serveMux, eventhttp.SubscriberConfig{
		Pattern:     "POST /publish",
		DefaultType: "http.command.nats",
	}, logger)
	if err != nil {
		return fmt.Errorf("new http subscriber: %w", err)
	}

	httpMux, err := newHTTPMux(httpSub, pub, logger)
	if err != nil {
		return err
	}
	natsMux, err := newNatsMux(natsSub, logger)
	if err != nil {
		return err
	}

	// Resources first, the HTTP server last: shutdown runs in reverse, so the
	// server stops accepting, the muxes drain, then the connection flushes.
	g := lifecycle.NewGroup(logger, 0)
	g.Add("nats", conn)
	g.Add(natsMux.Name(), natsMux)
	g.Add(httpMux.Name(), httpMux)
	g.Add("http-server", httpserver.New(serveMux, logger, server.WithAddr(getenvDefault(envHTTPAddr, ":8080"))))
	return g.Run(context.Background()) //nolint:wrapcheck // lifecycle errors already name the component
}

// newPublisher builds a JetStream publisher whose subject is derived from the
// event type (events.<ce-type>).
func newPublisher(js jetstream.JetStream, logger *slog.Logger) (eventmux.Publisher, error) {
	pub, err := natsjs.NewPublisher(js, natsjs.PublisherConfig{
		Subject:   subjectNS, // overridden per-event by subjectFromType
		Marshaler: subjectFromType{},
	}, logger)
	if err != nil {
		return nil, fmt.Errorf("new nats publisher: %w", err)
	}
	return pub, nil
}

// newSubscriber builds the ephemeral fan-out subscriber and auto-provisions the
// EVENTS stream so the demo runs against a fresh NATS.
func newSubscriber(js jetstream.JetStream, logger *slog.Logger) (eventmux.Subscription, error) {
	sub, err := natsjs.NewSubscriber(js, natsjs.SubscriberConfig{
		Stream:        natsStream,
		FilterSubject: natsSubjects,
		StreamConfig: &jetstream.StreamConfig{
			Name:      natsStream,
			Subjects:  []string{natsSubjects},
			Storage:   jetstream.FileStorage,
			Retention: jetstream.LimitsPolicy,
		},
		MaxOutstandingMessages: 50,
	}, logger)
	if err != nil {
		return nil, fmt.Errorf("new nats subscriber: %w", err)
	}
	return sub, nil
}

// newHTTPMux routes HTTP commands of type http.command.nats to the NATS publisher.
func newHTTPMux(httpSub eventmux.Subscription, pub eventmux.Publisher, logger *slog.Logger) (*eventmux.Mux, error) {
	mux, err := eventmux.New(eventmux.Config{
		Name:         "http-mux",
		Subscription: httpSub,
		Logger:       logger,
	})
	if err != nil {
		return nil, fmt.Errorf("new http mux: %w", err)
	}
	mux.UsePreflight(validate.New())
	mux.Handle(
		pub,
		cefilter.ByType("http.command.nats"),
		func(ctx context.Context, p Person) (jsontext.Value, error) {
			logger.InfoContext(ctx, "http command published to nats")
			data, err := json.Marshal(p)
			if err != nil {
				return nil, fmt.Errorf("marshal person: %w", err)
			}
			return data, nil
		},
	)
	return mux, nil
}

// newNatsMux logs events received from NATS (terminal route, no republish).
func newNatsMux(sub eventmux.Subscription, logger *slog.Logger) (*eventmux.Mux, error) {
	mux, err := eventmux.New(eventmux.Config{
		Name:         "nats-mux",
		Subscription: sub,
		Logger:       logger,
	})
	if err != nil {
		return nil, fmt.Errorf("new nats mux: %w", err)
	}
	mux.Handle(
		nil, // terminal: log only, no republish
		cefilter.ByType("http.command.nats.result"),
		func(ctx context.Context, p Person) (jsontext.Value, error) {
			logger.InfoContext(ctx, "nats event received")
			return nil, nil
		},
	)
	return mux, nil
}
