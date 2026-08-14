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
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"strings"
	"sync/atomic"
	"time"

	cloudevents "github.com/cloudevents/sdk-go/v2/event"
	natsgo "github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"

	"github.com/norlis/httpgate/logging"

	"github.com/norlis/event-driven/pkg/eventmux"
	"github.com/norlis/event-driven/pkg/filter/cefilter"
	"github.com/norlis/event-driven/pkg/kit/logfields"
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

	envLogLevel    = "LOG_LEVEL"       // "debug" or "info" (default)
	envServiceName = "SERVICE_NAME"    // service.name (default "event-driven-nats-example")
	envServiceVer  = "SERVICE_VERSION" // service.version (default "dev")
	envEnvironment = "ENVIRONMENT"     // deployment.environment.name (default "local")
)

// errMuxFailed signals run() exiting because a mux crashed rather than a
// clean OS-signal shutdown, so main() can exit non-zero and let a supervisor
// restart the process.
var errMuxFailed = errors.New("mux run failed")

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
		if errors.Is(err, errMuxFailed) {
			// A mux crash surfaces here long after startup succeeded; the mux
			// already logged its own "mux run failed" record, this is the
			// distinct process-level exit record.
			logger.Error("app stopped after mux failure", logging.Err(err))
		} else {
			logger.Error("app start failed", logging.Err(err))
		}
		os.Exit(1)
	}
}

func run(logger *slog.Logger) error {
	ctx, stop := signal.NotifyContext()
	defer stop()

	nc, js, err := dialNATS(logger)
	if err != nil {
		return err
	}
	defer func() { _ = nc.Drain() }()

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
		Pattern: "POST /publish",
		Logger:  logger,
	})
	if err != nil {
		return fmt.Errorf("new http subscriber: %w", err)
	}

	httpMux := newHTTPMux(httpSub, pub, logger)
	natsMux := newNatsMux(natsSub, logger)

	// A fatal mux error must not be silently tolerated: the mux already logs it
	// (ERROR "mux run failed"), but without remediation the process would keep
	// serving HTTP while never consuming NATS again, looking healthy to any
	// orchestrator. Cancelling ctx unblocks run() below and drives the same
	// graceful-shutdown path as an OS signal; the sentinel error then makes
	// main() exit non-zero so a supervisor restarts the process.
	var fatal atomic.Bool
	onMuxFailure := func(error) {
		fatal.Store(true)
		stop()
	}
	stopHTTP := httpMux.RunBackground(ctx, onMuxFailure)
	stopNats := natsMux.RunBackground(ctx, onMuxFailure)

	server := &http.Server{Addr: httpAddr, Handler: serveMux, ReadHeaderTimeout: 30 * time.Second}
	go func() {
		logger.Info("http server started", slog.String(logfields.KeyServerAddress, httpAddr))
		if serr := server.ListenAndServe(); serr != nil && !errors.Is(serr, http.ErrServerClosed) {
			logger.Error("http server failed", logging.Err(serr))
		}
	}()

	<-ctx.Done()
	logger.Info("app stopping")

	shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_ = server.Shutdown(shutdownCtx)
	_ = stopHTTP(5 * time.Second)
	_ = stopNats(5 * time.Second)

	if fatal.Load() {
		return errMuxFailed
	}
	return nil
}

// dialNATS connects to NATS (NATS_URL, default nats://localhost:4222) and opens
// a JetStream context.
func dialNATS(logger *slog.Logger) (*natsgo.Conn, jetstream.JetStream, error) {
	url := os.Getenv("NATS_URL")
	if url == "" {
		url = natsgo.DefaultURL
	}
	logger.Info("nats connecting", slog.String(logfields.KeyServerAddress, url))

	nc, err := natsgo.Connect(url, natsgo.Name("nats-example"), natsgo.MaxReconnects(-1))
	if err != nil {
		return nil, nil, fmt.Errorf("nats connect %q: %w", url, err)
	}
	js, err := jetstream.New(nc)
	if err != nil {
		_ = nc.Drain()
		return nil, nil, fmt.Errorf("jetstream new: %w", err)
	}
	return nc, js, nil
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
	}, logger)
	if err != nil {
		return nil, fmt.Errorf("new nats subscriber: %w", err)
	}
	return sub, nil
}

// newHTTPMux routes HTTP commands of type http.command.nats to the NATS publisher.
func newHTTPMux(httpSub eventmux.Subscription, pub eventmux.Publisher, logger *slog.Logger) *eventmux.Mux {
	mux := eventmux.New(eventmux.Config{
		Name:            "http-mux",
		Subscription:    httpSub,
		Logger:          logger,
		ReportOnNoMatch: true,
	})
	mux.Use(recover.Middleware)
	mux.UsePreflight(validate.New())
	mux.Register(
		pub,
		cefilter.ByType("http.command.nats"),
		Person{},
		eventmux.Wrap(func(ctx context.Context, p Person) (json.RawMessage, error) {
			logger.InfoContext(ctx, "http command published to nats")
			data, err := json.Marshal(p)
			if err != nil {
				return nil, fmt.Errorf("marshal person: %w", err)
			}
			return data, nil
		}),
	)
	return mux
}

// newNatsMux logs events received from NATS (terminal route, no republish).
func newNatsMux(sub eventmux.Subscription, logger *slog.Logger) *eventmux.Mux {
	mux := eventmux.New(eventmux.Config{
		Name:         "nats-mux",
		Subscription: sub,
		Logger:       logger,
	})
	mux.Use(recover.Middleware)
	mux.Register(
		nil, // terminal: log only, no republish
		cefilter.ByType("http.command.nats.result"),
		Person{},
		eventmux.Wrap(func(ctx context.Context, p Person) (json.RawMessage, error) {
			logger.InfoContext(ctx, "nats event received")
			return nil, nil
		}),
	)
	return mux
}
