# Event-Driven — CloudEvents Multiplexer Library

[![Go Reference](https://pkg.go.dev/badge/github.com/norlis/event-driven/v2.svg)](https://pkg.go.dev/github.com/norlis/event-driven/v2)

A Go library for services that consume and produce events: it routes [CloudEvents](https://cloudevents.io/) from Google Cloud Pub/Sub, AWS SNS+SQS, NATS core and JetStream, or HTTP to typed handlers, the way `http.ServeMux` routes requests.

> **Status:** v2 is in release candidate (`v2.0.0-rc.*`); the API may still change before `v2.0.0`. Every change is listed in the [CHANGELOG](CHANGELOG.md).

## Key Features

- **EventMux** — Multiplexes events to handlers based on filters, analogous to `http.ServeMux` for events.
- **CloudEvents native** — `Message` composes `cloudevents.Event` with delivery mechanics (Ack/Nack).
- **Stateless** — No in-memory queue. The transport SDK controls concurrency. Crash-safe by design.
- **Multiple transports** — GCP Pub/Sub, AWS SNS (publish) + SQS (subscribe), NATS core + JetStream (fan-out), HTTP (sync). Swappable through `eventmux.Publisher` / `eventmux.Subscription` interfaces.
- **Filter families** — CloudEvent attributes (`cefilter`), payload content (`jmespath`), combinations.
- **Middleware pipeline** — validation, error skipping, in-process retry with exponential backoff and jitter. Panics are always recovered by the mux.
- **One wire format** — every transport maps CloudEvents with the same binary / structured codec; the NATS and SNS publishers accept a custom `Marshaler` (subject from the event type, FIFO attributes).
- **Non-retryable errors** — `event.NonRetryableError` triggers Ack (discard) instead of Nack (redeliver).
- **Poison-message policy** — decode, validation, panic and `NonRetryableError` failures are discarded on the first delivery; retryable errors respect `Config.MaxRetries` when the broker reports a delivery count, and are definitive on a transport that cannot redeliver (reason `noredelivery`); every discarded message can be copied to a `Config.DeadLetter` publisher.
- **Structured logging** — `log/slog` with the platform logging standard via [`httpgate/logging`](https://github.com/norlis/httpgate) (OTel field names, ISO 8601 UTC timestamps, structured `error.*` objects) and W3C `traceparent` propagation across every transport (HTTP headers, Pub/Sub / SQS / SNS message attributes, NATS headers, CloudEvents `traceparent` extension).
- **FX integration** — `pkg/kit/fxmux` plugs the mux + FX lifecycle hooks + slog-based fxevent logger.

## Installation

```bash
go get github.com/norlis/event-driven/v2
```

Requires Go 1.27+.

### Transport dependencies are on demand

The module declares every transport SDK (AWS, Google Cloud Pub/Sub, NATS, resty) in one `go.mod`, but a consumer only pays for the transports it imports. Since Go 1.17 the `go` command prunes the module graph and loads modules lazily: the SDKs behind a transport you never import are not downloaded, compiled, linked or recorded in your `go.sum`. They only remain visible in `go list -m all` and `go mod graph`, and they set a minimum version if your own module also depends on the same SDK directly.

Measured on 2026-10-21 with a module that imports only `pkg/eventmux` and `pkg/transport/eventhttp`:

| What the consumer gets | Result |
|---|---|
| `require` lines in its `go.mod` | 9 indirect (cloudevents, httpgate, resty, zap, …), none from AWS, Pub/Sub, NATS or gRPC |
| AWS / Pub/Sub / NATS entries in its `go.sum` | 0 |
| Packages from those SDKs compiled (`go list -deps`) | 0 |
| Modules from those SDKs in `go list -m all` | 21 of 167 |

Reproduce it with:

```bash
mkdir consumer && cd consumer && go mod init consumer
cat > main.go <<'EOF'
package main

import (
	"github.com/norlis/event-driven/v2/pkg/eventmux"
	"github.com/norlis/event-driven/v2/pkg/transport/eventhttp"
)

func main() { _ = eventmux.New; _ = eventhttp.NewPublisher }
EOF
go mod tidy && grep -c aws-sdk go.sum; go list -deps ./... | grep -c 'aws-sdk\|cloud.google.com/go/pubsub\|nats-io'
```

Both counts print `0`. See the [module graph pruning](https://go.dev/ref/mod#graph-pruning) section of the Go Modules Reference.

## Quick Start

An HTTP subscription routed by CloudEvent type to a typed handler. Any other transport plugs into `Config.Subscription` the same way. This is `Example` in [`pkg/eventmux/example_test.go`](pkg/eventmux/example_test.go), compiled and checked by `go test`.

```go
serveMux := http.NewServeMux()
sub, err := eventhttp.NewSubscriber(serveMux, eventhttp.SubscriberConfig{
    Pattern:     "POST /orders",
    DefaultType: "com.example.order.created", // type for plain JSON requests
}, logger)
if err != nil {
    return err
}

mux, err := eventmux.New(eventmux.Config{Name: "orders", Subscription: sub, Logger: logger})
if err != nil {
    return err
}
mux.Handle(
    nil, // result publisher; nil = fire-and-forget
    cefilter.ByType("com.example.order.created"),
    func(ctx context.Context, o Order) (jsontext.Value, error) { // payload decoded into Order
        return nil, nil // nil error → Ack (HTTP 202); NonRetryableError → discard; other errors → Nack
    },
)

if err := mux.Start(ctx, nil); err != nil { // returns once the subscription is up
    return err
}
defer mux.Stop(shutdownCtx) // drains in-flight handlers until shutdownCtx expires
```

## Documentation

- [Guide](GUIDE.md): architecture, message flow, transports, wire format, filters, middleware, error handling and dead-lettering, shutdown, FX integration and concurrency tuning.
- [API reference](https://pkg.go.dev/github.com/norlis/event-driven/v2) on pkg.go.dev.
- [CHANGELOG](CHANGELOG.md): changes and migrations per version.

## Example

A full end-to-end example lives under `example/` showing four cross-transport scenarios (HTTP↔Pub/Sub round-trip, JMESPath filtering, validation preflight) plus an `Observer` that counts outcomes. See [`example/README.md`](example/README.md) for the env vars and `example/test.http` for ready-to-fire requests.

`example/nats/` is a self-contained HTTP → JetStream → log demo that only needs a local NATS server: `docker compose -f example/nats/compose.yaml up -d && go run ./example/nats`.

## Contributing

Setup, checks and the release process are in [CONTRIBUTING.md](CONTRIBUTING.md).
