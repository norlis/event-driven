# Example

End-to-end demo showing cross-transport routing: HTTP → Pub/Sub → HTTP webhook (and the reverse). Four routes are wired in `events.go`; both muxes validate payloads with `UsePreflight(validate.New())` and report outcomes to the `Observer` in `observer.go`.

A second, self-contained demo for NATS JetStream lives in [`nats/`](#nats-jetstream-demo).

## Environment variables

| Variable | Required | Default | Description |
|---|---|---|---|
| `GCP_PROJECT_ID` | yes | — | GCP project that owns the Pub/Sub topic + subscription. |
| `EVT_SUBSCRIPTION` | yes | — | Pub/Sub subscription ID consumed by `PubSubMux`. |
| `EVT_PUBLISH` | no | — | Pub/Sub topic where handler results are published. If empty, the result publisher is disabled. |
| `WEBHOOK_URL` | yes (for HTTP-2 / PS-1 routes) | — | HTTPS endpoint used by the webhook publisher (e.g. a `https://webhook.site/...` URL). |
| `LOG_LEVEL` | no | `info` | `debug` enables verbose logging; anything else maps to `info`. |
| `SERVICE_NAME` | no | `event-driven-example` | `service.name` field on every log record. |
| `SERVICE_VERSION` | no | `dev` | `service.version` field on every log record. |
| `ENVIRONMENT` | no | `local` | `deployment.environment.name` field on every log record. |
| `HTTP_ADDR` | no | `:8880` | HTTP listen address (`:8080` in the NATS demo). |

```bash
export GCP_PROJECT_ID=my-project
export EVT_SUBSCRIPTION=s-event
export EVT_PUBLISH=t-event
export WEBHOOK_URL="https://webhook.site/<your-uuid>"
export LOG_LEVEL=debug

go run ./example/cmd
```

The server listens on `HTTP_ADDR` (default `:8880`). Health endpoints: `/status`, `/live`, `/ready`.

`BindLifecycle` (`module.go`) fixes the start/stop order in one place with a `lifecycle.Group`: Pub/Sub client and publisher first, then the Pub/Sub and HTTP muxes, the HTTP server last. Shutdown runs in reverse, so the server stops accepting requests, the muxes drain, and the publisher flushes its batch before the client closes.

## Publishers used in this example

This example wires two publisher implementations; both satisfy
`eventmux.Publisher` and are passed interchangeably to `mux.Handle(...)`.

| Publisher | Package | Target | Use case |
|---|---|---|---|
| `pubsub.Publisher` | `pkg/transport/gcp/pubsub` | Google Cloud Pub/Sub topic | Async event pipelines |
| `eventhttp.Publisher` | `pkg/transport/eventhttp` | Any HTTP endpoint | Webhooks, external APIs |

> The library also ships `pkg/transport/aws/sns.Publisher` and
> `pkg/transport/aws/sqs.Subscriber`. They are not wired in this demo —
> see the top-level README for the full transport catalogue.

### `eventhttp.Publisher`

Publishes CloudEvents using **binary content mode** (attributes as `Ce-*` headers, data as body).

```go
pub, err := eventhttp.NewPublisher(eventhttp.PublisherConfig{ // error unless TargetURL is an absolute http(s) URL
    TargetURL: os.Getenv("WEBHOOK_URL"),
    Timeout:   5 * time.Second,
}, logger)
```

### `pubsub.Publisher`

Publishes CloudEvents to a Pub/Sub topic. Attributes are stored as message attributes with the `ce-` prefix.

```go
pub, err := pubsub.NewPublisher(client, pubsub.PublisherConfig{TopicID: "my-topic"}, logger)
defer pub.Close() // flushes the batch; in fx, register it in an OnStop hook
```

## HTTP Content Modes

The HTTP subscriber (`pkg/transport/eventhttp`) accepts CloudEvents in three ways:

### Binary content mode

CloudEvent attributes go in HTTP headers, event data is the body.

```
POST /command
Content-Type: application/json
Ce-Id: evt-001
Ce-Specversion: 1.0
Ce-Type: http.command
Ce-Source: //my-app/client

{"name": "test", "age": 25}
```

Headers: `Ce-Id`, `Ce-Specversion`, `Ce-Type`, `Ce-Source`, `Ce-Time`, `Ce-Subject`.

### Structured content mode

The entire CloudEvent is a single JSON body.

```
POST /command
Content-Type: application/cloudevents+json

{
  "specversion": "1.0",
  "id": "evt-002",
  "type": "http.command",
  "source": "//my-app/client",
  "datacontenttype": "application/json",
  "data": {"name": "test", "age": 25}
}
```

### Plain HTTP (fallback)

No CloudEvents headers (requires `Content-Type: application/json`). The subscriber builds a CloudEvent with defaults:
- `id`: from `X-Message-UUID` header or auto-generated UUID v7
- `type`: `SubscriberConfig.DefaultType` (`http.command` in this example, so plain requests hit route HTTP-1)
- `correlationid`: from `X-Correlation-ID` header, otherwise the event `id`; echoed as `correlationId` in the response and propagated to the result event
- `source`: derived from request URL

```
POST /command
Content-Type: application/json

{"name": "test", "age": 25}
```

## Routes wired in this example

See `events.go`. Same handlers (`UseCase.Execute` / `UseCase.Command`) are reused.

| ID | Mux | Filter | Publisher | Notes |
|---|---|---|---|---|
| HTTP-1 | `HTTPMux` | `ByType("http.command")` | Pub/Sub topic | Cross: HTTP → Pub/Sub. Result picked up by PS-1. |
| HTTP-2 | `HTTPMux` | `ByType("http.command.webhook") AND JMESPath` | Webhook | Demonstrates `cefilter.All` + JMESPath. |
| PS-1 | `PubSubMux` | `ByType("http.command.result")` | Webhook | Closes the HTTP → Pub/Sub → HTTP round trip. |
| PS-2 | `PubSubMux` | `ByType("com.example.person.created", "com.example.person.updated")` | Pub/Sub topic | Domain event pipeline. |

## Observer

`observer.go` implements `eventmux.Observer`: it counts outcomes per event type and logs each one (with the dead-letter reason when there is one). A real deployment would feed a metrics SDK from the same two callbacks.

## Test scenarios

See `test.http` for ready-to-use requests.

## NATS JetStream demo

`nats/` is an HTTP → JetStream → log round trip that needs nothing but a local NATS server with JetStream enabled:

```bash
docker compose -f example/nats/compose.yaml up -d
go run ./example/nats

curl -XPOST localhost:8080/publish \
  -H 'Ce-Id: 1' -H 'Ce-Source: demo' -H 'Ce-Specversion: 1.0' \
  -H 'Ce-Type: http.command.nats' -H 'Content-Type: application/json' \
  -d '{"name":"ada","age":36}'
```

The HTTP mux handles the command and publishes the result on subject `events.http.command.nats.result`; the NATS mux consumes it through an ephemeral per-instance consumer (fan-out) and logs it. `NATS_URL` overrides the server address (default `nats://localhost:4222`) and `HTTP_ADDR` the HTTP one (default `:8080`); `nats/test.http` has more requests. The demo has no fx: `natsconn.Dial` opens the connection and a `lifecycle.Group` runs it with both muxes and the HTTP server (`Group.Run` handles signals and exits non-zero if a component dies).
