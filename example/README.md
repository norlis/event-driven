# Example

End-to-end demo showing cross-transport routing: HTTP → Pub/Sub → HTTP webhook
(and the reverse). Four routes are wired in `events.go`.

## Environment variables

| Variable | Required | Default | Description |
|---|---|---|---|
| `GCP_PROJECT_ID` | yes | — | GCP project that owns the Pub/Sub topic + subscription. |
| `EVT_SUBSCRIPTION` | yes | — | Pub/Sub subscription ID consumed by `PrincipalMux`. |
| `EVT_PUBLISH` | no | — | Pub/Sub topic where handler results are published. If empty, the result publisher is disabled. |
| `WEBHOOK_URL` | yes (for HTTP-2 / PS-1 routes) | — | HTTPS endpoint used by the webhook publisher (e.g. a `https://webhook.site/...` URL). |
| `LOG_LEVEL` | no | `info` | `debug` enables verbose logging; anything else maps to `info`. |

```bash
export GCP_PROJECT_ID=my-project
export EVT_SUBSCRIPTION=s-event
export EVT_PUBLISH=t-event
export WEBHOOK_URL="https://webhook.site/<your-uuid>"
export LOG_LEVEL=debug

go run ./example/cmd
```

The server listens on `:8880`. Health endpoints: `/status`, `/live`, `/ready`.

## Publishers

The library provides two publisher implementations; both satisfy
`eventmux.Publisher` and can be passed interchangeably to `mux.Register(...)`.

| Publisher | Package | Target | Use case |
|---|---|---|---|
| `pubsub.Publisher` | `pkg/transport/gcp/pubsub` | Google Cloud Pub/Sub topic | Async event pipelines |
| `eventhttp.Publisher` | `pkg/transport/eventhttp` | Any HTTP endpoint | Webhooks, external APIs |

### `eventhttp.Publisher`

Publishes CloudEvents using **binary content mode** (attributes as `Ce-*` headers, data as body).

```go
pub := eventhttp.NewPublisher(eventhttp.PublisherConfig{
    TargetURL: os.Getenv("WEBHOOK_URL"),
    Timeout:   5 * time.Second,
}, logger)
```

### `pubsub.Publisher`

Publishes CloudEvents to a Pub/Sub topic. Attributes are stored as message attributes with the `ce-` prefix.

```go
pub := pubsub.NewPublisher(client, pubsub.PublisherConfig{
    ProjectID: "my-project",
    TopicID:   "my-topic",
}, logger)
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

No CloudEvents headers. The subscriber builds a CloudEvent with defaults:
- `id`: from `X-Message-UUID` header or auto-generated UUID
- `type`: `com.example.http.command`
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
| HTTP-1 | `HttpMux` | `ByType("http.command")` | Pub/Sub topic | Cross: HTTP → Pub/Sub. Result picked up by PS-1. |
| HTTP-2 | `HttpMux` | `ByType("http.command.webhook") AND JMESPath` | Webhook | Demonstrates `cefilter.All` + JMESPath. |
| PS-1 | `PrincipalMux` | `ByType("http.command.result")` | Webhook | Closes the HTTP → Pub/Sub → HTTP round trip. |
| PS-2 | `PrincipalMux` | `ByType("com.example.person.created", "com.example.person.updated")` | Pub/Sub topic | Domain event pipeline. |

## Test scenarios

See `test.http` for ready-to-use requests.
