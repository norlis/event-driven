# Example

## Environment variables

```bash
export EVT_PUBLISH=t-event
export EVT_SUBSCRIPTION=s-event
export GCP_PROJECT_ID=project-test
export LOG_LEVEL=debug
```

## Publishers

The library provides two publisher implementations:

| Publisher | Package | Target | Use case |
|---|---|---|---|
| `GCPPublisher` | `pkg/adapter/pubsub` | Google Cloud Pub/Sub topic | Async event pipelines |
| `HTTPPublisher` | `pkg/adapter/httpdriven` | Any HTTP endpoint | Webhooks, external APIs |

Both implement `port.Publisher` — they receive a `cloudevents.Event` and deliver it to their target.

### HTTPPublisher

Publishes CloudEvents using **binary content mode** (attributes as `Ce-*` headers, data as body).

```go
pub := httpdriven.NewHTTPPublisher(httpdriven.HTTPPublisherConfig{
    TargetURL: "https://webhook.site/...",
    Timeout:   5 * time.Second,
}, logger)
```

### GCPPublisher

Publishes CloudEvents to a Pub/Sub topic. Attributes are stored as message attributes with `ce-` prefix.

```go
pub := pubsub.NewPublisher(client, pubsub.PublisherConfig{
    ProjectID: "my-project",
    TopicID:   "my-topic",
}, logger)
```

## HTTP Content Modes

The HTTP subscriber (`pkg/adapter/httpdriven`) accepts CloudEvents in three ways:

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
- `type`: `http.command`
- `source`: derived from request URL

```
POST /command
Content-Type: application/json

{"name": "test", "age": 25}
```

## Test scenarios

See `test.http` for ready-to-use requests:

| Scenario | Flow | Ce-Type |
|---|---|---|
| 1 — Direct webhook | HTTP → handler → webhook.site | `http.command.webhook` |
| 2 — Via Pub/Sub | HTTP → handler → Pub/Sub → handler → webhook.site | `http.command` |
| Fallback | HTTP (no CE headers) → handler → Pub/Sub | plain JSON |
