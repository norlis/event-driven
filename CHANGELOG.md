# Changelog

## v2.0.0-rc.3 (unreleased)

Changes since `v2.0.0-rc.2`.

### Fixed

- `aws/sqs.Subscriber`: every message now carries a `Delivery.Nack` (a no-op: not deleting lets the visibility timeout redeliver). In rc.2 it was nil, so the router treated SQS as a transport that cannot redeliver: every retryable handler error was discarded on the first delivery with reason `noredelivery` (copied to `Config.DeadLetter` when set, otherwise only logged) instead of being retried, and `Config.MaxRetries` never applied to SQS.

## v2.0.0-rc.1 (unreleased)

Changes since `v1.0.0-rc.1`.

### Incompatible

- Module path is `github.com/norlis/event-driven/v2`. Update every import to `github.com/norlis/event-driven/v2/pkg/...`.
- `eventmux.Publisher` is now `Publish(ctx context.Context, ce cloudevents.Event) error`. Every transport publisher honours the context's cancellation and deadline for the outbound call and reads the W3C trace context from it.
- Logging follows the platform logging standard through [`httpgate/logging`](https://github.com/norlis/httpgate): OTel field names (`messaging.message.id`, `messaging.destination.name`, `cloudevents.event_type`, `log.logger`, …), structured `error.type` / `error.message` / `error.stack_trace` objects and fixed lowercase messages. Every key and message from v1 (`id`, `totalRoutes`, `Route registered`, `Mux starting subscription...`, …) changed: update dashboards and alerts. The catalog is in `docs/superpowers/specs/2026-08-12-logging-standard-design.md`.
- `eventhttp.Response.JSON` takes only the `http.ResponseWriter`; the request was never read. `logfields.KeyMessageBodySize` and `logfields.KeyPayloadType` are removed: nothing emitted them.
- `Mux.Register`, `eventmux.Wrap` and `eventmux.Route` are removed; `Mux.Handle[T](pub, filter, func(context.Context, T) (jsontext.Value, error))` replaces them. The payload type comes from the handler, so a mismatch is impossible; a payload that does not decode is dead-lettered with reason `decode` as before.
- `Handle`, `Use` and `UsePreflight` panic when called after `Start` (`eventmux: <method> after Start`), and a second `Handle` with the same filter panics (`eventmux: duplicate route`) whatever its publisher: the first matching route wins, so the second one could never run. Middleware chains are built once at `Start`.
- Middleware order is documented as it always worked: the first middleware passed to `Use` (or `Chain`) is the outermost. The README example now registers `skiperr` before `retry`.
- One CloudEvents codec for every transport (`pkg/transport/internal/envelope`): `ce-time` is written as RFC 3339 with nanoseconds; non-string extensions (bool, int32, URI, Timestamp) are serialized instead of dropped; extensions use the `ce-` prefix on every transport (Pub/Sub previously wrote and read them without it, and no longer treats unprefixed attributes as extensions; during a rolling upgrade a v1 and a v2 service on the same Pub/Sub topic lose every extension, `correlationid` included, in both directions until both sides run v2); a `ce-*` attribute repeated with a different letter case is a decode error; a missing `ce-time` leaves the event time zero instead of `time.Now()`; an invalid `ce-time` or an invalid `application/cloudevents+json` envelope is a decode error (dead-lettered) instead of a silent fallback to binary mode; the transport `Content-Type` is written from `datacontenttype` (default `application/json`) on Pub/Sub, SNS, NATS and HTTP, and read back on Pub/Sub, SQS and NATS, so SQS now accepts structured mode; the NATS codec fills a missing `ce-id` with `Nats-Msg-Id` or a UUID v7; `dataschema` round-trips as `ce-dataschema` and a `ce-datacontenttype` attribute from another binding is ignored; a structured envelope missing required attributes (`id`, `type`, `source`) is a decode error too.
- `retry.HTTPBackoff` is renamed `retry.Backoff`, and it panics on an invalid configuration (`maxRetries < 0`, `baseDelay <= 0` or `maxDelay < baseDelay`): a negative `maxRetries` used to ack every message without calling the handler.
- Removed: `event.ErrSubscriptionFailed`, `ErrFilterFailed`, `ErrHandlerFailed`; `sqs.GenerateDeleteMessageInputFunc`, `GenerateDeleteMessageInputDefault`, `SubscriberConfig.GenerateDeleteMessageInput`; `sqs.NoSleep`.
- `go.mod` requires Go 1.27.
- Event payloads are decoded with `encoding/json/v2`: field names are matched case-sensitively, and duplicate member names, invalid UTF-8 and trailing data after the value are rejected. A payload rejected this way fails before the handler runs like any other decode error, and the error wraps the new `event.ErrDecode` sentinel.
- Payload types with `time.Duration` fields no longer decode (`no default representation`; Go 1.27 offers no struct tag for it): use an `int64` field in nanoseconds, or a named type that implements `UnmarshalText` (e.g. via `time.ParseDuration`) to accept `"1s"`. Fixed-size array fields (`[N]T`) now require exactly N elements, and a `,string` tag on a `string` field is an error. Every event of such a type fails to decode and is discarded on the first delivery (copied to `eventmux.Config.DeadLetter` when configured, otherwise only logged).
- The JMESPath filter decodes with the same rules: a payload rejected by `encoding/json/v2` is "no match" (logged at WARN as `filter payload decode failed`).
- `HandlerFunc` and the `validate`, `retry` and `skiperr` middlewares declare `jsontext.Value` instead of `json.RawMessage`. Source compatible: `json.RawMessage` is an alias of `jsontext.Value` since Go 1.27.
- Generated IDs (result-event `id` and the fallback message ID when `X-Message-UUID` is missing) are UUID v7 (time-ordered) instead of v4.
- The `eventhttp` subscriber response body no longer ends with a trailing newline.
- The `eventhttp` subscriber answers `400 INVALID-PAYLOAD` when the payload cannot be decoded into the route's type (previously `202 Accepted` although the handler never ran).
- The `eventhttp` subscriber answers a plain `500` when its JSON response cannot be encoded, instead of a `202` whose body was the encoder error text. Client-supplied `id` and `type` that are not valid UTF-8 are rejected earlier with `400 INVALID-REQUEST`, so this path is not reachable from client input.
- `event.Message`: `event.NewMessage(ce, ackFn, nackFn)` is now `event.NewMessage(parent, ce, event.Delivery{Ack, Nack, Discard, Count})`; `Nack()` is now `Nack(error)`; new `Discard(error)` (acknowledge and drop, recording the cause), `Outcome() (Outcome, error)` and `DeliveryCount()` (the broker's 1-based delivery number, 0 = unknown). `NewMessageWithoutAck`, `PreflightCallback`, `SetPreflightCallback` and `NotifyPreflightDone` are removed.
- A message whose transport envelope cannot be decoded (an invalid `ce-time`, a broken `application/cloudevents+json` body, a custom `Unmarshaler` error) is no longer confirmed and logged by the transport (`message unmarshal failed`). Every broker subscriber (SQS, Pub/Sub, JetStream, Core NATS) now hands the router an undecodable rescue message (`event.NewUndecodable`: broker id, the transport's default type and source, the raw body as data) and the router applies its poison-message policy: `Discard` with reason `decode`, a recoverable copy to `eventmux.Config.DeadLetter` when configured, one `message discarded` / `message dead-lettered` record and `Observer.OnMessage`. Consequences: JetStream terminates with reason `discarded by eventmux` instead of `unmarshal failed`; when the dead-letter publish fails with a retryable error the message is nacked and redelivered (bounded by `MaxRetries` or the broker's own limit), where it used to be dropped. The HTTP subscriber still answers `400` itself.
- The codec seams with a single implementation are removed: `pubsub.Marshaler` / `DefaultMarshaler` / `PublisherConfig.Marshaler`, `pubsub.Unmarshaler` / `DefaultUnmarshaler` / `SubscriberConfig.Unmarshaler`, `sqs.Unmarshaler` / `DefaultUnmarshaler` / `SubscriberConfig.Unmarshaler`, `codec.Unmarshaler` / `DefaultUnmarshaler` and the `Unmarshaler` field of the NATS core and JetStream `SubscriberConfig`. Those transports always use the shared envelope codec; `codec.Unmarshal(headers, data)` is the NATS decoder as a function. `codec.Marshaler` (a subject derived from the event, see `example/nats`) and `sns.Marshaler` (FIFO attributes) stay pluggable.
- Removed without replacement: `eventhttp.ResponseBuilder` / `NewResponseBuilder` (the subscriber and `ErrorResponder` fill `eventhttp.Response` themselves; `Response` and `Response.JSON` stay), `eventmux.Mux.Logger()`, `metadata.Store.Get` and the `pkg/kit/signal` package (`lifecycle.Group.Run` listens with `os/signal`; use `signal.NotifyContext(ctx, os.Interrupt, syscall.SIGTERM)` directly).
- `fxmux.NewLogger` reports a failed start once (`app start failed`) and a failed stop once (`app stop failed`): the `onstart hook failed`, `onstop hook failed` and `rolling back after start failure` records, which repeated the same error, are gone. `rollback failed` stays. Update alerts that matched those messages.
- `eventmux.OnErrorFunc` is removed: `Mux.Start(ctx, onError func(error))`, `lifecycle.Component.Start(ctx, onFatal func(error))`, `lifecycle.Group.Start`, `httpserver.Server.Start` and `natsconn.Conn.Start` take a plain `func(error)`. `eventmux.ErrAlreadyStarted` moves to `event.ErrAlreadyStarted` (message `event: already started`) and is returned by `Mux.Start` and by every transport subscriber. Neither the transports nor `pkg/kit` import `eventmux` any more.
- `eventmux.Config.ReportOnNoMatch` is removed: a message with no matching route is always discarded (`Outcome() = Discarded, ErrNoRoute`) and logged at INFO (`no route matched`) instead of DEBUG. A `NonRetryableError` from the handler is now `Discard(err)` (still an ack on brokers).
- Every per-message mux record carries `messaging.message.correlation_id` when the event has a `correlationid` extension. A handler that sets `correlationid` through `metadata.Store` is ignored with a WARN (`reserved extension ignored`).
- `eventhttp.NewSubscriber(server, cfg, logger) (*Subscriber, error)`: `SubscriberConfig` loses `Logger` and gains `DefaultType` (required, replaces the hard-coded `com.example.http.command`), `MaxBodyBytes` (default 256 KiB, the SNS/SQS message limit) and `ErrorRules` (evaluated before the defaults). The route is registered in `NewSubscriber`; `Start` only wires the handler (503 before that) and a second `Start` returns `ErrAlreadyStarted`.
- HTTP contract: `202` is returned only when the router acked the event. A handler failure or a discarded event without a matching rule is `500 HANDLER-FAILED` with a generic detail (previously `202`). Malformed CloudEvents, unreadable bodies and invalid `X-Message-UUID` are `400 INVALID-REQUEST` (previously `500 "001"`); bodies above `MaxBodyBytes` are `413 PAYLOAD-TOO-LARGE`; plain requests without `application/json` are `415 UNSUPPORTED-MEDIA-TYPE`. The 5-second preflight timeout is gone.
- `eventhttp.Response`: `requestId` and `sessionId` are removed; `type` and `correlationId` are added; `instance` is always the ServeMux pattern. `ResponseBuilder.WithID`/`WithName` are replaced by `WithEvent(ce)`. `ErrorRule.ErrorCode` is `eventhttp.ErrorCode` and `ErrorRule.LogMessage` is removed; `NewRuleForType` takes a fixed detail instead of a log message (`NewRuleForTypeWithDetail` derives it from the error); `NewRuleForValue(target, status, code, detail)` loses its log-message parameter; `NewErrorResponder` accepts extra rules; `ErrorResponder.Respond` takes the event and the instance instead of a logger and a message ID.
- Every event entering through `eventhttp` carries a `correlationid` extension: `Ce-Correlationid` (binary), `correlationid` (structured) or `X-Correlation-ID` (plain), validated as 1–128 bytes of printable ASCII without spaces (`400 INVALID-CORRELATION-ID` otherwise), or the event `id` when absent. It is propagated unchanged to result events.
- `aws/sns.DefaultMarshaler` keeps the binary mode while the event fits 9 `MessageAttributes` (the 10th, the SQS limit, is the native `traceparent` that `Publisher.Publish` adds). Above that it publishes the event in structured mode: the whole CloudEvent as JSON in the SNS `Message` (valid JSON data inline as `data`, anything else as `data_base64`), and only `Content-Type: application/cloudevents+json`, `ce-id`, `ce-type`, `ce-source` and `traceparent` as attributes. Result events with handler metadata, dead-letter copies and events with many extensions are therefore published instead of rejected. Consumers built on this library (`sqs.Subscriber`) need no change; a consumer that only reads binary `ce-*` attributes receives those events in structured mode, and only SNS filter policies on `ce-type` and `ce-source` are reliable: a policy on any other `ce-*` attribute (`ce-correlationid`, a custom extension) misses the events that go out in structured mode, and a `FilterPolicyScope: MessageBody` policy misses the binary ones. To filter on other fields, publish with a custom `Marshaler` that always uses structured mode. The `event published` DEBUG record carries `cloudevents.content_mode`. `sns.Publisher` still rejects input with more than 10 attributes with `ErrTooManyAttributes` wrapped in `event.NonRetryableError`, which now only a custom `Marshaler` can produce. `NewPublisher` accepts the `PublishAPI` interface (`*sns.Client` satisfies it).
- Poison-message policy: a payload that does not decode, a preflight (validation) failure, a panic and a `NonRetryableError` are discarded on the **first** delivery instead of nacked and redelivered. Every discarded message is copied to `eventmux.Config.DeadLetter` when set, as the original CloudEvent plus the reserved extensions `deadletterreason` (`decode` | `validation` | `panic` | `nonretryable` | `maxretries` | `publish`) and `deadlettererror` (≤ 256 bytes). "No route" is still discarded without a dead-letter copy. A dead-letter publish that fails with a `NonRetryableError`, or on a delivery already past `MaxRetries`, drops the copy and still discards (`messaging.deadletter.publish_error` in the log); any other publish error nacks the message so the copy is retried on redelivery. `deadlettererror` is sanitized (valid UTF-8, no control characters) so a broker cannot reject the copy. Handler metadata with the keys `correlationid`, `deadletterreason` or `deadlettererror` is ignored (`reserved extension ignored` WARN) and result events never carry the two dead-letter extensions.
- `eventmux.Config.MaxRetries` limits redeliveries of retryable handler errors when the transport reports a delivery count (JetStream always; SQS via `ApproximateReceiveCount`, requested by the default `ReceiveMessageInput`; Pub/Sub only when the subscription has a dead-letter policy). Without a count the error is nacked as before and a single WARN is logged per mux.
- `pkg/middleware/recover` is removed: the mux always recovers panics from preflight and handlers, logs the panic-site stack trace in `error.stack_trace` and discards the message with `event.ErrPanic` as cause (HTTP: `500 HANDLER-FAILED`).
- JetStream: a discarded message is `TermWithReason`-ed instead of acked; an envelope that fails to unmarshal is terminated instead of nacked. Pub/Sub and SQS acknowledge an envelope that fails to unmarshal instead of letting it redeliver; the mux dead-letters the raw body with reason `decode` and logs it once.
- Per-message mux logs carry `messaging.message.delivery_count` when known and `messaging.deadletter.reason` on discard; a payload that does not decode is logged as `message dead-lettered` / `message discarded` with `messaging.deadletter.reason=decode`.
- `eventmux.Subscription` is now `Start(ctx, handler, onError) error` (returns once the subscription is established; `ctx` bounds the setup calls only) plus `Stop(ctx) error` (drains in-flight handlers until `ctx` expires). A failure that stops delivery for good after `Start` is reported once through `onError`, never for a `Stop`.
- `eventmux.Mux`: `Start(ctx, onError)` and `Stop(ctx)` replace `Run` and `RunBackground`; a second `Start` returns the new `ErrAlreadyStarted`; `Stop` without `Start` is a no-op. `eventmux.New` returns `(*Mux, error)` and rejects a nil `Subscription`. Logs: `mux started`, `mux stopped`, `subscription failed` (ERROR).
- `fxmux.Bind` is removed. A mux is now a `lifecycle.Component`: add it to a `lifecycle.Group` and bind the group with `fxmux.BindGroup(lc, shutdowner, group, logger)`. One hook per group means the `Add` order decides start and stop (resources first, the HTTP server last), so a Pub/Sub publisher is closed after the muxes that publish through it have drained; with one hook per mux, fx's construction order decided it. `OnStop` uses fx's own stop context, so `fx.StopTimeout` governs the drain.
- `event.Delivery.Ack/Nack/Discard` are `func() error` and `Message.Ack/Nack/Discard` return the broker's error; the outcome is recorded either way. The mux logs a single `message ack failed` (ERROR, with `messaging.operation.outcome`) when the broker refuses a decision; transports no longer log ack failures themselves.
- `gcp/pubsub.NewPublisher` and `NewSubscriber` return `(T, error)` and validate client, `TopicID`/`SubscriptionID` and flow-control settings; `ProjectID` is removed from both configs. The publisher keeps a single `*pubsub.Publisher` (batching and flow control apply again); `Publisher.Close()` is new and must be called on shutdown to flush it. `Subscriber.Start` checks the subscription with `GetSubscription` (needs `pubsub.subscriptions.get`, part of `roles/pubsub.subscriber`).
- `jmespath.New` returns `(*Filter, error)`: the expression is compiled once and an invalid one fails at construction instead of logging a WARN per message.
- `eventhttp.NewPublisher` returns `(*Publisher, error)`: `TargetURL` must be an absolute http(s) URL and `Timeout` non-negative.
- `nats/jetstream.SubscriberConfig.AutoProvisionStream` is removed: a non-nil `StreamConfig` (whose `Name` must equal `Stream`) provisions at `Start`. `Stop` drains the consumer; a consumer the server stops without a `Stop` is reported through `onError` as `jetstream.ErrConsumeStopped`. Transient consume errors are logged at WARN (`consume error`).
- `aws/sqs.SubscriberConfig.Identity` is removed: a queue name is resolved with `GetQueueUrl` at `Start` (no STS, any partition, fails fast when the queue is missing). New `AckTimeout` (default 5 s) bounds each `DeleteMessage`, which runs on its own context so a shutdown no longer cancels the ack of a processed message. `Subscriber.Close()` is removed; `Stop(ctx)` cuts the long-poll and waits for the workers.
- `aws/sns.Publisher.Close()` (a no-op) is removed. A topic name resolves to `arn:<partition>:sns:…` using the caller's partition.
- `aws.Identity` caches only a successful `GetCallerIdentity` (a transient STS failure no longer disables the instance) and gains `Partition(ctx)`. `aws.NewConfig` no longer defaults the region to `us-east-1`.
- `eventhttp.ClientError` (`error` + `ClientDetail() string`) replaces the hard dependency on `middleware/validate`: the default 400 `VALIDATION-FAILED` rule matches any `ClientError`. `validate.Error` implements it and now `Unwrap`s the validator error, so `errors.As`/`skiperr.ByType[validator.ValidationErrors]` work.
- The handler's result is published **before** the incoming message is acknowledged. A publish failure nacks the message (retryable) or dead-letters it with the new reason `publish` (`NonRetryableError`, or a delivery already past `MaxRetries`); a result whose data cannot be encoded is dead-lettered too (previously the encoding error was ignored and the event published without data). Handlers must be idempotent. `eventmux.Config.PublishAfterAck` restores the previous Ack-then-publish order with the error only logged.
- `eventhttp.Publisher.Publish` wraps a `4xx` response other than `408` and `429` in `event.NonRetryableError`, so a result the destination rejects is dead-lettered instead of redelivered forever.
- `causationid` is a reserved extension: handler metadata with that key is ignored (`reserved extension ignored` WARN) and an incoming `causationid` is not propagated to the result event.
- An event without `correlationid` gets `correlationid = id` when it enters the mux, on every transport.

### Added

- `pkg/kit/lifecycle`: `Component` (`Start(ctx, onFatal func(error)) error` / `Stop(ctx) error`, satisfied by `*eventmux.Mux`), `Closer(fn)` for resources that only need closing, and `Group` (`NewGroup(logger, shutdownTimeout)`, `Add`, `Start`, `Stop`, `Run`): components start in `Add` order and stop in reverse; `Run` handles SIGINT/SIGTERM and returns `ErrComponentFailed` when a component dies. The group logs only DEBUG `component started` / `component stopped` (`lifecycle.component`); failures are logged once by the component that suffers them.
- `pkg/kit/httpserver`: an `http.Server` with httpgate's hardened defaults as a component. The bind happens in `Start`, so a busy port aborts startup; a nil handler panics instead of silently serving `http.DefaultServeMux`.
- `pkg/transport/nats/natsconn`: `Dial(Config, logger)` returns a `*Conn` (embeds `*nats.Conn`) whose `Stop` drains and waits until the connection is closed, so pending JetStream publishes are flushed. The URL is logged and reported without credentials.
- `fxmux.BindGroup` and `logfields.KeyComponentName`.

- W3C `traceparent` propagation on every transport. Subscribers inherit the trace from the native carrier (HTTP `traceparent` header, Pub/Sub / SQS message attribute, NATS header) or the CloudEvents `traceparent` extension, and start a new trace when neither is valid; publishers write both the native carrier and the extension. `event.ContextWithTrace` and `event.InjectTraceFromContext` let a custom transport do the same.
- `pkg/kit/logfields`: the log field keys, the `Outcome` type (`OutcomeAcked`, `OutcomeNacked`, `OutcomeDiscarded`) with an `Attr()` helper, and `Component` / `Transport` to build component loggers.
- `retry.WithLogger` option for `retry.Backoff`.
- `retry.SkipOnRedelivery` option for `retry.Backoff`: when the broker reports a delivery count greater than 1 the handler runs once, without in-process retries, so `Backoff` and the broker's redeliveries (and `eventmux.Config.MaxRetries`) do not multiply. Transports without a delivery count are unaffected.
- `event.NewUndecodable(parent, ce, delivery, err)` and `Message.DecodeError()`: how a transport hands the router an envelope it could not decode. `envelope.Rescue` (internal) and `codec.Rescue(headers, data)` build the rescue event.
- `event.DeliveryCountFromContext(ctx)`: the broker's 1-based delivery number of the message behind a handler context, for middlewares and handlers. Set only when the transport reports one.
- `eventmux.Observer` and `eventmux.Config.Observer`: `OnMessage(ctx, msg, outcome, reason, elapsed)` once per message and `OnPublish(ctx, ce, err)` once per publish, for metrics and tracing without a telemetry dependency in the mux. A panic in the Observer is logged (`observer panicked`) and does not affect the message.
- `eventmux.Config.PublishAfterAck`, `event.ExtCausationID` (`causationid`, set on every result event to the incoming event `id`) and `event.ReasonPublish`.

### Fixed

- `lifecycle.Group`: a failure a component reports once `Stop` has begun (a JetStream consumer closed by its own drain) is no longer forwarded to `onFatal`, so an ordered shutdown never asks fx for exit code 1. The group's lock is not held while a component starts, so an `onFatal` may call `Stop` synchronously: `Start` then stops the component that had just started and returns the new `ErrStoppedDuringStart`.
- `eventmux.Observer.OnPublish` documents how to see a result lost after the ack with `Config.PublishAfterAck`: the call carries the publisher's error and the event's `causationid`.
- A retryable failure on a transport that cannot redeliver (Core NATS: `Delivery.Nack` nil, see the new `Message.CanNack`) is definitive: the message is dead-lettered with the new reason `event.ReasonNoRedelivery` (`noredelivery`) and discarded, instead of being logged and reported to the `Observer` as `nacked` while it was silently lost. A result publish that fails retryably there is dead-lettered with reason `publish`. The `eventhttp` subscriber keeps `nacked` semantics (its caller gets a 5xx and retries) through an explicit no-op `Nack`.
- A dead-letter copy that fails to publish with a retryable error nacks the message only while `Config.MaxRetries` bounds the redelivery (the transport reports a delivery count still within the limit). Without that bound, or on a transport without `Nack`, the copy is lost and the message discarded (`message discarded` with `messaging.deadletter.publish_error`): a deterministic failure no longer loops forever on a broken dead-letter topic.
- SQS, Pub/Sub, JetStream and Core NATS subscribers: a `Stop` whose `ctx` expires no longer forgets the running workers, receive loop or drain. The subscriber stays started (`Start` returns `event.ErrAlreadyStarted` instead of spawning a duplicate) and a later `Stop` waits for the same work again without draining twice.
- `sns.NewPublisher` bounds the STS lookup it performs for a topic name with the new `PublisherConfig.ResolveTimeout` (default `sns.DefaultResolveTimeout`, 10 s): an unreachable STS fails startup instead of hanging it.
- The `eventhttp` subscriber builds the `source` of a plain JSON request as `http://<host><path>` (`https://` behind TLS); it used to be `HTTP/1.1//<host><path>`, the protocol version instead of the scheme.
- `Mux.Stop` stays started when the transport's `Stop` fails (a drain that outlives `ctx`): a later `Stop` retries and a `Start` returns `event.ErrAlreadyStarted`, instead of the mux believing it was stopped while the transport kept delivering.
- A panic in a `Filter.Match` or in the payload decoder is recovered like a handler panic: the message is dead-lettered with reason `panic` and discarded, instead of unwinding into the broker SDK's goroutine. A panic after the decision (a `Delivery` callback, the dead-letter publisher) is logged (`panic after message decision`) and swallowed.
- `skiperr.ByErr(name, err)` logs `name` in `skiperr.predicate` instead of the error text.
- `retry`: the exponential delay no longer overflows into a negative duration on high attempt counts; it saturates at `maxDelay`.

### Changed

- The `metadata.Store` is only created for routes that have a Publisher: a handler on a terminal route (`Handle(nil, …)`) no longer finds one with `metadata.FromContext` (it returned `ok=true` with a store nobody read). The DEBUG `message received` record is built only when the logger accepts DEBUG.
- `go.uber.org/multierr` is no longer a direct dependency (`errors.Join`).
- `pubsub.NewHealthChecker` and `jetstream.NewHealthChecker` accept `WithTimeout(d)`; the previous fixed 15 s cap is now `DefaultCheckTimeout`.
- `eventhttp`'s private URL redaction moved to `pkg/transport/internal/redact` and now handles comma-separated URL lists; behaviour for a single URL is unchanged.
- The `eventhttp` default `User-Agent` is `go event-driven/<module version>` read from the binary's build info (was a stale `v1.0.0-beta.2` literal). Logs and errors of the HTTP publisher carry the target URL without userinfo, query or fragment, so credentials passed in the URL never reach them.
- Toolchain pinned to Go 1.27.3 (`mise.toml`, `go.mod`, `tools/go.mod`, `example/nats/Dockerfile`).
- `github.com/norlis/httpgate` v1.0.0 → v1.3.0, now also used for logging (`logging.New`, `logging.Err`) and trace context; records carrying `event.duration` also get `event.duration_human`.
- `resty.dev/v3` v3.0.0-rc.3 → v3.0.0-rc.4.
- `github.com/google/uuid` replaced by the standard library `uuid` package; depguard forbids it, and it remains only as an indirect dependency.
- `encoding/json` (v1) is forbidden repo-wide by depguard.
- `gopls` is no longer part of `tools/`; it is installed through `mise.toml`.

### Performance

Mux hot path (`BenchmarkHandleNoPublish` / `BenchmarkHandlePublish` in `pkg/eventmux/bench_test.go`, `-count=6`, Apple M3 Pro): `event.CorrelationID`, the trace lookup and result-extension propagation read the extension map in place instead of through `Event.Extensions()` (which copies it per call), hot INFO records use `LogAttrs`, and the per-message `metadata.Store` is skipped on terminal routes. Terminal route: 782 → 382 ns/op, 22 → 6 allocs/op. Publishing route: 1.74 → 1.24 µs/op, 38 → 23 allocs/op.

`encoding/json` v1 → v2, `go test -count=10 -bench . -benchmem` on an Apple M3 Pro, compared with `benchstat`. Measured during the migration: `DecodeInto` lived in `pkg/eventmux/decode_test.go`, removed when `Mux.Handle[T]` replaced the reflection-based decoder, so only `ResponseJSON` can be rerun today. Payload decoding is about 15% faster with the same allocations. `eventhttp` response encoding takes the same time, with one extra allocation (+9.5% bytes) because the body is now encoded before the headers are written.

```
pkg: github.com/norlis/event-driven/v2/pkg/eventmux
                    │ encoding-json-v1.txt │        encoding-json-v2.txt         │
                    │        sec/op        │   sec/op     vs base                │
DecodeInto/small-12           223.9n ±  1%   186.8n ± 1%  -16.57% (p=0.000 n=10)
DecodeInto/large-12           39.31µ ± 14%   33.63µ ± 1%  -14.45% (p=0.000 n=10)
geomean                       2.967µ         2.506µ       -15.51%

                    │ encoding-json-v1.txt │         encoding-json-v2.txt          │
                    │         B/op         │     B/op      vs base                 │
DecodeInto/small-12             48.00 ± 0%     48.00 ± 0%       ~ (p=1.000 n=10) ¹
DecodeInto/large-12           31.83Ki ± 0%   31.83Ki ± 0%       ~ (p=1.000 n=10)
geomean                       1.221Ki        1.221Ki       +0.00%
¹ all samples are equal

                    │ encoding-json-v1.txt │        encoding-json-v2.txt         │
                    │      allocs/op       │ allocs/op   vs base                 │
DecodeInto/small-12             2.000 ± 0%   2.000 ± 0%       ~ (p=1.000 n=10) ¹
DecodeInto/large-12             409.0 ± 0%   409.0 ± 0%       ~ (p=1.000 n=10) ¹
geomean                         28.60        28.60       +0.00%
¹ all samples are equal

                    │ encoding-json-v1.txt │         encoding-json-v2.txt         │
                    │         B/s          │     B/s       vs base                │
DecodeInto/large-12          105.8Mi ± 13%   123.7Mi ± 1%  +16.89% (p=0.000 n=10)

pkg: github.com/norlis/event-driven/v2/pkg/transport/eventhttp
                │ encoding-json-v1.txt │     encoding-json-v2.txt      │
                │        sec/op        │   sec/op     vs base          │
ResponseJSON-12            608.5n ± 1%   605.9n ± 2%  ~ (p=0.172 n=10)

                │ encoding-json-v1.txt │        encoding-json-v2.txt         │
                │         B/op         │     B/op      vs base               │
ResponseJSON-12           1.156Ki ± 0%   1.267Ki ± 0%  +9.54% (p=0.000 n=10)

                │ encoding-json-v1.txt │        encoding-json-v2.txt        │
                │      allocs/op       │ allocs/op   vs base                │
ResponseJSON-12             10.00 ± 0%   11.00 ± 0%  +10.00% (p=0.000 n=10)
```
