package example

import (
	"log/slog"
	"time"

	"github.com/norlis/event-driven/pkg/eventmux"
	"github.com/norlis/event-driven/pkg/filter/cefilter"
	"github.com/norlis/event-driven/pkg/filter/jmespath"
	"github.com/norlis/event-driven/pkg/provider/eventhttp"
)

// RegisterEventHandlers wires routes onto both muxes. The four scenarios
// below cover the two transports in both directions:
//
//	HTTP-1: HTTP command → Pub/Sub topic           (cross: HTTP → Pub/Sub)
//	HTTP-2: HTTP command + JMESPath → webhook
//	PS-1:   Pub/Sub result event → webhook         (cross: Pub/Sub → HTTP)
//	PS-2:   Pub/Sub event by CE type → Pub/Sub topic
func RegisterEventHandlers(params EventParams, routers RouterParams, logger *slog.Logger, pubsubPub eventmux.Publisher) {
	webhookPub := eventhttp.NewPublisher(eventhttp.PublisherConfig{
		TargetURL: WebhookURL(),
		Timeout:   5 * time.Second,
	}, logger.With(slog.String("logger", "webhook-publisher")))

	// ── HttpMux (HTTP → handler) ────────────────────────────────────────

	// HTTP-1: HTTP command → Pub/Sub topic.
	// The result (*.result) is consumed by PS-1 and forwarded to the webhook,
	// completing the HTTP → Pub/Sub → HTTP round trip.
	routers.HttpMux.Register(
		pubsubPub,
		cefilter.ByType("http.command"),
		Person{},
		eventmux.Wrap(params.Handler.Command),
	)

	// HTTP-2: HTTP command + JMESPath filter on payload → webhook directly.
	routers.HttpMux.Register(
		webhookPub,
		cefilter.All(
			cefilter.ByType("http.command.webhook"),
			jmespath.New("contains(['webhook', 'pepe'], name)", logger.With(slog.String("logger", "jmes-http"))),
		),
		Person{},
		eventmux.Wrap(params.Handler.Command),
	)

	// ── PrincipalMux (Pub/Sub → handler) ────────────────────────────────

	// PS-1: Result events from HTTP-1 land here as `http.command.result` and
	// get forwarded to the webhook.
	routers.PrincipalMux.Register(
		webhookPub,
		cefilter.ByType("http.command.result"),
		Person{},
		eventmux.Wrap(params.Handler.Execute),
	)

	// PS-2: Domain events by CE type → publish result to Pub/Sub topic.
	routers.PrincipalMux.Register(
		pubsubPub,
		cefilter.ByType("com.example.person.created", "com.example.person.updated"),
		Person{},
		eventmux.Wrap(params.Handler.Execute),
	)
}
