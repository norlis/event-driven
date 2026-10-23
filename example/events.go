// Package example wires the eventmux library against GCP Pub/Sub + HTTP
// using uber/fx and demonstrates four cross-transport routing scenarios.
package example

import (
	"fmt"
	"log/slog"
	"time"

	"github.com/norlis/event-driven/v2/pkg/eventmux"
	"github.com/norlis/event-driven/v2/pkg/filter/cefilter"
	"github.com/norlis/event-driven/v2/pkg/filter/jmespath"
	"github.com/norlis/event-driven/v2/pkg/transport/eventhttp"
)

// RegisterEventHandlers wires routes onto both muxes. The four scenarios
// below cover the two transports in both directions:
//
//	HTTP-1: HTTP command → Pub/Sub topic           (cross: HTTP → Pub/Sub)
//	HTTP-2: HTTP command + JMESPath → webhook
//	PS-1:   Pub/Sub result event → webhook         (cross: Pub/Sub → HTTP)
//	PS-2:   Pub/Sub event by CE type → Pub/Sub topic
func RegisterEventHandlers(pubsubPub eventmux.Publisher, params EventParams, routers RouterParams, logger *slog.Logger) error {
	webhookPub, err := eventhttp.NewPublisher(eventhttp.PublisherConfig{
		TargetURL: WebhookURL(),
		Timeout:   5 * time.Second,
	}, logger)
	if err != nil {
		return fmt.Errorf("example: webhook publisher: %w", err)
	}
	webhookFilter, err := jmespath.New("contains(['webhook', 'pepe'], name)", logger)
	if err != nil {
		return fmt.Errorf("example: webhook filter: %w", err)
	}

	// ── HTTPMux (HTTP → handler) ────────────────────────────────────────

	// HTTP-1: HTTP command → Pub/Sub topic.
	// The result (*.result) is consumed by PS-1 and forwarded to the webhook,
	// completing the HTTP → Pub/Sub → HTTP round trip.
	routers.HTTPMux.Handle(
		pubsubPub,
		cefilter.ByType("http.command"),
		params.Handler.Command,
	)

	// HTTP-2: HTTP command + JMESPath filter on payload → webhook directly.
	routers.HTTPMux.Handle(
		webhookPub,
		cefilter.All(
			cefilter.ByType("http.command.webhook"),
			webhookFilter,
		),
		params.Handler.Command,
	)

	// ── PubSubMux (Pub/Sub → handler) ───────────────────────────────────

	// PS-1: Result events from HTTP-1 land here as `http.command.result` and
	// get forwarded to the webhook.
	routers.PubSubMux.Handle(
		webhookPub,
		cefilter.ByType("http.command.result"),
		params.Handler.Execute,
	)

	// PS-2: Domain events by CE type → publish result to Pub/Sub topic.
	routers.PubSubMux.Handle(
		pubsubPub,
		cefilter.ByType("com.example.person.created", "com.example.person.updated"),
		params.Handler.Execute,
	)
	return nil
}
