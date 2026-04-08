package example

import (
	"time"

	"github.com/norlis/event-driven/pkg/adapter/cefilter"
	"github.com/norlis/event-driven/pkg/adapter/httpdriven"
	"github.com/norlis/event-driven/pkg/adapter/jmspath"
	"github.com/norlis/event-driven/pkg/application/router"
	"github.com/norlis/event-driven/pkg/port"
	"go.uber.org/zap"
)

func RegisterEventHandlers(params EventParams, routers RouterParams, logger *zap.Logger, publisher port.Publisher) {
	webhookPub := httpdriven.NewHTTPPublisher(httpdriven.HTTPPublisherConfig{
		TargetURL: "https://webhook.site/cd8bbff8-0c50-4770-a09f-06ea53f464b3",
		Timeout:   5 * time.Second,
	}, logger.Named("webhook-publisher"))

	// ── PrincipalMux (Pub/Sub → handler) ────────────────────────────────

	// PS-1: Result events arriving via Pub/Sub → forward to webhook.
	// This is the second leg of Scenario 2: HTTP → Pub/Sub → here → webhook.
	routers.PrincipalMux.Register(
		webhookPub,
		cefilter.ByType("http.command.result"),
		Person{},
		router.WrapHandler(params.Handler1.Execute),
	)

	// PS-2: Pub/Sub events by CE type → publish result to GCP topic.
	routers.PrincipalMux.Register(
		publisher,
		cefilter.ByType("com.example.person.created", "com.example.person.updated"),
		Person{},
		router.WrapHandler(params.Handler1.Execute),
	)

	// PS-3: Pub/Sub events by CE type + JMESPath → publish to webhook.
	routers.PrincipalMux.Register(
		webhookPub,
		cefilter.All(
			cefilter.ByType("com.example.person.command"),
			jmspath.New("contains(['webhook'], name)", logger.Named("jmes-person-filter")),
		),
		Person{},
		router.WrapHandler(params.Handler1.Execute),
	)

	// PS-4: Pub/Sub catch-all by JMESPath → fire-and-forget (no publish).
	routers.PrincipalMux.Register(
		nil,
		jmspath.New("contains(['test', 'test-x'], name)", logger.Named("jmes-filter")),
		Person{},
		router.WrapHandler(params.Handler1.Execute),
	)

	// ── HttpMux (HTTP → handler) ────────────────────────────────────────

	// HTTP-1: Scenario 1 → publish result directly to webhook.
	routers.HttpMux.Register(
		webhookPub,
		cefilter.ByType("http.command.webhook"),
		Person{},
		router.WrapHandler(params.Handler1.Command),
	)

	// HTTP-2: Scenario 2 first leg → publish result to Pub/Sub topic.
	// The result (type *.result) is read by PS-1 and forwarded to webhook.
	routers.HttpMux.Register(
		publisher,
		cefilter.ByType("http.command"),
		Person{},
		router.WrapHandler(params.Handler1.Command),
	)

	// HTTP-3: JMESPath filter → publish result to GCP topic.
	routers.HttpMux.Register(
		publisher,
		jmspath.New("contains(['webhook', 'pepe'], name)", logger.Named("jmes-http-filter")),
		Person{},
		router.WrapHandler(params.Handler1.Command),
	)
}
