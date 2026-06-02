package main

import (
	"fmt"
	"log"
	"net/http"

	sdkpubsub "cloud.google.com/go/pubsub/v2"
	"github.com/norlis/event-driven/example"
	"github.com/norlis/event-driven/pkg/kit/fxmux"
	"github.com/norlis/event-driven/pkg/transport/gcp/pubsub"
	"github.com/norlis/httpgate/health"
	"go.uber.org/fx"
)

var GitHash string

const banner = `
▗▄▄▄▖▗▖  ▗▖ ▗▄▖ ▗▖  ▗▖▗▄▄▖ ▗▖   ▗▄▄▄▖
▐▌    ▝▚▞▘ ▐▌ ▐▌▐▛▚▞▜▌▐▌ ▐▌▐▌   ▐▌
▐▛▀▀▘  ▐▌  ▐▛▀▜▌▐▌  ▐▌▐▛▀▘ ▐▌   ▐▛▀▀▘
▐▙▄▄▖▗▞▘▝▚▖▐▌ ▐▌▐▌  ▐▌▐▌   ▐▙▄▄▖▐▙▄▄▖
`

func main() {
	fmt.Print(banner)

	app := fx.New(
		fx.WithLogger(fxmux.NewLogger),

		fx.Provide(example.NewLogger),
		fx.Provide(example.NewHTTPServerMux),
		fx.Provide(example.NewPubSubClient),
		fx.Provide(example.NewHandler),
		fx.Provide(example.NewEventPublisher),
		fx.Provide(fx.Annotate(example.NewHTTPSubscriber, fx.ResultTags(`name:"HTTPSubscription"`))),
		fx.Provide(fx.Annotate(example.NewAppSubscription, fx.ResultTags(`name:"AppSubscription"`))),
		fx.Provide(fx.Annotate(example.NewHTTPMux, fx.ResultTags(`name:"HTTPMux"`))),
		fx.Provide(fx.Annotate(example.NewPrincipalMux, fx.ResultTags(`name:"PrincipalMux"`))),

		fx.Provide(func() *health.Status { return health.NewStatus(GitHash) }),
		fx.Provide(func(c *sdkpubsub.Client) *pubsub.HealthChecker {
			return pubsub.NewHealthChecker(
				c,
				example.ProjectID(),
				pubsub.WithTopics(example.PublishTopic()),
				pubsub.WithSubscriptions(example.SubscriptionID()),
			)
		}),

		fx.Invoke(example.RegisterEventHandlers),
		fx.Invoke(func(router *http.ServeMux, status *health.Status, checker *pubsub.HealthChecker) {
			router.Handle("GET /status", status)
			router.Handle("GET /live", health.NewProbe(nil))
			router.Handle("GET /ready", health.NewProbe(map[string]health.Checker{
				"pub/sub": checker,
			}))
		}),
	)

	if err := app.Err(); err != nil {
		log.Panicf("FX init error: %v\n", err)
	}

	app.Run()
}
