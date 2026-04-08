package main

import (
	"fmt"
	"log"
	"net/http"
	"time"

	"cloud.google.com/go/pubsub/v2"
	"github.com/norlis/event-driven/example"
	pb "github.com/norlis/event-driven/pkg/adapter/pubsub"
	"github.com/norlis/httpgate/pkg/adapter/apidriven/middleware"
	"github.com/norlis/httpgate/pkg/adapter/apidriven/presenters"
	"github.com/norlis/httpgate/pkg/application/health"
	port2 "github.com/norlis/httpgate/pkg/port"
	"go.uber.org/fx"
	"go.uber.org/fx/fxevent"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

var (
	GitHash string
	Date    string
)

// banner
// https://patorjk.com/software/taag/#p=display&f=DiamFont&t=EXAMPLE
const banner = `
▗▄▄▄▖▗▖  ▗▖ ▗▄▖ ▗▖  ▗▖▗▄▄▖ ▗▖   ▗▄▄▄▖
▐▌    ▝▚▞▘ ▐▌ ▐▌▐▛▚▞▜▌▐▌ ▐▌▐▌   ▐▌
▐▛▀▀▘  ▐▌  ▐▛▀▜▌▐▌  ▐▌▐▛▀▘ ▐▌   ▐▛▀▀▘
▐▙▄▄▖▗▞▘▝▚▖▐▌ ▐▌▐▌  ▐▌▐▌   ▐▙▄▄▖▐▙▄▄▖
`

func main() {
	fmt.Print(banner)

	app := fx.New(
		fx.WithLogger(func(log *zap.Logger) fxevent.Logger {
			return &fxevent.ZapLogger{Logger: log.WithOptions(zap.IncreaseLevel(zapcore.WarnLevel))}
		}),

		// fx.StartTimeout(10*time.Second),
		fx.Provide(example.NewConfigurationExample),
		fx.Provide(example.NewLogger),
		fx.Provide(example.NewHttpServerMux),
		fx.Provide(example.NewPubSubClient),
		fx.Provide(
			fx.Annotate(
				example.NewHttpSubscriber,
				fx.ResultTags(`name:"HttpSubscription"`),
			),
		),
		fx.Provide(
			fx.Annotate(
				example.NewAppSubscription,
				fx.ResultTags(`name:"AppSubscription"`),
			),
		),
		fx.Provide(example.NewEventPublisher),
		fx.Provide(
			fx.Annotate(
				example.NewPrincipalMux,
				fx.ResultTags(`name:"PrincipalMux"`),
			),
		),
		fx.Provide(
			fx.Annotate(
				example.NewHttpMux,
				fx.ResultTags(`name:"HttpMux"`),
			),
		),
		fx.Provide(example.NewHandler),
		fx.Provide(func() *health.Status {
			return health.NewStatus(GitHash)
		}),
		fx.Provide(presenters.NewPresenters),
		fx.Invoke(example.RegisterEventHandlers),
		fx.Provide(func(client *pubsub.Client, cfg *example.Configuration) *pb.GooglePubSubHealthChecker {
			return pb.NewGooglePubSubHealthChecker(
				client,
				cfg.Cloud.GCloudProjectId,
				pb.WithTopics(cfg.Messaging.PublishDestinationTopic),
				pb.WithSubscriptions(cfg.Messaging.SubscribeDestination),
			)
		}),
		fx.Invoke(func(router *http.ServeMux, status *health.Status, logger *zap.Logger, render presenters.Presenters, checker *pb.GooglePubSubHealthChecker) {
			commons := []middleware.Middleware{
				middleware.Recover(logger, render),
				middleware.RequestLogger(logger),
				middleware.NewCors(
					middleware.CorsOptions{
						Logger:           logger,
						AllowedOrigins:   []string{"*"},
						AllowedMethods:   []string{http.MethodHead, http.MethodGet, http.MethodPost, http.MethodPut, http.MethodPatch, http.MethodDelete},
						AllowedHeaders:   []string{"*"},
						AllowCredentials: true,
						MaxAge:           int((12 * time.Hour).Seconds()),
					},
				).Middleware,
			}

			public := middleware.Chain(commons...)

			base := http.NewServeMux()

			base.Handle("GET /status", status)
			base.Handle("GET /live", health.NewProbe(nil))
			base.Handle("GET /ready", health.NewProbe(map[string]port2.Checker{
				"pub/sub": checker,
			}))

			api := http.NewServeMux()
			api.HandleFunc("GET /test", func(w http.ResponseWriter, r *http.Request) {
				w.WriteHeader(http.StatusOK)
				w.Header().Set("Content-Type", "application/json")
				_, _ = w.Write([]byte("api test"))
			})

			router.Handle("/", public(base))
			router.Handle("/api/", public(http.StripPrefix("/api", api)))
		}),
	)

	if err := app.Err(); err != nil {
		log.Panicf("Error en la inicialización de la aplicación FX: %v\n", err)
	}

	app.Run()
}
