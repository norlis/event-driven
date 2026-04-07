package main

import (
	"fmt"
	"log"
	"net/http"
	"time"

	"cloud.google.com/go/pubsub/v2"
	"github.com/norlis/event-driven/example"
	pb "github.com/norlis/event-driven/pkg/adapter/pubsub"
	"github.com/norlis/event-driven/pkg/application/worker"
	"github.com/norlis/event-driven/pkg/port"
	"github.com/norlis/httpgate/pkg/adapter/apidriven/middleware"
	"github.com/norlis/httpgate/pkg/adapter/apidriven/presenters"
	"github.com/norlis/httpgate/pkg/application/health"
	port2 "github.com/norlis/httpgate/pkg/port"
	"go.uber.org/fx"
	"go.uber.org/zap"
)

var (
	GitHash string
	Date    string
)

type AppComponents struct {
	fx.In

	Logger         *zap.Logger
	Config         *example.Configuration // Asumiendo que tienes esto
	PubSubClient   *pubsub.Client         // Cliente Pub/Sub real
	Dispatcher     *worker.Dispatcher
	Routers        example.RouterParams // Contiene PrincipalRouter y TraceRouter
	EventParams    example.EventParams  // Contiene Handler1, etc. para RegisterEventHandlers
	EventPublisher port.Publisher       // Si lo necesitas para RegisterEventHandlers
	// Añade aquí cualquier otra dependencia que RegisterEventHandlers o la lógica de inicio necesite
}

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
	// ctx := context.Background()

	app := fx.New(
		fx.StartTimeout(10*time.Second),
		// fx.StopTimeout(120*time.Second),
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
		fx.Provide(example.NewWorkerDispatcher),
		fx.Provide(
			fx.Annotate(
				example.NewPrincipalRouter,
				fx.ResultTags(`name:"PrincipalRouter"`),
			),
		),
		fx.Provide(
			fx.Annotate(
				example.NewHttpRouter,
				fx.ResultTags(`name:"HttpRouter"`),
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
			// pprof
			// "net/http/pprof"
			// router.HandleFunc("GET /debug/pprof/", pprof.Index)
			// router.HandleFunc("GET /debug/pprof/cmdline", pprof.Cmdline)
			// router.HandleFunc("GET /debug/pprof/profile", pprof.Profile)
			// router.HandleFunc("GET /debug/pprof/symbol", pprof.Symbol)
			// router.HandleFunc("GET /debug/pprof/trace", pprof.Trace)

			// opaConfig := opa.Config{
			//	Query:        "data.authz.allow",
			//	PoliciesPath: "policies/authz", // Directorio con authz.rego
			//	DataFiles: []string{
			//		//"policies/authz/whitelist.json",
			//		//"policies/authz/roles.json",
			//		//"policies/authz/permissions.json",
			//	},
			//}

			// authz, err := opa.NewOpaSdkClientFromConfig(context.Background(), opaConfig, logger)

			// if err != nil {
			//	log.Fatalf("No se pudo inicializar el cliente OPA: %v", err)
			//}

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
			// protected := middleware.Chain(
			//	append(
			//		commons,
			//		[]middleware.Middleware{middleware.AuthorizationMiddleware(authz, FromContextExtractor)}...,
			//	)...,
			//)

			// use := httpmiddleware.Chain(
			//	httpmiddleware.Recover(logger),
			//	httpmiddleware.RequestLogger(logger),
			//	httpmiddleware.Cors(),
			//	httpmiddleware.AuthorizationMiddleware(authz),
			//)
			base := http.NewServeMux()

			base.Handle("GET /status", status)
			base.Handle("GET /live", health.NewProbe(nil))
			base.Handle("GET /ready", health.NewProbe(map[string]port2.Checker{
				"pub/sub": checker,
			})) // listo para aceptar trafico

			api := http.NewServeMux()
			api.HandleFunc("GET /test", func(w http.ResponseWriter, r *http.Request) {
				w.WriteHeader(http.StatusOK)
				w.Header().Set("Content-Type", "application/json")
				_, _ = w.Write([]byte("api test"))
			})

			router.Handle("/", public(base))
			router.Handle("/api/", public(http.StripPrefix("/api", api)))
			// router.Handle("/api/", protected(http.StripPrefix("/api", api)))
			// router.Handle("/api/", use(api))
		}),
	)

	if err := app.Err(); err != nil {
		log.Panicf("Error en la inicialización de la aplicación FX: %v\n", err)
	}

	app.Run()
}
