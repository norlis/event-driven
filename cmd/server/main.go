package main

import (
	"context"
	"log"
	"net/http"

	"github.com/norlis/event-driven/pkg/port"

	"cloud.google.com/go/pubsub"
	"github.com/norlis/event-driven/cmd/server/example"
	"github.com/norlis/event-driven/pkg/application/worker"
	"github.com/norlis/httpgate/pkg/health"
	"github.com/norlis/httpgate/pkg/middleware"
	"github.com/norlis/httpgate/pkg/opa"
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

func main() {
	//ctx := context.Background()

	app := fx.New(
		//fx.StartTimeout(10*time.Second),
		//fx.StopTimeout(120*time.Second),
		fx.Provide(example.NewConfigurationExample),
		fx.Provide(example.NewLogger),
		//fx.Provide(example.NewHttpServer),
		fx.Provide(example.NewHttpServerMux),
		//fx.Provide(example.NewOpenTelemetry),
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
		fx.Provide(
			fx.Annotate(
				example.NewTraceSubscription,
				fx.ResultTags(`name:"TraceSubscription"`),
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
				example.NewTraceRouter,
				fx.ResultTags(`name:"TraceRouter"`),
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
		fx.Invoke(example.RegisterEventHandlers),
		fx.Invoke(func(router *http.ServeMux, status *health.Status, logger *zap.Logger) {

			opaConfig := opa.Config{
				Query:        "data.authz.allow",
				PoliciesPath: "policies/authz", // Directorio con authz.rego
				DataFiles:    []string{
					//"policies/authz/whitelist.json",
					//"policies/authz/roles.json",
					//"policies/authz/permissions.json",
				},
			}

			authz, err := opa.NewOpaSdkClientFromConfig(context.Background(), opaConfig, logger)

			if err != nil {
				log.Fatalf("No se pudo inicializar el cliente OPA: %v", err)
			}

			commons := []middleware.Middleware{
				middleware.Recover(logger),
				middleware.RequestLogger(logger),
				middleware.Cors(),
			}

			public := middleware.Chain(commons...)
			protected := middleware.Chain(
				append(
					commons,
					[]middleware.Middleware{middleware.AuthorizationMiddleware(authz)}...,
				)...,
			)

			//use := httpmiddleware.Chain(
			//	httpmiddleware.Recover(logger),
			//	httpmiddleware.RequestLogger(logger),
			//	httpmiddleware.Cors(),
			//	httpmiddleware.AuthorizationMiddleware(authz),
			//)
			base := http.NewServeMux()

			base.Handle("GET /status", status)
			base.Handle("GET /live", health.NewProbe(nil))
			base.Handle("GET /ready", health.NewProbe(nil)) // listo para aceptar trafico

			api := http.NewServeMux()
			api.HandleFunc("GET /test", func(w http.ResponseWriter, r *http.Request) {
				w.WriteHeader(http.StatusOK)
				w.Header().Set("Content-Type", "application/json")
				w.Write([]byte("api test"))
			})

			router.Handle("/", public(base))
			router.Handle("/api/", protected(http.StripPrefix("/api", api)))
			//router.Handle("/api/", use(api))
		}),
	)

	if err := app.Err(); err != nil {
		log.Panicf("Error en la inicialización de la aplicación FX: %v\n", err)
	}

	app.Run()

}
