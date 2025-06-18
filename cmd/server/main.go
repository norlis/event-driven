package main

import (
	"log"
	"net/http"

	"cloud.google.com/go/pubsub"
	"github.com/norlis/event-driven/cmd/server/example"
	"github.com/norlis/event-driven/pkg/domain"
	"github.com/norlis/event-driven/pkg/health"
	"github.com/norlis/event-driven/pkg/usecase/worker"
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
	EventPublisher domain.Publisher     // Si lo necesitas para RegisterEventHandlers
	// Añade aquí cualquier otra dependencia que RegisterEventHandlers o la lógica de inicio necesite
}

func main() {
	//ctx := context.Background()

	app := fx.New(
		//fx.StartTimeout(10*time.Second),
		//fx.StopTimeout(120*time.Second),
		fx.Provide(example.NewConfigurationExample),
		fx.Provide(example.NewLogger),
		fx.Provide(example.NewHttpServer),
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
		fx.Invoke(func(router *http.ServeMux, status *health.Status) {
			router.Handle("GET /status", status)
			router.Handle("GET /live", health.NewProbe(nil))
			router.Handle("GET /ready", health.NewProbe(nil)) // listo para aceptar trafico
		}),
	)

	if err := app.Err(); err != nil {
		log.Panicf("Error en la inicialización de la aplicación FX: %v\n", err)
	}
	//
	app.Run()

}
