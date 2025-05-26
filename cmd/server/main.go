package main

import (
	"context"
	"errors"
	"fmt"
	"github.com/norlis/event-driven/cmd/server/example"
	"github.com/norlis/event-driven/pkg/usecase/worker"
	"go.uber.org/fx"
	"go.uber.org/zap"
	"log"
)

func main() {
	ctx := context.Background()

	app := fx.New(
		fx.Provide(example.NewConfigurationExample),
		fx.Provide(example.NewLogger),
		//fx.Provide(example.NewOpenTelemetry),
		fx.Provide(example.NewPubSubClient),
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
		fx.Provide(example.NewHandler),
		fx.Invoke(example.RegisterEventHandlers),
		fx.Invoke(func(routers example.RouterParams, disp *worker.Dispatcher, logger *zap.Logger) {
			logger.Info("Iniciando Event Router...")
			go func() {
				if err := routers.PrincipalRouter.Run(ctx); err != nil {
					if !errors.Is(err, context.Canceled) {
						logger.Error("Error crítico en Event Router", zap.Error(err))
					} else {
						logger.Info("Event Router Run cancelado como se esperaba.")
					}
				}
				logger.Info("Event Router Run ha terminado.")
			}()
		}),
	)

	if err := app.Err(); err != nil {
		log.Panic(fmt.Sprintf("Error en la inicialización de la aplicación FX: %v\n", err))
	}
	app.Run()
}
