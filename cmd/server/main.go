package main

import (
	"context"
	"event-router/internal/infrastructure/jmspath"
	"event-router/internal/infrastructure/pubsub"
	"event-router/internal/usecase/router"
	"log"
	"os"
	"os/signal"
	"syscall"
)

func main() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	pubsubRouter := router.New(
		pubsub.NewSubscription("s-apolo-replica-go-qa"),
	)

	pubsubRouter.Register(
		pubsub.NewPublisher("t-apolo-trace-qa"),
		jmspath.NewFilter("contains(['040', '041'], encabezado.codEvento)"),
		func(data interface{}) error {
			log.Println("[CuentasBancarias] Procesado:", data)
			return nil
		},
	)

	// pubsubRouter.Register(
	// 	pubsub.NewPublisher("t-apolo-trace-qa"),
	// 	jmspath.NewFilter("contains(['041'], encabezado.codEvento)"),
	// 	func(data interface{}) error {
	// 		log.Println("[DatosBasicos] Procesado:", data)
	// 		return nil
	// 	},
	// )

	go func() {
		if err := pubsubRouter.Run(ctx); err != nil {
			log.Fatal(err)
		}
	}()

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)

	<-sigCh
	log.Println("[Shutdown] Signal received, waiting for cleanup...")
	cancel()
	log.Println("[Shutdown] Completed.")
}
