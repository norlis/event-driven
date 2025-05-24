package main

import (
	"context"
	"event-router/internal/domain"
	"event-router/internal/infrastructure/jmspath"
	"event-router/internal/infrastructure/pubsub"
	"event-router/internal/usecase/router"
	"log"
	"os"
	"os/signal"
	"syscall"
)

func handler1(data domain.Event[domain.Account]) (any, error) {
	log.Printf("[CuentasBancarias] event: %s Procesado: %v\n", data.Header.EventId, data.Body)
	return nil, nil
}

func main() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	pubsubRouter := router.New(
		pubsub.NewSubscription("s-apolo-replica-go-qa"),
	)

	pubsubRouter.Register(
		nil,
		// pubsub.NewPublisher("t-apolo-trace-qa"),
		jmspath.NewFilter("contains(['040', '041'], encabezado.codEvento)"),
		domain.Event[domain.Account]{},
		router.WrapHandler(handler1),
	)

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
