package main

import (
	"context"
	"errors"
	"event-router/internal/domain"
	"event-router/internal/infrastructure/jmspath"
	"event-router/internal/infrastructure/pubsub"
	"event-router/internal/logger"
	"event-router/internal/usecase/router"
	"event-router/internal/usecase/worker"
	"fmt"
	"os"
	"os/signal"
	"syscall"
	"time"

	gcp "cloud.google.com/go/pubsub" // Cliente Pub/Sub de GCP

	"go.uber.org/zap"
)

// handler1 es un ejemplo de manejador de eventos específico.
func handler1(ctx context.Context, data domain.Event[domain.Account], logger *zap.Logger) (any, error) {
	// Usar el contexto del mensaje si es necesario: data.Msg.Context()
	// O el contexto global pasado al handler si se prefiere.
	select {
	case <-ctx.Done():
		logger.Warn("[CuentasBancarias Handler] Contexto cancelado antes de procesar", zap.String("eventId", data.Header.EventId))
		return nil, ctx.Err()
	default:
		// Continuar procesamiento
	}

	logger.Info("[CuentasBancarias Handler] Procesando evento", zap.String("eventId", data.Header.EventId), zap.Any("body", data.Body))
	// Simular trabajo
	time.Sleep(100 * time.Millisecond)
	return nil, nil // El 'any' retornado podría ser usado para una publicación posterior
}

// wrapHandlerForRouter adapta un handler con dependencias (como logger) al HandlerFunc del router.
func wrapHandlerForRouter[T any](
	actualHandler func(ctx context.Context, data T, logger *zap.Logger) (any, error),
	appCtx context.Context, // Contexto de la aplicación para el handler
	logger *zap.Logger,
) router.HandlerFunc {
	return func(data any) (any, error) {
		castedData, ok := data.(T)
		if !ok {
			return nil, errors.New("tipo de datos inesperado en el manejador envuelto")
		}
		return actualHandler(appCtx, castedData, logger)
	}
}

func main() {
	appLogger, err := logger.New(true)

	if err != nil {
		// No se puede usar appLogger aquí porque podría ser nil
		// log.Fatalf no está disponible si estamos construyendo una librería sin "log"
		// directamente. Así que imprimimos a stderr y salimos.
		fmt.Fprintf(os.Stderr, "Error inicializando logger: %v\n", err)
		os.Exit(1)
	}
	defer appLogger.Sync() // Asegurar que todos los logs en buffer se escriban al final

	appLogger.Info("Iniciando Event Router Example...")

	// 2. Configuración de la Aplicación (ejemplo desde variables de entorno o hardcodeado para el ejemplo)
	// ESTO DEBERÍA VENIR DE VARIABLES DE ENTORNO O ARCHIVOS DE CONFIGURACIÓN EN PRODUCCIÓN
	// gcpProjectID := os.Getenv("GCP_PROJECT_ID")
	gcpProjectID := "proteccion-davinci-iaas"
	if gcpProjectID == "" {
		appLogger.Fatal("Variable de entorno GCP_PROJECT_ID no establecida.")
	}
	// subID := os.Getenv("PUBSUB_SUBSCRIPTION_ID") // ej: "s-apolo-replica-go-qa"
	subID := "s-apolo-replica-go-qa"
	if subID == "" {
		appLogger.Fatal("Variable de entorno PUBSUB_SUBSCRIPTION_ID no establecida.")
	}
	// topicIDTrace := "t-apolo-trace-qa" // Si se necesitara un publicador

	rootCtx, rootCancel := context.WithCancel(context.Background())
	defer rootCancel()

	psClient, err := gcp.NewClient(rootCtx, gcpProjectID)
	if err != nil {
		appLogger.Fatal("Fallo al crear cliente Pub/Sub", zap.Error(err))
	}
	defer func() {
		appLogger.Info("Cerrando cliente Pub/Sub...")
		if err := psClient.Close(); err != nil {
			appLogger.Error("Error al cerrar cliente Pub/Sub", zap.Error(err))
		}
	}()

	// 4. Configurar y Crear Componentes de la Librería
	// Configuración del Suscriptor Pub/Sub
	subscriberCfg := pubsub.SubscriberConfig{
		ProjectID:              gcpProjectID,
		SubscriptionID:         subID,
		MaxOutstandingMessages: 50, // Configurable
		NumGoroutines:          10, // Configurable
		MaxExtension:           60 * time.Second,
	}
	eventSubscription := pubsub.NewSubscription(psClient, subscriberCfg, appLogger.Named("pubsub_subscriber"))

	// Configuración del Dispatcher de Workers
	dispatcherCfg := worker.DispatcherConfig{
		NumWorkers: 10, // Configurable
		QueueSize:  20, // Configurable
	}
	eventDispatcher := worker.NewDispatcher(dispatcherCfg, appLogger.Named("worker_dispatcher"))

	// Configuración del Router
	routerCfg := router.Config{
		Subscription:     eventSubscription,
		WorkerDispatcher: eventDispatcher,
		Logger:           appLogger.Named("router"),
	}
	eventRouter := router.New(routerCfg)

	// 5. Registrar Rutas
	// Filtro JMESPath
	eventFilter := jmspath.New("contains(['040', '041'], encabezado.codEvento)", appLogger.Named("jmes_filter"))

	// Envolver el handler específico para que coincida con router.HandlerFunc
	// y para inyectar dependencias como el logger y el contexto de la app.
	wrappedHandler1 := wrapHandlerForRouter(handler1, rootCtx, appLogger.Named("handler1"))

	eventRouter.Register(
		nil, // Sin publicador para esta ruta por ahora
		eventFilter,
		domain.Event[domain.Account]{}, // Tipo de objeto esperado para esta ruta
		wrappedHandler1,
	)

	// 6. Iniciar el Dispatcher y el Router
	// El dispatcher debe iniciarse antes que el router (o la suscripción)
	// para que los workers estén listos para recibir trabajos.
	eventDispatcher.Run(rootCtx) // Inicia los workers y el bucle de escucha del dispatcher

	go func() {
		appLogger.Info("Router.Run iniciando en una goroutine...")
		if err := eventRouter.Run(rootCtx); err != nil {
			// Si Run devuelve un error (y no es context.Canceled), es un problema serio.
			if !errors.Is(err, context.Canceled) {
				appLogger.Error("Error fatal en Router.Run", zap.Error(err))
				rootCancel() // Señalar a toda la aplicación que se detenga
			} else {
				appLogger.Info("Router.Run cancelado como se esperaba.")
			}
		}
		appLogger.Info("Router.Run ha terminado.")
	}()

	// 7. Manejar Señales de Apagado Ordenado
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)

	appLogger.Info("Aplicación iniciada. Presiona Ctrl+C para salir.")
	select {
	case s := <-sigCh:
		appLogger.Info("Señal de apagado recibida", zap.String("signal", s.String()))
	case <-rootCtx.Done(): // Si el contexto raíz se cancela por otra razón (ej. error fatal en router)
		appLogger.Info("Contexto raíz cancelado, iniciando apagado.")
	}

	appLogger.Info("Iniciando apagado ordenado...")

	// La cancelación de rootCtx (hecha con defer o por error fatal) debería propagarse.
	// Dar un tiempo para que las goroutines terminen.
	// Primero, cancelar el contexto si no se hizo ya (por ejemplo, si se recibió señal).
	rootCancel()

	// Esperar a que el dispatcher y sus workers terminen.
	// Stop() debería ser llamado después de que la fuente de trabajos (router.Run) haya sido señalada para detenerse
	// y ya no envíe más trabajos al JobQueue del dispatcher.
	// El cierre de JobQueue se maneja ahora en Dispatcher.Stop().
	eventDispatcher.Stop() // Esto bloqueará hasta que los workers terminen.

	// El cliente Pub/Sub se cierra con defer psClient.Close().

	appLogger.Info("Apagado completado.")
}
