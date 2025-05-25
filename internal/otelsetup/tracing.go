package otelsetup

//import (
//	"fmt"
//	"go.opentelemetry.io/otel"
//	"go.opentelemetry.io/otel/propagation"
//	"go.opentelemetry.io/otel/sdk/resource"
//	sdktrace "go.opentelemetry.io/otel/sdk/trace"
//	"go.uber.org/zap"
//	"google.golang.org/grpc"
//	"google.golang.org/grpc/credentials/insecure"
//)

import (
	"context"
	"fmt"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracegrpc"
	"go.opentelemetry.io/otel/propagation"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure" // Para conexiones locales sin TLS
)

// InitTracerProvider configura y registra el proveedor de trazas OpenTelemetry.
// Debería ser llamado una vez al inicio de la aplicación.
// Retorna una función de apagado que debe ser llamada con defer.
func InitTracerProvider(ctx context.Context, serviceName string, serviceVersion string, tempoEndpoint string, logger *zap.Logger) (func(context.Context) error, error) {
	//res, err := resource.New(ctx,
	//	resource.WithAttributes(
	//		attribute.Key{"service", attribute.Value{"test"}},
	//	),
	//)
	//if err != nil {
	//	return nil, fmt.Errorf("fallo al crear recurso: %w", err)
	//}

	// Configurar el exportador OTLP gRPC para Tempo
	// Asegúrate de que Tempo esté accesible en tempoEndpoint (ej. "localhost:4317")
	conn, err := grpc.NewClient(tempoEndpoint,
		grpc.WithTransportCredentials(insecure.NewCredentials()), // Para conexiones locales sin TLS
		grpc.WithBlock(), // Esperar a que la conexión se establezca
	)
	if err != nil {
		return nil, fmt.Errorf("fallo al crear conexión gRPC con el colector OTLP: %w", err)
	}

	traceExporter, err := otlptracegrpc.New(ctx, otlptracegrpc.WithGRPCConn(conn))
	if err != nil {
		return nil, fmt.Errorf("fallo al crear exportador de trazas OTLP: %w", err)
	}

	// Usar BatchSpanProcessor para producción es recomendado.
	// Para desarrollo local, SimpleSpanProcessor podría ser más inmediato, pero Batch es mejor práctica.
	bsp := sdktrace.NewBatchSpanProcessor(traceExporter)

	// Configurar el SDK del TracerProvider
	tp := sdktrace.NewTracerProvider(
		sdktrace.WithSampler(sdktrace.AlwaysSample()), // Muestrear todas las trazas para desarrollo/pruebas
		//sdktrace.WithResource(res),
		sdktrace.WithSpanProcessor(bsp),
	)

	// Establecer el TracerProvider global
	otel.SetTracerProvider(tp)

	// Establecer el propagador de contexto de trazas global (W3C Trace Context es común)
	otel.SetTextMapPropagator(propagation.NewCompositeTextMapPropagator(propagation.TraceContext{}, propagation.Baggage{}))

	logger.Info("Proveedor de Trazas OpenTelemetry inicializado", zap.String("tempoEndpoint", tempoEndpoint))

	// Retornar la función de apagado
	return func(shutdownCtx context.Context) error {
		logger.Info("Apagando proveedor de trazas OpenTelemetry...")
		ctxTimeout, cancel := context.WithTimeout(shutdownCtx, 5*time.Second)
		defer cancel()
		if err := tp.Shutdown(ctxTimeout); err != nil {
			logger.Error("Error apagando proveedor de trazas", zap.Error(err))
			return err
		}
		if err := conn.Close(); err != nil {
			logger.Error("Error cerrando conexión gRPC del exportador de trazas", zap.Error(err))
			return err
		}
		return nil
	}, nil
}

// Helper para propagar el contexto de trazas desde los atributos de un mensaje Pub/Sub.
type PubSubAttributesCarrier map[string]string

func (c PubSubAttributesCarrier) Get(key string) string {
	return c[key]
}

func (c PubSubAttributesCarrier) Set(key string, value string) {
	c[key] = value
}

func (c PubSubAttributesCarrier) Keys() []string {
	keys := make([]string, 0, len(c))
	for k := range c {
		keys = append(keys, k)
	}
	return keys
}

// ExtractSpanContextFromPubSubMessage intenta extraer un contexto de span de los atributos del mensaje.
func ExtractSpanContextFromPubSubMessage(ctx context.Context, attributes map[string]string) context.Context {
	if attributes == nil {
		return ctx
	}
	propagator := otel.GetTextMapPropagator()
	return propagator.Extract(ctx, PubSubAttributesCarrier(attributes))
}

// InjectSpanContextIntoPubSubMessage inyecta el contexto de span actual en los atributos del mensaje.
func InjectSpanContextIntoPubSubMessage(ctx context.Context, attributes map[string]string) {
	if attributes == nil {
		// No se puede inyectar si el mapa de atributos es nil
		return
	}
	propagator := otel.GetTextMapPropagator()
	propagator.Inject(ctx, PubSubAttributesCarrier(attributes))
}
