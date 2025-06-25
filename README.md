# Event Router - Clean Architecture

## Summary

This project implements an event routing system using Google Cloud Pub/Sub and the Clean Architecture pattern.

## Environment Variables

Set the following variables to configure the example application:

- `GCP_PROJECT_ID` – Google Cloud project identifier.
- `EVT_APP_SUBSCRIPTION` – Pub/Sub subscription from which application events are received.
- `EVT_TRACE_SUBSCRIPTION` – Pub/Sub subscription used for receiving trace messages.
- `EVT_PUBLISH_TRACE_TOPIC` – Topic where processed trace information is published.

```shell
go run cmd/server/main.go 
```

## Architecture Overview

```
├── go.mod
├── go.sum
├── cmd/
│   └── server/
│       ├── main.go                 # Punto de entrada, configuración de Fx, gestión del ciclo de vida
│       └── example/                # Módulo específico de la aplicación de ejemplo
│           ├── module.go           # Providers de Fx, configuración de componentes
│           ├── events.go           # Registro de rutas y middlewares específicos
│           ├── handlers.go         # Implementación de los UseCase (lógica de negocio)
│           ├── configuration.go    # Structs y carga de configuración
│           └── middlewares.go      # (Opcional) Middlewares específicos de este ejemplo
├── pkg/
│   ├── domain/                     # Interfaces y tipos principales del negocio
│   │   ├── message.go
│   │   ├── publisher.go
│   │   └── subscription.go
│   │   └── errors.go
│   ├── infrastructure/             # Implementaciones concretas de interfaces de dominio
│   │   ├── pubsub/                 # Lógica de Pub/Sub
│   │   │   ├── publisher.go
│   │   │   └── subscriber.go
│   │   └── jmspath/                # Lógica de JMESPath
│   │       └── filter.go
│   ├── usecase/                    # Lógica de aplicación y orquestación
│   │   ├── router/
│   │   │   ├── router.go           # Lógica central del router, registro de rutas
│   │   │   ├── middleware.go       # Definición de Middleware y helpers
│   │   │   ├── wrap.go             # Adaptadores para handlers
│   │   │   ├── cast.go             # Ayudantes para conversión de tipos
│   │   │   └── middlewares/        # (Opcional) Middlewares genéricos reutilizables
│   │   │       └── ignore_errors.go
│   │   └── worker/
│   │       ├── dispatcher.go
│   │       └── worker.go
│   ├── logger/                     # (Opcional, si es una librería compartida)
│   │   └── logger.go
│   ├── otelsetup/                  # (Opcional, si es una librería compartida)
│   │   └── tracing.go
│   └── httpclient/                 # (Añadido basado en discusiones recientes)
│       └── client.go
├── deployments/                    # Archivos de despliegue (Docker, K8s, etc.)
└── tools/                          # Herramientas de desarrollo y CI/CD

```

### Flow Diagram

**version compacta**
```mermaid
flowchart TD
    %% Definición de Estilos para Nodos
    classDef subscriber fill:#85C1E9,stroke:#2E86C1,stroke-width:2px,color:#1B4F72;
    classDef router fill:#EAECEE,stroke:#7F8C8D,stroke-width:2px,color:#2C3E50;
    classDef dispatcher fill:#D5DBDB,stroke:#707B7C,stroke-width:2px,color:#2C3E50;
    classDef worker fill:#F2F3F4,stroke:#BDC3C7,stroke-width:2px,color:#2C3E50;
    classDef publisher fill:#F9E79F,stroke:#F39C12,stroke-width:2px,color:#7E5109;

    A["<b>Subscriber</b><br/>(ej. Pub/Sub)"]:::subscriber;
    B["<b>Router</b><br/>- Filtra<br/>- Deserializa<br/>- Decide Handler"]:::router;
    C["<b>Dispatcher</b><br/>(JobQueue)"]:::dispatcher;
    D1["<b>Worker</b>"]:::worker;
    D2["<b>Worker</b>"]:::worker;
    E["<b>Publisher</b><br/>(ej. Pub/Sub)"]:::publisher;

    A -- "events" --> B;
    B -- "jobs..." --> C;
    C --> D1;
    C --> D2; 
    
    D1 -- "processed" --> E;    
    D2 -- "processed" --> E;    

    B -- "processed message" --> E;
    
    B -. "dispatch" .-> E;
```

**sequenceDiagram**

```mermaid
sequenceDiagram
    autonumber
    participant PubSub as Pub/Sub (GCP)
    participant Webhook as Webhook HTTP
    participant Router as Event Router
    participant Filter as JMESPath Filter
    participant Decoder as JSON Decoder
    participant Dispatcher as Dispatcher
    participant Worker as Worker
    participant Handler as Handler Func
    participant Publisher as Publisher (opcional)
    participant Tempo as OTel Tempo
    participant Zap as Logger
    participant Prom as Prometheus

    PubSub->>Router: Recibe mensaje
    Webhook->>Router: Recibe petición JSON

    Router->>Filter: Aplica filtro JMESPath
    Filter-->>Router: Coincide / No coincide

    Router->>Decoder: Deserializa según tipo
    Decoder-->>Router: Instancia del objeto

    Router->>Dispatcher: Encola Job

    Dispatcher->>Worker: Asigna Job
    Worker->>Handler: Ejecuta lógica
    Handler-->>Worker: Resultado / error

    Worker->>Publisher: (opcional) Publica salida
    Worker->>Tempo: Envía trazas OTel
    Worker->>Zap: Registra log estructurado
    Worker->>Prom: Registra métricas

    Worker-->>Dispatcher: Ack / Nack
```

```mermaid
flowchart TD
    %% Definición de Estilos para Nodos y Subgrafos
    classDef inicio fill:#D4EFDF,stroke:#27AE60,stroke-width:2px,color:#27AE60;
    classDef decision fill:#FCF3CF,stroke:#F39C12,stroke-width:2px,color:#F39C12;
    classDef proceso fill:#D6EAF8,stroke:#3498DB,stroke-width:2px,color:#3498DB;
    classDef io fill:#EBDEF0,stroke:#8E44AD,stroke-width:2px,color:#8E44AD;
    classDef resultadoOk fill:#A9DFBF,stroke:#229954,stroke-width:2px,color:#229954;
    classDef resultadoErr fill:#F5B7B1,stroke:#C0392B,stroke-width:2px,color:#C0392B;
    classDef worker fill:#EADCF5,stroke:#7D3C98,stroke-width:2px,color:#7D3C98;
    classDef fin fill:#E5E7E9,stroke:#839192,stroke-width:2px,color:#839192;

    %% Para los subgrafos
    style Router_Pipeline fill:#fcfcfc,stroke:#777,stroke-dasharray: 5 5
    style Worker_Section fill:#f9f9f9,stroke:#555,stroke-dasharray: 3 3

    A(["Router.Run Inicia"]):::inicio --> B{"Msj Recibido?"}:::io;

    subgraph Router_Pipeline ["Pipeline del Router"]
        B --> C{"Iterar Rutas"}:::proceso;
        C --> D{"Filtro OK?"}:::decision;
        D -- No Coincide --> E["Sgte. Ruta"];
        E --> C;
        D -- Si / Sin Filtro --> F(["Deserializar Payload"]):::proceso;
        F --> G{"Deserial. OK?"}:::decision;
        G -- No --> H(["Nack Msj"]):::resultadoErr;
        G -- Si --> J(["Crear Job"]):::proceso;
        J --> K(["Enviar a JobQueue"]):::io;
        
        K -- Falla (Cola/Ctx) --> M(["Nack Msj"]):::resultadoErr;
        H --> FIN_ROUTER_ERR["Fin (Router Nack)"]:::fin;
        M --> FIN_ROUTER_ERR;
    end
    
    K -- Éxito al Enviar --> L["Job para Worker"];

    subgraph Worker_Section ["Procesamiento en Worker"]
        L --> N(["Ejecutar Handler"]):::worker;
        N --> O{"Handler OK?"}:::decision;
        O -- Si --> P(["Ack Msj"]):::resultadoOk;
        O -- No (Error/Cancel) --> Q(["Nack Msj"]):::resultadoErr;
        P --> R{"Publicar Resultado?"}:::decision;
        R -- Si --> S(["Publicar Msj"]):::io;
        R -- No --> T(["Job Completado"]):::proceso;
        Q --> T;
        S --> T;
    end
    
    T --> FIN_WORKER["Fin (Worker)"]:::fin;

    C -.-> U{"Sin Rutas Coincidentes?"}:::decision;
    U -- Si --> V(["Ack Msj (Evitar Reintentos)"]):::resultadoOk;
    V --> FIN_SIN_RUTA["Fin (Ack sin Ruta)"]:::fin;
    U -- No --> FIN_PROCESO_RUTAS["Fin Iteración Rutas"]:::fin;
```

## Versioning

```shell
VERSION=v0.1.1
git tag "${VERSION}" && git push origin "${VERSION}"
```

## Consideraciones de rendimiento 
https://medium.com/smsjunk/handling-1-million-requests-per-minute-with-golang-f70ac505fcaa

- Pub/Sub `MaxOutstandingMessages` (`Dispatcher QueueSize + Dispatcher NumWorkers`) y `NumGoroutines` (en `SubscriberConfig` de `messaging.NewSubscription`):
  - `MaxOutstandingMessages`: El número máximo de mensajes que la librería cliente de Pub/Sub mantendrá en memoria sin haberles hecho ACK/NACK. Si tu `JobQueue` se llena, y el `Router` Nackea mensajes, estos volverán a contar contra este límite eventualmente.
  - `NumGoroutines` (en `ReceiveSettings` del cliente Pub/Sub, que messaging.NewSubscription debería usar): Controla cuántas goroutines usa la librería cliente para recibir mensajes y llamar a tu callback (el que tienes en Router.Run). Un valor demasiado bajo aquí será un cuello de botella antes de que los mensajes lleguen a tu `JobQueue`.

- Dispatcher `NumWorkers` y `QueueSize` (en `DispatcherConfig`):
  - `NumWorkers`: Tu capacidad de procesamiento real.
  - `QueueSize`: Un buffer para absorber picos de mensajes.

## OPA server

```shell
opa run --server policy.rego

```