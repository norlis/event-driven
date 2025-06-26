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
│       ├── main.go                 # Punto de entrada, configuración de Fx, gestión del ciclo de vida.
│       └── example/                # Módulo de ejemplo que ensambla la aplicación.
│           ├── module.go           # Providers de Fx para todas las dependencias.
│           ├── events.go           # Registro de rutas y handlers para los routers.
│           ├── handlers.go         # Implementación de los casos de uso (lógica de negocio).
│           └── configuration.go    # Carga y estructuración de la configuración.
├── pkg/
│   ├── domain/                     # Lógica y tipos de negocio puros. No depende de nada externo.
│   │   ├── event/
│   │   │   └── event.go            # Definición del sobre del evento (antes message.go).
│   │   └── error.go                # Errores de dominio.
│   ├── application/                # Lógica de aplicación que orquesta el flujo de datos.
│   │   ├── router/                 # Lógica central del router, middlewares y registro de rutas.
│   │   └── worker/                 # Implementación del dispatcher y los workers concurrentes.
│   ├── port/                       # Interfaces (Puertos) que definen los contratos con el exterior.
│   │   ├── publisher.go
│   │   ├── subscriber.go
│   │   └── filter.go
│   ├── adapter/                    # Implementaciones concretas (Adaptadores) de los puertos.
│   │   ├── gcppubsub/              # Adaptador para Google Cloud Pub/Sub.
│   │   ├── http/                   # Adaptador para recibir eventos vía HTTP.
│   │   └── jmspath/                # Adaptador para el filtrado con JMESPath.
│   └── kit/                        # "Tool-kit" con utilidades transversales para la aplicación.
│       ├── logger/                 # Configuración del logger estructurado (zap).
│       └── otelsetup/              # Configuración del tracing con OpenTelemetry.
├── deployments/                    # Archivos de despliegue (Docker, K8s, etc.).
└── tools/                          # Herramientas de desarrollo y CI/CD.

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


## Guía Rápida de Arquitectura: ¿Dónde va mi código?

### `domain` (_El Corazón️_):
- ¿Qué es? La lógica pura de tu negocio.

- ¿Qué pongo? Entidades (Order, User), reglas de negocio (order.AddItem()) y eventos de dominio (OrderCreated).

> Regla clave: No depende de NADA. Cero librerías externas.

### `application` (_El Cerebro_):
- ¿Qué es? El orquestador que dirige a la lógica de negocio.

- ¿Qué pongo? Los casos de uso (handlers), el Router que decide qué handler llamar y el Worker que procesa en segundo plano.

> Regla clave: Usa el domain para hacer el trabajo y llama a las interfaces de port para hablar con el exterior.

### `port` (_El Contrato_):
- ¿Qué es? Las "interfaces" de Go. Define qué necesita la aplicación del mundo exterior, pero no el cómo.

- ¿Qué pongo? Solo interface { ... }. (Ej: Publisher, Subscription).

> Regla clave: Es el enchufe. No tiene código de implementación.

### `adapter` (*El Traductor*):
- ¿Qué es? El código que conecta tu aplicación con el mundo real (HTTP, Pub/Sub, bases de datos).

- ¿Qué pongo? La implementación de las interfaces de port. (Ej: GcpPubSubPublisher, HttpSubscriber).

> Regla clave: Es el cable que se conecta al enchufe (port). Aquí viven las librerías externas.

### `kit` (_La Caja de Herramientas️_):
¿Qué es? Utilidades que ayudan a toda la aplicación.

¿Qué pongo? logger y otelsetup (tracing).

> Regla clave: Es código de soporte, no es lógica de negocio ni un adaptador


### Resumen

```mermaid
graph TD
    subgraph Layers
        direction LR
        A(adapter) --> B(port);
        B -- uses --> C(app);
        C -- uses --> D(domain);
        E(kit) -- "usado por todos" --> A;
        E -- "usado por todos" --> C;
    end

    style D fill:#D4EFDF,stroke:#27AE60,stroke-width:2px,color:#1B5E20
    style C fill:#D6EAF8,stroke:#3498DB,stroke-width:2px,color:#1A5276
    style B fill:#FCF3CF,stroke:#F39C12,stroke-width:2px,color:#7D6608
    style A fill:#EBDEF0,stroke:#8E44AD,stroke-width:2px,color:#512E5F
    style E fill:#E5E7E9,stroke:#839192,stroke-width:2px,color:#2C3E50
```

| Directorio    | Propósito Principal                        | ¿Qué Pongo Aquí?                                                                 | Ejemplo Concreto                                                                           |
|---------------|--------------------------------------------|----------------------------------------------------------------------------------|--------------------------------------------------------------------------------------------|
| `domain`      | El Corazón del Negocio                     | Entidades, Eventos de Dominio, Lógica de Negocio pura.                          | `struct Order`, `func (o *Order) AddItem(...)`                                             |
| `application` | El Orquestador de Casos de Uso             | Lógica de aplicación que coordina el flujo. `Router`, `Worker`, `Handlers`.     | `OrderHandler` que llama al repositorio y al publicador.                                  |
| `port`        | El Contrato con el Exterior                | Interfaces de Go (`type MyInterface interface { ... }`).                        | `type Publisher interface { ... }`                                                         |
| `adapter`     | La Implementación del Mundo Real           | Código que implementa los puertos usando tecnología concreta (HTTP, Pub/Sub, SQL). | `GcpPubSubPublisher`, `HttpSubscriber` que maneja `http.Request`.                         |
| `kit`         | La Caja de Herramientas                    | Utilidades transversales y de soporte.                                           | `logger/New()`, `otelsetup/InitTracerProvider()`                                          |
