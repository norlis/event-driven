# Minimal static build for the NATS example, shipped on scratch.
# Build context is the repo root:
#   docker build -f example/nats/Dockerfile -t nats-example .
# (docker compose -f example/nats/compose.yaml up --build does this for you.)

FROM golang:1.26-alpine AS build
WORKDIR /src

# Cache module downloads.
COPY go.mod go.sum ./
RUN go mod download

COPY pkg/ ./pkg/
COPY example/nats/ ./example/nats/

RUN CGO_ENABLED=0 GOOS=linux go build -trimpath -ldflags='-s -w' -o /nats-example ./example/nats

FROM scratch
COPY --from=build /nats-example /nats-example
EXPOSE 8080
ENTRYPOINT ["/nats-example"]
