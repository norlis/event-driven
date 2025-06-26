
# --- Configuración y Variables ---
APP_IMPORT_PATH := $(shell go list -m)
ALL_PKGS := $(sort $(shell go list ./...))

# Variables para inyectar en el build
GIT_SHA := $(shell git rev-parse HEAD)
DATE := $(shell date -u +'%Y-%m-%dT%H:%M:%SZ')

LDFLAGS := -ldflags "-s -w \
	-X $(APP_IMPORT_PATH)/internal/application.GitHash=$(GIT_SHA) \
	-X $(APP_IMPORT_PATH)/internal/application.Date=$(DATE)"

# --- Herramientas y Módulos ---
TOOLS_BIN_DIR := $(abspath ./bin)
TOOLS_MOD_DIR := $(abspath ./tools)

# --- Comandos Base ---
GOBUILD := GO111MODULE=on CGO_ENABLED=0 go build -trimpath

# ====================================================================================
# Comandos Públicos
# ====================================================================================
.PHONY: help all build clean test lint format check-format tools

help:
	@echo "Uso: make [comando]"
	@echo ""
	@echo "## --- Ciclo de Vida del Build ---"
	@echo "  build            Construye los binarios para todas las plataformas."
	@echo "  amd64-build      Construye el binario para linux/amd64."
	@echo "  arm64-build      Construye el binario para linux/arm64."
	@echo "  clean            Elimina la carpeta de build."
	@echo ""
	@echo "## --- Calidad de Código ---"
	@echo "  lint             Ejecuta todos los linters (golangci-lint y staticcheck)."
	@echo "  format           Formatea automáticamente el código con goimports."
	@echo "  check-format     Verifica el formato sin modificar archivos (ideal para CI)."
	@echo "  test             Ejecuta las pruebas unitarias."
	@echo "  test-sonar       Ejecuta pruebas generando reportes para SonarQube."
	@echo ""
	@echo "## --- Gestión de Dependencias ---"
	@echo "  tools            Instala/actualiza las herramientas de desarrollo en ./bin."
	@echo "  mod-tidy         Ejecuta 'go mod tidy' en el módulo principal."
	@echo "  mod-vendor       Ejecuta 'go mod vendor'."


all: check-format lint test build


## ----------------------------------------
## Gestión de Herramientas
## ----------------------------------------
tools:
	@echo "==> Instalando herramientas de desarrollo en $(TOOLS_BIN_DIR)..."
	@# Nos aseguramos de que el directorio de binarios exista
	@mkdir -p $(TOOLS_BIN_DIR)
	@# Usamos el go.mod de ./tools para no ensuciar el go.mod principal
	@cd $(TOOLS_MOD_DIR) && go mod tidy
	@# Obtenemos la lista de paquetes y la instalamos con un bucle for, que es más robusto
	@cd $(TOOLS_MOD_DIR) && \
		for pkg in $$(cat tools.go | grep '_' | awk -F'"' '{print $$2}'); do \
			echo "Instalando $$pkg..."; \
			GOBIN=$(TOOLS_BIN_DIR) go install -v $$pkg; \
		done
	@echo "==> Herramientas instaladas correctamente."


## ----------------------------------------
## Calidad de Código
## ----------------------------------------
lint: tools lint-static-check
	@echo "==> Ejecutando golangci-lint..."
	@$(TOOLS_BIN_DIR)/golangci-lint run --timeout 5m --enable gosec

lint-static-check: tools
	@echo "==> Ejecutando staticcheck..."
	@STATIC_CHECK_OUT=`$(TOOLS_BIN_DIR)/staticcheck $(ALL_PKGS) 2>&1`; \
	if [ "$$STATIC_CHECK_OUT" ]; then \
		echo "ERROR: Falló staticcheck:\n"; \
		echo "\033[0;31m$$STATIC_CHECK_OUT\033[0m\n"; \
		exit 1; \
	else \
		echo "SUCCESS: Staticcheck finalizado correctamente."; \
	fi

format: tools
	@echo "==> Formateando código..."
	@$(TOOLS_BIN_DIR)/goimports -w .

check-format: tools
	@echo "==> Verificando formato del código..."
	@WARNINGS_CHECK_OUT=`$(TOOLS_BIN_DIR)/goimports -l .`; \
	if [ "$$WARNINGS_CHECK_OUT" ]; then \
		echo "ERROR: El código no está formateado. Ejecuta 'make format'.\n"; \
		echo "\033[0;31m$$WARNINGS_CHECK_OUT\033[0m\n"; \
		exit 1; \
	else \
		echo "SUCCESS: El formato del código es correcto."; \
	fi


## ----------------------------------------
## Pruebas y SonarQube
## ----------------------------------------
test:
	@echo "==> Ejecutando pruebas unitarias..."
	@go test ./... --cover

test-sonar:
	@echo "==> Generando reportes para SonarQube..."
	@go test -covermode=atomic -coverprofile=coverage.out ./...
	@go test -json ./... > report.json

run-local-sonar:
	docker run -d --name sonarqube -p 9000:9000 sonarqube


## ----------------------------------------
## Build y Clean
## ----------------------------------------
build: clean
	@echo "==> Construyendo binarios..."
	GOOS=darwin GOARCH=amd64 $(GOBUILD) $(LDFLAGS) -o ./build/darwin/amd64/microservice ./cmd/microservice
	GOOS=linux GOARCH=amd64 $(GOBUILD) $(LDFLAGS) -o ./build/linux/amd64/microservice ./cmd/microservice
	GOOS=linux GOARCH=arm64 $(GOBUILD) $(LDFLAGS) -o ./build/linux/arm64/microservice ./cmd/microservice
	GOOS=windows GOARCH=amd64 $(GOBUILD) $(LDFLAGS) -o ./build/windows/amd64/microservice ./cmd/microservice

amd64-build:
	GOOS=linux GOARCH=amd64 $(GOBUILD) -o ./build/linux/amd64/microservice ./cmd/microservice

arm64-build:
	GOOS=linux GOARCH=arm64 $(GOBUILD) $(LDFLAGS) -o ./build/linux/arm64/microservice ./cmd/microservice

clean:
	@echo "==> Limpiando builds anteriores..."
	@rm -rf ./build


## ----------------------------------------
## Gestión de Módulos
## ----------------------------------------
mod-tidy:
	go mod tidy

mod-vendor:
	go mod vendor