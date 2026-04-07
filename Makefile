
# --- Configuración y Variables ---
APP_IMPORT_PATH := $(shell go list -m)
ALL_PKGS := $(sort $(shell go list ./...))

# --- Herramientas y Módulos ---
TOOLS_BIN_DIR := $(abspath ./bin)
TOOLS_MOD_DIR := $(abspath ./tools)

export PATH := $(TOOLS_BIN_DIR):$(PATH)

# ====================================================================================
# Comandos Públicos
# ====================================================================================
.PHONY: help all clean test lint format check-format tools

help:
	@echo "Uso: make [comando]"
	@echo ""
	@echo "## --- Calidad de Código ---"
	@echo "  lint             Ejecuta todos los linters (golangci-lint y staticcheck)."
	@echo "  format           Formatea automáticamente el código con gofumpt."
	@echo "  check-format     Verifica el formato sin modificar archivos (ideal para CI)."
	@echo "  test             Ejecuta las pruebas unitarias."
	@echo "  test-sonar       Ejecuta pruebas generando reportes para SonarQube."
	@echo "  vulncheck        Escanea vulnerabilidades conocidas."
	@echo "  modernize        Muestra cambios sugeridos por go fix (sin aplicar)."
	@echo ""
	@echo "## --- Gestión de Dependencias ---"
	@echo "  tools            Instala/actualiza las herramientas de desarrollo en ./bin."
	@echo "  tools-force      Reinstala todas las herramientas desde cero."
	@echo "  mod-tidy         Ejecuta 'go mod tidy' en el módulo principal."

all: check-format lint test


## ----------------------------------------
## Gestión de Herramientas
## ----------------------------------------
tools: $(TOOLS_BIN_DIR)

$(TOOLS_BIN_DIR): $(TOOLS_MOD_DIR)/tools.go $(TOOLS_MOD_DIR)/go.mod
	@echo "==> Instalando herramientas desde tools/go.mod..."
	@mkdir -p $(TOOLS_BIN_DIR)
	@cd $(TOOLS_MOD_DIR) && go mod tidy
	@cd $(TOOLS_MOD_DIR) && \
		go list -e -f '{{range .Imports}}{{.}} {{end}}' -tags=tools tools.go | \
		xargs -n1 env GOBIN=$(TOOLS_BIN_DIR) go install -v
	@touch $(TOOLS_BIN_DIR)
	@echo "==> Herramientas actualizadas."

.PHONY: tools-force
tools-force:
	@rm -rf $(TOOLS_BIN_DIR)
	@$(MAKE) tools


## ----------------------------------------
## Calidad de Código
## ----------------------------------------
lint: tools
	@echo "==> Ejecutando golangci-lint..."
	@$(TOOLS_BIN_DIR)/golangci-lint run --fix

vulncheck: tools
	@echo "==> Escaneando vulnerabilidades (govulncheck)..."
	@$(TOOLS_BIN_DIR)/govulncheck ./...

format: tools
	@echo "==> Formateando código..."
	@$(TOOLS_BIN_DIR)/gofumpt -l -w .

check-format: tools
	@echo "==> Verificando formato..."
	@if [ -n "$$($(TOOLS_BIN_DIR)/gofumpt -l .)" ]; then \
		echo "ERROR: El código no está formateado con gofumpt."; \
		$(TOOLS_BIN_DIR)/gofumpt -l .; \
		exit 1; \
	fi


## ----------------------------------------
## Pruebas
## ----------------------------------------
test:
	@echo "==> Ejecutando pruebas unitarias..."
	@go test ./... --cover

test-sonar:
	@echo "==> Generando reportes para SonarQube..."
	@go test -covermode=atomic -coverprofile=coverage.out ./...
	@go test -json ./... > report.json


## ----------------------------------------
## Modernización
## ----------------------------------------
modernize:
	@echo "==> Aplicando modernizaciones (go fix)..."
	go fix -diff ./...


## ----------------------------------------
## Gestión de Módulos
## ----------------------------------------
mod-tidy:
	go mod tidy