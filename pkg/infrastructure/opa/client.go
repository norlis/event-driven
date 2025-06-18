package opa

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"time"
)

// Client implementa PolicyEnforcer para comunicarse con un servidor OPA.
type Client struct {
	opaURL     string
	httpClient *http.Client
}

// NewClient crea una nueva instancia del cliente OPA.
// opaServerURL debe ser la URL base del servidor OPA, ej: "http://localhost:8181".
func NewClient(opaServerURL string) *Client {
	return &Client{
		opaURL: opaServerURL,
		httpClient: &http.Client{
			Timeout: 2 * time.Second,
		},
	}
}

// Estructuras para la petición y respuesta de OPA.
type opaRequest struct {
	Input interface{} `json:"input"`
}

type opaResponse struct {
	Result bool `json:"result"`
}

// IsAllowed envía una consulta al endpoint de OPA para verificar si una acción está permitida.
// El endpoint consultado se construye a partir del paquete de la política,
// en este caso "httpapi/authz" -> /v1/data/httpapi/authz/allow
func (c *Client) IsAllowed(ctx context.Context, input interface{}) (bool, error) {
	// Construir la URL completa del endpoint de la política.
	// TODO dejar como variable Reemplaza "httpapi/authz/allow" por la ruta de tu política específica.
	endpointURL := c.opaURL + "/v1/data/httpapi/authz/allow"

	requestBody, err := json.Marshal(opaRequest{Input: input})
	if err != nil {
		return false, fmt.Errorf("error al serializar el input de OPA: %w", err)
	}

	req, err := http.NewRequestWithContext(ctx, "POST", endpointURL, bytes.NewBuffer(requestBody))
	if err != nil {
		return false, fmt.Errorf("error al crear la petición a OPA: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return false, fmt.Errorf("error al contactar al servidor de OPA: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return false, fmt.Errorf("el servidor de OPA devolvió un estado inesperado: %s", resp.Status)
	}

	var opaResult opaResponse
	if err := json.NewDecoder(resp.Body).Decode(&opaResult); err != nil {
		return false, fmt.Errorf("error al decodificar la respuesta de OPA: %w", err)
	}

	return opaResult.Result, nil
}
