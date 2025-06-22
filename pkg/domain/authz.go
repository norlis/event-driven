package domain

import "context"

type PolicyInput struct {
	Roles  []string `json:"roles"`
	Action string   `json:"action"`
}

// PolicyEnforcer define la interfaz para un motor de políticas.
// Esto desacopla la lógica de la aplicación de la implementación específica.
type PolicyEnforcer interface {
	IsAllowed(ctx context.Context, input PolicyInput) (bool, error)
}
