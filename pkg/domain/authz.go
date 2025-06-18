package domain

import "context"

// PolicyEnforcer define la interfaz para un motor de políticas.
// Esto desacopla la lógica de la aplicación de la implementación específica.
type PolicyEnforcer interface {
	IsAllowed(ctx context.Context, input interface{}) (bool, error)
}
