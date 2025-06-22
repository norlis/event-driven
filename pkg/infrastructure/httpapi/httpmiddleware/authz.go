package httpmiddleware

import (
	"fmt"
	"github.com/norlis/event-driven/pkg/domain"
	"github.com/norlis/event-driven/pkg/problem"
	"net/http"
	"strings"
)

func AuthorizationMiddleware(policyEnforcer domain.PolicyEnforcer) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {

			rolesHeader := r.Header.Get("X-User-Roles")
			roles := make([]string, 0)
			if rolesHeader != "" {
				roles = strings.Split(rolesHeader, ",")
			}

			//action "METODO:/ruta"
			action := fmt.Sprintf("%s:%s", strings.ToUpper(r.Method), r.URL.Path)

			input := domain.PolicyInput{
				Roles:  roles,
				Action: action,
			}

			// Esta llamada es agnóstica a si OPA es un servicio o una librería.
			allowed, err := policyEnforcer.IsAllowed(r.Context(), input)
			if err != nil {
				// Si hay un error al contactar o evaluar OPA, es más seguro denegar el acceso.
				// Devolvemos un 500 Internal Server Error para indicar un fallo en el sistema.
				problem.RespondError(w, problem.FromError(err, http.StatusInternalServerError, problem.WithInstance(r)))
				return
			}

			if !allowed {
				// Si la política de OPA devuelve 'false', denegamos el acceso.
				// Devolvemos un 403 Forbidden, que es el código estándar para un fallo de autorización.
				p := problem.New("access denied", http.StatusForbidden,
					problem.WithDetail("You do not have permission to perform this action."),
					problem.WithInstance(r),
				)
				problem.RespondError(w, p)
				return
			}

			// Si la política lo permite, la petición continúa hacia el manejador final.
			next.ServeHTTP(w, r)
		})
	}
}
