# policy.rego
package httpapi.authz

# Por defecto, denegar todas las peticiones.
default allow = false

# Un administrador (admin) tiene permiso para todo.
allow if {
    input.user.role == "admin"
}

# Un usuario con rol "viewer" solo puede hacer peticiones GET a recursos bajo "/account/".
allow if {
    input.user.role == "viewer"
    input.request.method == "GET"
    startswith(input.request.path, "/account/")
}
