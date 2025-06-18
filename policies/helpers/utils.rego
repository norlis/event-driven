package httpapi.authz.utils

# is_admin es una función de ayuda que verifica si un rol es "admin".
is_admin(role) {
    role == "admin"
}

# can_view_account es una función que verifica si un usuario tiene permisos de lectura.
can_view_account(role, method) {
    role == "viewer"
    method == "GET"
}
