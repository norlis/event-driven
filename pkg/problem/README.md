## Example

### Error de validación (400 Bad Request)
```go
p := problem.New(
    "Parámetro Faltante",
    http.StatusBadRequest,
    problem.WithDetail("El parámetro 'user_id' es requerido."),
    problem.WithInstance(r),
)
problem.RespondError(w, p)
```

### Simular una regla de negocio.
```go
p := problem.New(
    "Crédito Insuficiente",
    http.StatusForbidden,
    problem.WithDetail("Tu saldo es 30, pero la operación requiere 50."),
    problem.WithType("https://example.com/probs/credito-insuficiente"),
    problem.WithInstance(r),
)
// Aquí podrías añadir campos de extensión si modificas el struct ProblemDetail.
// p.Balance = 30
// p.Accounts = []string{"/accounts/123", "/accounts/456"}
problem.RespondError(w, p)
```

### error inesperado del sistema.

```go
p := problem.FromError(
    dbErr, // El error original.
    http.StatusInternalServerError,
    problem.WithInstance(r),
)
// En un entorno de producción, loguearías el error original con más detalle.
// log.Printf("Error interno: %v", dbErr)
problem.RespondError(w, p)
```

