package validate

import (
	"context"
	"encoding/json/jsontext"
	"errors"
	"testing"

	"github.com/go-playground/validator/v10"
)

type person struct {
	Name string `validate:"required"`
}

func TestErrorUnwrapsAndExposesClientDetail(t *testing.T) {
	t.Parallel()

	mw := New()
	_, err := mw(func(context.Context, any) (jsontext.Value, error) { return nil, nil })(t.Context(), person{})
	if err == nil {
		t.Fatal("empty Name accepted")
	}
	if _, ok := errors.AsType[validator.ValidationErrors](err); !ok {
		t.Errorf("errors.As(ValidationErrors) = false; Unwrap missing")
	}
	var vErr Error
	if !errors.As(err, &vErr) || vErr.ClientDetail() != vErr.OriginalError.Error() {
		t.Errorf("ClientDetail() = %q, want %q", vErr.ClientDetail(), vErr.OriginalError.Error())
	}
}
