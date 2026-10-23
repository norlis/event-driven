package skiperr

import (
	"context"
	"encoding/json/jsontext"
	"errors"
	"testing"

	"github.com/norlis/event-driven/v2/pkg/eventmux"
)

var errNotFound = errors.New("row not found")

func TestByErrDescriptionUsesName(t *testing.T) {
	t.Parallel()

	p := ByErr("not-found", errNotFound)
	if p.Description != "Err: (not-found)" {
		t.Errorf("Description = %q, want %q", p.Description, "Err: (not-found)")
	}
	if !p.Matches(errors.Join(errors.New("ctx"), errNotFound)) || p.Matches(errors.New("other")) {
		t.Error("Matches does not follow errors.Is")
	}
}

func TestMiddlewareSwallowsMatchingErrorOnly(t *testing.T) {
	t.Parallel()

	mw := New(nil, ByErr("not-found", errNotFound)).Middleware
	failing := func(err error) eventmux.HandlerFunc {
		return func(context.Context, any) (jsontext.Value, error) { return nil, err }
	}
	if _, err := mw(failing(errNotFound))(t.Context(), nil); err != nil {
		t.Errorf("matching error = %v, want nil", err)
	}
	other := errors.New("boom")
	if _, err := mw(failing(other))(t.Context(), nil); !errors.Is(err, other) {
		t.Errorf("non-matching error = %v, want %v", err, other)
	}
}
