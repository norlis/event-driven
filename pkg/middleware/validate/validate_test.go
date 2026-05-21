package validate

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"strings"
	"testing"
)

type sampleInput struct {
	Email string `validate:"required,email"`
	Age   int    `validate:"gte=0,lte=130"`
}

func discardLogger() *slog.Logger {
	return slog.New(slog.DiscardHandler)
}

func TestValidate_CallsNextOnValidInput(t *testing.T) {
	t.Parallel()

	called := false
	want := json.RawMessage(`{"ok":true}`)
	h := New(discardLogger())(func(context.Context, any) (json.RawMessage, error) {
		called = true
		return want, nil
	})

	out, err := h(context.Background(), &sampleInput{Email: "user@example.com", Age: 30})
	if err != nil {
		t.Fatalf("err = %v, want nil", err)
	}
	if !called {
		t.Error("next was not called")
	}
	if string(out) != string(want) {
		t.Errorf("out = %s, want %s", out, want)
	}
}

func TestValidate_ReturnsErrorOnInvalidInput(t *testing.T) {
	t.Parallel()

	called := false
	h := New(discardLogger())(func(context.Context, any) (json.RawMessage, error) {
		called = true
		return nil, nil
	})

	_, err := h(context.Background(), &sampleInput{Email: "not-an-email", Age: 200})
	if err == nil {
		t.Fatal("expected validation error, got nil")
	}
	if called {
		t.Error("next was called despite validation failure")
	}

	var vErr Error
	if !errors.As(err, &vErr) {
		t.Fatalf("err is not validate.Error: %T", err)
	}
	if vErr.OriginalError == nil {
		t.Error("Error.OriginalError is nil")
	}
	if !strings.Contains(vErr.Error(), "validation failed") {
		t.Errorf("Error() = %q, want it to start with 'validation failed'", vErr.Error())
	}
}