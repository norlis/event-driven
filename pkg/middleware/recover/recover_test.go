package recover

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"
)

func TestRecover_PassthroughOnSuccess(t *testing.T) {
	t.Parallel()

	want := json.RawMessage(`{"ok":true}`)
	h := Middleware(func(context.Context, any) (json.RawMessage, error) {
		return want, nil
	})

	got, err := h(context.Background(), nil)
	if err != nil {
		t.Fatalf("err = %v, want nil", err)
	}
	if string(got) != string(want) {
		t.Errorf("got = %s, want %s", got, want)
	}
}

func TestRecover_PassthroughOnError(t *testing.T) {
	t.Parallel()

	want := errors.New("handler boom")
	h := Middleware(func(context.Context, any) (json.RawMessage, error) {
		return nil, want
	})

	_, err := h(context.Background(), nil)
	if !errors.Is(err, want) {
		t.Errorf("err = %v, want %v", err, want)
	}
}

func TestRecover_PanicWithError(t *testing.T) {
	t.Parallel()

	panicErr := errors.New("kaboom")
	h := Middleware(func(context.Context, any) (json.RawMessage, error) {
		panic(panicErr)
	})

	_, err := h(context.Background(), nil)
	if err == nil {
		t.Fatal("expected error, got nil")
	}
	var pe PanicError
	if !errors.As(err, &pe) {
		t.Fatalf("err is not PanicError: %T", err)
	}
	if pe.V != panicErr {
		t.Errorf("PanicError.V = %v, want %v", pe.V, panicErr)
	}
	if pe.Stacktrace == "" {
		t.Error("Stacktrace is empty")
	}
	if !strings.Contains(pe.Error(), "panic occurred") {
		t.Errorf("Error() = %q, want it to contain 'panic occurred'", pe.Error())
	}
}

func TestRecover_PanicWithString(t *testing.T) {
	t.Parallel()

	h := Middleware(func(context.Context, any) (json.RawMessage, error) {
		panic("plain string panic")
	})

	_, err := h(context.Background(), nil)
	if err == nil {
		t.Fatal("expected error, got nil")
	}
	var pe PanicError
	if !errors.As(err, &pe) {
		t.Fatalf("err is not PanicError: %T", err)
	}
	if pe.V != "plain string panic" {
		t.Errorf("PanicError.V = %v, want %q", pe.V, "plain string panic")
	}
}