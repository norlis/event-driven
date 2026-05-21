package skiperr

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"testing"
)

func discardLogger() *slog.Logger {
	return slog.New(slog.DiscardHandler)
}

func TestSkiperr_PassthroughOnSuccess(t *testing.T) {
	t.Parallel()

	want := json.RawMessage(`{"ok":true}`)
	h := New(discardLogger(), ByErr("never", errors.New("nope"))).
		Middleware(func(context.Context, any) (json.RawMessage, error) {
			return want, nil
		})

	out, err := h(context.Background(), nil)
	if err != nil {
		t.Fatalf("err = %v, want nil", err)
	}
	if string(out) != string(want) {
		t.Errorf("out = %s, want %s", out, want)
	}
}

func TestSkiperr_SwallowsMatchingError(t *testing.T) {
	t.Parallel()

	sentinel := errors.New("ignore me")
	skipper := New(discardLogger(), ByErr("sentinel", sentinel))

	h := skipper.Middleware(func(context.Context, any) (json.RawMessage, error) {
		return nil, sentinel
	})

	_, err := h(context.Background(), nil)
	if err != nil {
		t.Errorf("err = %v, want nil (sentinel should be swallowed)", err)
	}
}

func TestSkiperr_PropagatesNonMatchingError(t *testing.T) {
	t.Parallel()

	sentinel := errors.New("ignore me")
	other := errors.New("other")
	skipper := New(discardLogger(), ByErr("sentinel", sentinel))

	h := skipper.Middleware(func(context.Context, any) (json.RawMessage, error) {
		return nil, other
	})

	_, err := h(context.Background(), nil)
	if !errors.Is(err, other) {
		t.Errorf("err = %v, want %v", err, other)
	}
}

func TestSkiperr_ByErr_MatchesWrappedSentinel(t *testing.T) {
	t.Parallel()

	sentinel := errors.New("root cause")
	wrapped := fmt.Errorf("layer 2: %w", fmt.Errorf("layer 1: %w", sentinel))

	skipper := New(discardLogger(), ByErr("sentinel", sentinel))

	h := skipper.Middleware(func(context.Context, any) (json.RawMessage, error) {
		return nil, wrapped
	})

	_, err := h(context.Background(), nil)
	if err != nil {
		t.Errorf("err = %v, want nil (wrapped sentinel should be matched via errors.Is)", err)
	}
}

// customErr is a typed error for ByType tests.
type customErr struct{ msg string }

func (e *customErr) Error() string { return e.msg }

func TestSkiperr_ByType_MatchesTypedError(t *testing.T) {
	t.Parallel()

	skipper := New(discardLogger(), ByType[*customErr]("custom"))

	h := skipper.Middleware(func(context.Context, any) (json.RawMessage, error) {
		return nil, &customErr{msg: "boom"}
	})

	_, err := h(context.Background(), nil)
	if err != nil {
		t.Errorf("err = %v, want nil (typed error should be swallowed)", err)
	}
}

func TestSkiperr_ByType_DoesNotMatchDifferentType(t *testing.T) {
	t.Parallel()

	skipper := New(discardLogger(), ByType[*customErr]("custom"))

	other := errors.New("not a customErr")
	h := skipper.Middleware(func(context.Context, any) (json.RawMessage, error) {
		return nil, other
	})

	_, err := h(context.Background(), nil)
	if !errors.Is(err, other) {
		t.Errorf("err = %v, want %v", err, other)
	}
}

func TestSkiperr_FirstMatchingPredicateWins(t *testing.T) {
	t.Parallel()

	sentinel := errors.New("sentinel")

	// Both predicates would match. We just verify the result is swallowed
	// (order doesn't change the outcome here, but exercises the loop).
	skipper := New(
		discardLogger(),
		ByErr("first", sentinel),
		ByErr("second", sentinel),
	)

	h := skipper.Middleware(func(context.Context, any) (json.RawMessage, error) {
		return nil, sentinel
	})

	_, err := h(context.Background(), nil)
	if err != nil {
		t.Errorf("err = %v, want nil", err)
	}
}

func TestSkiperr_PredicateDescriptions(t *testing.T) {
	t.Parallel()

	sentinel := errors.New("boom")
	pErr := ByErr("name", sentinel)
	if pErr.Description == "" {
		t.Error("ByErr.Description is empty")
	}

	pType := ByType[*customErr]("custom")
	if pType.Description == "" {
		t.Error("ByType.Description is empty")
	}
}