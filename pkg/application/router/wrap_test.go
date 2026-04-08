package router

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"testing"
)

type MyData struct {
	ID string
}

func mySpecificHandler(_ context.Context, data MyData) (json.RawMessage, error) {
	if data.ID == "error" {
		return nil, errors.New("handler error")
	}
	return []byte("processed: " + data.ID), nil
}

func TestWrapHandler(t *testing.T) {
	t.Parallel()

	wrapped := WrapHandler(mySpecificHandler)

	tests := []struct {
		name           string
		input          any
		expectedOut    json.RawMessage
		expectErr      bool
		expectedErrMsg string
	}{
		{
			name:        "Correct type, no error",
			input:       MyData{ID: "123"},
			expectedOut: []byte("processed: 123"),
			expectErr:   false,
		},
		{
			name:           "Correct type, handler returns error",
			input:          MyData{ID: "error"},
			expectedOut:    nil,
			expectErr:      true,
			expectedErrMsg: "handler error",
		},
		{
			name:        "Incorrect type",
			input:       "not MyData",
			expectedOut: nil,
			expectErr:   true,
			// reflect.TypeFor[T]() prints the full type name
			expectedErrMsg: fmt.Sprintf("handler: expected router.MyData but got %T", "not MyData"),
		},
		{
			name:           "Nil input, incorrect type",
			input:          nil,
			expectedOut:    nil,
			expectErr:      true,
			expectedErrMsg: "handler: expected router.MyData but got <nil>",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			out, err := wrapped(context.Background(), tt.input)

			if (err != nil) != tt.expectErr {
				t.Fatalf("WrapHandler() error = %v, expectErr %v", err, tt.expectErr)
			}

			if tt.expectErr && err != nil {
				if err.Error() != tt.expectedErrMsg {
					t.Errorf("WrapHandler() error message = %q, want %q", err.Error(), tt.expectedErrMsg)
				}
			}

			if !tt.expectErr && !bytes.Equal(out, tt.expectedOut) {
				t.Errorf("WrapHandler() out = %v, want %v", out, tt.expectedOut)
			}
		})
	}
}
