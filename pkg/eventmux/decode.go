// Package eventmux is a transport-agnostic router that decodes incoming
// CloudEvents into a typed payload, matches them against registered routes
// (filter + handler), runs middleware chains, and optionally publishes a
// result event back to a Publisher. The transport (Pub/Sub, SQS, HTTP, …) is
// plugged in via the Subscription and Publisher interfaces.
package eventmux

import (
	"encoding/json"
	"fmt"
	"reflect"
)

func decodeInto(typ reflect.Type, data []byte) (any, error) {
	if typ.Kind() == reflect.Pointer {
		typ = typ.Elem()
		dst := reflect.New(typ).Elem()
		if err := json.Unmarshal(data, dst.Addr().Interface()); err != nil {
			return nil, fmt.Errorf("unmarshal into %s: %w", typ.String(), err)
		}
		return dst.Addr().Interface(), nil
	}

	dst := reflect.New(typ).Elem()
	if err := json.Unmarshal(data, dst.Addr().Interface()); err != nil {
		return nil, fmt.Errorf("unmarshal into %s: %w", typ.String(), err)
	}
	return dst.Interface(), nil
}
