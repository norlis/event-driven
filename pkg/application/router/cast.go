package router

import (
	"encoding/json"
	"fmt"
	"reflect"
)

func NewInterface(typ reflect.Type, data []byte) (any, error) {
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
