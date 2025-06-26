package router

import (
	"encoding/json"
	"reflect"
)

func NewInterface(typ reflect.Type, data []byte) (interface{}, error) {
	if typ.Kind() == reflect.Ptr {
		typ = typ.Elem()
		dst := reflect.New(typ).Elem()
		err := json.Unmarshal(data, dst.Addr().Interface())
		if err != nil {
			return nil, err
		}
		return dst.Addr().Interface(), nil
	}

	dst := reflect.New(typ).Elem()
	err := json.Unmarshal(data, dst.Addr().Interface())
	if err != nil {
		return nil, err
	}
	return dst.Interface(), nil

}
