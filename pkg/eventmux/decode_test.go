package eventmux

import (
	"encoding/json"
	"reflect"
	"testing"
)

type SampleStruct struct {
	Name  string `json:"name"`
	Value int    `json:"value"`
}

func TestDecodeInto(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		typ         reflect.Type
		data        []byte
		expectedVal any
		expectErr   bool
	}{
		{
			name:        "Valid struct",
			typ:         reflect.TypeFor[SampleStruct](),
			data:        []byte(`{"name":"test","value":123}`),
			expectedVal: SampleStruct{Name: "test", Value: 123},
			expectErr:   false,
		},
		{
			name:        "Valid pointer to struct",
			typ:         reflect.TypeFor[*SampleStruct](),
			data:        []byte(`{"name":"ptr_test","value":456}`),
			expectedVal: &SampleStruct{Name: "ptr_test", Value: 456},
			expectErr:   false,
		},
		{
			name:        "Invalid JSON data",
			typ:         reflect.TypeFor[SampleStruct](),
			data:        []byte(`{"name":"test","value":`), // JSON inválido
			expectedVal: nil,
			expectErr:   true,
		},
		{
			name:        "Mismatched fields (extra field in JSON)",
			typ:         reflect.TypeFor[SampleStruct](),
			data:        []byte(`{"name":"test","value":123,"extra":"field"}`),
			expectedVal: SampleStruct{Name: "test", Value: 123}, // json.Unmarshal ignora campos extra
			expectErr:   false,
		},
		{
			name:        "Mismatched fields (missing field in JSON, non-required)",
			typ:         reflect.TypeFor[SampleStruct](), // Asumiendo que Name no es 'omitempty'
			data:        []byte(`{"value":123}`),
			expectedVal: SampleStruct{Name: "", Value: 123}, // Name será zero-value
			expectErr:   false,
		},
		{
			name:        "Nil data",
			typ:         reflect.TypeFor[SampleStruct](),
			data:        nil,
			expectedVal: nil,
			expectErr:   true, // json.Unmarshal(nil, ...) devuelve error
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			val, err := decodeInto(tt.typ, tt.data)

			if (err != nil) != tt.expectErr {
				t.Fatalf("decodeInto() error = %v, expectErr %v", err, tt.expectErr)
			}

			if !tt.expectErr {
				// Usar reflect.DeepEqual para comparar structs y punteros a structs.
				if !reflect.DeepEqual(val, tt.expectedVal) {
					// Para una mejor salida de error, podemos marshallear a JSON si es complejo
					expectedJSON, _ := json.Marshal(tt.expectedVal)
					actualJSON, _ := json.Marshal(val)
					t.Errorf("decodeInto() got = %s (%T), want %s (%T)", actualJSON, val, expectedJSON, tt.expectedVal)
				}
			}
		})
	}
}
