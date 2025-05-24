package jmspath

import (
	"encoding/json"
	"event-router/internal/domain"

	"github.com/jmespath/go-jmespath"
)

type jmesFilter struct {
	expr string
}

func NewFilter(expr string) *jmesFilter {
	return &jmesFilter{expr: expr}
}

func (f *jmesFilter) Match(msg domain.Message) bool {
	var data map[string]interface{}
	if err := json.Unmarshal(msg.Data, &data); err != nil {
		return false
	}

	res, err := jmespath.Search(f.expr, data)
	if err != nil {
		return false
	}

	match, ok := res.(bool)
	return ok && match
}
