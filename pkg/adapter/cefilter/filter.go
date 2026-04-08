package cefilter

import (
	"strings"

	"github.com/norlis/event-driven/pkg/domain/event"
	"github.com/norlis/event-driven/pkg/port"
)

// TypeFilter matches messages by their CloudEvent type attribute.
type TypeFilter struct {
	allowedTypes map[string]struct{}
}

func ByType(types ...string) *TypeFilter {
	m := make(map[string]struct{}, len(types))
	for _, t := range types {
		m[t] = struct{}{}
	}
	return &TypeFilter{allowedTypes: m}
}

func (f *TypeFilter) Match(msg *event.Message) bool {
	_, ok := f.allowedTypes[msg.Type()]
	return ok
}

// SourceFilter matches messages whose CloudEvent source starts with the given prefix.
type SourceFilter struct {
	prefix string
}

func BySource(prefix string) *SourceFilter {
	return &SourceFilter{prefix: prefix}
}

func (f *SourceFilter) Match(msg *event.Message) bool {
	return strings.HasPrefix(msg.Source(), f.prefix)
}

// CompositeFilter combines multiple filters with logical AND.
type CompositeFilter struct {
	filters []port.Filter
}

func All(filters ...port.Filter) *CompositeFilter {
	return &CompositeFilter{filters: filters}
}

func (f *CompositeFilter) Match(msg *event.Message) bool {
	for _, filter := range f.filters {
		if !filter.Match(msg) {
			return false
		}
	}
	return true
}
