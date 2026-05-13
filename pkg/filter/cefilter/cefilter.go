package cefilter

import (
	"strings"

	"github.com/norlis/event-driven/pkg/event"
	"github.com/norlis/event-driven/pkg/eventmux"
)

// typeFilter matches messages by their CloudEvent type attribute.
type typeFilter struct {
	allowedTypes map[string]struct{}
}

// ByType returns a filter that matches messages whose CloudEvent type is in the given list.
func ByType(types ...string) eventmux.Filter {
	m := make(map[string]struct{}, len(types))
	for _, t := range types {
		m[t] = struct{}{}
	}
	return &typeFilter{allowedTypes: m}
}

func (f *typeFilter) Match(msg *event.Message) bool {
	_, ok := f.allowedTypes[msg.Type()]
	return ok
}

// sourceFilter matches messages whose CloudEvent source starts with the given prefix.
type sourceFilter struct {
	prefix string
}

// BySource returns a filter that matches messages whose CloudEvent source starts with prefix.
func BySource(prefix string) eventmux.Filter {
	return &sourceFilter{prefix: prefix}
}

func (f *sourceFilter) Match(msg *event.Message) bool {
	return strings.HasPrefix(msg.Source(), f.prefix)
}

// compositeFilter combines multiple filters with logical AND.
type compositeFilter struct {
	filters []eventmux.Filter
}

// All returns a filter that matches only when every supplied filter matches.
func All(filters ...eventmux.Filter) eventmux.Filter {
	return &compositeFilter{filters: filters}
}

func (f *compositeFilter) Match(msg *event.Message) bool {
	for _, filter := range f.filters {
		if !filter.Match(msg) {
			return false
		}
	}
	return true
}
