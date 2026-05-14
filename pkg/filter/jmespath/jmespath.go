// Package jmespath provides a filter that runs a JMESPath expression
// against the JSON-decoded body of a CloudEvent.
package jmespath

import (
	"encoding/json"
	"log/slog"

	gojmespath "github.com/jmespath/go-jmespath"

	"github.com/norlis/event-driven/pkg/event"
)

// Filter matches messages whose JSON-decoded body satisfies a JMESPath
// expression that returns a boolean.
type Filter struct {
	expr   string
	logger *slog.Logger
}

// New returns a Filter for the given JMESPath expression. Pass nil logger to
// silence logs.
func New(expr string, logger *slog.Logger) *Filter {
	if logger == nil {
		logger = slog.New(slog.DiscardHandler)
	}
	return &Filter{expr: expr, logger: logger}
}

// Match implements eventmux.Filter. Returns false on decode/evaluation errors
// and logs them at error level.
func (f *Filter) Match(msg *event.Message) bool {
	var data map[string]any
	if err := json.Unmarshal(msg.Data(), &data); err != nil {
		f.logger.Error("Filter: failed to decode JSON",
			slog.Any("error", err),
			slog.String("id", msg.ID()),
		)
		return false
	}

	res, err := gojmespath.Search(f.expr, data)
	if err != nil {
		f.logger.Error("Filter: expression evaluation error",
			slog.Any("error", err),
			slog.String("expression", f.expr),
			slog.String("id", msg.ID()),
		)
		return false
	}

	match, ok := res.(bool)
	if !ok {
		f.logger.Warn("Filter: expression result is not boolean",
			slog.Any("result", res),
			slog.String("expression", f.expr),
			slog.String("id", msg.ID()),
		)
		return false
	}
	return match
}
