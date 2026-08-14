// Package jmespath provides a filter that runs a JMESPath expression
// against the JSON-decoded body of a CloudEvent.
package jmespath

import (
	"encoding/json"
	"log/slog"

	gojmespath "github.com/jmespath/go-jmespath"

	"github.com/norlis/httpgate/logging"

	"github.com/norlis/event-driven/pkg/event"
	"github.com/norlis/event-driven/pkg/kit/logfields"
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
	logger = logger.With(
		slog.String(logfields.KeyLogLogger, "jmespath-filter"),
		slog.String(logfields.KeyFilterExpression, expr),
	)
	return &Filter{expr: expr, logger: logger}
}

// Match implements eventmux.Filter. Returns false on decode/evaluation
// errors or a non-boolean result: these are per-message failures treated as
// "no match" — the filter keeps functioning, so they are logged at WARN
// rather than ERROR.
func (f *Filter) Match(msg *event.Message) bool {
	var data map[string]any
	if err := json.Unmarshal(msg.Data(), &data); err != nil {
		f.logger.WarnContext(msg.Context(), "filter payload decode failed",
			logging.Err(err),
			slog.String(logfields.KeyMessagingMessageID, msg.ID()))
		return false
	}

	res, err := gojmespath.Search(f.expr, data)
	if err != nil {
		f.logger.WarnContext(msg.Context(), "filter evaluation failed",
			logging.Err(err),
			slog.String(logfields.KeyMessagingMessageID, msg.ID()))
		return false
	}

	match, ok := res.(bool)
	if !ok {
		f.logger.WarnContext(msg.Context(), "filter result not boolean",
			slog.String(logfields.KeyMessagingMessageID, msg.ID()))
		return false
	}
	return match
}
