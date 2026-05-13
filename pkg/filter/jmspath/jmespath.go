package jmspath

import (
	"encoding/json"

	"github.com/jmespath/go-jmespath"
	"github.com/norlis/event-driven/pkg/event"
	"go.uber.org/zap"
)

type Filter struct {
	expr   string
	logger *zap.Logger
}

func New(expr string, logger *zap.Logger) *Filter {
	if logger == nil {
		logger = zap.NewNop()
	}
	return &Filter{expr: expr, logger: logger}
}

func (f *Filter) Match(msg *event.Message) bool {
	var data map[string]any
	if err := json.Unmarshal(msg.Data(), &data); err != nil {
		f.logger.Error("Filter: failed to decode JSON",
			zap.Error(err),
			zap.String("id", msg.ID()),
		)
		return false
	}

	res, err := jmespath.Search(f.expr, data)
	if err != nil {
		f.logger.Error("Filter: expression evaluation error",
			zap.Error(err),
			zap.String("expression", f.expr),
			zap.String("id", msg.ID()),
		)
		return false
	}

	match, ok := res.(bool)
	if !ok {
		f.logger.Warn("Filter: expression result is not boolean",
			zap.Any("result", res),
			zap.String("expression", f.expr),
			zap.String("id", msg.ID()),
		)
		return false
	}
	return match
}
