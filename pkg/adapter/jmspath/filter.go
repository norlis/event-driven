package jmspath

import (
	"encoding/json"

	"github.com/jmespath/go-jmespath"
	"github.com/norlis/event-driven/pkg/domain/event"
	"go.uber.org/zap"
)

type JMESFilter struct {
	expr   string
	logger *zap.Logger
}

func New(expr string, logger *zap.Logger) *JMESFilter {
	if logger == nil {
		logger = zap.NewNop()
	}
	return &JMESFilter{expr: expr, logger: logger}
}

func (f *JMESFilter) Match(msg *event.Message) bool {
	var data map[string]any
	if err := json.Unmarshal(msg.Data(), &data); err != nil {
		f.logger.Error("JMESFilter: failed to decode JSON",
			zap.Error(err),
			zap.String("id", msg.ID()),
		)
		return false
	}

	res, err := jmespath.Search(f.expr, data)
	if err != nil {
		f.logger.Error("JMESFilter: expression evaluation error",
			zap.Error(err),
			zap.String("expression", f.expr),
			zap.String("id", msg.ID()),
		)
		return false
	}

	match, ok := res.(bool)
	if !ok {
		f.logger.Warn("JMESFilter: expression result is not boolean",
			zap.Any("result", res),
			zap.String("expression", f.expr),
			zap.String("id", msg.ID()),
		)
		return false
	}
	return match
}
