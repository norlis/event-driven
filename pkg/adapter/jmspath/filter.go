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
	if err := json.Unmarshal(msg.Payload, &data); err != nil {
		f.logger.Error(
			"JMESFilter: Fallo al decodificar JSON para filtro",
			zap.Error(err),
			zap.String("messageUUID", msg.UUID),
		)
		return false
	}

	res, err := jmespath.Search(f.expr, data)
	if err != nil {
		f.logger.Error("JMESFilter: Error al evaluar expresión JMESPath", zap.Error(err), zap.String("expression", f.expr), zap.String("messageUUID", msg.UUID))
		return false
	}

	match, ok := res.(bool)
	if !ok {
		f.logger.Warn("JMESFilter: Resultado de la expresión no es booleano", zap.Any("result", res), zap.String("expression", f.expr), zap.String("messageUUID", msg.UUID))
		return false
	}
	return match
}
