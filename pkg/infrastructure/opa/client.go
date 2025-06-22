package opa

import (
	"context"
	"fmt"
	"github.com/norlis/event-driven/pkg/domain"
	"github.com/open-policy-agent/opa/rego"
	"go.uber.org/zap"
)

type Config struct {
	Query        string   `yaml:"query"`
	PoliciesPath string   `yaml:"policiesPath"`
	DataFiles    []string `yaml:"dataFiles"` // opcional, usar si el path es diferente a policiesPath
}

type SdkClient struct {
	preparedQuery rego.PreparedEvalQuery
	logger        *zap.Logger
}

func NewOpaSdkClientFromConfig(ctx context.Context, cfg Config, logger *zap.Logger) (*SdkClient, error) {
	if cfg.Query == "" || cfg.PoliciesPath == "" {
		return nil, fmt.Errorf("la consulta y la ruta de políticas de OPA no pueden estar vacías")
	}

	if logger == nil {
		logger = zap.NewNop()
	}

	logger = logger.Named("OPA").With(zap.String("query", cfg.Query))

	r, err := rego.New(
		rego.Query(cfg.Query),
		//rego.Store(store),
		rego.Load(append([]string{cfg.PoliciesPath}, cfg.DataFiles...), nil),
	).PrepareForEval(ctx)

	if err != nil {
		logger.Error("Error al preparar la consulta de OPA", zap.Error(err))
		return nil, fmt.Errorf("error al preparar la consulta de OPA: %w", err)
	}

	return &SdkClient{
		preparedQuery: r,
		logger:        logger,
	}, nil

}

// IsAllowed evalúa la política cargada con el input proporcionado.
func (c *SdkClient) IsAllowed(ctx context.Context, input domain.PolicyInput) (bool, error) {
	results, err := c.preparedQuery.Eval(ctx, rego.EvalInput(input))
	if err != nil {
		return false, fmt.Errorf("error al evaluar la política de OPA: %w", err)
	}

	if len(results) == 0 {
		return false, nil
	}

	allowed, ok := results[0].Expressions[0].Value.(bool)
	if !ok {
		//c.logger.Warn("not pass authz", zap.Bool("allowed", allowed), zap.Any("input", input))
		return false, fmt.Errorf("la política de OPA no devolvió un resultado booleano")
	}

	return allowed, nil
}
