package example

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/norlis/event-driven/pkg/domain"
	"github.com/norlis/event-driven/pkg/infrastructure/jmspath"
	"github.com/norlis/event-driven/pkg/usecase/router"
	"go.uber.org/zap"
)

func handle(ctx context.Context, event map[string]any) (json.RawMessage, error) {
	fmt.Println(event)
	return []byte(`{"status": "OK"}`), nil
}

func RegisterEventHandlers(params EventParams, routers RouterParams, logger *zap.Logger, publisher domain.Publisher) {
	routers.PrincipalRouter.Register(
		publisher,
		jmspath.New("contains(['040', '041'], encabezado.codEvento)", logger.Named("jmes-filter")),
		map[string]any{},
		router.WrapHandler(params.Handler1.Execute),
	)

	routers.HttpRouter.Register(
		publisher,
		nil,
		map[string]string{},
		router.WrapHandler(handle),
	)

}
