package example

import (
	"github.com/norlis/event-driven/pkg/domain"
	"github.com/norlis/event-driven/pkg/infrastructure/jmspath"
	"github.com/norlis/event-driven/pkg/usecase/router"
	"go.uber.org/zap"
)

func RegisterEventHandlers(params EventParams, routers RouterParams, logger *zap.Logger, publisher domain.Publisher) {
	routers.PrincipalRouter.Register(
		publisher,
		jmspath.New("contains(['040', '041'], encabezado.codEvento)", logger.Named("jmes-filter")),
		map[string]any{},
		router.WrapHandler(params.Handler1.Execute),
	)

}
