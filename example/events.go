package example

import (
	"github.com/norlis/event-driven/pkg/adapter/jmspath"
	"github.com/norlis/event-driven/pkg/application/router"
	"github.com/norlis/event-driven/pkg/port"
	"go.uber.org/zap"
)

func RegisterEventHandlers(params EventParams, routers RouterParams, logger *zap.Logger, publisher port.Publisher) {
	routers.PrincipalRouter.Register(
		nil,
		jmspath.New("contains(['test', 'test-x'], name)", logger.Named("jmes-filter")),
		Person{},
		router.WrapHandler(params.Handler1.Execute),
	)

	routers.HttpRouter.Register(
		publisher,
		jmspath.New("contains(['test', 'nviamonte'], name)", logger.Named("jmes-filter")),
		Person{},
		router.WrapHandler(params.Handler1.Command),
	)
}
