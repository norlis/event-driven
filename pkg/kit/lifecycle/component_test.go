package lifecycle_test

import (
	"github.com/norlis/event-driven/v2/pkg/eventmux"
	"github.com/norlis/event-driven/v2/pkg/kit/httpserver"
	"github.com/norlis/event-driven/v2/pkg/kit/lifecycle"
	"github.com/norlis/event-driven/v2/pkg/transport/nats/natsconn"
)

// The lifecycle package must not import eventmux; these assertions keep the
// router and the other components satisfying Component without an adapter.
var (
	_ lifecycle.Component = (*eventmux.Mux)(nil)
	_ lifecycle.Component = (*httpserver.Server)(nil)
	_ lifecycle.Component = (*natsconn.Conn)(nil)
)
