package cli

import (
	_ "kodema/internal/adapter/store/postgres"
	_ "kodema/internal/control/identity"
	_ "kodema/internal/engine/flow"
	_ "kodema/internal/installation"
	_ "kodema/internal/server"
	_ "kodema/internal/transport/httpapi"
)
