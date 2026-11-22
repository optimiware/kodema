package engine_test

import (
	_ "database/sql"
	_ "net/http"

	_ "example.com/testify/require"

	_ "kodema/internal/adapter/store/postgres"
	_ "kodema/internal/engine"
	_ "kodema/internal/id"
)
