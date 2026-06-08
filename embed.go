package user

import "embed"

// MigrationsFS holds the SQL migrations applied at service startup.
//
//go:embed all:db/migrations
var MigrationsFS embed.FS
