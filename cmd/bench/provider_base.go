package main

import (
	_ "github.com/pageton/bridge-db/providers/cockroachdb"
	_ "github.com/pageton/bridge-db/providers/mariadb"
	_ "github.com/pageton/bridge-db/providers/mysql"
	_ "github.com/pageton/bridge-db/providers/postgres"
)
