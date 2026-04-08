package main

import (
	"github.com/pageton/bridge-db/internal/cli"

	_ "github.com/pageton/bridge-db/providers/cockroachdb"
	_ "github.com/pageton/bridge-db/providers/mariadb"
	_ "github.com/pageton/bridge-db/providers/mysql"
	_ "github.com/pageton/bridge-db/providers/postgres"
)

func main() {
	cli.Execute()
}
