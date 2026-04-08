package main

import (
	"github.com/pageton/bridge-db/internal/cli"
)

var version = "dev"

func main() {
	cli.SetVersion(version)
	cli.Execute()
}
