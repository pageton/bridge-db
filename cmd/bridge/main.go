package main

import (
	"github.com/pageton/bridge-db/internal/cli"
)

var version = "0.1.1"

func main() {
	cli.SetVersion(version)
	cli.Execute()
}
