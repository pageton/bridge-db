package main

import (
	"os"

	"github.com/pageton/bridge-db/internal/cli"
)

var version = "0.1.2"

func main() {
	cli.SetVersion(version)
	os.Exit(cli.Execute())
}
