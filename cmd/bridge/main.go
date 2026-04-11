package main

import (
	"os"

	"github.com/pageton/bridge-db/internal/cli"
)

// Build-time variables injected via ldflags (see Makefile/justfile).
//
//nolint:govet // set via -ldflags at build time
var (
	version   = "dev"
	commit    string //nolint:unused
	buildTime string //nolint:unused
)

func main() {
	cli.SetVersion(version)
	os.Exit(cli.Execute())
}
