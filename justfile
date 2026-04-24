# Binary settings
binary_name  := "bridge"
build_dir    := "build"
cmd          := "./cmd/bridge"

# Version info (inject via ldflags)
version      := env_var_or_default("VERSION", `git describe --tags --always --dirty 2>/dev/null || echo "dev"`)
commit       := env_var_or_default("COMMIT", `git rev-parse --short HEAD 2>/dev/null || echo "unknown"`)
build_time   := `date -u '+%Y-%m-%dT%H:%M:%SZ'`
ldflags      := "-s -w -X main.version=" + version + " -X main.commit=" + commit + " -X main.buildTime=" + build_time

# Provider build tags (comma-separated, no spaces).
# "base" = mysql,postgres,cockroachdb,mariadb (always included).
# Heavy providers: mongodb,mssql,redis,sqlite.
providers    := env_var_or_default("PROVIDERS", "")
build_tags   := if providers != "" { "-tags \"" + providers + "\"" } else { "" }

# Packages with concurrency (need race detector).
race_pkgs    := "./internal/bridge/... ./internal/progress/... ./internal/transform/... ./internal/tunnel/... ./pkg/provider/..."
# Packages without concurrency (safe to skip race).
no_race_pkgs := "./internal/config/... ./internal/retry/... ./internal/verify/..."

# -------------------------------------------------------
# Build
# -------------------------------------------------------

build:
    go build {{ build_tags }} -trimpath -ldflags "{{ ldflags }}" -o {{ build_dir }}/{{ binary_name }} {{ cmd }}

build-base:
    go build -trimpath -ldflags "{{ ldflags }}" -o {{ build_dir }}/{{ binary_name }} {{ cmd }}

build-all:
    go build -tags "mongodb,mssql,sqlite,redis" -trimpath -ldflags "{{ ldflags }}" -o {{ build_dir }}/{{ binary_name }} {{ cmd }}

clean:
    rm -rf {{ build_dir }}

install: build
    cp {{ build_dir }}/{{ binary_name }} /usr/local/bin/

# -------------------------------------------------------
# Development
# -------------------------------------------------------

run: build
    ./{{ build_dir }}/{{ binary_name }} {{ env_var_or_default("ARGS", "") }}

dev *args:
    go run {{ cmd }} {{ args }}

fmt:
    gofmt -w -s .

fmt-check:
    #!/usr/bin/env bash
    test -z "$(gofmt -l -s .)" || (echo "gofmt needs to run on:" && gofmt -l -s . && exit 1)

vet:
    go vet {{ build_tags }} ./...

lint:
    golangci-lint run --timeout=3m {{ build_tags }} ./...

# -------------------------------------------------------
# Testing
# -------------------------------------------------------

test: fmt-check
    go test {{ build_tags }} ./...

test-ci: fmt-check
    go test -v -count=1 -race {{ build_tags }} {{ race_pkgs }}
    go test -v -count=1 {{ build_tags }} {{ no_race_pkgs }}

test-all:
    go test -tags "mongodb,mssql,sqlite,redis" ./...

test-race: build
    go test -v -race -count=1 ./...

test-short:
    go test -short ./...

test-coverage:
    go test -coverprofile=coverage.out ./...
    go tool cover -html=coverage.out -o coverage.html

test-coverage-race:
    go test -race -coverprofile=coverage.out ./...
    go tool cover -html=coverage.out -o coverage.html

# -------------------------------------------------------
# Tidy / deps
# -------------------------------------------------------

tidy:
    go mod tidy

deps:
    go mod download

verify-deps:
    go mod verify

# -------------------------------------------------------
# All-in-one checks
# -------------------------------------------------------

check: build fmt-check vet lint test

ci: fmt-check lint test-ci

all: clean build check

# -------------------------------------------------------
# Benchmarking
# -------------------------------------------------------

benchmark:
    just benchmark-small

benchmark-small:
    go run -tags sqlite ./cmd/bench -sizes small -output docs/benchmark-results.md

benchmark-all:
    go run -tags sqlite ./cmd/bench -sizes small,medium -batch-sizes 500,1000,5000 -workers 1,2,4 -verify -runs 1 -output docs/benchmark-results.md

benchmark-large:
    go run -tags sqlite ./cmd/bench -sizes large -batch-sizes 500,1000,5000 -workers 1,2,4,8 -verify -runs 1 -output docs/benchmark-results.md -json docs/benchmark-results.json

benchmark-resume:
    go run -tags sqlite ./cmd/bench -sizes small,medium -resume -output docs/benchmark-results.md
