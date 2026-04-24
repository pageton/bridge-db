.PHONY: build clean test lint fmt vet install run fmt-check check ci build-all build-base test-ci test-ci-race benchmark benchmark-small benchmark-all benchmark-large benchmark-resume

# Binary settings
BINARY_NAME  := bridge
BUILD_DIR    := build
CMD          := ./cmd/bridge

# Go settings
GOCMD        := go
GOBUILD      := $(GOCMD) build
GOTEST       := $(GOCMD) test
GOFMT        := gofmt
GOVET        := $(GOCMD) vet
GOLINT       := golangci-lint

# Version info (inject via ldflags)
VERSION      ?= $(shell git describe --tags --always --dirty 2>/dev/null || echo "dev")
COMMIT       ?= $(shell git rev-parse --short HEAD 2>/dev/null || echo "unknown")
BUILD_TIME   := $(shell date -u '+%Y-%m-%dT%H:%M:%SZ')
LDFLAGS      := -s -w \
	-X main.version=$(VERSION) \
	-X main.commit=$(COMMIT) \
	-X main.buildTime=$(BUILD_TIME)

# Provider build tags (comma-separated, no spaces).
# "base" = mysql,postgres,cockroachdb,mariadb (always included).
# Heavy providers: mongodb,mssql,redis,sqlite.
PROVIDERS    ?=

BUILD_TAGS   := $(if $(PROVIDERS),-tags "$(PROVIDERS)",)

# Packages with concurrency (need race detector).
RACE_PKGS    := ./internal/bridge/... ./internal/progress/... ./internal/transform/... ./internal/tunnel/... ./pkg/provider/...
# Packages without concurrency (safe to skip race).
NO_RACE_PKGS := ./internal/config/... ./internal/retry/... ./internal/verify/...

# -------------------------------------------------------
# Build
# -------------------------------------------------------

build:
	$(GOBUILD) $(BUILD_TAGS) -trimpath -ldflags "$(LDFLAGS)" -o $(BUILD_DIR)/$(BINARY_NAME) $(CMD)

build-base:
	$(GOBUILD) -trimpath -ldflags "$(LDFLAGS)" -o $(BUILD_DIR)/$(BINARY_NAME) $(CMD)

build-all:
	$(GOBUILD) -tags "mongodb,mssql,sqlite,redis" -trimpath -ldflags "$(LDFLAGS)" -o $(BUILD_DIR)/$(BINARY_NAME) $(CMD)

clean:
	rm -rf $(BUILD_DIR)

install: build
	cp $(BUILD_DIR)/$(BINARY_NAME) /usr/local/bin/

# -------------------------------------------------------
# Development
# -------------------------------------------------------

run: build
	./$(BUILD_DIR)/$(BINARY_NAME) $(ARGS)

dev:
	$(GOCMD) run $(CMD) $(ARGS)

fmt:
	$(GOFMT) -w -s .

fmt-check:
	@test -z "$$(gofmt -l -s .)" || (echo "gofmt needs to run on:" && gofmt -l -s . && exit 1)

vet:
	$(GOVET) $(BUILD_TAGS) ./...

lint:
	$(GOLINT) run --timeout=3m $(BUILD_TAGS) ./...

# -------------------------------------------------------
# Testing
# -------------------------------------------------------

test: fmt-check
	$(GOTEST) $(BUILD_TAGS) ./...

test-ci: fmt-check
	$(GOTEST) -v -count=1 -race $(BUILD_TAGS) $(RACE_PKGS)
	$(GOTEST) -v -count=1 $(BUILD_TAGS) $(NO_RACE_PKGS)

test-all:
	$(GOTEST) -tags "mongodb,mssql,sqlite,redis" ./...

test-race: build
	$(GOTEST) -v -race -count=1 ./...

test-short:
	$(GOTEST) -short ./...

test-coverage:
	$(GOTEST) -coverprofile=coverage.out ./...
	$(GOCMD) tool cover -html=coverage.out -o coverage.html

test-coverage-race:
	$(GOTEST) -race -coverprofile=coverage.out ./...
	$(GOCMD) tool cover -html=coverage.out -o coverage.html

# -------------------------------------------------------
# Tidy / deps
# -------------------------------------------------------

tidy:
	$(GOCMD) mod tidy

deps:
	$(GOCMD) mod download

verify-deps:
	$(GOCMD) mod verify

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
	@$(MAKE) benchmark-small

benchmark-small:
	$(GOCMD) run -tags sqlite ./cmd/bench -sizes small -output docs/benchmark-results.md

benchmark-all:
	$(GOCMD) run -tags sqlite ./cmd/bench -sizes small,medium -batch-sizes 500,1000,5000 -workers 1,2,4 -verify -runs 1 -output docs/benchmark-results.md

benchmark-large:
	$(GOCMD) run -tags sqlite ./cmd/bench -sizes large -batch-sizes 500,1000,5000 -workers 1,2,4,8 -verify -runs 1 -output docs/benchmark-results.md -json docs/benchmark-results.json

benchmark-resume:
	$(GOCMD) run -tags sqlite ./cmd/bench -sizes small,medium -resume -output docs/benchmark-results.md
