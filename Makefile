.PHONY: build build-dev build-prod install-dev install-prod plugin-install plugin-uninstall test test-unit test-integration test-perf clean

# Version info from VERSION file
VERSION := $(shell cat VERSION 2>/dev/null || echo "dev")
COMMIT := $(shell git rev-parse --short HEAD 2>/dev/null || echo "unknown")
DATE := $(shell date +%s)
LDFLAGS_BASE := -X 'main.commit=$(COMMIT)'

# Go workspace control: set GOWORK=off for production builds
# Default: uses go.work if present (local dev with ../go-smb2, ../go-nfs)
# Production: GOWORK=off uses go.mod only (published modules)
GOWORK ?=
GO_ENV := $(if $(GOWORK),GOWORK=$(GOWORK),)

# Build (default: dev build)
build: build-dev

# Development build: version with -dev suffix (uses go.work by default)
build-dev:
	@mkdir -p bin
	$(GO_ENV) go build -ldflags "$(LDFLAGS_BASE) -X 'main.version=$(VERSION)-dev' -X 'main.date=$(DATE)'" -o bin/latentfs ./cmd/latentfs

# Production build: uses VERSION file, ignores go.work
build-prod:
	@mkdir -p bin
	GOWORK=off go build -ldflags "$(LDFLAGS_BASE) -X 'main.version=$(VERSION)' -X 'main.date=$(DATE)'" -o bin/latentfs ./cmd/latentfs

# Build with goreleaser and install
# Usage: make install-dev [INSTALL_PATH=/custom/path]
#        make install-prod [INSTALL_PATH=/custom/path]
# install-dev: version X.Y.Z-dev
# install-prod: version X.Y.Z
DEFAULT_INSTALL_PATH := /usr/local/bin
INSTALL_PATH ?=

install-dev install-prod:
	@command -v goreleaser >/dev/null 2>&1 || brew install goreleaser
	$(eval SUFFIX := $(if $(filter install-dev,$@),-dev,))
	GORELEASER_CURRENT_TAG=v$(VERSION) VERSION_SUFFIX=$(SUFFIX) goreleaser build --snapshot --clean --single-target
	@BINARY=$$(find dist -name 'latentfs' -type f -perm +111 | head -1); \
	if [ -z "$$BINARY" ]; then \
		echo "Error: binary not found in dist/"; exit 1; \
	fi; \
	if [ -z "$(INSTALL_PATH)" ]; then \
		printf "Install to $(DEFAULT_INSTALL_PATH)/latentfs? (Y/n) "; \
		read CHOICE; \
		CHOICE=$${CHOICE:-Y}; \
		if [ "$$CHOICE" != "Y" ] && [ "$$CHOICE" != "y" ]; then \
			echo "Aborted. Use: make $@ INSTALL_PATH=/your/path"; \
			exit 1; \
		fi; \
		TARGET="$(DEFAULT_INSTALL_PATH)"; \
	else \
		TARGET="$(INSTALL_PATH)"; \
	fi; \
	sudo cp "$$BINARY" "$$TARGET/latentfs"; \
	echo "Installed: $$("$$TARGET/latentfs" --version)"

# Run all tests (unit: 30s, integration: 120s)
test: test-unit test-integration

# Run unit tests only (exclude integration)
test-unit:
	@command -v gotestsum >/dev/null 2>&1 || go install gotest.tools/gotestsum@latest
	$(GO_ENV) GOTRACEBACK=all gotestsum --format testdox -- -timeout 30s $$(go list ./... | grep -v /tests/integration)

# Run integration tests only (excludes perf tests)
# Usage: make test-integration [TIMEOUT=60s] [PARALLEL=1] [LATENTFS_CACHE=0]
# Use parallel 1 for now to avoid macOS NFS mount contention
# LATENTFS_CACHE=0 disables all caching to verify logic works without cache
# Generates reports: tests/report/integration.json, tests/report/junit.xml
TIMEOUT ?= 90s
PARALLEL ?= 1
LATENTFS_PRESERVE_DEBUG ?= 0
LATENTFS_CACHE ?= 1
test-integration:
	@command -v gotestsum >/dev/null 2>&1 || go install gotest.tools/gotestsum@latest
	@mkdir -p tests/log tests/report
	$(GO_ENV) GOTRACEBACK=all LATENTFS_CACHE=$(LATENTFS_CACHE) gotestsum \
		--format testdox \
		--jsonfile tests/report/integration.json \
		--junitfile tests/report/junit.xml \
		-- -timeout $(TIMEOUT) -parallel $(PARALLEL) -skip TestNFSPerf ./tests/integration/ 2>&1 | tee tests/log/integration.log

# Run performance tests only (TestNFSPerf)
# Usage: make test-perf [TIMEOUT=120s] [PARALLEL=1] [LATENTFS_CACHE=0]
# Generates timestamped report in tests/report/perf_<timestamp>.json for comparison
PERF_TIMEOUT ?= 120s
PERF_TIMESTAMP := $(shell date +%Y%m%d_%H%M%S)
test-perf:
	@command -v gotestsum >/dev/null 2>&1 || go install gotest.tools/gotestsum@latest
	@mkdir -p tests/log tests/report
	$(GO_ENV) GOTRACEBACK=all LATENTFS_CACHE=$(LATENTFS_CACHE) gotestsum \
		--format testdox \
		--jsonfile tests/report/perf_$(PERF_TIMESTAMP).json \
		-- -timeout $(PERF_TIMEOUT) -parallel $(PARALLEL) -run TestNFSPerf ./tests/integration/ 2>&1 | tee tests/log/perf.log
	@echo ""
	@echo "Performance report saved to: tests/report/perf_$(PERF_TIMESTAMP).json"
	@echo "To compare with previous runs: ls -la tests/report/perf_*.json"

# Clean build artifacts and reports
clean:
	rm -rf bin/
	rm -rf dist/
	rm -rf tests/report/
	rm -rf tests/log/
	rm -f integration.test
