# A command runner, deliberately - every target is .PHONY and nothing here
# declares a file dependency.
#
# That is not laziness, it is the correct shape for a Go repository. Go
# already has a build cache that tracks source contents, imports and build
# flags, so `go build` on an unchanged tree is near-instant and on a
# changed one is correct. Layering make's timestamp rules on top of that
# adds no speed and one new way to be wrong: an earlier version of this
# file declared `bin/worker` as a target with no prerequisites on the
# sources, so once the binary existed `make build` reported "Nothing to be
# done" and happily kept shipping a stale binary after every edit. A build
# system that silently produces yesterday's artefact is worse than no build
# system at all.
#
# So: make decides *what to run*, Go decides *what to rebuild*.

VERSION ?= $(shell git describe --tags --always --dirty 2>/dev/null || echo dev)
DATE    ?= $(shell date -u +%Y-%m-%dT%H:%M:%SZ)
PKG     := statusengine-worker/internal/version
LDFLAGS := -X $(PKG).Version=$(VERSION) -X $(PKG).Date=$(DATE)

BINDIR  := bin
GO      := go

# The commit is deliberately not passed in: `go build` stamps the VCS
# revision itself, including a -dirty marker, and re-deriving it here would
# only risk disagreeing with the toolchain.
BUILD   := $(GO) build -ldflags "$(LDFLAGS)"

.PHONY: all build test test-all vet fmt clean install install-systemd help

all: build

## help: list the targets
help:
	@grep -E '^## ' $(MAKEFILE_LIST) | sed 's/^## /  /'

## build: compile every binary into bin/, stamped with version information
build:
	@mkdir -p $(BINDIR)
	$(BUILD) -o $(BINDIR)/worker             ./cmd/app
	$(BUILD) -o $(BINDIR)/db_cleanup         ./cmd/db_cleanup
	$(BUILD) -o $(BINDIR)/db_verifier        ./cmd/db_verifier
	$(BUILD) -o $(BINDIR)/simulator          ./cmd/simulator
	$(BUILD) -o $(BINDIR)/gearman_publisher  ./cmd/gearman_publisher
	$(BUILD) -o $(BINDIR)/rabbitmq_publisher ./cmd/rabbitmq_publisher
	$(BUILD) -o $(BINDIR)/losstest           ./cmd/losstest

## test: run the suite; tests whose service is unreachable skip
test:
	$(GO) test ./... -race -count=1

## test-all: run the suite; a missing MySQL/gearmand/RabbitMQ FAILS (what CI runs)
test-all:
	STATUSENGINE_TEST_REQUIRE_SERVICES=1 $(GO) test ./... -race -count=1

## vet: go vet ./...
vet:
	$(GO) vet ./...

## fmt: list files that are not gofmt-clean (prints nothing when clean)
fmt:
	@gofmt -l ./cmd ./internal

## clean: remove bin/
clean:
	rm -rf $(BINDIR)

## install: install the two long-running binaries under the names the units expect
install: build
	install -m 0755 $(BINDIR)/worker     /usr/local/bin/statusengine-worker
	install -m 0755 $(BINDIR)/db_cleanup /usr/local/bin/statusengine-db-cleanup

## install-systemd: install the unit files; does not enable or start them
install-systemd:
	install -m 0644 packaging/systemd/statusengine-worker.service /etc/systemd/system/
	install -m 0644 packaging/systemd/statusengine-db-cleanup.service /etc/systemd/system/
	install -m 0644 packaging/systemd/statusengine-db-cleanup.timer /etc/systemd/system/
	systemctl daemon-reload
	@echo
	@echo "Installed. Review /etc/systemd/system/statusengine-worker.service, then:"
	@echo "  systemctl enable --now statusengine-worker"
	@echo "  systemctl enable --now statusengine-db-cleanup.timer"
