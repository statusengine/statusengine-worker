# Build entry point for this repository. Its main job is that the binaries
# know what they are: `go build` on its own produces a worker that reports
# version "dev", which is honest but not useful in a changelog or a bug
# report.
#
# VERSION comes from the closest git tag, so a tagged build reports that
# tag and an untagged one reports the tag plus how far past it the commit
# is. Override it for a release built outside git:
#
#	make VERSION=1.4.0
#
# The commit is not passed in: `go build` stamps the VCS revision into the
# binary by itself, including a -dirty marker when the working tree has
# uncommitted changes, and re-deriving it here would only risk disagreeing
# with the toolchain.

VERSION ?= $(shell git describe --tags --always --dirty 2>/dev/null || echo dev)
DATE    ?= $(shell date -u +%Y-%m-%dT%H:%M:%SZ)
PKG     := statusengine-worker/internal/version
LDFLAGS := -X $(PKG).Version=$(VERSION) -X $(PKG).Date=$(DATE)

BINDIR  := bin
BINARIES := worker db_cleanup db_verifier simulator gearman_publisher rabbitmq_publisher losstest

.PHONY: all build test test-all vet fmt clean install install-systemd

all: build

## build: compile every binary into bin/ with version information
build: $(addprefix $(BINDIR)/,$(BINARIES))

$(BINDIR)/worker: | $(BINDIR)
	go build -ldflags "$(LDFLAGS)" -o $@ ./cmd/app

$(BINDIR)/%: | $(BINDIR)
	go build -ldflags "$(LDFLAGS)" -o $@ ./cmd/$*

$(BINDIR):
	mkdir -p $(BINDIR)

## test: run the suite, skipping tests whose services are unavailable
test:
	go test ./... -race -count=1

## test-all: run the suite and FAIL if MySQL, gearmand or RabbitMQ is missing
## (what CI runs - see .github/workflows/ci.yml)
test-all:
	STATUSENGINE_TEST_REQUIRE_SERVICES=1 go test ./... -race -count=1

vet:
	go vet ./...

fmt:
	gofmt -l ./cmd ./internal

clean:
	rm -rf $(BINDIR)

## install: install the two long-running binaries under the names the unit
## files expect. Everything else in bin/ is a development tool and stays out.
install: build
	install -m 0755 $(BINDIR)/worker /usr/local/bin/statusengine-worker
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
