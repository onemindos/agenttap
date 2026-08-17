BINARY := agenttap
WEBBINARY := agentweb

# `make` with no target builds everything.
.DEFAULT_GOAL := all

.PHONY: all build web web-demo install run demo test vet fmt fmt-check tidy clean ci

## all: build both binaries — agenttap (tap + TUI) and agentweb (browser visualizer)
all: build web

## build: compile the agenttap binary into ./$(BINARY)
build:
	go build -o $(BINARY) .

## web: compile the agentweb browser visualizer into ./$(WEBBINARY)
web:
	go build -o $(WEBBINARY) ./cmd/agentweb

## install: install agenttap into $GOBIN / $GOPATH/bin
install:
	go install .

## run: run against a nats context, e.g. `make run CTX=my-context VIEW=viz`
CTX ?=
VIEW ?= viz
run:
	@test -n "$(CTX)" || (echo "usage: make run CTX=<nats-context> [VIEW=log|viz]"; exit 2)
	go run . -view $(VIEW) $(CTX)

## demo: render the scripted demo exchange (no NATS connection)
demo:
	go run . -demo -view $(VIEW)

## web-demo: build, then serve the scripted demo in the browser visualizer (replayable).
## Remote dev: `make web-demo ADDR=0.0.0.0:8088`, then open http://<this-host>:8088
ADDR ?= 127.0.0.1:8088
web-demo: all
	./$(BINARY) -demo -view json > /tmp/agentweb-demo.ndjson
	./$(WEBBINARY) -replay /tmp/agentweb-demo.ndjson -addr $(ADDR)

## test: run unit tests with the race detector
test:
	go test -race ./...

## vet: run go vet
vet:
	go vet ./...

## fmt: format all sources
fmt:
	gofmt -w .

## fmt-check: fail if any file is not gofmt-clean
fmt-check:
	@out="$$(gofmt -l .)"; if [ -n "$$out" ]; then echo "not gofmt-clean:"; echo "$$out"; exit 1; fi

## tidy: tidy the module
tidy:
	go mod tidy

## clean: remove build artifacts
clean:
	rm -f $(BINARY) $(WEBBINARY)
	go clean

## ci: everything CI runs (format check, vet, test, build both binaries)
ci: fmt-check vet test all
