# Thin wrapper over the Go toolchain. Nothing here is required to build the
# project; `go build ./...` and `go test ./...` work on their own.

IMAGE_REPO ?= docker.io/tanchwa/llm-d-stream-handler-epp
IMAGE_TAG  ?= latest
IMAGE      ?= $(IMAGE_REPO):$(IMAGE_TAG)
VERSION    ?= $(shell git rev-parse --short HEAD 2>/dev/null || echo dev)

.PHONY: all
all: fmt vet test build

.PHONY: fmt
fmt:
	go fmt ./...

.PHONY: vet
vet:
	go vet ./...

.PHONY: test
test:
	go test -race -cover ./...

.PHONY: build
build:
	go build -o bin/epp ./cmd/epp

.PHONY: image
image:
	docker build --build-arg VERSION=$(VERSION) -t $(IMAGE) .

.PHONY: push
push: image
	docker push $(IMAGE)

.PHONY: clean
clean:
	rm -rf bin
