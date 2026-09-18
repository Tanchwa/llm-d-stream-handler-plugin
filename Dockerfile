# Builds llm-d's Endpoint Picker with the stream handler provisioner compiled in.
#
# An out-of-tree plugin needs its own binary: the framework's plugin registry is
# a package-level map, so a plugin type only exists for a process that registered
# it. Everything except that one registration is stock llm-d.

ARG GO_VERSION=1.27
ARG BUILDER_IMAGE=golang:${GO_VERSION}
ARG BASE_IMAGE=gcr.io/distroless/static-debian12:nonroot

FROM ${BUILDER_IMAGE} AS builder
WORKDIR /workspace

# Dependencies resolve in their own layer so day-to-day source edits do not
# re-download a dependency tree this size.
COPY go.mod go.sum ./
RUN go mod download

COPY cmd/ cmd/
COPY pkg/ pkg/

ARG TARGETOS=linux
ARG TARGETARCH
ARG VERSION=dev
RUN CGO_ENABLED=0 GOOS=${TARGETOS} GOARCH=${TARGETARCH} \
    go build -trimpath -ldflags="-s -w -X main.version=${VERSION}" -o /workspace/epp ./cmd/epp

FROM ${BASE_IMAGE}
WORKDIR /
COPY --from=builder /workspace/epp /epp
# distroless nonroot: uid 65532, no shell, read-only friendly.
USER 65532:65532
ENTRYPOINT ["/epp"]
