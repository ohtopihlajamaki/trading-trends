ARG GO_IMAGE=golang:1.27.2
ARG RUNTIME_IMAGE=alpine:3.23.6

FROM ${GO_IMAGE} AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN --mount=type=cache,target=/go/pkg/mod go mod download
COPY *.go ./
RUN --mount=type=cache,target=/go/pkg/mod --mount=type=cache,target=/root/.cache/go-build \
    CGO_ENABLED=0 go build -trimpath -ldflags='-s -w' -o /out/replayer .

FROM ${RUNTIME_IMAGE}
COPY --from=build /out/replayer /usr/local/bin/replayer
USER 65534
ENTRYPOINT ["/usr/local/bin/replayer"]
