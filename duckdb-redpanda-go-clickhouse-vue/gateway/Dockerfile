ARG GO_IMAGE=golang:1.27.2
ARG RUNTIME_IMAGE=alpine:3.23.6

FROM ${GO_IMAGE} AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN --mount=type=cache,target=/go/pkg/mod go mod download
COPY *.go ./
RUN --mount=type=cache,target=/go/pkg/mod --mount=type=cache,target=/root/.cache/go-build \
    CGO_ENABLED=0 go build -trimpath -ldflags='-s -w' -o /out/gateway .

FROM ${RUNTIME_IMAGE}
COPY --from=build /out/gateway /usr/local/bin/gateway
USER 65534
EXPOSE 8000
ENTRYPOINT ["/usr/local/bin/gateway"]
