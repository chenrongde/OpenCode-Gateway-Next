FROM golang:1.22-alpine AS build
WORKDIR /src
COPY go.mod ./
COPY cmd ./cmd
COPY internal ./internal
RUN CGO_ENABLED=0 go build -trimpath -ldflags='-s -w' -o /out/opencode-gateway ./cmd/gateway && \
    CGO_ENABLED=0 go build -trimpath -ldflags='-s -w' -o /out/edgetunnel-config ./cmd/edgetunnel-config && \
    CGO_ENABLED=0 go build -trimpath -ldflags='-s -w' -o /out/control-plane ./cmd/control-plane

FROM gcr.io/distroless/static-debian12:nonroot
COPY --from=build /out/opencode-gateway /opencode-gateway
COPY --from=build /out/edgetunnel-config /edgetunnel-config
COPY --from=build /out/control-plane /control-plane
EXPOSE 13337 13338 13339
ENTRYPOINT ["/opencode-gateway"]
