FROM golang:1.26 AS builder

WORKDIR /src/besreceiver

# Install OCB
RUN go install go.opentelemetry.io/collector/cmd/builder@v0.146.1

# Cache Go module downloads in a separate layer
COPY go.mod go.sum ./
RUN go mod download

COPY . .

RUN builder --config builder-config.yaml

FROM gcr.io/distroless/static-debian12:nonroot

COPY --from=builder /src/besreceiver/build/otelcol-bazel /otelcol-bazel
COPY collector-config.yaml /etc/otelcol-bazel/config.yaml

EXPOSE 8082

ENTRYPOINT ["/otelcol-bazel"]
CMD ["--config", "/etc/otelcol-bazel/config.yaml"]
