FROM golang:1.26 AS builder

WORKDIR /src

# Install OCB
RUN go install go.opentelemetry.io/collector/cmd/builder@v0.146.1

# Generate and build the collector
COPY builder-config.yaml .
RUN builder --config builder-config.yaml

FROM gcr.io/distroless/static-debian12:nonroot AS final

COPY --from=builder /src/build/otelcol-custom /otelcol-custom
COPY collector-config.yaml /etc/otelcol-custom/config.yaml

EXPOSE 8082
EXPOSE 13133

ENTRYPOINT ["/otelcol-custom"]
CMD ["--config", "/etc/otelcol-custom/config.yaml"]
