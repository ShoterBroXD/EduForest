# Imagen única con api, node y clusterbench; Compose elige con "command".

# Etapa 1 (builder): compila binarios estáticos.
FROM golang:1.26-alpine AS builder
WORKDIR /src
# Primero solo go.mod y go.sum: si no cambian, Docker reutiliza la capa con
# las dependencias descargadas.
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 GOOS=linux go build -ldflags="-s -w" -o /out/api ./cmd/api && \
    CGO_ENABLED=0 GOOS=linux go build -ldflags="-s -w" -o /out/node ./cmd/node && \
    CGO_ENABLED=0 GOOS=linux go build -ldflags="-s -w" -o /out/clusterbench ./cmd/clusterbench

# Etapa 2 (runtime): imagen mínima sin el compilador de Go.
FROM alpine:3.21
RUN adduser -D -u 10001 eduforest && mkdir -p /models /results && chown eduforest /models /results
WORKDIR /app
COPY --from=builder /out/ /app/
USER eduforest
EXPOSE 8080 9100
CMD ["/app/api"]
