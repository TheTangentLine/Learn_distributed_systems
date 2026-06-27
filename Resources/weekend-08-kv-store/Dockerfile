FROM golang:1.22-alpine AS builder
WORKDIR /app
COPY go.mod .
RUN go mod download
COPY *.go .
RUN CGO_ENABLED=0 GOOS=linux go build -o kvnode .

FROM alpine:3.19
WORKDIR /app
COPY --from=builder /app/kvnode .
EXPOSE 8081
CMD ["/app/kvnode"]
