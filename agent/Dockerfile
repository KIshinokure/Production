FROM golang:alpine AS builder
WORKDIR /app
COPY go.mod ./
RUN go mod download
COPY agent.go .
RUN go mod tidy
RUN CGO_ENABLED=0 GOOS=linux go build -o agent agent.go

FROM alpine:latest
WORKDIR /root/
COPY --from=builder /app/agent .
CMD ["./agent"]
