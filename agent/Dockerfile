FROM golang:1.21-alpine AS builder
WORKDIR /app
COPY go.mod ./
RUN go mod download
COPY main.go ./
RUN CGO_ENABLED=0 GOOS=linux go build -o /production-agent main.go

FROM alpine:latest
COPY --from=builder /production-agent /production-agent
CMD ["/production-agent"]
