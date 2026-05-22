FROM golang:1.21-alpine AS builder
WORKDIR /app

COPY go.mod go.sum ./
RUN go mod download

COPY . .

RUN go mod tidy
RUN CGO_ENABLED=0 GOOS=linux go build -o main main.go

# Используем тот же образ, который уже есть на компьютере
FROM golang:1.21-alpine
WORKDIR /root/
COPY --from=builder /app/main .
CMD ["./main"]