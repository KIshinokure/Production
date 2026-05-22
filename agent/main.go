package main

import (
"context"
"encoding/json"
"fmt"
"log"
"math/rand"
"os"
"os/signal"
"syscall"
"time"

"github.com/go-redis/redis/v8"
"github.com/nats-io/nats.go"
"go.opentelemetry.io/otel"
"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracegrpc"
"go.opentelemetry.io/otel/propagation"
"go.opentelemetry.io/otel/sdk/resource"
"go.opentelemetry.io/otel/sdk/trace"
semconv "go.opentelemetry.io/otel/semconv/v1.4.0"
)

var ctx = context.Background()
var rdb *redis.Client
var tracer otel.Tracer

type AuctionBid struct {
AgentID  string  `json:"agent_id"`
TaskType string  `json:"task_type"`
Cost     float64 `json:"cost"`
Eta      int     `json:"eta"`
}

type TaskResult struct {
TaskType string `json:"task_type"`
Status   string `json:"status"`
AgentID  string `json:"agent_id"`
}

func initTracer(serviceName string, collectorAddr string) (*trace.TracerProvider, error) {
exporter, err := otlptracegrpc.New(ctx, otlptracegrpc.WithInsecure(), otlptracegrpc.WithEndpoint(collectorAddr))
if err != nil {
return nil, err
}
tp := trace.NewTracerProvider(
trace.WithSampler(trace.AlwaysSample()),
trace.WithBatcher(exporter),
trace.WithResource(resource.NewWithAttributes(semconv.SchemaURL, semconv.ServiceNameKey.String(serviceName))),
)
otel.SetTracerProvider(tp)
otel.SetTextMapPropagator(propagation.NewCompositeTextMapPropagator(propagation.TraceContext{}, propagation.Baggage{}))
return tp, nil
}

func main() {
agentType := os.Getenv("AGENT_TYPE")
if agentType == "" {
agentType = "dispatcher"
}

jaegerHost := os.Getenv("JAEGER_HOST")
if jaegerHost == "" {
jaegerHost = "localhost:4317"
}

tp, err := initTracer("agent-"+agentType, jaegerHost)
if err != nil {
log.Printf("Trace init failed: %v", err)
} else {
defer tp.Shutdown(ctx)
}
tracer = otel.Tracer("production-agent")

redisHost := os.Getenv("REDIS_HOST")
if redisHost == "" {
redisHost = "localhost:6379"
}
rdb = redis.NewClient(&redis.Options{Addr: redisHost})

natsURL := os.Getenv("NATS_URL")
if natsURL == "" {
natsURL = "nats://localhost:4223"
}
nc, err := nats.Connect(natsURL)
if err != nil {
log.Fatal(err)
}
defer nc.Close()

log.Printf("[Agent %s] Успешно запущен", agentType)

// Восстановление состояния (Задание 4)
processedCount, _ := rdb.Get(ctx, fmt.Sprintf("agent:%s:processed", agentType)).Int()
log.Printf("[Agent %s] Состояние восстановлено. Выполнено задач: %d", agentType, processedCount)

// Настоящий Request/Reply для Аукциона (Задание 6)
nc.Subscribe(fmt.Sprintf("auction.%s", agentType), func(m *nats.Msg) {
// Извлекаем переданный контекст трассировки из заголовков сообщения NATS
propagator := otel.GetTextMapPropagator()
remoteCtx := propagator.Extract(context.Background(), nats.HeaderCarrier(m.Header))
_, span := tracer.Start(remoteCtx, "CalculateBid")
defer span.End()

rand.Seed(time.Now().UnixNano())
bid := AuctionBid{
AgentID:  agentType + "-" + fmt.Sprintf("%d", rand.Intn(900)+100),
TaskType: agentType,
Cost:     round(10.0 + rand.Float64()*40.0),
Eta:      rand.Intn(4) + 1,
}
data, _ := json.Marshal(bid)
m.Respond(data)
})

// Выполнение реальной задачи
nc.Subscribe(fmt.Sprintf("task.%s", agentType), func(m *nats.Msg) {
propagator := otel.GetTextMapPropagator()
remoteCtx := propagator.Extract(context.Background(), nats.HeaderCarrier(m.Header))
trCtx, span := tracer.Start(remoteCtx, "ExecuteTask")
defer span.End()

log.Printf("[Agent %s] Выполняю технологическую операцию...", agentType)
time.Sleep(time.Duration(rand.Intn(400)+200) * time.Millisecond)

// Запись метрики в Redis
rdb.Incr(trCtx, fmt.Sprintf("agent:%s:processed", agentType))

// Фиксация длины очереди для честного масштабирования
rdb.Incr(trCtx, fmt.Sprintf("queue:load:%s", agentType))

res := TaskResult{TaskType: agentType, Status: "Success", AgentID: agentType}
resData, _ := json.Marshal(res)
m.Respond(resData)
})

sigChan := make(chan os.Signal, 1)
signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)
<-sigChan
}

func round(val float64) float64 {
return float64(int(val*100)) / 100
}
