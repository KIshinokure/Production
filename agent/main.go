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
"go.opentelemetry.io/otel/sdk/resource"
"go.opentelemetry.io/otel/sdk/trace"
semconv "go.opentelemetry.io/otel/semconv/v1.4.0"
)

var ctx = context.Background()
var rdb *redis.Client
var tracer = otel.Tracer("production-agent")

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

func initTracer(serviceName string) (*trace.TracerProvider, error) {
exporter, err := otlptracegrpc.New(ctx, otlptracegrpc.WithInsecure(), otlptracegrpc.WithEndpoint("localhost:4317"))
if err != nil {
return nil, err
}
tp := trace.NewTracerProvider(
trace.WithSampler(trace.AlwaysSample()),
trace.WithBatcher(exporter),
trace.WithResource(resource.NewWithAttributes(semconv.SchemaURL, semconv.ServiceNameKey.String(serviceName))),
)
otel.SetTracerProvider(tp)
return tp, nil
}

func main() {
agentType := os.Getenv("AGENT_TYPE")
if agentType == "" {
agentType = "dispatcher"
}

tp, err := initTracer("agent-" + agentType)
if err != nil {
log.Printf("Failed to init tracer: %v", err)
} else {
defer tp.Shutdown(ctx)
}

redisHost := os.Getenv("REDIS_HOST")
if redisHost == "" {
redisHost = "localhost"
}
rdb = redis.NewClient(&redis.Options{Addr: redisHost + ":6379"})

natsURL := os.Getenv("NATS_URL")
if natsURL == "" {
natsURL = "nats://localhost:4223"
}
nc, err := nats.Connect(natsURL)
if err != nil {
log.Fatal(err)
}
defer nc.Close()

log.Printf("[Agent %s] Started and ready", agentType)

processedCount, _ := rdb.Get(ctx, fmt.Sprintf("agent:%s:processed", agentType)).Int()
log.Printf("[Agent %s] Restored state from Redis. Tasks processed so far: %d", agentType, processedCount)

nc.Subscribe(fmt.Sprintf("auction.%s", agentType), func(m *nats.Msg) {
_, span := tracer.Start(ctx, "CalculateBid")
defer span.End()

rand.Seed(time.Now().UnixNano())
bid := AuctionBid{
AgentID:  agentType + "-" + fmt.Sprintf("%d", rand.Intn(1000)),
TaskType: agentType,
Cost:     10.0 + rand.Float64()*90.0,
Eta:      rand.Intn(5) + 1,
}
data, _ := json.Marshal(bid)
nc.Publish(m.Reply, data)
})

nc.Subscribe(fmt.Sprintf("task.%s", agentType), func(m *nats.Msg) {
trCtx, span := tracer.Start(ctx, "ExecuteTask")
defer span.End()

log.Printf("[Agent %s] Executing task...", agentType)
time.Sleep(time.Duration(rand.Intn(1000)+500) * time.Millisecond)

rdb.Incr(trCtx, fmt.Sprintf("agent:%s:processed", agentType))

res := TaskResult{TaskType: agentType, Status: "Success", AgentID: agentType}
resData, _ := json.Marshal(res)
nc.Publish(m.Reply, resData)
})

sigChan := make(chan os.Signal, 1)
signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)
<-sigChan
}
