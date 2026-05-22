package main

import (
"encoding/json"
"fmt"
"log"
"os"
"os/signal"
"syscall"
"time"

"github.com/nats-io/nats.go"
)

type ProductionOrder struct {
ID        string    `json:"id"`
Product   string    `json:"product"`
Quantity  int       `json:"quantity"`
Status    string    `json:"status"`
History   []string  `json:"history"`
}

func main() {
agentType := os.Getenv("AGENT_TYPE")
if agentType == "" {
log.Fatal("AGENT_TYPE env variable is not set")
}

natsURL := os.Getenv("NATS_URL")
if natsURL == "" {
natsURL = nats.DefaultURL
}
nc, err := nats.Connect(natsURL)
if err != nil {
log.Fatalf("NATS connection error: %v", err)
}
defer nc.Close()

log.Printf("[Agent: %s] Started and waiting for tasks...", agentType)

switch agentType {
case "dispatcher":
processMessages(nc, "production.start", "production.inventory", "Диспетчеризация", func(order *ProductionOrder) {
order.Status = "В обработке"
order.History = append(order.History, fmt.Sprintf("[%s] Заказ распределен на конвейер в %s", time.Now().Format("15:04:05"), agentType))
})
case "inventory":
processMessages(nc, "production.inventory", "production.planning", "Управление запасами", func(order *ProductionOrder) {
order.History = append(order.History, fmt.Sprintf("[%s] Компоненты для %d шт. зарезервированы на складе", time.Now().Format("15:04:05"), order.Quantity))
})
case "planner":
processMessages(nc, "production.planning", "production.quality", "Планирование загрузки", func(order *ProductionOrder) {
order.History = append(order.History, fmt.Sprintf("[%s] Выделена Линия №2. Ожидаемое время сборки: %d мин", time.Now().Format("15:04:05"), order.Quantity*2))
})
case "quality":
processMessages(nc, "production.quality", "production.finished", "Контроль качества", func(order *ProductionOrder) {
order.Status = "Завершен"
order.History = append(order.History, fmt.Sprintf("[%s] Проверка ОТК пройдена успешно. Брак: 0%%", time.Now().Format("15:04:05")))
})
}

sigChan := make(chan os.Signal, 1)
signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)
<-sigChan
}

func processMessages(nc *nats.Conn, subSubject, pubSubject, agentName string, businessLogic func(*ProductionOrder)) {
_, err := nc.Subscribe(subSubject, func(m *nats.Msg) {
var order ProductionOrder
if err := json.Unmarshal(m.Data, &order); err != nil {
log.Printf("Unmarshal error: %v", err)
return
}

log.Printf("[%s] Received order %s (%s)", agentName, order.ID, order.Product)
time.Sleep(1 * time.Second)
businessLogic(&order)

responseData, _ := json.Marshal(order)
nc.Publish(pubSubject, responseData)
log.Printf("[%s] Order %s forwarded to %s", agentName, order.ID, pubSubject)
})
if err != nil {
log.Fatalf("Subscribe error: %v", err)
}
}
