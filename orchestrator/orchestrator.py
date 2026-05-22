import asyncio
import json
import uuid
import nats
import os

class ProductionOrchestrator:
    def __init__(self):
        self.nc = None
        self.future = None

    async def connect(self):
        nats_url = os.getenv("NATS_URL", "nats://localhost:4222")
        self.nc = await nats.connect(nats_url)
        print(f"?? Оркестратор успешно подключен к NATS по адресу: {nats_url}")

    async def create_production_order(self, product: str, quantity: int, timeout: int = 15):
        order_id = str(uuid.uuid4())[:8]
        order = {
            "id": order_id,
            "product": product,
            "quantity": quantity,
            "status": "Создан",
            "history": []
        }

        self.future = asyncio.Future()
        sub = await self.nc.subscribe("production.finished", cb=self.on_order_complete)

        print(f"\n?? Запуск производства: Заказ #{order_id} — {product} ({quantity} шт.)")
        await self.nc.publish("production.start", json.dumps(order).encode())

        try:
            result_order = await asyncio.wait_for(self.future, timeout)
            self.print_report(result_order)
        except asyncio.TimeoutError:
            print(f"? Ошибка: Превышено время ожидания для заказа #{order_id}!")
        finally:
            await sub.unsubscribe()

    async def on_order_complete(self, msg):
        order_data = json.loads(msg.data.decode())
        if not self.future.done():
            self.future.set_result(order_data)

    def print_report(self, order):
        print("\n" + "="*50)
        print(f"?? ОТЧЕТ ВЫПОЛНЕНИЯ ЗАКАЗА #{order['id']}")
        print(f"Продукт: {order['product']} | Количество: {order['quantity']}")
        print(f"Финальный статус: {order['status']}")
        print("-"*50)
        print("История технологического процесса:")
        for step in order['history']:
            print(f"  {step}")
        print("="*50 + "\n")

async def main():
    orchestrator = ProductionOrchestrator()
    await orchestrator.connect()
    await asyncio.sleep(2)
    await orchestrator.create_production_order(product="Smart Watch X-100", quantity=50)
    await orchestrator.nc.close()

if __name__ == "__main__":
    asyncio.run(main())
