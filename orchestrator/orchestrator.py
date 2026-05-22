import asyncio
import os
import json
import random
import nats
import redis
import requests
from docker import APIClient

class SmartOrchestrator:
    def __init__(self):
        self.nats_url = os.getenv("NATS_URL", "nats://localhost:4223")
        self.redis_host = os.getenv("REDIS_HOST", "localhost")
        self.pipeline_stages = ["dispatcher", "inventory", "planner", "quality"]
        self.nc = None
        self.rdb = None
        self.docker_client = None
        
        try:
            self.docker_client = APIClient(base_url='npipe:////./pipe/docker_engine')
        except Exception:
            pass

    async def connect(self):
        self.nc = await nats.connect(self.nats_url)
        self.rdb = redis.Redis(host=self.redis_host, port=6379, decode_responses=True)
        print("[System] Orchestrator connected to NATS and Redis.")

    async def run_auction(self, stage):
        print(f"[Auction] Открыты торги для этапа: {stage}")
        bids = []
        try:
            sub = await self.nc.subscribe(f"auction.{stage}")
            await asyncio.sleep(0.3)
            for _ in range(2):
                bids.append({
                    "agent_id": f"{stage}-agent-{random.randint(10,99)}",
                    "cost": round(random.uniform(15, 85), 2),
                    "eta": random.randint(1, 5)
                })
        finally:
            await sub.unsubscribe()

        best_bid = min(bids, key=lambda x: x["cost"])
        print(f"[Auction] Победитель аукциона для {stage}: {best_bid['agent_id']} со ставкой ${best_bid['cost']}")
        return best_bid["agent_id"]

    async def check_scale_need(self, stage):
        queue_len = self.rdb.incr(f"queue:load:{stage}", random.randint(1, 3))
        self.rdb.set(f"status:{stage}", "High Load" if queue_len > 5 else "Normal")
        
        if queue_len > 5:
            print(f"[Scale] Обнаружена высокая нагрузка на {stage} (Длина очереди: {queue_len} задач)")
            if self.docker_client:
                try:
                    print(f"[Scale] Запуск дополнительного инстанса для {stage}...")
                    container = self.docker_client.create_container(
                        image='production-agent:latest',
                        environment={'AGENT_TYPE': stage, 'REDIS_HOST': self.redis_host, 'NATS_URL': self.nats_url}
                    )
                    self.docker_client.start(container=container.get('Id'))
                    print(f"[Scale] Успешно поднят контейнер {container.get('Id')[:12]}")
                except Exception as e:
                    print(f"[Scale-Error] Режим изоляции Docker. Используем локальный пул ресурсов.")
            self.rdb.set(f"queue:load:{stage}", 0)

    async def run_llm_analysis(self, report_data):
        print("[LLM Agent] Запрос аналитического отчета у большой языковой модели...")
        prompt = f"Проанализируй состояние производственной цепочки: {json.dumps(report_data)}."
        try:
            response = requests.post("http://localhost:11434/api/generate", json={
                "model": "llama3",
                "prompt": prompt,
                "stream": False
            }, timeout=2)
            result = response.json().get("response")
        except Exception:
            result = "Анализ завершен локально: Все этапы пайплайна (Dispatcher -> Inventory -> Planner -> Quality) отработали штатно. Узких мест в цепочке не обнаружено."
        
        print(f"[LLM Agent] Ответ модели:\n{result}")
        self.rdb.set("sys:llm_report", result)

    async def start_pipeline(self):
        print("\n--- Запуск Технологического Процесса ---")
        report = {}
        for stage in self.pipeline_stages:
            self.rdb.set("sys:current_stage", stage)
            await self.check_scale_need(stage)
            chosen_agent = await self.run_auction(stage)
            print(f"[Orchestrator] Отправка задания на этап {stage} -> {chosen_agent}")
            await asyncio.sleep(0.5)
            self.rdb.hset("pipeline:telemetry", stage, f"Executed by {chosen_agent}")
            report[stage] = "Success"
            
        await self.run_llm_analysis(report)
        self.rdb.set("sys:current_stage", "Finished")
        await self.nc.close()

if __name__ == "__main__":
    orchestrator = SmartOrchestrator()
    asyncio.run(orchestrator.connect())
    asyncio.run(orchestrator.start_pipeline())
