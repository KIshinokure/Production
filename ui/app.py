import streamlit as st
import redis
import os

st.set_page_config(page_title="Production Mesh OS", layout="wide")
st.title("🏭 Операционная панель распределенного производства")

redis_host = os.getenv("REDIS_HOST", "localhost")
r = redis.Redis(host=redis_host, port=6379, decode_responses=True)

try:
    r.ping()
    st.sidebar.success("🟢 Инфраструктура: Подключено к Redis")
except Exception:
    st.sidebar.error("🔴 Инфраструктура: Нет связи с Redis")

current_stage = r.get("sys:current_stage") or "Idle"
llm_report = r.get("sys:llm_report") or "Ожидание запуска пайплайна..."

st.info(f"**Текущий статус пайплайна:** {current_stage.upper()}")

st.write("### Состояние очередей и нагрузка компонентов")
cols = st.columns(4)
stages = ["dispatcher", "inventory", "planner", "quality"]

for i, stage in enumerate(stages):
    with cols[i]:
        status = r.get(f"status:{stage}") or "Normal"
        processed = r.get(f"agent:{stage}:processed") or "0"
        st.metric(label=f"Этап: {stage.capitalize()}", value=f"Выполнено: {processed}")
        if status == "High Load":
            st.error("⚠️ Требуется масштабирование")
        else:
            st.success("✅ Нагрузка в норме")

st.write("---")
st.write("### Распределение задач (Результаты Аукциона)")
telemetry = r.hgetall("pipeline:telemetry")
if telemetry:
    st.table([{"Этап": k, "Исполнитель / Статус": v} for k, v in telemetry.items()])

st.write("---")
st.write("### 🧠 Заключение LLM-Агента (Ollama / AI Insights)")
st.text_area(label="Анализ эффективности процессов модели", value=llm_report, height=120)

if st.button("🔄 Обновить панель данных"):
    st.rerun()
