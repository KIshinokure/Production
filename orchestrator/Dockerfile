FROM python:3.10-slim
WORKDIR /app
RUN pip install --no-cache-dir nats-py redis requests opentelemetry-api opentelemetry-sdk opentelemetry-exporter-otlp
COPY orchestrator.py .
CMD ["python", "orchestrator.py"]
