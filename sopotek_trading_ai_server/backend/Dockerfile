FROM python:3.12-slim

WORKDIR /app/backend

COPY requirements.txt ./requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

ENV PYTHONPATH=/app/backend
ENV SOPOTEK_PLATFORM_DATABASE_URL=postgresql+asyncpg://sopotek:sopotek_local@postgres:5432/sopotek_platform
ENV SOPOTEK_KAFKA_BOOTSTRAP_SERVERS=kafka:9092

EXPOSE 8000

CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8000"]
