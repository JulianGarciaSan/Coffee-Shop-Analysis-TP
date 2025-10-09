FROM python:3.9-slim

WORKDIR /app

COPY requirements.txt .
COPY ./server/gateway/ .
COPY ./rabbitmq/ ./rabbitmq/
COPY common/ /app/common/
COPY common/entities /app/entities
COPY ./server/dtos/ ./dtos/

RUN pip install --no-cache-dir -r requirements.txt

ENV PYTHONPATH=/app

ENTRYPOINT ["python3", "main.py"]