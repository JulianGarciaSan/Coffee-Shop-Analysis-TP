FROM python:3.9-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY common/ /app/common/
COPY ./rabbitmq /app/rabbitmq
COPY ./server/aggregator /app/
COPY ./server/dtos/ ./dtos/

CMD ["python", "main.py"]