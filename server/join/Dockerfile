FROM python:3.9-slim

WORKDIR /app

COPY requirements.txt .
COPY ./server/join/ .
COPY ./rabbitmq/ ./rabbitmq/
COPY ./server/dtos/ ./dtos/
COPY common/ /app/common/

RUN pip install --no-cache-dir -r requirements.txt

ENTRYPOINT ["python3", "main.py"]