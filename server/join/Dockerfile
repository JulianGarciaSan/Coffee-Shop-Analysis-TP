FROM python:3.9-slim

WORKDIR /app

COPY requirements.txt .
COPY ./server/join/ .
COPY ./rabbitmq/ ./rabbitmq/
COPY ./server/dtos/ ./dtos/
COPY common/ /app/common/
COPY ./server/client_routing/ ./client_routing/
COPY ./server/logger_monitor/ ./logger_monitor/
COPY ./server/healthchecker/ ./healthchecker/
COPY ./server/message_range_tracker/ ./message_range_tracker/
COPY ./server/checkpoint_handler/ ./checkpoint_handler/

RUN pip install --no-cache-dir -r requirements.txt

ENTRYPOINT ["python3", "main.py"]