FROM python:3.9-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY ./rabbitmq/ ./rabbitmq/
COPY ./server/dtos/ ./dtos/
COPY ./common/ ./common/
COPY ./server/coordinator/ ./coordinator/
COPY ./server/healthchecker/ ./healthchecker/
COPY ./server/checkpoint_handler/ ./checkpoint_handler/
COPY ./server/message_range_tracker/ ./message_range_tracker/
COPY ./server/logger_monitor/ ./logger_monitor/


COPY ./server/duplicate/ .

ENTRYPOINT ["python3", "dedup.py"]