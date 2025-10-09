FROM python:3.9-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY rabbitmq/ ./rabbitmq/
COPY server/report_generator/ ./
COPY server/config.ini ./config.ini
COPY ./server/dtos/ ./dtos/
COPY common/ /app/common/

RUN mkdir -p /app/reports

CMD ["python", "main.py"]
