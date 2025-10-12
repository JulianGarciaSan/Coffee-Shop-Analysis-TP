FROM python:3.9-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copiar dependencias comunes
COPY ./rabbitmq/ ./rabbitmq/
COPY ./server/dtos/ ./dtos/
COPY ./common/ ./common/
COPY ./server/coordinator/ ./coordinator/

# Copiar código del filtro AL FINAL (para mejor cache)
COPY ./server/filter/ .

ENTRYPOINT ["python3", "main.py"]