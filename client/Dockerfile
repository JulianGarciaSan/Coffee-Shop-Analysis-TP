FROM python:3.9-slim

WORKDIR /app

COPY requirements.txt .
COPY ./client/ .
COPY common/ /app/common/


ENV PYTHONPATH=/app:/app/common:/app/client
RUN pip install --no-cache-dir -r requirements.txt

ENTRYPOINT ["python3", "main.py"]