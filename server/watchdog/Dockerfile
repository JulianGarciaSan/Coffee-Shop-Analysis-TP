FROM python:3.9-slim

WORKDIR /app

RUN apt-get update && \
    apt-get install -y docker.io iputils-ping netcat-openbsd && \
    rm -rf /var/lib/apt/lists/*

RUN pip install docker

COPY server/watchdog/watchdog.py .

CMD ["python", "watchdog.py"]