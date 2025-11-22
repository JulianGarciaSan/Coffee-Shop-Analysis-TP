FROM python:3.9-slim

WORKDIR /app

RUN pip install docker

COPY chaos_monkey.py /app/chaos_monkey.py

CMD ["python", "-u", "/app/chaos_monkey.py"]