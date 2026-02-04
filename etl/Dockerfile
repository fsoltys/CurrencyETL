FROM python:3.9-slim

RUN apt-get update && apt-get install -y cron && rm -rf /var/lib/apt/lists/*
WORKDIR /app

COPY requirements.txt .
COPY main.py .
COPY crontab /etc.cron.d/etl-cron

RUN pip install --no-cache-dir -r requirements.txt

RUN chmod 0644 /etc.cron.d/etl-cron
RUN crontab /etc.cron.d/etl-cron
RUN touch /var/log/cron.log
CMD cron && tail -f /var/log/cron.log