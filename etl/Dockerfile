FROM python:3.9-slim

RUN apt-get update && apt-get install -y cron dos2unix && rm -rf /var/lib/apt/lists/*
WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

RUN dos2unix crontab

RUN mv crontab /etc/cron.d/etl-cron
RUN chmod 0644 /etc/cron.d/etl-cron
RUN crontab /etc/cron.d/etl-cron

RUN touch /var/log/cron.log
CMD printenv > /etc/environment && cron && tail -f /var/log/cron.log