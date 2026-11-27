# Build context: repository root (see docker-compose.yml)
FROM python:3.12.12-slim-bookworm

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    TZ=UTC

WORKDIR /app
COPY producer/requirements.txt producer/requirements.txt
RUN pip install --no-cache-dir --default-timeout=120 --retries 10 -r producer/requirements.txt

RUN useradd --uid 1000 --create-home chargepulse \
 && mkdir -p /var/lib/chargepulse/wal \
 && chown -R chargepulse:chargepulse /var/lib/chargepulse

COPY chargepulse_common chargepulse_common
COPY config config
COPY producer producer

USER chargepulse
WORKDIR /app/producer
CMD ["python", "producer.py"]
