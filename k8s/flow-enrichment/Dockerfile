# ──────────────────────────────────────────────────────────────
# Flow Enrichment Consumer
# Joins sFlow data with ifindex map + NetBox metadata
# Target: linux/arm64 (Raspberry Pi 4 / k3s cluster)
# ──────────────────────────────────────────────────────────────
FROM python:3.11-slim AS base

# librdkafka build deps (confluent-kafka needs the C library)
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
        build-essential \
        librdkafka-dev \
        libssl-dev \
        libsasl2-dev \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Install Python deps first (better layer caching)
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy application code
COPY consumer.py .

# Non-root user for prod
RUN useradd --create-home --shell /bin/false appuser
USER appuser

ENV PYTHONUNBUFFERED=1

ENTRYPOINT ["python", "consumer.py"]
