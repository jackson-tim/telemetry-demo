FROM python:3.11-slim AS base

RUN apt-get update && \
    apt-get install -y --no-install-recommends \
        build-essential \
        librdkafka-dev \
        libssl-dev \
        libsasl2-dev \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY consumer.py .

RUN useradd --create-home --shell /bin/false appuser
USER appuser

ENV PYTHONUNBUFFERED=1
ENTRYPOINT ["python", "consumer.py"]
